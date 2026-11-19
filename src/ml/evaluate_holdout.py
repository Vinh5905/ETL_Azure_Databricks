"""Đánh giá luật kết hợp trên holdout validation/test và baseline phổ biến."""

import argparse
from collections import defaultdict
from collections.abc import Iterable
from itertools import combinations
from pathlib import Path

import mlflow
from pyspark.sql import SparkSession

HOLDOUT_METRICS = "instacart.ml.holdout_metrics"
EVAL_SQL = Path(__file__).with_name("sql") / "eval_holdout.sql"


def _recommend(context: Iterable[int], rules: Iterable[dict], popular: Iterable[int], k: int = 10) -> list[int]:
    """Trả top-k món theo confidence/lift, rồi bù từ danh sách phổ biến."""
    if k < 1:
        raise ValueError("k phải >= 1")
    basket = set(context)
    ctx = sorted(basket)
    best: dict[int, tuple[float, float]] = {}
    index: dict[tuple[int, ...], list[dict]] = defaultdict(list)
    max_left = 0
    for rule in rules:
        antecedent = tuple(sorted(rule["antecedent"]))
        if antecedent:
            index[antecedent].append(rule)
            max_left = max(max_left, len(antecedent))
    for size in range(1, min(max_left, len(ctx)) + 1):
        for antecedent in combinations(ctx, size):
            for rule in index.get(antecedent, ()):
                for item in rule["consequent"]:
                    item = int(item)
                    score = (float(rule["confidence"]), float(rule.get("lift", 0.0)))
                    if item not in basket and score > best.get(item, (-1.0, -1.0)):
                        best[item] = score
    ranked = sorted(best, key=lambda item: (-best[item][0], -best[item][1], item))[:k]
    for item in popular:
        if len(ranked) >= k:
            break
        if item not in basket and item not in best and item not in ranked:
            ranked.append(int(item))
    return ranked


def evaluate_holdout(
    rules: Iterable[dict],
    holdout: Iterable[dict],
    popular_items: Iterable[int],
    *,
    k: int = 10,
    min_confidence: float = 0.0,
    catalog_size: int | None = None,
) -> dict[str, float | int]:
    """Tính HitRate, Precision, Recall, order coverage và catalog coverage."""
    if not 0 <= min_confidence <= 1:
        raise ValueError("min_confidence phải nằm trong [0, 1]")
    filtered_rules = [r for r in rules if float(r["confidence"]) >= min_confidence]
    popular = list(popular_items)
    n_rows = hits = covered = precision_sum = recall_sum = 0.0
    recommended_items: set[int] = set()
    for row in holdout:
        context, targets = list(row["context"]), set(row["targets"])
        if not targets:
            continue
        recommended = _recommend(context, filtered_rules, popular, k)
        matched = len(set(recommended) & targets)
        n_rows += 1
        hits += matched > 0
        precision_sum += matched / k
        recall_sum += matched / len(targets)
        covered += any(
            tuple(sorted(rule["antecedent"])) == subset and any(i not in context for i in rule["consequent"])
            for rule in filtered_rules
            for size in range(1, min(len(context), len(rule["antecedent"])) + 1)
            for subset in combinations(sorted(set(context)), size)
        )
        recommended_items.update(recommended)
    if n_rows == 0:
        raise ValueError("holdout không có dòng với targets khác rỗng")
    denominator = catalog_size if catalog_size is not None else len(popular)
    return {
        "n_eval": int(n_rows),
        "hit_rate_at_k": hits / n_rows,
        "precision_at_k": precision_sum / n_rows,
        "recall_at_k": recall_sum / n_rows,
        "order_coverage": covered / n_rows,
        "catalog_coverage": len(recommended_items) / denominator if denominator else 0.0,
    }


def evaluate_popularity_baseline(
    holdout: Iterable[dict], popular_items: Iterable[int], *, k: int = 10, catalog_size: int | None = None
) -> dict[str, float | int]:
    """Đánh giá baseline top-k phổ biến, loại item đã có trong context."""
    if k < 1:
        raise ValueError("k phải >= 1")
    popular = list(popular_items)
    n_rows = hits = precision_sum = recall_sum = 0.0
    recommended_items: set[int] = set()
    for row in holdout:
        context, targets = set(row["context"]), set(row["targets"])
        if not targets:
            continue
        recs = [item for item in popular if item not in context][:k]
        matched = len(set(recs) & targets)
        n_rows += 1
        hits += matched > 0
        precision_sum += matched / k
        recall_sum += matched / len(targets)
        recommended_items.update(recs)
    if not n_rows:
        raise ValueError("holdout không có dòng với targets khác rỗng")
    denominator = catalog_size if catalog_size is not None else len(popular)
    return {
        "n_eval": int(n_rows),
        "hit_rate_at_k": hits / n_rows,
        "precision_at_k": precision_sum / n_rows,
        "recall_at_k": recall_sum / n_rows,
        "order_coverage": 0.0,
        "catalog_coverage": len(recommended_items) / denominator if denominator else 0.0,
    }


def evaluate_run(spark: SparkSession, run_key: str, min_confidence: float, eval_part: str) -> dict:
    """Chạy truy vấn SQL holdout và MERGE kết quả theo khóa idempotent."""
    from delta.tables import DeltaTable

    if eval_part not in {"val", "test"}:
        raise ValueError("eval_part chỉ nhận val hoặc test")
    query = EVAL_SQL.read_text(encoding="utf-8")
    result = spark.sql(query, args={"run_key": run_key, "min_conf": float(min_confidence), "part": eval_part}).first()
    if result is None or not result["n_eval"]:
        raise ValueError(f"Không có holdout {eval_part} để đánh giá")
    row = result.asDict(recursive=True)
    frame = spark.createDataFrame([row], spark.table(HOLDOUT_METRICS).schema)
    (
        DeltaTable.forName(spark, HOLDOUT_METRICS)
        .alias("target")
        .merge(
            frame.alias("source"),
            "target.run_key = source.run_key AND target.min_confidence = source.min_confidence "
            "AND target.eval_part = source.eval_part AND target.k = source.k",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    try:
        mlflow.set_experiment("/Shared/instacart-ds")
        with mlflow.start_run(run_name=f"holdout-{run_key[:8]}-{eval_part}"):
            mlflow.log_params({"run_key": run_key, "min_confidence": min_confidence, "eval_part": eval_part})
            mlflow.log_metrics(
                {
                    "hit_rate_at_10": float(row["hit_rate_at_k"]),
                    "precision_at_10": float(row["precision_at_k"]),
                    "recall_at_10": float(row["recall_at_k"]),
                    "order_coverage": float(row["order_coverage"]),
                    "catalog_coverage": float(row["catalog_coverage"]),
                }
            )
    except Exception:  # noqa: BLE001 - bảng Delta là nguồn sự thật
        pass
    return row


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--all-e7", action="store_true", help="Đánh giá 9 cấu hình E7 trên val và baseline val/test")
    parser.add_argument("--run-key")
    parser.add_argument("--min-confidence", type=float)
    parser.add_argument("--part", choices=("val", "test"), default="val")
    args = parser.parse_args()
    spark = SparkSession.builder.getOrCreate()
    if args.all_e7:
        runs = (
            spark.table("instacart.ml.experiment_runs")
            .where("experiment = 'E7' AND status = 'ok'")
            .select("run_key", "min_confidence")
            .distinct()
            .collect()
        )
        for run in runs:
            evaluate_run(spark, run.run_key, float(run.min_confidence), "val")
        evaluate_run(spark, "popularity", 0.0, "val")
        evaluate_run(spark, "popularity", 0.0, "test")
    else:
        if not args.run_key or args.min_confidence is None:
            parser.error("--run-key և --min-confidence потрібні nếu không dùng --all-e7")
        evaluate_run(spark, args.run_key, args.min_confidence, args.part)


if __name__ == "__main__":
    main()
