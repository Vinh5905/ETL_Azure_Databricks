"""Đánh giá luật kết hợp trên holdout validation/test và baseline phổ biến."""

import argparse
import json
import logging
from pathlib import Path

import mlflow
from pyspark.sql import SparkSession

LOGGER = logging.getLogger(__name__)
HOLDOUT_METRICS = "instacart.ml.holdout_metrics"
EVAL_SQL = Path(__file__).with_name("sql") / "eval_holdout.sql"


def expand_e7_confidences(rows) -> list[tuple[str, float]]:
    """Expand confidence thresholds persisted in an E7 run's sweep column."""
    expanded: set[tuple[str, float]] = set()
    for row in rows:
        sweep = row["sweep"]
        values = json.loads(sweep).get("min_confidence", []) if sweep else []
        if not values:
            values = [row["min_confidence"]]
        expanded.update((row["run_key"], float(value)) for value in values)
    return sorted(expanded, key=lambda item: (item[0], item[1]))


def _execute_eval_sql(
    spark: SparkSession,
    run_key: str,
    min_confidence: float,
    eval_part: str,
    *,
    sql_text: str | None = None,
):
    query = sql_text if sql_text is not None else EVAL_SQL.read_text(encoding="utf-8")
    return spark.sql(query, args={"run_key": run_key, "min_conf": float(min_confidence), "part": eval_part}).first()


def evaluate_run(
    spark: SparkSession,
    run_key: str,
    min_confidence: float,
    eval_part: str,
    *,
    sql_text: str | None = None,
    metrics_table: str = HOLDOUT_METRICS,
    log_mlflow: bool = True,
) -> dict:
    """Chạy truy vấn SQL holdout và MERGE kết quả theo khóa idempotent.

    sql_text, metrics_table và log_mlflow chỉ để test chạy trên bảng đồ chơi local.
    """
    from delta.tables import DeltaTable

    if eval_part not in {"val", "test"}:
        raise ValueError("eval_part chỉ nhận val hoặc test")
    result = _execute_eval_sql(spark, run_key, min_confidence, eval_part, sql_text=sql_text)
    if result is None or not result["n_eval"]:
        raise ValueError(f"Không có holdout {eval_part} để đánh giá")
    row = result.asDict(recursive=True)
    frame = spark.createDataFrame([row], spark.table(metrics_table).schema)
    (
        DeltaTable.forName(spark, metrics_table)
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
    if not log_mlflow:
        return row
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
                    "rule_only_hit_rate_at_10": float(row["rule_only_hit_rate_at_k"]),
                    "rule_only_precision_at_10": float(row["rule_only_precision_at_k"]),
                    "rule_only_recall_at_10": float(row["rule_only_recall_at_k"]),
                    "rule_only_catalog_coverage": float(row["rule_only_catalog_coverage"]),
                }
            )
    except Exception as exc:  # noqa: BLE001 - cảnh báo ghi lại lỗi nhưng giữ Delta làm nguồn sự thật
        LOGGER.warning("Không ghi được metrics holdout vào MLflow cho %s: %s", run_key, exc)
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
            .select("run_key", "min_confidence", "sweep")
            .collect()
        )
        for run_key, min_confidence in expand_e7_confidences(runs):
            evaluate_run(spark, run_key, min_confidence, "val")
        evaluate_run(spark, "popularity", 0.0, "val")
        evaluate_run(spark, "popularity", 0.0, "test")
    else:
        if not args.run_key or args.min_confidence is None:
            parser.error("--run-key và --min-confidence là bắt buộc nếu không dùng --all-e7")
        evaluate_run(spark, args.run_key, args.min_confidence, args.part)


if __name__ == "__main__":
    main()
