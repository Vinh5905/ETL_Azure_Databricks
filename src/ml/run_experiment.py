"""Run one planned frequent-itemset experiment and record its results."""

from __future__ import annotations

import argparse
import json
import math
import time
from datetime import UTC, datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from ml.data import load_transactions, to_rdd
from ml.fim.dapriori import dapriori
from ml.fim.datid import datid
from ml.fim.fpgrowth import fpgrowth
from ml.fim.rules import gen_rules
from ml.measure import PeakRss, time_limit

QUEUE = "instacart.ml.experiment_queue"
RUNS = "instacart.ml.experiment_runs"
ITEMSETS = "instacart.ml.frequent_itemsets"
RULES = "instacart.ml.association_rules"
ALGORITHMS = ("dapriori", "datid", "fpgrowth")
PARAMS = (
    "algo",
    "grain",
    "scale_pct",
    "top_n",
    "min_support",
    "min_confidence",
    "max_k",
    "cores",
    "rep",
    "data_version",
)


def _fp_freq(df: DataFrame, cfg: dict[str, Any]) -> tuple[dict[tuple[int, ...], int], Any]:
    model = fpgrowth(df, cfg["min_support"], cfg["min_confidence"])
    max_k = cfg.get("max_k")
    freq = {
        tuple(sorted(row["items"])): int(row["freq"])
        for row in model.freqItemsets.toLocalIterator()
        if max_k is None or len(row["items"]) <= max_k
    }
    return freq, model


def _same_freq(left: dict[tuple[int, ...], int], right: dict[tuple[int, ...], int]) -> bool:
    return left == right


def _rule_values(rows: list[Any]) -> dict[tuple[tuple[int, ...], tuple[int, ...]], tuple[float, float, float]]:
    return {
        (tuple(sorted(row["antecedent"])), tuple(sorted(row["consequent"]))): (
            float(row["support"]),
            float(row["confidence"]),
            float(row["lift"]),
        )
        for row in rows
    }


def _same_rules(left: dict, right: dict, *, tol: float = 1e-9) -> bool:
    if left.keys() != right.keys():
        return False
    return all(all(abs(a - b) < tol for a, b in zip(left[key], right[key], strict=True)) for key in left)


def _calculate(spark: SparkSession, df: DataFrame, cfg: dict[str, Any], n_tx: int) -> dict[str, Any]:
    """Calculate itemsets/rules and comparison metrics without writing external state."""
    min_count = math.ceil(float(cfg["min_support"]) * n_tx)
    if min_count < 1:
        min_count = 1
    algorithm = cfg["algo"]
    selected: dict[tuple[int, ...], int]
    selected_rules: DataFrame
    checks: list[str] = []
    wall_s = rules_s = 0.0

    if algorithm == "all":
        results: dict[str, dict[tuple[int, ...], int]] = {}
        rule_results: dict[str, dict] = {}
        fp_model = None
        for name in ALGORITHMS:
            started = time.perf_counter()
            if name == "fpgrowth":
                freq, fp_model = _fp_freq(df, cfg)
            else:
                fn = dapriori if name == "dapriori" else datid
                freq = fn(to_rdd(df), min_count, cfg.get("max_k"))
            wall_s += time.perf_counter() - started
            results[name] = freq

            rules_started = time.perf_counter()
            single_rules = gen_rules(spark, freq, n_tx, float(cfg["min_confidence"]), single_consequent=True)
            rule_results[name] = _rule_values(single_rules.collect())
            if name == "dapriori":
                selected_rules = gen_rules(spark, freq, n_tx, float(cfg["min_confidence"])).cache()
                selected_rules.count()
            rules_s += time.perf_counter() - rules_started
        selected = results["dapriori"]
        freq_ok = _same_freq(results["dapriori"], results["datid"]) and _same_freq(
            results["dapriori"], results["fpgrowth"]
        )
        rule_ok = _same_rules(rule_results["dapriori"], rule_results["datid"]) and _same_rules(
            rule_results["dapriori"], rule_results["fpgrowth"]
        )
        if fp_model is not None:
            mllib_rows = []
            max_k = cfg.get("max_k")
            for row in fp_model.associationRules.toLocalIterator():
                if max_k is None or len(row["antecedent"]) + len(row["consequent"]) <= max_k:
                    mllib_rows.append(row)
            mllib_rules = _rule_values(mllib_rows)
            rule_ok = rule_ok and _same_rules(rule_results["fpgrowth"], mllib_rules)
        checks = [f"itemsets_equal={freq_ok}", f"single_consequent_rules_equal={rule_ok}"]
        check_passed = freq_ok and rule_ok
    elif algorithm == "fpgrowth":
        started = time.perf_counter()
        selected, _model = _fp_freq(df, cfg)
        wall_s = time.perf_counter() - started
        rules_started = time.perf_counter()
        selected_rules = gen_rules(spark, selected, n_tx, float(cfg["min_confidence"])).cache()
        selected_rules.count()
        rules_s = time.perf_counter() - rules_started
        check_passed = None
    elif algorithm in {"dapriori", "datid"}:
        started = time.perf_counter()
        fn = dapriori if algorithm == "dapriori" else datid
        selected = fn(to_rdd(df), min_count, cfg.get("max_k"))
        wall_s = time.perf_counter() - started
        rules_started = time.perf_counter()
        selected_rules = gen_rules(spark, selected, n_tx, float(cfg["min_confidence"])).cache()
        selected_rules.count()
        rules_s = time.perf_counter() - rules_started
        check_passed = None
    else:
        raise ValueError(f"Thuật toán không hỗ trợ: {algorithm}")

    rule_count = selected_rules.count()
    single_count = selected_rules.where("size(consequent) = 1").count()
    by_k: dict[str, int] = {}
    for itemset in selected:
        key = str(len(itemset))
        by_k[key] = by_k.get(key, 0) + 1
    return {
        "freq": selected,
        "rules": selected_rules,
        "metrics": {
            "wall_s": round(wall_s, 2),
            "rules_s": round(rules_s, 2),
            "n_itemsets": len(selected),
            "n_itemsets_by_k": json.dumps(by_k, sort_keys=True),
            "n_rules_single": single_count,
            "n_rules_all": rule_count,
            "check_passed": check_passed,
            "check_detail": "; ".join(checks) if checks else None,
        },
    }


def _persist(spark: SparkSession, cfg: dict[str, Any], freq: dict[tuple[int, ...], int], rules: DataFrame) -> None:
    """Idempotently replace only this run's itemsets and association rules."""
    run_key = str(cfg["run_key"])
    if not run_key or "'" in run_key:
        raise ValueError("run_key không hợp lệ")
    n_tx = int(cfg["n_tx"])
    itemset_rows = [(run_key, list(items), int(count), count / n_tx) for items, count in freq.items()]
    itemset_frame = spark.createDataFrame(
        itemset_rows,
        "run_key string, items array<int>, count long, support double",
    )
    predicate = f"run_key = '{run_key}'"
    (itemset_frame.write.format("delta").mode("overwrite").option("replaceWhere", predicate).saveAsTable(ITEMSETS))
    rule_frame = rules.selectExpr(
        f"'{run_key}' AS run_key", "antecedent", "consequent", "support", "confidence", "lift"
    )
    (rule_frame.write.format("delta").mode("overwrite").option("replaceWhere", predicate).saveAsTable(RULES))


def _log_mlflow(cfg: dict[str, Any], row: dict[str, Any]) -> tuple[str | None, str | None]:
    """Log one run; return MLflow's id and any logging error without hiding the DS result."""
    try:
        import mlflow

        experiment = "/Shared/instacart-ds"
        mlflow.set_experiment(experiment)
        with mlflow.start_run(run_name=f"{cfg['experiment']}-{cfg['run_key'][:8]}") as run:
            params = {key: cfg.get(key) for key in PARAMS}
            mlflow.log_params({key: "null" if value is None else value for key, value in params.items()})
            metrics = {
                key: row[key]
                for key in ("wall_s", "rules_s", "peak_mem_mb", "n_itemsets", "n_rules_single", "n_rules_all")
                if row.get(key) is not None
            }
            if metrics:
                mlflow.log_metrics(metrics)
            mlflow.set_tags(
                {
                    "experiment": str(cfg.get("experiment", "")),
                    "platform": str(cfg.get("platform", "databricks")),
                    "status": str(row.get("status", "")),
                    "run_key": str(cfg["run_key"]),
                }
            )
            return run.info.run_id, None
    except Exception as exc:  # noqa: BLE001 - MLflow is auxiliary; keep experiment outcome
        return None, str(exc)[:2000]


def _merge_result(spark: SparkSession, row: dict[str, Any]) -> None:
    """Upsert the result row on its stable run_key."""
    from delta.tables import DeltaTable

    schema = spark.table(RUNS).schema
    values = [tuple(row.get(field.name) for field in schema.fields)]
    frame = spark.createDataFrame(values, schema)
    (
        DeltaTable.forName(spark, RUNS)
        .alias("target")
        .merge(frame.alias("source"), "target.run_key = source.run_key")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def execute_run(
    spark: SparkSession,
    cfg: dict[str, Any],
    df: DataFrame,
    *,
    timeout_factory=time_limit,
    peak_factory=PeakRss,
) -> tuple[dict[str, Any], dict[tuple[int, ...], int] | None, DataFrame | None]:
    """Run calculation and turn exceptions/timeouts into a result row."""
    n_tx = df.count()
    if n_tx < 1:
        raise ValueError("Không có giao dịch để chạy thực nghiệm")
    now = datetime.now(UTC)
    row = {
        key: cfg.get(key)
        for key in (
            "run_key",
            "algo_version",
            "experiment",
            "platform",
            "algo",
            "grain",
            "scale_pct",
            "top_n",
            "min_support",
            "min_confidence",
            "max_k",
            "cores",
            "rep",
            "data_version",
            "timeout_min",
            "persist",
            "sweep",
        )
    }
    row.update(status="ok", n_tx=n_tx, started_at=now, finished_at=None)
    expired = None
    result = None
    try:
        with (
            peak_factory() as memory,
            timeout_factory(spark.sparkContext, cfg["timeout_min"], cfg["run_key"]) as expired,
        ):
            result = _calculate(spark, df, cfg, n_tx)
        row["peak_mem_mb"] = memory.delta_mb
        if expired.is_set():
            row["status"] = "timeout"
            row["error"] = f"Vượt quá timeout {cfg['timeout_min']} phút"
        else:
            row.update(result["metrics"])
    except Exception as exc:  # noqa: BLE001 - every failed run must be recorded
        row["status"] = "timeout" if expired is not None and expired.is_set() else "failed"
        row["error"] = str(exc)[:2000]
    row["finished_at"] = datetime.now(UTC)
    if row["status"] != "ok":
        return row, None, None
    return row, result["freq"], result["rules"]


def run_config(spark: SparkSession, cfg: dict[str, Any], *, log_mlflow: bool = True) -> dict[str, Any]:
    """Load, run, optionally persist and record a planned configuration."""
    df = load_transactions(
        spark,
        grain=cfg["grain"],
        scale_pct=cfg["scale_pct"],
        top_n=cfg.get("top_n"),
        data_version=cfg.get("data_version", "v1"),
    )
    row, freq, rules = execute_run(spark, cfg, df)
    try:
        if row["status"] == "ok" and cfg.get("persist"):
            try:
                _persist(spark, {**cfg, "n_tx": row["n_tx"]}, freq, rules)
            except Exception as exc:  # noqa: BLE001
                row["status"] = "failed"
                row["error"] = str(exc)[:2000]
        if log_mlflow:
            row["mlflow_run_id"], row["mlflow_error"] = _log_mlflow(cfg, row)
        _merge_result(spark, row)
    finally:
        # Các lần chạy nối tiếp dùng chung driver: bỏ cache luật để peak_mem_mb của lần sau không bị cộng dồn.
        if rules is not None:
            rules.unpersist()
    if row["status"] == "failed":
        raise SystemExit(1)
    return row


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-key", required=True, help="12 ký tự đầu hoặc run_key đầy đủ")
    args = parser.parse_args()
    spark = SparkSession.builder.getOrCreate()
    prefix = args.run_key.replace("'", "''")
    matches = spark.table(QUEUE).where(f"run_key LIKE '{prefix}%' ").limit(2).collect()
    if len(matches) != 1:
        raise ValueError(f"run-key prefix phải khớp đúng một cấu hình, tìm thấy {len(matches)}")
    cfg = matches[0].asDict(recursive=True)
    run_config(spark, cfg)


if __name__ == "__main__":
    main()
