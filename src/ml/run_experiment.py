"""Chạy và ghi nhận một cấu hình khai phá trên Databricks."""

import argparse
import json
import math
import time
from datetime import UTC, datetime
from typing import Any

import mlflow
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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


def _config(spark: SparkSession, short_key: str) -> dict[str, Any]:
    if not short_key or not short_key.isalnum() or len(short_key) > 40:
        raise ValueError("run-key phải là tiền tố chữ/số của run_key")
    rows = spark.table(QUEUE).where(F.col("run_key").startswith(short_key)).limit(2).collect()
    if len(rows) != 1:
        raise ValueError(f"run-key {short_key!r} khớp {len(rows)} cấu hình; cần đúng một cấu hình")
    return rows[0].asDict(recursive=True)


def _log_mlflow(cfg: dict[str, Any], row: dict[str, Any]) -> str | None:
    """Ghi tham số, phép đo và trạng thái vào MLflow nếu workspace có experiment."""
    try:
        mlflow.set_experiment("/Shared/instacart-ds")
        with mlflow.start_run(run_name=f"{cfg['experiment']}-{cfg['algo']}-{cfg['run_key'][:12]}") as run:
            for key in (
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
            ):
                value = cfg.get(key)
                if value is not None:
                    mlflow.log_param(key, value)
            for key in ("wall_s", "rules_s", "peak_mem_mb", "n_tx", "n_itemsets", "n_rules_single", "n_rules_all"):
                value = row.get(key)
                if isinstance(value, (int, float)) and math.isfinite(value):
                    mlflow.log_metric(key, value)
            mlflow.set_tags(
                {
                    "experiment": str(cfg["experiment"]),
                    "platform": "databricks",
                    "status": str(row["status"]),
                    "run_key": str(cfg["run_key"]),
                }
            )
            if row.get("error"):
                mlflow.set_tag("error", row["error"][:500])
            return run.info.run_id
    except Exception as exc:  # noqa: BLE001 - kết quả Delta vẫn phải được giữ nếu MLflow không sẵn sàng
        row["mlflow_error"] = str(exc)[:1000]
        return None


def _merge_result(spark: SparkSession, row: dict[str, Any]) -> None:
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


def _persist(spark: SparkSession, cfg: dict[str, Any], freq: dict[tuple[int, ...], int], rules) -> None:
    """Ghi đúng partition run_key để retry cập nhật kết quả thay vì nhân bản."""
    run_key = cfg["run_key"]
    items = [(run_key, list(key), int(count), float(count / cfg["n_tx"])) for key, count in freq.items()]
    item_frame = spark.createDataFrame(items, "run_key string, items array<int>, count long, support double")
    item_frame.write.format("delta").mode("overwrite").option("replaceWhere", f"run_key = '{run_key}'").saveAsTable(
        ITEMSETS
    )

    rule_frame = rules.withColumn("run_key", F.lit(run_key)).select(
        "run_key", "antecedent", "consequent", "support", "confidence", "lift"
    )
    rule_frame.write.format("delta").mode("overwrite").option("replaceWhere", f"run_key = '{run_key}'").saveAsTable(
        RULES
    )


def run_one(spark: SparkSession, short_key: str) -> dict[str, Any]:
    cfg = _config(spark, short_key)
    cfg["run_key"] = str(cfg["run_key"])
    df = load_transactions(
        spark,
        grain=cfg["grain"],
        scale_pct=int(cfg["scale_pct"]),
        top_n=cfg.get("top_n"),
        data_version=cfg.get("data_version", "v1"),
    )
    n_tx = df.count()
    if n_tx < 1:
        raise ValueError("Giao dịch rỗng; kiểm tra snapshot ds_data.transactions")
    min_count = math.ceil(float(cfg["min_support"]) * n_tx)
    row: dict[str, Any] = {
        **cfg,
        "platform": "databricks",
        "n_tx": n_tx,
        "status": "ok",
        "started_at": datetime.now(UTC),
        "error": None,
    }
    expired = None
    freq: dict[tuple[int, ...], int] = {}
    rules = None
    try:
        with PeakRss() as memory, time_limit(spark.sparkContext, int(cfg["timeout_min"]), cfg["run_key"]) as expired:
            started = time.perf_counter()
            algo = cfg["algo"]
            if algo == "fpgrowth":
                model = fpgrowth(df, float(cfg["min_support"]), float(cfg["min_confidence"]))
                freq = {
                    tuple(sorted(map(int, itemset["items"]))): int(itemset["freq"])
                    for itemset in model.freqItemsets.toLocalIterator()
                    if cfg.get("max_k") is None or len(itemset["items"]) <= int(cfg["max_k"])
                }
            elif algo in {"dapriori", "datid"}:
                algorithm = dapriori if algo == "dapriori" else datid
                freq = algorithm(to_rdd(df), min_count, cfg.get("max_k"))
            elif algo == "all":
                results = {
                    name: function(to_rdd(df), min_count, cfg.get("max_k"))
                    for name, function in (("dapriori", dapriori), ("datid", datid))
                }
                model = fpgrowth(df, float(cfg["min_support"]), float(cfg["min_confidence"]))
                results["fpgrowth"] = {
                    tuple(sorted(map(int, itemset["items"]))): int(itemset["freq"])
                    for itemset in model.freqItemsets.toLocalIterator()
                    if cfg.get("max_k") is None or len(itemset["items"]) <= int(cfg["max_k"])
                }
                row["check_passed"] = results["dapriori"] == results["datid"] == results["fpgrowth"]
                row["check_detail"] = json.dumps({name: len(value) for name, value in results.items()})
                freq = results["fpgrowth"]
            else:
                raise ValueError(f"Thuật toán không hỗ trợ: {algo}")
            row["wall_s"] = round(time.perf_counter() - started, 3)
            started = time.perf_counter()
            rules = gen_rules(spark, freq, n_tx, float(cfg["min_confidence"])).cache()
            row["n_rules_all"] = rules.count()
            row["n_rules_single"] = rules.where(F.size("consequent") == 1).count()
            row["rules_s"] = round(time.perf_counter() - started, 3)
        row["peak_mem_mb"] = memory.delta_mb
        row["n_itemsets"] = len(freq)
        row["n_itemsets_by_k"] = json.dumps(
            {str(k): sum(len(items) == k for items in freq) for k in sorted({len(items) for items in freq})}
        )
        if cfg.get("persist"):
            _persist(spark, {**cfg, "n_tx": n_tx}, freq, rules)
    except Exception as exc:  # noqa: BLE001 - failed/timeout are durable experiment outcomes
        row["status"] = "timeout" if expired is not None and expired.is_set() else "failed"
        row["error"] = str(exc)[:2000]
    finally:
        if rules is not None:
            rules.unpersist()
    row["finished_at"] = datetime.now(UTC)
    row["mlflow_run_id"] = _log_mlflow(cfg, row)
    _merge_result(spark, row)
    if row["status"] == "failed":
        raise RuntimeError(row.get("error", "Lần chạy thất bại"))
    return row


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-key", required=True, help="run_key đầy đủ hoặc 12 ký tự đầu")
    cfg = parser.parse_args()
    spark = SparkSession.builder.getOrCreate()
    row = run_one(spark, cfg.run_key)
    print(json.dumps({"run_key": row["run_key"], "status": row["status"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
