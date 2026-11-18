"""Chọn cấu hình trên validation, đóng gói và đăng ký model luật kết hợp."""

import argparse
import os
import tempfile
from collections.abc import Iterable
from datetime import UTC, datetime

import mlflow
import pandas as pd
from mlflow import MlflowClient
from mlflow.models import infer_signature
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ml.serving.basket_rules import BasketRules

MODEL = "instacart.ml.basket_rules"
SERVING_PATH = "/Volumes/instacart/ds_data/exports/serving"


def select_champion(rows: Iterable[dict], *, min_coverage: float = 0.5) -> dict:
    """Chọn HitRate@10 cao nhất trên val, hòa thì lấy cấu hình ít luật hơn."""
    rows = list(rows)
    if not 0 <= min_coverage <= 1:
        raise ValueError("min_coverage phải nằm trong [0, 1]")
    candidates = [
        row
        for row in rows
        if row.get("eval_part") == "val"
        and row.get("run_key") != "popularity"
        and float(row.get("order_coverage", 0.0)) >= min_coverage
    ]
    if not candidates:
        raise ValueError("Không có cấu hình val nào đạt order_coverage tối thiểu")
    chosen = max(
        candidates,
        key=lambda row: (
            float(row.get("hit_rate_at_10", row.get("hit_rate", 0.0))),
            -int(row.get("n_rules", 0)),
            str(row.get("run_key", "")),
        ),
    )
    baseline = next((row for row in rows if row.get("eval_part") == "val" and row.get("run_key") == "popularity"), None)
    chosen_hit = float(chosen.get("hit_rate_at_10", chosen.get("hit_rate_at_k", chosen.get("hit_rate", 0.0))))
    baseline_hit = (
        float(baseline.get("hit_rate_at_10", baseline.get("hit_rate_at_k", baseline.get("hit_rate", 0.0))))
        if baseline
        else None
    )
    return {
        **chosen,
        "baseline_hit_rate_at_10": baseline_hit,
        "beats_baseline": chosen_hit > baseline_hit if baseline_hit is not None else None,
    }


def register_model(rules_path: str, items_path: str, run_key: str, min_conf: float) -> int:
    """Đăng ký pyfunc UC model; không gán alias ở đây để alias được cập nhật cuối."""
    mlflow.set_registry_uri("databricks-uc")
    mlflow.set_experiment("/Shared/instacart-ds")
    example = pd.DataFrame({"product_ids": [[24852, 13176]]})
    model = BasketRules()
    context = type("ModelContext", (), {"artifacts": {"rules": rules_path, "items": items_path}})()
    model.load_context(context)
    signature = infer_signature(example, model.predict(None, example), params={"k": 10})
    package_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with mlflow.start_run(run_name=f"champion-{run_key[:8]}"):
        mlflow.log_params({"source_run_key": run_key, "min_confidence": min_conf})
        info = mlflow.pyfunc.log_model(
            artifact_path="model",
            python_model=BasketRules(),
            signature=signature,
            input_example=example,
            artifacts={"rules": rules_path, "items": items_path},
            pip_requirements=["pandas>=2,<3", "pyarrow>=17,<20"],
            code_paths=[package_dir],
            registered_model_name=MODEL,
        )
        mlflow.set_tag("source_run_key", run_key)
    return int(info.registered_model_version)


def _export(spark: SparkSession, champion: dict, base_path: str = SERVING_PATH) -> tuple[str, str]:
    rules_path = f"{base_path.rstrip('/')}/rules.parquet"
    items_path = f"{base_path.rstrip('/')}/items.parquet"
    rules = (
        spark.table("instacart.ml.association_rules")
        .where(
            (F.col("run_key") == champion["run_key"])
            & (F.col("confidence") >= float(champion["min_confidence"]))
            & (F.size("consequent") == 1)
        )
        .select("antecedent", F.element_at("consequent", 1).alias("item"), "confidence", "lift")
    )
    items = (
        spark.table("instacart.ds_data.item_index")
        .where((F.col("data_version") == "v1") & (F.col("grain") == "product"))
        .select(F.col("item_key").cast("long").alias("product_id"), "idx", F.col("item_name").alias("product_name"))
    )
    rules.write.mode("overwrite").parquet(rules_path)
    items.write.mode("overwrite").parquet(items_path)
    return rules_path, items_path


def _write_champion(spark: SparkSession, chosen: dict, version: int) -> None:
    from delta.tables import DeltaTable

    row = {
        "alias": "champion",
        "run_key": chosen["run_key"],
        "min_confidence": float(chosen["min_confidence"]),
        "model_version": str(version),
        "hit_rate_at_10": float(chosen.get("hit_rate_at_10", chosen.get("hit_rate_at_k", 0.0))),
        "baseline_hit_rate_at_10": chosen.get("baseline_hit_rate_at_10"),
        "chosen_at": datetime.now(UTC),
    }
    schema = spark.table("instacart.ml.champion").schema
    frame = spark.createDataFrame([tuple(row.get(field.name) for field in schema.fields)], schema)
    (
        DeltaTable.forName(spark, "instacart.ml.champion")
        .alias("target")
        .merge(frame.alias("source"), "target.alias = source.alias")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def promote(spark: SparkSession, *, min_coverage: float = 0.5) -> dict:
    """Chọn trên val, xuất artifacts, lưu metadata và cuối cùng cập nhật alias champion."""
    rows = [row.asDict(recursive=True) for row in spark.table("instacart.ml.holdout_metrics").collect()]
    chosen = select_champion(rows, min_coverage=min_coverage)
    # Chấm test sau khi chọn; test không tham gia quy tắc chọn cấu hình.
    test = next(
        (
            row
            for row in rows
            if row.get("run_key") == chosen["run_key"]
            and row.get("eval_part") == "test"
            and float(row.get("min_confidence", -1)) == float(chosen["min_confidence"])
        ),
        None,
    )
    if test is None:
        from ml.evaluate_holdout import evaluate_run

        test = evaluate_run(spark, chosen["run_key"], float(chosen["min_confidence"]), "test")
    with tempfile.TemporaryDirectory() as directory:
        # Export volume trước; Parquet local dùng để log artifact vào MLflow.
        rules_path, items_path = _export(spark, chosen)
        local_rules, local_items = f"{directory}/rules.parquet", f"{directory}/items.parquet"
        spark.read.parquet(rules_path).toPandas().to_parquet(local_rules, index=False)
        spark.read.parquet(items_path).toPandas().to_parquet(local_items, index=False)
        version = register_model(local_rules, local_items, chosen["run_key"], float(chosen["min_confidence"]))
    _write_champion(spark, chosen, version)
    MlflowClient(registry_uri="databricks-uc").set_registered_model_alias(MODEL, "champion", version)
    chosen["model_version"] = str(version)
    chosen["test_metrics"] = test
    return chosen


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--min-coverage", type=float, default=0.5)
    args = parser.parse_args()
    result = promote(SparkSession.builder.getOrCreate(), min_coverage=args.min_coverage)
    print(
        f"Champion {result['run_key']} → version {result['model_version']}; beats baseline={result['beats_baseline']}"
    )


if __name__ == "__main__":
    main()
