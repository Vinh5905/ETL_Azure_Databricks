"""Chọn, đánh giá và đăng ký model luật kết hợp tốt nhất của E7."""

from __future__ import annotations

import logging
import os
import tempfile
from collections.abc import Iterable
from importlib.metadata import version
from pathlib import Path
from typing import Any

import mlflow
import pandas as pd
from mlflow import MlflowClient
from mlflow.models import infer_signature
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ml.evaluate_holdout import evaluate_run
from ml.serving.basket_rules import BasketRules

LOGGER = logging.getLogger(__name__)
MODEL = "instacart.ml.basket_rules"
CHAMPION_TABLE = "instacart.ml.champion"
ML_PKG_ROOT = str(Path(__file__).resolve().parents[1])
PINNED_PACKAGES = ("mlflow", "pandas", "numpy", "pyarrow")


def select_champion(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Chọn E7 val@10 đạt coverage; hòa thì lấy cấu hình có ít luật hơn."""
    eligible = [
        dict(row)
        for row in rows
        if row.get("experiment") == "E7"
        and row.get("eval_part") == "val"
        and int(row.get("k", 0)) == 10
        and float(row.get("order_coverage", 0.0)) >= 0.5
    ]
    if not eligible:
        raise ValueError("Không có cấu hình E7 val@10 đạt order_coverage >= 0.5")
    return min(
        eligible,
        key=lambda row: (
            -float(row["hit_rate_at_k"]),
            int(row["rule_count"]),
            float(row["min_confidence"]),
            str(row["run_key"]),
        ),
    )


def _candidate_rows(spark: SparkSession) -> list[dict[str, Any]]:
    rows = spark.sql(
        """
        SELECT m.run_key, m.min_confidence, m.eval_part, m.k,
               m.hit_rate_at_k, m.order_coverage, r.experiment,
               count(a.run_key) AS rule_count
        FROM instacart.ml.holdout_metrics m
        JOIN instacart.ml.experiment_runs r
          ON r.run_key = m.run_key AND r.experiment = 'E7' AND r.status = 'ok'
        LEFT JOIN instacart.ml.association_rules a
          ON a.run_key = m.run_key AND size(a.consequent) = 1
         AND a.confidence >= m.min_confidence
        WHERE m.eval_part = 'val' AND m.k = 10
        GROUP BY m.run_key, m.min_confidence, m.eval_part, m.k,
                 m.hit_rate_at_k, m.order_coverage, r.experiment
        """
    )
    return [row.asDict() for row in rows.collect()]


def _baseline_hit_rate(spark: SparkSession) -> float:
    rows = (
        spark.table("instacart.ml.holdout_metrics")
        .where("run_key = 'popularity' AND min_confidence = 0.0 AND eval_part = 'val' AND k = 10")
        .select("hit_rate_at_k")
        .limit(2)
        .collect()
    )
    if len(rows) != 1:
        raise ValueError("Cần đúng một metrics val@10 cho baseline run_key='popularity'")
    return float(rows[0].hit_rate_at_k)


def _write_local_parquet(frame: Any, path: str) -> None:
    """Ghi Parquet vào filesystem local mà MLflow và pyfunc có thể đọc."""
    frame.toPandas().to_parquet(path, index=False)


def _export_artifacts(spark: SparkSession, run_key: str, min_confidence: float, root: str) -> tuple[str, str]:
    rules_path = os.path.join(root, "rules")
    items_path = os.path.join(root, "items")
    rules = spark.table("instacart.ml.association_rules")
    _write_local_parquet(
        rules.where((F.col("run_key") == run_key) & (F.col("confidence") >= min_confidence))
        .where("size(consequent) = 1")
        .selectExpr("antecedent", "CAST(element_at(consequent, 1) AS INT) AS item", "confidence", "lift"),
        rules_path,
    )
    _write_local_parquet(
        spark.table("instacart.ds_data.item_index")
        .where("data_version = 'v1' AND grain = 'product'")
        .selectExpr("idx", "CAST(item_key AS INT) AS product_id", "item_name AS product_name"),
        items_path,
    )
    return rules_path, items_path


def _register_model(
    rules_path: str,
    items_path: str,
    champion: dict[str, Any],
    baseline_hit_rate: float,
    test_metrics: dict[str, Any],
) -> str:
    package_pins = [f"{name}=={version(name)}" for name in PINNED_PACKAGES]
    mlflow.set_registry_uri("databricks-uc")
    mlflow.set_experiment("/Shared/instacart-ds")
    example = pd.DataFrame({"product_ids": [[24852, 13176]]})
    model = BasketRules()
    context = type("ModelContext", (), {"artifacts": {"rules": rules_path, "items": items_path}})()
    model.load_context(context)
    signature = infer_signature(example, model.predict(None, example), params={"k": 10})
    hit_rate = float(champion["hit_rate_at_k"])
    conclusion = "beats_popularity" if hit_rate > baseline_hit_rate else "does_not_beat_popularity"
    with mlflow.start_run(run_name=f"champion-{champion['run_key'][:8]}"):
        mlflow.log_params(
            {
                "source_run_key": champion["run_key"],
                "min_confidence": float(champion["min_confidence"]),
                "baseline_run_key": "popularity",
                "selection": "E7 val@10, order_coverage >= 0.5, then rule_count",
                "validation_conclusion": conclusion,
                **{f"pinned_{name}": version(name) for name in PINNED_PACKAGES},
            }
        )
        mlflow.log_metrics(
            {
                "val_hit_rate_at_10": hit_rate,
                "baseline_val_hit_rate_at_10": baseline_hit_rate,
                "val_hit_rate_delta_at_10": hit_rate - baseline_hit_rate,
                "test_hit_rate_at_10": float(test_metrics["hit_rate_at_k"]),
                "test_order_coverage": float(test_metrics["order_coverage"]),
            }
        )
        mlflow.set_tags({"validation_conclusion": conclusion, "source_run_key": champion["run_key"]})
        if conclusion == "does_not_beat_popularity":
            LOGGER.warning(
                "Kết luận âm: E7 hit_rate@10 %.6f không cao hơn popularity %.6f; vẫn đăng ký model.",
                hit_rate,
                baseline_hit_rate,
            )
        info = mlflow.pyfunc.log_model(
            artifact_path="model",
            python_model=BasketRules(),
            signature=signature,
            input_example=example,
            artifacts={"rules": rules_path, "items": items_path},
            pip_requirements=package_pins,
            code_paths=[ML_PKG_ROOT],
            registered_model_name=MODEL,
        )
        model_version = str(info.registered_model_version)
        MlflowClient(registry_uri="databricks-uc").set_registered_model_alias(MODEL, "champion", model_version)
    return model_version


def _merge_champion(
    spark: SparkSession,
    champion: dict[str, Any],
    model_version: str,
    baseline_hit_rate: float,
) -> None:
    from delta.tables import DeltaTable

    frame = spark.createDataFrame(
        [
            (
                "champion",
                str(champion["run_key"]),
                float(champion["min_confidence"]),
                model_version,
                float(champion["hit_rate_at_k"]),
                baseline_hit_rate,
            )
        ],
        "alias string, run_key string, min_confidence double, model_version string, "
        "hit_rate_at_10 double, baseline_hit_rate_at_10 double",
    )
    DeltaTable.forName(spark, CHAMPION_TABLE).alias("target").merge(
        frame.alias("source"), "target.alias = source.alias"
    ).whenMatchedUpdate(
        set={
            "run_key": "source.run_key",
            "min_confidence": "source.min_confidence",
            "model_version": "source.model_version",
            "hit_rate_at_10": "source.hit_rate_at_10",
            "baseline_hit_rate_at_10": "source.baseline_hit_rate_at_10",
            "chosen_at": "current_timestamp()",
        }
    ).whenNotMatchedInsert(
        values={
            "alias": "source.alias",
            "run_key": "source.run_key",
            "min_confidence": "source.min_confidence",
            "model_version": "source.model_version",
            "hit_rate_at_10": "source.hit_rate_at_10",
            "baseline_hit_rate_at_10": "source.baseline_hit_rate_at_10",
            "chosen_at": "current_timestamp()",
        }
    ).execute()


def run(spark: SparkSession) -> dict[str, Any]:
    """Chọn champion, score test, đăng ký UC model và upsert bảng champion."""
    champion = select_champion(_candidate_rows(spark))
    baseline_hit_rate = _baseline_hit_rate(spark)
    test_metrics = evaluate_run(spark, champion["run_key"], float(champion["min_confidence"]), "test")
    with tempfile.TemporaryDirectory(prefix="instacart-champion-") as root:
        rules_path, items_path = _export_artifacts(
            spark, str(champion["run_key"]), float(champion["min_confidence"]), root
        )
        model_version = _register_model(rules_path, items_path, champion, baseline_hit_rate, test_metrics)
    _merge_champion(spark, champion, model_version, baseline_hit_rate)
    conclusion = "beats popularity" if champion["hit_rate_at_k"] > baseline_hit_rate else "does not beat popularity"
    LOGGER.info(
        "Champion run=%s confidence=%.3f val_hit_rate=%.6f baseline=%.6f (%s), test_hit_rate=%.6f, model_version=%s",
        champion["run_key"],
        champion["min_confidence"],
        champion["hit_rate_at_k"],
        baseline_hit_rate,
        conclusion,
        test_metrics["hit_rate_at_k"],
        model_version,
    )
    return {
        **champion,
        "baseline_hit_rate_at_10": baseline_hit_rate,
        "test": test_metrics,
        "model_version": model_version,
    }


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    spark = SparkSession.builder.getOrCreate()
    run(spark)


if __name__ == "__main__":
    main()
