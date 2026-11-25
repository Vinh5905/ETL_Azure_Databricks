from __future__ import annotations

import re
import uuid
from pathlib import Path

import pytest
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

SQL_PATH = Path(__file__).resolve().parents[2] / "src/ml/sql/publish_gold.sql"


def _replace_catalog(sql: str, prefix: str) -> str:
    return (
        sql.replace("instacart.gold.", f"{prefix}_gold.")
        .replace("instacart.ml.", f"{prefix}_ml.")
        .replace("instacart.ds_data.", f"{prefix}_ds_data.")
    )


def _run_statements(spark, sql: str) -> None:
    for statement in re.split(r";\s*(?=(?:[^']*'[^']*')*[^']*$)", sql):
        statement = "\n".join(line for line in statement.splitlines() if not line.lstrip().startswith("--")).strip()
        if statement:
            spark.sql(statement)


def test_publish_gold_views_map_champion_rules_and_experiment_metrics(spark):
    suffix = uuid.uuid4().hex[:10]
    schemas = {name: f"{suffix}_{name}" for name in ("gold", "ml", "ds_data")}
    for database in schemas.values():
        spark.sql(f"CREATE DATABASE `{database}`")

    try:
        spark.createDataFrame(
            [("champion", "champion-run", 0.5)],
            "alias string, run_key string, min_confidence double",
        ).write.saveAsTable(f"{schemas['ml']}.champion")
        spark.createDataFrame(
            [
                ("champion-run", [1], [2], 0.2, 0.8, 2.0),
                ("champion-run", [1], [3], 0.1, 0.7, 1.5),
                ("champion-run", [2], [1], 0.2, 0.6, 1.2),
                ("other-run", [1], [3], 0.5, 0.9, 3.0),
            ],
            "run_key string, antecedent array<int>, consequent array<int>, support double, "
            "confidence double, lift double",
        ).write.saveAsTable(f"{schemas['ml']}.association_rules")
        spark.createDataFrame(
            [
                ("v1", "product", 1, "101", "Apple"),
                ("v1", "product", 2, "202", "Banana"),
                ("v1", "product", 3, "303", "Carrot"),
                ("v1", "aisle", 1, "1", "Fresh"),
            ],
            "data_version string, grain string, idx int, item_key string, item_name string",
        ).write.saveAsTable(f"{schemas['ds_data']}.item_index")
        metric_rows = [
            ("E2", "databricks", "dapriori", "product", 25, None, 0.01, 1, "", "ok", 1.0, 100.0, 0.0, 10, 5, 6),
            ("E2", "databricks", "dapriori", "product", 25, None, 0.01, 1, "", "ok", 2.0, 105.0, 0.5, 11, 5, 6),
            ("E2", "databricks", "dapriori", "product", 25, None, 0.01, 1, "", "timeout", None, 120.0, 0.0, 0, 0, 0),
            ("E2", "databricks", "dapriori", "product", 25, None, 0.01, 1, "", "ok", 3.0, 110.0, 1.0, 12, 6, 7),
            ("E2", "databricks", "dapriori", "product", 25, None, 0.01, 4, "", "ok", 4.0, 130.0, 2.0, 12, 6, 7),
            ("E2", "databricks", "dapriori", "product", 25, None, 0.01, 4, "", "ok", 5.0, 135.0, 3.0, 13, 7, 8),
            ("E2", "databricks", "dapriori", "product", 25, None, 0.01, 4, "", "ok", 6.0, 140.0, 4.0, 14, 8, 9),
            ("E2", "databricks", "dapriori", "product", 25, None, 0.01, 4, "", "timeout", None, 150.0, 5.0, 0, 0, 0),
            # 2 lần ok (số chẵn): trung vị chính xác là 3.0, percentile_approx sẽ trả 2.0.
            ("E2", "databricks", "dapriori", "product", 25, None, 0.01, 2, "", "ok", 2.0, 100.0, 0.0, 12, 6, 7),
            ("E2", "databricks", "dapriori", "product", 25, None, 0.01, 2, "", "ok", 4.0, 100.0, 0.0, 12, 6, 7),
        ]
        run_schema = StructType(
            [
                StructField("experiment", StringType()),
                StructField("platform", StringType()),
                StructField("algo", StringType()),
                StructField("grain", StringType()),
                StructField("scale_pct", IntegerType()),
                StructField("top_n", IntegerType()),
                StructField("min_support", DoubleType()),
                StructField("cores", IntegerType()),
                StructField("sweep", StringType()),
                StructField("status", StringType()),
                StructField("wall_s", DoubleType()),
                StructField("peak_mem_mb", DoubleType()),
                StructField("spill_mb", DoubleType()),
                StructField("n_itemsets", IntegerType()),
                StructField("n_rules_single", IntegerType()),
                StructField("n_rules_all", IntegerType()),
            ]
        )
        spark.createDataFrame(metric_rows, run_schema).write.saveAsTable(f"{schemas['ml']}.experiment_runs")
        spark.createDataFrame(
            [("champion-run", 0.5, "test", 10, 2, 0.5, 0.25, 0.4, 0.7, 0.1, 0.3, 0.2, 0.5, None, "2026-01-01")],
            "run_key string, min_confidence double, eval_part string, k int, n_eval long, "
            "hit_rate_at_k double, precision_at_k double, recall_at_k double, order_coverage double, "
            "catalog_coverage double, rule_only_hit_rate_at_k double, rule_only_precision_at_k double, "
            "rule_only_recall_at_k double, rule_only_catalog_coverage double, evaluated_at string",
        ).write.saveAsTable(f"{schemas['ml']}.holdout_metrics")

        sql = _replace_catalog(SQL_PATH.read_text(encoding="utf-8"), suffix)
        _run_statements(spark, sql)

        rules = spark.table(f"{schemas['gold']}.association_rules").orderBy("antecedent_names").collect()
        assert len(rules) == 3
        assert {(row.antecedent_names, row.consequent_names) for row in rules} == {
            ("Apple", "Banana"),
            ("Apple", "Carrot"),
            ("Banana", "Apple"),
        }
        recommendations = spark.table(f"{schemas['gold']}.recommendations").orderBy("product_id", "rank").collect()
        assert [(row.product_id, row.recommended_product_id, row.rank) for row in recommendations] == [
            (101, 202, 1),
            (101, 303, 2),
            (202, 101, 1),
        ]
        metrics = spark.table(f"{schemas['gold']}.experiment_metrics").collect()
        baseline = next(row for row in metrics if row.cores == 1)
        scaled = next(row for row in metrics if row.cores == 4)
        assert (baseline.n_ok, baseline.n_timeout) == (3, 1)
        assert (baseline.wall_s, scaled.wall_s) == (2.0, 5.0)
        assert (scaled.n_ok, scaled.n_timeout) == (3, 1)
        assert scaled.speedup == 0.4
        assert scaled.efficiency == 0.1
        even = next(row for row in metrics if row.cores == 2)
        assert even.wall_s == 3.0
        assert even.speedup == pytest.approx(2 / 3)
        assert spark.table(f"{schemas['gold']}.holdout_metrics").count() == 1
    finally:
        for database in schemas.values():
            spark.sql(f"DROP DATABASE IF EXISTS `{database}` CASCADE")
