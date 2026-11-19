"""Integration checks for the holdout SQL against toy Spark views."""

from pathlib import Path

from ml.evaluate_holdout import _execute_eval_sql


def test_eval_sql_handles_rule_run_and_popularity_baseline(spark):
    rules = spark.createDataFrame(
        [("rules-v1", [1], [3], 0.8, 1.5)],
        "run_key string, antecedent array<int>, consequent array<int>, confidence double, lift double",
    )
    holdout = spark.createDataFrame(
        [
            (10, [1, 2], [3], "v1", "val"),
            (11, [], [7], "v1", "val"),
        ],
        "order_id long, context array<int>, targets array<int>, data_version string, eval_part string",
    )
    # idx deliberately differs from item_key: the production query must recommend idx.
    item_index = spark.createDataFrame(
        [(7, 70, "v1", "product"), (8, 80, "v1", "product"), (9, 90, "v1", "product")],
        "idx int, item_key int, data_version string, grain string",
    )
    rules.createOrReplaceTempView("toy_association_rules")
    holdout.createOrReplaceTempView("toy_holdout_eval")
    item_index.createOrReplaceTempView("toy_item_index")

    sql_text = (
        (Path(__file__).parents[2] / "src/ml/sql/eval_holdout.sql")
        .read_text(encoding="utf-8")
        .replace("instacart.ml.association_rules", "toy_association_rules")
        .replace("instacart.ds_data.holdout_eval", "toy_holdout_eval")
        .replace("instacart.ds_data.item_index", "toy_item_index")
    )

    rule_result = _execute_eval_sql(spark, "rules-v1", 0.5, "val", sql_text=sql_text)
    baseline_result = _execute_eval_sql(spark, "popularity", 0.0, "val", sql_text=sql_text)

    assert rule_result.n_eval == 2
    assert rule_result.order_coverage == 0.5
    assert rule_result.rule_only_hit_rate_at_k == 0.5
    # The baseline's first recommendation is idx=7 (not item_key=70).
    assert baseline_result.hit_rate_at_k == 0.5
    assert baseline_result.order_coverage == 0.0
