"""Keep serving recommendations aligned with the holdout SQL ranking."""

from pathlib import Path

import pandas as pd

from ml.serving.basket_rules import BasketRules


def test_basket_rules_ranking_matches_holdout_sql(spark):
    rules = spark.createDataFrame(
        [("rules-v1", [1], [3], 0.8, 1.5)],
        "run_key string, antecedent array<int>, consequent array<int>, confidence double, lift double",
    )
    holdout = spark.createDataFrame(
        [(10, [1, 2], [3], "v1", "val")],
        "order_id long, context array<int>, targets array<int>, data_version string, eval_part string",
    )
    items = pd.DataFrame(
        {
            "product_id": [101, 102, 103, 104, 105, 106],
            "idx": [1, 2, 3, 4, 5, 6],
            "product_name": [f"item-{idx}" for idx in range(1, 7)],
            "data_version": ["v1"] * 6,
            "grain": ["product"] * 6,
        }
    )
    item_index = spark.createDataFrame(items[["idx", "data_version", "grain"]])
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
    sql_ctes = sql_text.split("SELECT :run_key AS run_key", 1)[0]
    sql_ranked = spark.sql(
        sql_ctes
        + "SELECT order_id, sort_array(collect_list(named_struct('rec_rank', rec_rank, 'item', item))) AS ranked "
        "FROM recommendations GROUP BY order_id",
        args={"run_key": "rules-v1", "min_conf": 0.5, "part": "val"},
    ).first()
    sql_indices = [item.item for item in sql_ranked.ranked]

    model = BasketRules()
    model.rules = [{"antecedent": [1], "consequent": [3], "confidence": 0.8, "lift": 1.5}]
    model.pid2idx = {int(row.product_id): int(row.idx) for row in items.itertuples(index=False)}
    model.idx2pid = {idx: pid for pid, idx in model.pid2idx.items()}
    model.idx2name = dict(zip(items["idx"], items["product_name"], strict=True))
    model.popular = items.sort_values("idx")["idx"].tolist()
    serving = model._recommend([101, 102], 3)
    serving_indices = [model.pid2idx[row["product_id"]] for row in serving]

    assert serving_indices == sql_indices == [3, 4, 5]
