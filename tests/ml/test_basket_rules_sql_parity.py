"""Top-k của BasketRules phải trùng top-k của eval_holdout.sql trên cùng luật và giỏ (AD-DS-19)."""

from pathlib import Path

import pandas as pd

from ml.serving.basket_rules import BasketRules

N_ITEMS = 14
RULES = [
    ("r1", [0], [3], 0.9, 2.0),
    ("r1", [0], [4], 0.6, 3.0),
    ("r1", [0, 1], [4], 0.95, 1.5),  # cùng món 4: giữ luật tốt nhất
    ("r1", [0, 1], [2], 0.7, 1.2),  # chỉ khớp qua vế trái 2 món
    ("r1", [2], [13], 0.8, 4.0),
    ("r1", [5], [7], 0.6, 1.0),  # hòa điểm với luật dưới: xếp theo idx tăng dần
    ("r1", [5], [6], 0.6, 1.0),
    ("r1", [5], [12], 0.4, 5.0),  # dưới min_conf
    ("r1", [5], [12, 13], 0.99, 5.0),  # vế phải 2 món
    ("other", [5], [12], 0.99, 5.0),
]
MIN_CONF = 0.5
BASKETS = {1: [0, 1], 2: [2], 3: [0, 3], 4: [5], 5: []}


def _sql_top10(spark) -> dict[int, list[int]]:
    spark.createDataFrame(
        RULES, "run_key string, antecedent array<int>, consequent array<int>, confidence double, lift double"
    ).createOrReplaceTempView("toy_association_rules")
    spark.createDataFrame(
        [(order_id, ctx, [N_ITEMS - 1], "v1", "val") for order_id, ctx in BASKETS.items()],
        "order_id long, context array<int>, targets array<int>, data_version string, eval_part string",
    ).createOrReplaceTempView("toy_holdout_eval")
    spark.createDataFrame(
        [(i, "v1", "product") for i in range(N_ITEMS)], "idx int, data_version string, grain string"
    ).createOrReplaceTempView("toy_item_index")
    sql_text = (
        (Path(__file__).parents[2] / "src/ml/sql/eval_holdout.sql")
        .read_text(encoding="utf-8")
        .replace("instacart.ml.association_rules", "toy_association_rules")
        .replace("instacart.ds_data.holdout_eval", "toy_holdout_eval")
        .replace("instacart.ds_data.item_index", "toy_item_index")
    )
    ctes = sql_text.split("SELECT :run_key AS run_key", 1)[0]
    rows = spark.sql(
        ctes + "SELECT order_id, sort_array(collect_list(struct(rec_rank, item))) AS ranked "
        "FROM recommendations GROUP BY order_id",
        args={"run_key": "r1", "min_conf": MIN_CONF, "part": "val"},
    ).collect()
    return {row.order_id: [entry.item for entry in row.ranked] for row in rows}


def _model() -> BasketRules:
    # Giống file Parquet champion xuất ra: đúng run, vế phải 1 món, confidence ≥ min_conf.
    model = BasketRules()
    model.rules = [
        {"antecedent": a, "consequent": c, "confidence": conf, "lift": lift}
        for run_key, a, c, conf, lift in RULES
        if run_key == "r1" and len(c) == 1 and conf >= MIN_CONF
    ]
    items = pd.DataFrame({"idx": range(N_ITEMS), "product_id": [1000 + i for i in range(N_ITEMS)]})
    model.pid2idx = dict(zip(items.product_id, items.idx, strict=True))
    model.idx2pid = {idx: pid for pid, idx in model.pid2idx.items()}
    model.idx2name = {idx: f"item-{idx}" for idx in model.idx2pid}
    model.popular = list(range(N_ITEMS))
    return model


def test_basket_rules_top10_matches_holdout_sql(spark):
    sql_top10 = _sql_top10(spark)
    model = _model()
    serving_top10 = {
        order_id: [model.pid2idx[rec["product_id"]] for rec in model._recommend([1000 + i for i in ctx], 10)]
        for order_id, ctx in BASKETS.items()
    }
    assert serving_top10 == sql_top10
    # Mốc tính tay để chắc cả hai không cùng sai.
    assert sql_top10[1] == [4, 3, 2, 5, 6, 7, 8, 9, 10, 11]
    assert sql_top10[3][0] == 4  # 0->3 bị loại vì 3 đã ở giỏ
    assert sql_top10[4][:2] == [6, 7]
    assert sql_top10[5] == list(range(10))
