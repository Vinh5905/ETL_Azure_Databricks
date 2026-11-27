"""Chạy eval_holdout.sql trên bảng đồ chơi, đối chiếu với số tính tay (AD-DS-18)."""

from pathlib import Path

import pytest

from ml.evaluate_holdout import _execute_eval_sql, evaluate_run, expand_e7_confidences

ROOT = Path(__file__).parents[2]
N_ITEMS = 14  # idx 0..13 ở grain product, data_version v1


@pytest.fixture(scope="module")
def toy_sql(spark):
    # idx 0 phổ biến nhất; item_key cố ý khác idx để bắt lỗi lấy nhầm item_key.
    item_index = [(i, 1000 + i, "v1", "product") for i in range(N_ITEMS)]
    item_index += [(0, 5000, "v1", "aisle"), (0, 6000, "v0", "product")]
    spark.createDataFrame(
        item_index, "idx int, item_key int, data_version string, grain string"
    ).createOrReplaceTempView("toy_item_index")
    spark.createDataFrame(
        [
            ("r1", [0], [3], 0.9, 2.0),
            ("r1", [0], [4], 0.6, 3.0),
            ("r1", [0, 1], [4], 0.95, 1.5),  # cùng món 4, giữ luật tốt nhất, không nhân đôi
            ("r1", [0, 1], [2], 0.7, 1.2),  # chỉ khớp qua vế trái 2 món
            ("r1", [2], [13], 0.8, 4.0),
            ("r1", [5], [12], 0.4, 5.0),  # dưới min_conf 0.5
            ("r1", [5], [12, 13], 0.99, 5.0),  # vế phải 2 món, bị bỏ
            ("other", [5], [12], 0.99, 5.0),  # run khác
        ],
        "run_key string, antecedent array<int>, consequent array<int>, confidence double, lift double",
    ).createOrReplaceTempView("toy_association_rules")
    spark.createDataFrame(
        [
            (1, [0, 1], [4, 2], "v1", "val"),
            (2, [2], [13, 12], "v1", "val"),
            (3, [0, 3], [11], "v1", "val"),  # luật 0->3 bị loại vì 3 đã ở context
            (4, [5], [12], "v1", "val"),
            (5, [0], [], "v1", "val"),  # targets rỗng, bỏ qua
            (6, [0, 1], [4], "v1", "test"),
            (7, [0, 1], [4], "v0", "val"),
        ],
        "order_id long, context array<int>, targets array<int>, data_version string, eval_part string",
    ).createOrReplaceTempView("toy_holdout_eval")
    return (
        (ROOT / "src/ml/sql/eval_holdout.sql")
        .read_text(encoding="utf-8")
        .replace("instacart.ml.association_rules", "toy_association_rules")
        .replace("instacart.ds_data.holdout_eval", "toy_holdout_eval")
        .replace("instacart.ds_data.item_index", "toy_item_index")
    )


METRICS = (
    "hit_rate_at_k",
    "precision_at_k",
    "recall_at_k",
    "order_coverage",
    "catalog_coverage",
    "rule_only_hit_rate_at_k",
    "rule_only_precision_at_k",
    "rule_only_recall_at_k",
    "rule_only_catalog_coverage",
)


def test_expand_e7_confidences_restores_all_nine_evaluations():
    rows = [
        {
            "run_key": f"support-{support}",
            "min_confidence": 0.1,
            "sweep": '{"min_confidence":[0.1,0.2,0.3]}',
        }
        for support in (1, 2, 5)
    ]
    expanded = expand_e7_confidences(rows)
    assert len(expanded) == 9
    assert expanded == [(f"support-{support}", confidence) for support in (1, 2, 5) for confidence in (0.1, 0.2, 0.3)]


def test_expand_e7_confidences_falls_back_to_run_min_confidence():
    assert expand_e7_confidences([{"run_key": "legacy", "min_confidence": 0.2, "sweep": None}]) == [("legacy", 0.2)]


def _metrics(row) -> dict:
    values = row.asDict()
    for name in METRICS:
        assert isinstance(values[name], float), f"{name} phải là DOUBLE, nhận {type(values[name])}"
    return {name: values[name] for name in ("n_eval", *METRICS)}


def test_eval_sql_rule_run_matches_hand_computed(spark, toy_sql):
    # Danh sách top-10 tính tay (luật trước, rồi bù idx nhỏ nhất ngoài context):
    #   đơn 1 ctx {0,1}: luật [4, 3, 2] + 5..11       targets {4,2}  -> trúng 2
    #   đơn 2 ctx {2}:   luật [13] + 0,1,3..10        targets {13,12} -> trúng 1
    #   đơn 3 ctx {0,3}: luật [4] + 1,2,5..11         targets {11}   -> trúng 1
    #   đơn 4 ctx {5}:   không luật, 0..4,6..10       targets {12}   -> trượt
    row = _execute_eval_sql(spark, "r1", 0.5, "val", sql_text=toy_sql)
    assert _metrics(row) == pytest.approx(
        {
            "n_eval": 4,
            "hit_rate_at_k": 3 / 4,
            "precision_at_k": (2 + 1 + 1 + 0) / 10 / 4,
            "recall_at_k": (1 + 1 / 2 + 1 + 0) / 4,
            "order_coverage": 3 / 4,
            "catalog_coverage": 13 / N_ITEMS,  # mọi idx trừ 12
            # chỉ luật: đơn 1 {4,3,2}, đơn 2 {13}, đơn 3 {4}, đơn 4 rỗng
            "rule_only_hit_rate_at_k": 2 / 4,
            "rule_only_precision_at_k": (2 + 1) / 10 / 4,
            "rule_only_recall_at_k": (1 + 1 / 2) / 4,
            "rule_only_catalog_coverage": 4 / N_ITEMS,
        }
    )


def test_eval_sql_popularity_baseline_matches_hand_computed(spark, toy_sql):
    # Baseline: 10 idx nhỏ nhất ngoài context.
    #   đơn 1: 2..11 -> trúng {4,2}; đơn 2: 0,1,3..10 -> trượt
    #   đơn 3: 1,2,4..11 -> trúng {11}; đơn 4: 0..4,6..10 -> trượt
    row = _execute_eval_sql(spark, "popularity", 0.0, "val", sql_text=toy_sql)
    assert _metrics(row) == pytest.approx(
        {
            "n_eval": 4,
            "hit_rate_at_k": 2 / 4,
            "precision_at_k": (2 + 1) / 10 / 4,
            "recall_at_k": (1 + 1) / 4,
            "order_coverage": 0.0,
            "catalog_coverage": 12 / N_ITEMS,  # idx 0..11
            "rule_only_hit_rate_at_k": 0.0,
            "rule_only_precision_at_k": 0.0,
            "rule_only_recall_at_k": 0.0,
            "rule_only_catalog_coverage": 0.0,
        }
    )


def test_evaluate_run_merges_idempotently(spark, toy_sql):
    table = "ml_holdout_test.holdout_metrics"
    # Lấy đúng câu CREATE của holdout_metrics trong ddl_ml.sql để bắt lệch cột giữa DDL và SQL đánh giá.
    statements = (ROOT / "src/ml/sql/ddl_ml.sql").read_text(encoding="utf-8").split(";")
    (ddl,) = [s for s in statements if "instacart.ml.holdout_metrics (" in s]
    spark.sql("CREATE DATABASE IF NOT EXISTS ml_holdout_test")
    try:
        spark.sql(ddl.replace("instacart.ml.holdout_metrics", table))
        for _ in range(2):
            evaluate_run(spark, "r1", 0.5, "val", sql_text=toy_sql, metrics_table=table, log_mlflow=False)
            evaluate_run(spark, "popularity", 0.0, "val", sql_text=toy_sql, metrics_table=table, log_mlflow=False)
        sql_columns = _execute_eval_sql(spark, "r1", 0.5, "val", sql_text=toy_sql).asDict().keys()
        assert set(spark.table(table).columns) == set(sql_columns)
        rows = {r.run_key: r for r in spark.table(table).collect()}
        assert set(rows) == {"r1", "popularity"}
        assert rows["r1"].hit_rate_at_k == pytest.approx(3 / 4)
        assert rows["popularity"].recall_at_k == pytest.approx(2 / 4)
        with pytest.raises(ValueError):
            evaluate_run(spark, "r1", 0.5, "train", sql_text=toy_sql, metrics_table=table, log_mlflow=False)
    finally:
        spark.sql("DROP DATABASE IF EXISTS ml_holdout_test CASCADE")
