"""Kiểm Spark/Arrow thật; dp chỉ giả lập đăng ký, không thay thế nghiệm thu LDP."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pandas as pd
import pytest
from pyspark import cloudpickle
from pyspark.sql import functions as F
from rapidfuzz import fuzz

from common import off, text
from common.off import (
    MATCH_RULES,
    assess_off,
    clean_off,
    match_statistics,
    product_matches,
    quarantine_off,
    score_names,
    score_series,
)
from common.text import norm
from ingest.off_crawler import hit_record

ROOT = Path(__file__).resolve().parents[1]
US = ["en:united-states"]


@pytest.fixture(autouse=True)
def ansi(spark):
    previous = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    yield
    spark.conf.set("spark.sql.ansi.enabled", previous)


def hit(pid=1, code="00123", name="Fresh Banana", **fields):
    return {
        "_product_id": pid,
        "code": code,
        "product_name": name,
        "countries_tags": US,
        "_ingest_ts": "2026-03-01T00:00:00Z",
        **fields,
    }


def frame(spark, records):
    return spark.read.json(spark.sparkContext.parallelize([json.dumps(r) for r in records], 1))


@pytest.mark.parametrize(
    ("ic", "off", "brand"),
    [
        ("Banana", "Fresh Banana", None),
        ("Robust Golden Unsweetened Oolong Tea", "Oolong Tea (Pokka)", "Pokka"),
        ("Organic Strawberries", "Yaourt fraise biologique", ""),
        ("Bag of Organic Bananas", "Plastic Bag", None),
    ],
)
def test_dictionary_pairs_use_shared_norm_and_rapidfuzz(ic, off, brand):
    assert score_names(ic, off, brand) == pytest.approx(fuzz.token_set_ratio(norm(ic), norm(off)))


def test_brand_bonus_respects_token_boundaries_and_cap():
    base = fuzz.token_set_ratio("acme green tea", "oolong tea")
    assert score_names("Acme Green Tea", "Oolong Tea", "Acme,Other") == pytest.approx(base + 5)
    assert score_names("Acme Green Tea", "Acme Green Tea", "Acme") == 100
    assert score_names("Vitamin Tea", "Oolong Tea", "Vita") == pytest.approx(
        fuzz.token_set_ratio("vitamin tea", "oolong tea")
    )
    assert score_names("Acme Green Tea", "Oolong Tea", "Other,Acme") == pytest.approx(base)


@pytest.mark.parametrize("ic,off", [(None, "Tea"), ("Tea", None), ("Organic 12 oz", "Organic"), ("", "")])
def test_empty_normalized_names_never_get_brand_bonus(ic, off):
    assert score_names(ic, off, "Tea") == 0.0


def test_arrow_udf_executes_on_spark_workers_and_handles_nulls(spark):
    pairs = [("Banana", "Fresh Banana", None), ("Tea", None, None), (None, "Tea", "Tea")]
    data = spark.createDataFrame(pairs, "ic STRING, off STRING, brand STRING")
    udf = F.pandas_udf(score_series, "double")
    scores = [r.score for r in data.select(udf("ic", "off", "brand").alias("score")).collect()]
    assert scores == [100.0, 0.0, 0.0]
    assert score_series(pd.Series([pd.NA]), pd.Series(["Tea"]), pd.Series([pd.NA])).tolist() == [0.0]


@pytest.mark.parametrize("brands", ["Acme, Other", ["Acme", "Other"]])
def test_flatten_crawler_payload_preserves_barcode_and_nested_fields(spark, brands):
    record = hit_record(
        {"product_id": 1, "product_name": "Banana"},
        {
            "code": "000123",
            "product_name": "Fresh Banana",
            "brands": brands,
            "categories_tags": ["en:fruit"],
            "countries_tags": US,
            "nova_group": "1",
            "nutriscore_grade": "a",
            "image_url": "https://images.openfoodfacts.org/example.jpg",
            "nutriments": {"energy-kcal_100g": "89", "sugars_100g": "12.5", "fat_100g": "0.3"},
        },
    )
    record["_ingest_ts"] = "2026-03-01T00:00:00Z"
    source = frame(spark, [record]).withColumn("nutriments", F.from_json(F.to_json("nutriments"), "map<string,string>"))
    row = clean_off(assess_off(source)).first()
    assert (row.product_id, row.off_code, row.brand) == (1, "000123", "Acme,Other")
    assert (row.energy_kcal_100g, row.sugars_100g, row.fat_100g, row.nova_group) == (89, 12.5, 0.3, 1)
    assert row.countries_tags == US and row.categories_tags == ["en:fruit"]
    assert row.image_url.endswith("example.jpg") and row.nutriscore_grade == "a"


def test_optional_fields_and_sparse_struct_nutrients_do_not_break_ansi(spark):
    source = frame(spark, [hit(nutriments={"sugars_100g": "bad"}, nova_group="unknown")])
    row = assess_off(source).first()
    assert row.energy_kcal_100g is None and row.fat_100g is None and row.sugars_100g is None
    assert row.nova_group is None and row.brand == "" and row.categories_tags == [] and row._dq_failed == []


def test_invalid_keys_timestamp_and_rescue_are_all_retained_in_quarantine(spark):
    source = frame(spark, [hit(pid="bad", code="not-code", _ingest_ts="bad", _rescued_data='{"extra":1}')])
    assessed = assess_off(source)
    bad = quarantine_off(assessed).first()
    assert bad._dq_failed == ["valid_product_id", "valid_off_code", "valid_ingest_ts", "no_rescued_data"]
    assert bad._raw._product_id == "bad" and bad._raw._rescued_data == '{"extra":1}'
    assert bad._quarantined_at is not None
    assert clean_off(assessed).count() == 0


def test_latest_per_pair_replay_and_timestamp_ties_are_deterministic(spark):
    records = [
        hit(name="Old Banana"),
        hit(name="Banana A", _ingest_ts="2026-03-02T00:00:00Z"),
        hit(name="Banana B", _ingest_ts="2026-03-02T00:00:00Z"),
        hit(pid=2),
        hit(code="00099"),
        hit(name="Broken", _ingest_ts="2026-03-03T00:00:00Z", _rescued_data="bad"),
    ]
    first = clean_off(assess_off(frame(spark, records))).orderBy("product_id", "off_code").collect()
    replay = clean_off(assess_off(frame(spark, list(reversed(records)) * 2).repartition(2)))
    assert replay.orderBy("product_id", "off_code").collect() == first
    assert len(first) == 3 and first[1].off_product_name in {"Banana A", "Banana B"}
    assert quarantine_off(assess_off(frame(spark, records))).count() == 1


def test_best_candidate_preserves_uncrawled_non_us_and_missing_name_products(spark):
    products = spark.createDataFrame(
        [(1, "Banana"), (1, "Banana"), (2, "Tea"), (3, "Coffee"), (4, "Milk"), (5, "Bread"), (6, "Red Apple")],
        "product_id INT, product_name STRING",
    )
    candidates = clean_off(
        assess_off(
            frame(
                spark,
                [
                    hit(code="00999"),
                    hit(code="00001"),
                    hit(code="00000", name="Plastic Bag"),
                    hit(pid=3, name="Coffee"),
                    hit(pid=3, name="Coffee", countries_tags=["en:france"], _ingest_ts="2026-03-02T00:00:00Z"),
                    hit(pid=4, name=None),
                    hit(pid=5, name="Plastic Bag"),
                    hit(pid=6, name="Green Apple"),
                ],
            )
        )
    )
    output = product_matches(products, candidates, F.pandas_udf(score_series, "double"))
    rows = {r.product_id: r for r in output.collect()}
    assert output.count() == len(rows) == 6
    assert (rows[1].off_code, rows[1].off_match_score, rows[1].off_match_status) == ("00001", 100, "matched")
    for pid in [2, 3]:
        assert (rows[pid].off_code, rows[pid].off_match_score, rows[pid].off_match_status) == (None, 0, "no_match")
    assert rows[4].off_match_score == 0 and rows[4].off_match_status == "no_match"
    assert rows[5].off_code is not None and rows[5].off_match_status == "no_match"
    assert 70 <= rows[6].off_match_score < 85 and rows[6].off_match_status == "low_confidence"


def test_thresholds_include_85_and_70_without_rounding(spark):
    products = spark.createDataFrame(
        [(i, str(s)) for i, s in enumerate([85, 84.999, 70, 69.999], 1)], "product_id INT, product_name STRING"
    )
    candidates = clean_off(assess_off(frame(spark, [hit(pid=i) for i in range(1, 5)])))
    # Chỉ cô lập phân loại để kiểm đúng các biên; test trên kiểm scorer Arrow thật.
    result = product_matches(products, candidates, lambda a, b, br: F.col(a).cast("double"))
    assert [r.off_match_status for r in result.orderBy("product_id").collect()] == [
        "matched",
        "low_confidence",
        "low_confidence",
        "no_match",
    ]


@pytest.mark.parametrize("empty", [False, True])
def test_statistics_always_include_three_labels_and_all_product_denominator(spark, empty):
    rows = [] if empty else [("matched",), ("matched",), ("no_match",), ("no_match",)]
    result = match_statistics(spark.createDataFrame(rows, "off_match_status STRING"))
    stats = {r.off_match_status: r for r in result.collect()}
    assert set(stats) == {"matched", "low_confidence", "no_match"}
    assert stats["low_confidence"]["count"] == 0
    assert stats["matched"].total_products == (0 if empty else 4)
    assert stats["matched"].pct_all_products == (0 if empty else 50)


@pytest.mark.parametrize("enabled", [False, True])
def test_notebook_registers_mvs_and_uses_shared_scorer(spark, monkeypatch, tmp_path, enabled):
    definitions = {}
    previous_registry = cloudpickle.list_registry_pickle_by_value()

    def mv(**options):
        def wrap(function):
            definitions[options["name"]] = (function, options)
            return function

        return wrap

    def expect(*args):
        return lambda f: f

    def expect_all_or_fail(rules):
        assert rules is MATCH_RULES
        return lambda f: f

    monkeypatch.setitem(
        sys.modules,
        "pyspark.pipelines",
        SimpleNamespace(
            materialized_view=mv,
            temporary_view=mv,
            expect=expect,
            expect_or_fail=expect,
            expect_all_or_fail=expect_all_or_fail,
        ),
    )
    settings = {
        "catalog": "test-catalog",
        "bronze": "p2_bronze",
        "silver": "p2_silver",
        "enable_off": str(enabled).lower(),
    }
    old = {key: spark.conf.get(key, None) for key in settings}
    try:
        for key, value in settings.items():
            spark.conf.set(key, value)
        module = ModuleType("silver_off_under_test")
        source = ROOT / "src/pipelines/batch/silver_off.py"
        exec(compile(source.read_text(), str(source), "exec"), module.__dict__)
        if not enabled:
            assert set(definitions) == {module.table_name("p2_silver", "dim_product"), "catalog_products_assessed"}
            assert not hasattr(module, "match_score")
            return
        assert module.score_series is score_series
        env = {key: value for key, value in os.environ.items() if key != "PYTHONPATH"}
        subprocess.run(
            [
                sys.executable,
                "-c",
                """
import importlib.util
import sys
import pandas as pd
from pyspark import cloudpickle

assert importlib.util.find_spec("common") is None
scorer = cloudpickle.loads(sys.stdin.buffer.read())
result = scorer(pd.Series(["Organic Banana"]), pd.Series(["Fresh Banana"]), pd.Series([""]))
assert result.tolist() == [100.0]
""",
            ],
            input=cloudpickle.dumps(module.score_series),
            cwd=tmp_path,
            env=env,
            check=True,
        )
        names = ["off_products_clean", "product_off_match", "quarantine_off_products", "dim_product"]
        expected = {module.table_name("p2_silver", name) for name in names}
        assert set(definitions) == expected | {"catalog_products_assessed"}
        for name in ["off_products_clean", "product_off_match"]:
            assert definitions[module.table_name("p2_silver", name)][1]["cluster_by"] == ["product_id"]

        tables = {
            module.table_name("p2_bronze", "products"): spark.createDataFrame(
                [(1, "Banana", None), (2, "Tea", "bad")], "product_id INT, product_name STRING, _rescued_data STRING"
            ),
            module.table_name("p2_bronze", "off_products"): frame(spark, [hit()]),
        }
        module.spark = SimpleNamespace(read=SimpleNamespace(table=lambda name: tables[name]))
        tables["catalog_products_assessed"] = module.catalog_products_assessed()
        tables[module.table_name("p2_silver", "off_products_clean")] = module.off_products_clean()
        actual = module.product_off_match().orderBy("product_id").collect()
        assert [r.off_match_status for r in actual] == ["matched", "no_match"]
        assert module.quarantine_off_products().count() == 0
    finally:
        for dependency in (off, text):
            name = dependency.__name__
            if name not in previous_registry and name in cloudpickle.list_registry_pickle_by_value():
                cloudpickle.unregister_pickle_by_value(dependency)
        for key, value in old.items():
            if value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, value)
