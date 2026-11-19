"""Kiểm join thật trên Spark; không mô phỏng nghiệm thu MV trên workspace."""

from __future__ import annotations

import datetime as dt
import json
import sys
from decimal import Decimal
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest
from pyspark.sql import functions as F

from common.catalog_rows import VALID_CATALOG_PRODUCT_ID
from common.off import assess_off, clean_off
from common.product import OFF_ATTRIBUTES, as_of_date, build_dim_product

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def sources(spark):
    price_rows = [
        (1, Decimal("9.99"), "USD", dt.datetime(2026, 1, 1), dt.datetime(2026, 3, 1)),
        (1, Decimal("3.40"), "USD", dt.datetime(2026, 3, 1), None),
        (2, Decimal("4.25"), "USD", dt.datetime(2026, 4, 5), None),
        (3, Decimal("1.00"), "USD", dt.datetime(2026, 2, 1), None),
        (4, Decimal("8.00"), "USD", dt.datetime(2026, 1, 1), dt.datetime(2026, 3, 25)),
    ]
    promo_rows = [
        (1, 10, dt.date(2026, 3, 1), dt.date(2026, 4, 1)),
        (1, 20, dt.date(2026, 3, 1), dt.date(2026, 4, 1)),
        (1, 80, dt.date(2026, 2, 1), dt.date(2026, 3, 29)),
        (1, 99, dt.date(2026, 3, 31), dt.date(2026, 4, 1)),
        (2, 25, dt.date(2026, 3, 30), dt.date(2026, 4, 1)),
        (3, 0, dt.date(2026, 3, 1), dt.date(2026, 3, 30)),
    ]
    records = [
        {
            "_product_id": i,
            "code": code,
            "product_name": name,
            "brands": brand,
            "categories_tags": [f"en:category-{i}"],
            "countries_tags": ["en:united-states"],
            "nova_group": 1,
            "nutriscore_grade": "a",
            "nutriments": {"sugars_100g": str(i)},
            "image_url": f"https://images.openfoodfacts.org/{code}.jpg",
            "_ingest_ts": "2026-03-01T00:00:00Z",
        }
        for i, code, name, brand in [
            (1, "001", "Name A", "Brand A"),
            (2, "001", "Name B", "Brand B"),
            (3, "003", "Name C", "Brand C"),
            (4, "004", "Name D", "Brand D"),
        ]
    ]
    raw = spark.read.json(spark.sparkContext.parallelize([json.dumps(r) for r in records], 1))
    return {
        "products": spark.createDataFrame(
            [
                (1, "P1", 1, 1),
                (1, "P1", 1, 1),
                (2, "P2", 1, 1),
                (3, "P3", 2, 1),
                (4, "P4", 2, 1),
                (5, "P5", 2, 1),
                (6, "P6", 99, 99),
            ],
            "product_id INT, product_name STRING, aisle_id INT, department_id INT",
        ),
        "aisles": spark.createDataFrame([(1, "A1"), (1, "A1"), (2, "A2")], "aisle_id INT, aisle STRING"),
        "departments": spark.createDataFrame([(1, "D1"), (1, "D1")], "department_id INT, department STRING"),
        "prices": spark.createDataFrame(
            price_rows,
            "product_id INT, unit_price DECIMAL(10,2), currency STRING, __START_AT TIMESTAMP, __END_AT TIMESTAMP",
        ),
        "promotions": spark.createDataFrame(
            promo_rows, "product_id INT, discount_pct INT, start_date DATE, end_date DATE"
        ),
        "matches": spark.createDataFrame(
            [
                (1, "001", 100.0, "matched"),
                (2, "001", 92.0, "matched"),
                (3, "003", 77.0, "low_confidence"),
                (4, "004", 52.0, "no_match"),
            ],
            "product_id INT, off_code STRING, off_match_score DOUBLE, off_match_status STRING",
        ),
        "off": clean_off(assess_off(raw)),
        "images": spark.createDataFrame(
            [
                (code, path, dt.datetime(2026, 3, mtime), dt.datetime(2026, 3, ingest))
                for code, path, mtime, ingest in [
                    ("001", "/old.jpg", 1, 3),
                    ("001", "/z.jpg", 2, 2),
                    ("001", "/a.jpg", 2, 2),
                    ("003", "/untrusted.jpg", 2, 2),
                    ("004", "/wrong.jpg", 2, 2),
                ]
            ],
            "off_code STRING, path STRING, modificationTime TIMESTAMP, _ingest_ts TIMESTAMP",
        ),
    }


def build(sources, date="2026-03-30"):
    base = {name: sources[name] for name in ("products", "aisles", "departments", "prices", "promotions")}
    off_sources = (sources["matches"], sources["off"], sources["images"])
    return build_dim_product(**base, date=date, off_sources=off_sources)


def test_dimension_keeps_products_and_aggregates_active_promotions(sources):
    output = build(sources)
    rows = {r.product_id: r for r in output.collect()}
    assert len(rows) == output.count() == 6
    assert all(r.dup_cnt == 1 and r._join_sources_unique for r in rows.values())
    assert (rows[1].aisle, rows[1].department) == ("A1", "D1")
    assert rows[6].aisle is None and rows[6].department is None
    assert rows[1].discount_pct == 20 and rows[1].on_promotion
    assert rows[2].discount_pct == 25 and rows[2].on_promotion
    assert rows[3].discount_pct == 0 and rows[3].on_promotion
    assert rows[4].discount_pct is None and not rows[4].on_promotion


def test_current_price_uses_open_scd2_row_not_max_or_promotion_date(sources):
    output = build(sources)
    rows = {r.product_id: r for r in output.collect()}
    assert rows[1].current_price == Decimal("3.40") and rows[1].currency == "USD"
    # as_of_date chỉ áp dụng KM; giá hiện hành được xác định bằng __END_AT NULL.
    assert rows[2].current_price == Decimal("4.25")
    assert rows[4].current_price is None and rows[5].current_price is None
    assert output.schema["current_price"].dataType.simpleString() == "decimal(10,2)"
    assert output.where("current_price IS NOT NULL").count() == 3


def test_off_joins_pair_key_and_never_enriches_low_confidence_or_no_match(sources):
    rows = {r.product_id: r for r in build(sources).collect()}
    assert rows[1].brand == "Brand A" and rows[2].brand == "Brand B"
    assert rows[1].off_product_name == "Name A" and rows[2].off_product_name == "Name B"
    assert rows[1].off_categories == ["en:category-1"] and rows[2].sugars_100g == 2.0
    assert rows[1].image_path == rows[2].image_path == "/a.jpg"
    for pid in [3, 4, 5, 6]:
        for field in [
            "off_code",
            "off_product_name",
            "brand",
            "sugars_100g",
            "off_categories",
            "image_path",
            "image_url",
        ]:
            assert rows[pid][field] is None
    assert (rows[3].off_match_score, rows[3].off_match_status) == (77, "low_confidence")
    assert (rows[4].off_match_score, rows[4].off_match_status) == (52, "no_match")
    assert (rows[5].off_match_score, rows[5].off_match_status) == (0, "no_match")


def test_configured_date_changes_promotions_without_changing_grain(sources):
    rows = {r.product_id: r for r in build(sources, "2026-03-31").collect()}
    assert len(rows) == 6 and rows[1].discount_pct == 99
    assert rows[3].discount_pct is None and not rows[3].on_promotion


@pytest.mark.parametrize("value", ["20260330", "2026-02-30", "2026-03-30T00:00:00", "", "2026-3-30"])
def test_invalid_configured_date_is_rejected(value):
    with pytest.raises(ValueError):
        as_of_date(value)


def test_replay_and_repartition_do_not_change_dimension(sources):
    before = build(sources).orderBy("product_id").collect()
    replay = {
        key: value.unionByName(value).repartition(2) if key not in ["prices"] else value
        for key, value in sources.items()
    }
    assert build(replay).orderBy("product_id").collect() == before


@pytest.mark.parametrize("source", ["products", "aisles", "departments", "prices", "matches", "off"])
def test_conflicting_source_rows_are_flagged_without_publishing_arbitrary_enrichment(sources, source):
    data = sources.copy()
    change = {
        "products": ("product_name", F.lit("Conflict")),
        "aisles": ("aisle", F.lit("Conflict")),
        "departments": ("department", F.lit("Conflict")),
        "prices": ("unit_price", F.lit(99).cast("decimal(10,2)")),
        "matches": ("off_match_score", F.lit(99.0)),
        "off": ("brand", F.lit("Conflict")),
    }
    column, value = change[source]
    extra = data[source].where("product_id = 1") if source not in ["aisles", "departments"] else data[source].limit(1)
    data[source] = data[source].unionByName(extra.withColumn(column, value))
    output = build(data)
    guard = "_catalog_sources_unique" if source in ("products", "aisles", "departments") else "_join_sources_unique"
    assert output.where(f"NOT {guard}").count() > 0
    assert output.where("dup_cnt <> 1").count() == 0


def test_empty_optional_sources_still_keep_whole_catalog(sources):
    data = {
        key: value if key in ["products", "aisles", "departments"] else value.limit(0) for key, value in sources.items()
    }
    rows = build(data).collect()
    assert len(rows) == 6
    assert all(r.current_price is None and not r.on_promotion and r.off_match_status == "no_match" for r in rows)


@pytest.mark.parametrize("enable_off", ["false", "true"])
def test_notebook_reads_configured_sources_and_wires_constraints(sources, spark, monkeypatch, enable_off):
    definitions, rules, warnings, reads = {}, {}, {}, []

    def mv(**options):
        def decorate(fn):
            definitions[options["name"]] = (fn, options)
            return fn

        return decorate

    def expect(name, predicate):
        warnings[name] = predicate
        return lambda fn: fn

    def fail(name, predicate):
        rules[name] = predicate
        return lambda fn: fn

    monkeypatch.setitem(
        sys.modules,
        "pyspark.pipelines",
        SimpleNamespace(
            materialized_view=mv,
            temporary_view=mv,
            expect=expect,
            expect_all_or_fail=lambda values: lambda fn: fn,
            expect_or_fail=fail,
        ),
    )
    monkeypatch.setattr("pyspark.cloudpickle.register_pickle_by_value", lambda _: None)
    settings = {
        "catalog": "qa-catalog",
        "bronze": "p2_bronze",
        "silver": "p2_silver",
        "as_of_date": "2026-03-31",
        "enable_off": enable_off,
    }
    old = {key: spark.conf.get(key, None) for key in settings}
    try:
        for key, value in settings.items():
            spark.conf.set(key, value)
        module = ModuleType("dimension_under_test")
        path = ROOT / "src/pipelines/batch/silver_off.py"
        exec(compile(path.read_text(), str(path), "exec"), module.__dict__)
        mapping = {
            module.table_name("p2_bronze", "products"): sources["products"],
            module.table_name("p2_bronze", "aisles"): sources["aisles"],
            module.table_name("p2_bronze", "departments"): sources["departments"],
            module.table_name("p2_silver", "product_prices_history"): sources["prices"],
            module.table_name("p2_silver", "promotions"): sources["promotions"],
        }
        if enable_off == "true":
            mapping.update(
                {
                    module.table_name("p2_silver", "product_off_match"): sources["matches"],
                    module.table_name("p2_silver", "off_products_clean"): sources["off"],
                    module.table_name("p2_bronze", "product_images"): sources["images"],
                }
            )

        def read(name):
            reads.append(name)
            if name == "catalog_products_assessed":
                return definitions[name][0]()
            return mapping[name]

        module.spark = SimpleNamespace(read=SimpleNamespace(table=read), conf=spark.conf)
        fn, options = definitions[module.table_name("p2_silver", "dim_product")]
        output = fn()
        assert set(reads) == {*mapping, "catalog_products_assessed"} and len(reads) == len(mapping) + 1
        assert options["cluster_by"] == ["product_id"]
        assert warnings["has_price"] == "current_price IS NOT NULL"
        assert warnings["catalog_payload_unique"] == "_catalog_sources_unique"
        assert warnings["catalog_unrescued"] == "_catalog_sources_unrescued"
        assert warnings["valid_catalog_product_id"] == VALID_CATALOG_PRODUCT_ID
        assert rules == {
            "no_fanout": "dup_cnt = 1",
            "unique_sources": "_join_sources_unique",
            "valid_product_id": "product_id IS NOT NULL AND product_id > 0",
        }
        row = output.where("product_id = 1").first()
        assert row.discount_pct == 99 and row.current_price == Decimal("3.40")
        if enable_off == "false":
            assert output.dtypes == build(sources).dtypes
            assert output.count() == 6
            assert output.where("off_match_status = 'no_match' AND off_match_score = 0").count() == 6
            attributes = ("off_code", *OFF_ATTRIBUTES, "image_path")
            assert output.where(" OR ".join(f"{name} IS NOT NULL" for name in attributes)).count() == 0
        else:
            assert row.off_match_status == "matched" and row.image_path == "/a.jpg"
        assert output.where(" OR ".join(f"NOT ({p})" for p in rules.values())).count() == 0
    finally:
        for key, value in old.items():
            if value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, value)


def test_catalog_uses_latest_valid_payload_and_preserves_conflict_status(sources):
    data = sources.copy()
    products = sources["products"].sparkSession.createDataFrame(
        [
            ("1", "Old", 1, 1, 1, None),
            ("1", "Updated", 2, 1, 2, None),
            ("1", "Rescued", 2, 1, 3, "bad"),
            ("2", "Fallback old", 1, 1, 1, "bad"),
            ("2", "Fallback", 1, 1, 2, "bad"),
            ("invalid", "Invalid key", 1, 1, 1, None),
            (None, "Missing key", 1, 1, 1, None),
            ("0", "Non-positive key", 1, 1, 1, None),
        ],
        "product_id STRING, product_name STRING, aisle_id INT, department_id INT, _ingest_ts INT, _rescued_data STRING",
    )
    data["products"] = products
    rows = build(data).orderBy("product_id").collect()
    assert [row.product_id for row in rows] == [1, 2]
    assert [(row.product_name, row.aisle_id, row._catalog_sources_unrescued) for row in rows] == [
        ("Updated", 2, True),
        ("Fallback", 1, False),
    ]
    assert all(not row._catalog_sources_unique and row._join_sources_unique for row in rows)
    assert products.where(f"NOT ({VALID_CATALOG_PRODUCT_ID})").count() == 3
