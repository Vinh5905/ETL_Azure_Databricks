"""Chạy SQL đối soát và MERGE Delta thật; nguồn nhỏ tự dựng không thay nghiệm thu cloud."""

from __future__ import annotations

import csv
import uuid
from collections import Counter, defaultdict
from pathlib import Path
from types import SimpleNamespace

import pytest
from pyspark.sql import functions as F

from expected_counts_sql import expected_counts, render

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests/fixtures/instacart"


def manifest():
    files = ["orders", "order_products__prior", "order_products__train", "products", "aisles", "departments"]
    records = {}
    for name in files:
        with (FIXTURE / f"{name}.csv").open(newline="") as handle:
            records[name] = list(csv.DictReader(handle))
    return {
        "rows": {f"{key}.csv": len(rows) for key, rows in records.items()},
        "orders_by_eval_set": dict(Counter(r["eval_set"] for r in records["orders"])),
    }


def test_manifest_renderer_emits_only_aggregates_and_quotes_schema():
    source = {**manifest(), "user_ids": [999999]}
    sql = render("a`b", "qa-de_ops", "sample_1pct", source)
    assert "`a``b`.`qa-de_ops`.`expected_counts`" in sql
    assert "999999" not in sql and "manifest_sha256:" in sql
    assert expected_counts(source)["orders"] == 18


@pytest.mark.parametrize("value", [True, -1, "18"])
def test_manifest_rejects_invalid_counts(value):
    source = manifest()
    source["rows"]["orders.csv"] = value
    with pytest.raises(ValueError):
        expected_counts(source)


def test_manifest_rejects_split_mismatch_and_unsafe_profile():
    source = manifest()
    source["orders_by_eval_set"]["train"] += 1
    with pytest.raises(ValueError, match="Tổng"):
        expected_counts(source)
    with pytest.raises(ValueError, match="Profile"):
        render("spark_catalog", "ops", "bad'profile", manifest())


@pytest.fixture
def recon(spark, request):
    prefix = "rec_" + uuid.uuid4().hex
    schemas = {layer: f"{prefix}_{layer}" for layer in ["ops", "bronze", "silver", "gold"]}
    previous = spark.catalog.currentDatabase()
    for schema in schemas.values():
        spark.sql(f"CREATE DATABASE `{schema}`")
    params = {
        "catalog": "spark_catalog",
        **{f"{k}_schema": v for k, v in schemas.items()},
        "profile": "fixture",
        "run_id": "run-a",
        "check_date": "2026-03-30",
    }

    def script(name):
        sql = (ROOT / "src/tasks" / name).read_text()
        for statement in sql.split(";"):
            if statement.strip():
                spark.sql(statement, args=params).collect()

    def put(layer, name, frame):
        frame.write.mode("overwrite").saveAsTable(f"`{schemas[layer]}`.`{name}`")

    def table(layer, name):
        return spark.table(f"`{schemas[layer]}`.`{name}`")

    def replace(layer, name, transform):
        old = table(layer, name)
        data = spark.createDataFrame(transform(old.collect()), old.schema)
        put(layer, name, data)

    ddl = {
        "orders": (
            "order_id BIGINT,user_id BIGINT,eval_set STRING,order_number INT,order_dow INT,"
            "order_hour_of_day INT,days_since_prior_order DOUBLE"
        ),
        "order_products_prior": "order_id BIGINT,product_id BIGINT,add_to_cart_order INT,reordered INT",
        "order_products_train": "order_id BIGINT,product_id BIGINT,add_to_cart_order INT,reordered INT",
        "products": "product_id BIGINT,product_name STRING,aisle_id INT,department_id INT",
        "aisles": "aisle_id INT,aisle STRING",
        "departments": "department_id INT,department STRING",
    }
    try:
        script("init_reconciliation.sql")
        spark.sql(render("spark_catalog", schemas["ops"], "fixture", manifest())).collect()
        for name, schema in ddl.items():
            filename = name.replace("order_products_", "order_products__")
            source = spark.read.schema(schema).option("header", "true").csv(str(FIXTURE / f"{filename}.csv"))
            put(
                "bronze",
                name,
                source.withColumn("_ingest_ts", F.lit("2026-03-29 00:00:00").cast("timestamp_ltz"))
                .withColumn("_file_mtime", F.col("_ingest_ts"))
                .withColumn("_source_file", F.lit(filename))
                .withColumn("_rescued_data", F.lit(None).cast("string")),
            )
        metadata = ("_ingest_ts", "_file_mtime", "_source_file", "_rescued_data")
        orders = table("bronze", "orders").drop(*metadata)
        prior = table("bronze", "order_products_prior").drop(*metadata)
        train = table("bronze", "order_products_train").drop(*metadata)
        products = table("bronze", "products").drop(*metadata)
        stamp = F.lit("2026-03-30T00:00:00Z").cast("timestamp_ltz")
        batch = (
            orders.where("eval_set <> 'train'")
            .withColumn("source", F.lit("batch"))
            .withColumn("is_test", F.col("eval_set") == "test")
            .withColumn("order_ts_sim", stamp)
        )
        put("silver", "orders_batch", batch)
        put("silver", "order_items_prior", prior)
        put("silver", "order_time_sim", orders.select("order_id").withColumn("order_ts_sim", stamp))
        if getattr(request, "param", "G3") != "batch":
            put(
                "silver",
                "dim_product",
                products.withColumn("dup_cnt", F.lit(1)).withColumn("_join_sources_unique", F.lit(True)),
            )
            put(
                "silver",
                "product_prices_history",
                products.select("product_id").withColumn("__END_AT", F.lit(None).cast("timestamp_ltz")),
            )
            put("silver", "stream_orders", orders.where("eval_set = 'train'").select("order_id", "user_id"))
            put("silver", "stream_order_items", train)
            put(
                "silver",
                "orders",
                orders.withColumn(
                    "source", F.when(F.col("eval_set") == "train", "stream").otherwise("batch")
                ).withColumn("is_test", F.col("eval_set") == "test"),
            )
            put(
                "silver",
                "order_items",
                prior.withColumn("source", F.lit("batch")).unionByName(train.withColumn("source", F.lit("stream"))),
            )
            users = {
                r.order_id: (r.user_id, "holdout" if r.eval_set == "train" else "prior")
                for r in orders.where("eval_set <> 'test'").collect()
            }
            grouped = defaultdict(list)
            for row in prior.unionByName(train).collect():
                grouped[row.order_id].append(row.product_id)
            rows = [
                (oid, *users[oid], sorted(set(items)), len(set(items)), True, True, 1) for oid, items in grouped.items()
            ]
            put(
                "silver",
                "baskets",
                spark.createDataFrame(
                    rows,
                    (
                        "order_id BIGINT,user_id BIGINT,split STRING,items ARRAY<BIGINT>,n_items INT,"
                        "_items_valid BOOLEAN,_source_disjoint BOOLEAN,dup_cnt BIGINT"
                    ),
                ),
            )
        yield SimpleNamespace(script=script, params=params, table=table, put=put, replace=replace, schemas=schemas)
    finally:
        spark.catalog.setCurrentDatabase(previous)
        for schema in reversed(list(schemas.values())):
            spark.sql(f"DROP DATABASE `{schema}` CASCADE")


def checks(recon):
    return {r.check_name: r for r in recon.table("ops", "reconciliation").collect()}


@pytest.mark.parametrize("recon", ["batch"], indirect=True)
def test_batch_stage_runs_twice_without_stream_or_gold_tables(recon):
    recon.script("reconcile.sql")
    first = checks(recon)
    recon.script("reconcile.sql")
    assert len(checks(recon)) == len(first) == 22
    assert all(r.passed and r.stage == "batch" for r in first.values())
    assert recon.table("ops", "expected_counts").where("profile = 'fixture'").count() == 9


def test_g3_g4_stages_require_fresh_prerequisites_and_do_not_add_daily_rows(recon):
    recon.script("reconcile.sql")
    recon.script("reconcile_g3.sql")
    assert len(checks(recon)) == 47 and all(r.passed for r in checks(recon).values())
    recon.put("gold", "fact_order_lines", recon.table("silver", "order_items"))
    recon.script("reconcile_g4.sql")
    recon.script("reconcile_g4.sql")
    assert len(checks(recon)) == 50
    recon.table("silver", "order_items").limit(1).write.mode("append").saveAsTable(
        f"`{recon.schemas['gold']}`.fact_order_lines"
    )
    with pytest.raises(Exception, match="reconciliation failed"):
        recon.script("reconcile_g4.sql")
    result = checks(recon)
    assert not result["g4.fact.no_fanout"].passed
    assert not result["g4.fact.keys"].passed and result["g4.fact.keys"].actual == 1
    recon.params["run_id"] = "run-b"
    with pytest.raises(Exception, match="prerequisite checks are stale"):
        recon.script("reconcile_g3.sql")


def test_replayed_bronze_files_are_not_expected_as_new_business_rows(recon):
    for name in ("orders", "order_products_prior", "order_products_train", "products", "aisles", "departments"):
        original = recon.table("bronze", name)
        replay = original.withColumn("_ingest_ts", F.expr("_ingest_ts + INTERVAL 1 DAY")).withColumn(
            "_source_file", F.concat("_source_file", F.lit("_fix1"))
        )
        replay.write.mode("append").saveAsTable(f"`{recon.schemas['bronze']}`.`{name}`")
    recon.script("reconcile.sql")
    recon.script("reconcile_g3.sql")
    assert len(checks(recon)) == 47 and all(row.passed for row in checks(recon).values())


def test_wrong_expected_count_is_recorded_red_and_repair_reuses_daily_key(recon, spark):
    recon.script("reconcile.sql")
    target = f"`{recon.schemas['ops']}`.expected_counts"
    spark.sql(f"UPDATE {target} SET expected = 7 WHERE profile = 'fixture' AND metric = 'products'")
    with pytest.raises(Exception, match="reconciliation failed"):
        recon.script("reconcile.sql")
    failed = checks(recon)["bronze.products"]
    assert not failed.passed and failed.actual == 4 and failed.expected == 7
    spark.sql(f"UPDATE {target} SET expected = 4 WHERE profile = 'fixture' AND metric = 'products'")
    recon.script("reconcile.sql")
    assert len(checks(recon)) == 22 and all(r.passed for r in checks(recon).values())


def test_matching_counts_with_wrong_payload_still_fail(recon):
    recon.replace(
        "silver",
        "order_items_prior",
        lambda rows: [
            (r.order_id, r.product_id, 99 if i == 0 else r.add_to_cart_order, r.reordered) for i, r in enumerate(rows)
        ],
    )
    with pytest.raises(Exception, match="reconciliation failed"):
        recon.script("reconcile.sql")
    result = checks(recon)
    assert result["silver.prior.rows"].passed and result["silver.prior.keys"].passed
    assert not result["silver.prior.payload"].passed


@pytest.mark.parametrize("change", ["manifest", "namespace"])
def test_profile_changes_cannot_reuse_green_results_from_old_configuration(recon, spark, change):
    recon.script("reconcile.sql")
    if change == "manifest":
        spark.sql(
            f"UPDATE `{recon.schemas['ops']}`.expected_counts SET source = 'new-manifest' WHERE profile = 'fixture'"
        )
    else:
        recon.params["silver_schema"] += "_different"
    with pytest.raises(Exception, match="prerequisite checks are stale"):
        recon.script("reconcile_g3.sql")


@pytest.mark.parametrize("defect", ["open_price", "stream_payload", "basket_items"])
def test_stream_payload_or_open_scd2_fanout_fails_g3(recon, defect):
    recon.script("reconcile.sql")
    if defect == "open_price":
        source = recon.table("silver", "product_prices_history")
        recon.put("silver", "extra_prices", source.limit(1))
        recon.table("silver", "extra_prices").write.mode("append").saveAsTable(
            f"`{recon.schemas['silver']}`.product_prices_history"
        )
        expected_failed = "g3.prices.open_duplicates"
    elif defect == "stream_payload":
        recon.replace(
            "silver",
            "stream_order_items",
            lambda rows: [
                (r.order_id, r.product_id, 99 if i == 0 else r.add_to_cart_order, r.reordered)
                for i, r in enumerate(rows)
            ],
        )
        expected_failed = "g3.stream.payload"
    else:

        def changed(rows):
            values = [list(row) for row in rows]
            values[0][3] = sorted([values[0][3][0] + 1000, *values[0][3][1:]])
            return [tuple(row) for row in values]

        recon.replace("silver", "baskets", changed)
        expected_failed = "g3.baskets.items"
    with pytest.raises(Exception, match="reconciliation failed"):
        recon.script("reconcile_g3.sql")
    assert not checks(recon)[expected_failed].passed
