"""Chạy ba file ds-prepare bằng Spark + Delta local, với schema riêng cho mỗi fixture."""

import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from tasks.ds_prepare import run_sql
from tasks.ds_prepare.run_sql import PREPARE_FILES, run_files, split_sql, sql_args


def test_runner_resolves_sql_dir_when_workspace_exec_has_no_file():
    task_path = Path(run_sql.__file__)
    env = {"__name__": "spark_workspace_task"}
    exec(compile(task_path.read_bytes(), str(task_path), "exec"), env)
    assert env["SQL_DIR"] == task_path.parent.resolve()


def test_split_preserves_literals_identifiers_and_nested_comments():
    sql = r"""
-- Mở đầu ; ' chưa đóng trong comment
SELECT 'a;''b', 'c\';d', r'e\;', "f;g", `h;``i`;
/* Outer ; /* Inner ; */ ; */ SELECT :v;
; -- Cuối file chỉ có comment ;
"""
    queries = split_sql(sql)
    assert len(queries) == 2
    assert queries[0].endswith(r"""SELECT 'a;''b', 'c\';d', r'e\;', "f;g", `h;``i`""")
    assert queries[1].endswith("SELECT :v")


@pytest.mark.parametrize("sql", ["SELECT 1; SELECT 'open", "SELECT 1; /* open", "SELECT `open"])
def test_split_rejects_unclosed_input(sql):
    with pytest.raises(ValueError, match="chưa đóng"):
        split_sql(sql)


def test_sql_parameters_do_not_mix_input_and_output_namespaces():
    args = sql_args("v1-local", catalog="spark_catalog", silver_schema="src`dev", ds_schema="dst-dev")
    assert args["v"] == "v1-local"
    assert args["source_baskets"] == "`spark_catalog`.`src``dev`.`baskets`"
    assert args["baskets"] == "`spark_catalog`.`dst-dev`.`baskets`"
    with pytest.raises(ValueError, match="data_version"):
        sql_args("v1'; delete")
    with pytest.raises(ValueError, match="tách khỏi"):
        sql_args("v1", silver_schema="Silver", ds_schema="silver")


def test_replace_where_checks_predicate_before_execution(monkeypatch, tmp_path):
    monkeypatch.setattr(run_sql, "SQL_DIR", tmp_path)
    script = tmp_path / "replace.sql"
    args = sql_args("v1")
    executed = []

    def sql(statement):
        executed.append(statement)
        return SimpleNamespace(collect=list)

    spark = SimpleNamespace(sql=sql)
    script.write_text("INSERT INTO IDENTIFIER(:other) REPLACE WHERE data_version = :v SELECT :v;")
    with pytest.raises(RuntimeError, match=r"replace.sql, câu 1.*REPLACE WHERE"):
        run_files(spark, args, (script.name,))
    assert not executed

    script.write_text("insert into identifier(:item_index)\nreplace where data_version=:v SELECT :v;")
    run_files(spark, args, (script.name,))
    assert executed == [f"insert into {args['item_index']}\nreplace where data_version='v1' SELECT 'v1'"]


def test_render_keeps_markers_in_comments_and_strings_and_checks_qualified_names():
    args = sql_args("v1", silver_schema="source`dev", ds_schema="ds-dev")
    statement = """-- :v
/* IDENTIFIER(:missing) /* :v */ */
SELECT 'IDENTIFIER(:baskets)', :v, :source_baskets FROM IDENTIFIER(:baskets)
"""
    rendered = run_sql._render_sql(statement, args)
    assert rendered.startswith("-- :v\n/* IDENTIFIER(:missing) /* :v */ */")
    assert "SELECT 'IDENTIFIER(:baskets)', 'v1', CAST(unhex(" in rendered
    assert rendered.rstrip().endswith("FROM `instacart`.`ds-dev`.`baskets`")
    with pytest.raises(ValueError, match="Tên bảng SQL không hợp lệ"):
        run_sql._render_sql("SELECT * FROM IDENTIFIER(:baskets)", args | {"baskets": "invalid marker"})


@pytest.fixture
def ds(spark):
    prefix = "test_ds_" + uuid.uuid4().hex[:12]
    source, target = prefix + "_silver", prefix + "_data"
    spark.sql(f"CREATE DATABASE {source}")
    spark.sql(f"CREATE DATABASE {target}")

    def save(name, rows, schema):
        spark.createDataFrame(rows, schema).write.mode("overwrite").saveAsTable(f"{source}.{name}")

    basket_schema = "order_id long, user_id long, split string, items array<long>, n_items int"
    orders = [
        (101, 1, "prior", [10, 2], 2),
        (102, 2, "prior", [2, 100], 2),
        (103, 3, "prior", [10, 100], 2),
        (104, 4, "prior", [10], 1),
        (201, 5, "holdout", [2, 10, 50, 100], 4),
        (202, 6, "holdout", [50], 1),
        (203, 7, "holdout", [2, 10], 2),
    ]
    prod_schema = (
        "product_id long, product_name string, aisle_id long, aisle string, department_id long, department string"
    )
    products = [
        (2, "Orange", 1, "fruit", 1, "produce"),
        (10, "Apple", 1, "fruit", 1, "produce"),
        (100, "Tea", 2, "tea", 2, "drinks"),
        (50, "Lime", 3, "citrus", 1, "produce"),
    ]
    lines_schema = "order_id long, product_id long, add_to_cart_order long, source string"
    lines = [
        (201, 10, 4, "stream"),
        (201, 2, 3, "stream"),
        (201, 100, 2, "stream"),
        (201, 50, 1, "stream"),
        (202, 50, 1, "stream"),
        (203, 10, 2, "stream"),
        (203, 2, 1, "stream"),
        (201, 99, 1, "batch"),  # Món batch không được dùng để dựng holdout.
    ]
    try:
        save("baskets", orders, basket_schema)
        save("dim_product", products, prod_schema)
        save("order_items", lines, lines_schema)
        yield source, target, save, (orders, basket_schema), (products, prod_schema), (lines, lines_schema)
    finally:
        spark.sql(f"DROP DATABASE {target} CASCADE")
        spark.sql(f"DROP DATABASE {source} CASCADE")


def test_three_sql_files_freeze_version_and_rebuild_deterministically(spark, ds):
    source, target, save, (orders, basket_schema), (products, product_schema), (lines, lines_schema) = ds
    args = sql_args("v1", catalog="spark_catalog", silver_schema=source, ds_schema=target)
    run_files(spark, args)

    def query(table, where="TRUE", order="1", version="v1"):
        return spark.sql(
            f"SELECT * FROM {target}.{table} WHERE data_version = :v AND ({where}) ORDER BY {order}",
            args={"v": version},
        ).collect()

    meta = query("data_versions")[0]
    assert (meta.n_prior, meta.n_holdout, meta.source) == (4, 3, args["source_baskets"])
    index = query("item_index", "grain = 'product'", "idx")
    assert [(r.item_key, r.idx, r.freq_prior) for r in index] == [("10", 0, 3), ("2", 1, 2), ("100", 2, 2)]
    assert [(r.item_key, r.freq_prior) for r in query("item_index", "grain = 'aisle'", "idx")] == [("1", 4), ("2", 2)]
    tx = {r.grain: r for r in query("transactions", "order_id = 101")}
    assert {key: r.items for key, r in tx.items()} == {"product": [0, 1], "aisle": [0], "department": [0]}
    expected_bucket = spark.sql("SELECT pmod(xxhash64(CAST(101 AS INT)), 100)").first()[0]
    assert {r.scale_bucket for r in tx.values()} == {expected_bucket}
    assert not query("transactions", "order_id = 202 AND grain = 'product'")
    assert query("transactions", "order_id = 202 AND grain = 'department'")[0].items == [0]
    eval_rows = query("holdout_eval", order="order_id")
    assert [(r.order_id, r.context, r.targets) for r in eval_rows] == [(201, [1, 2], [0]), (203, [1], [0])]
    for row in eval_rows:
        part = spark.sql("SELECT pmod(xxhash64(CAST(:order AS INT), 'eval'), 2)", args={"order": row.order_id}).first()[
            0
        ]
        assert row.eval_part == ("val" if part == 0 else "test")

    sort_by = {
        "data_versions": "data_version",
        "baskets": "order_id",
        "ds_prepare_products": "product_id",
        "ds_prepare_holdout": "order_id",
        "item_index": "grain, idx",
        "transactions": "grain, order_id",
        "holdout_eval": "order_id",
    }
    v1_before = {table: query(table, order=key) for table, key in sort_by.items()}

    # Bảng live thay đổi sau lần đầu, nhưng v1 dùng ánh xạ và thứ tự đã cố định.
    save("baskets", [o for o in orders if o[0] not in (102, 201, 203)], basket_schema)
    save(
        "dim_product",
        [(pid, f"New {name}", a, aisle, dep, dept) for pid, name, a, aisle, dep, dept in products],
        product_schema,
    )
    save("order_items", [(o, p, 9 - pos, src) for o, p, pos, src in lines], lines_schema)
    run_files(spark, args | {"v": "v2"})
    v2_before = {table: query(table, order=key, version="v2") for table, key in sort_by.items()}
    assert v2_before["data_versions"][0].n_prior == 3
    assert not v2_before["holdout_eval"]

    run_files(spark, args)
    assert v1_before == {table: query(table, order=key) for table, key in sort_by.items()}
    assert v2_before == {table: query(table, order=key, version="v2") for table, key in sort_by.items()}


def test_snapshot_fails_on_unmatched_stream_then_can_repair(spark, ds):
    source, target, save, _, _, (lines, schema) = ds
    params = sql_args("v1", catalog="spark_catalog", silver_schema=source, ds_schema=target)
    save("order_items", [row for row in lines if row[:2] != (201, 100)], schema)
    with pytest.raises(RuntimeError, match="không khớp giỏ holdout"):
        run_files(spark, params, PREPARE_FILES[:1])
    assert spark.table(f"{target}.data_versions").count() == 0
    assert spark.table(f"{target}.ds_prepare_holdout").count() == 0
    save("order_items", lines, schema)
    run_files(spark, params, PREPARE_FILES[:1])
    assert spark.table(f"{target}.data_versions").count() == 1
    assert spark.table(f"{target}.baskets").count() == 7


def test_export_writes_expected_prior_rows_and_manifest(spark, tmp_path):
    from tasks.ds_prepare.export_txt import GRAINS, SCALES, export_transactions

    view = "export_prior_fixture"
    rows = [
        (version, split, grain, bucket, items)
        for grain in GRAINS
        for version, split, bucket, items in [
            ("v1", "prior", 0, [0, 2]),
            ("v1", "prior", 49, [1]),
            ("v1", "prior", 99, [3, 4]),
            ("v1", "holdout", 0, [5]),
            ("v2", "prior", 0, [6]),
        ]
    ]
    schema = "data_version string, split string, grain string, scale_bucket int, items array<int>"
    spark.createDataFrame(rows, schema).createOrReplaceTempView(view)
    try:
        manifest = export_transactions(spark, {"v": "v1", "transactions": view}, tmp_path)
        expected_rows = (["0 2"], ["0 2"], ["0 2", "1"], ["0 2", "1", "3 4"])
        for grain in GRAINS:
            assert manifest[grain] == {"10": 1, "25": 1, "50": 2, "100": 3}
            for scale, expected in zip(SCALES, expected_rows, strict=True):
                files = (tmp_path / "v1" / grain / f"scale={scale}").glob("part-*.txt")
                lines = [line for file in files for line in file.read_text().splitlines()]
                assert sorted(lines) == sorted(expected)
        assert (tmp_path / "v1" / "_manifest.json").is_file()
    finally:
        spark.catalog.dropTempView(view)


def test_check_accepts_complete_version_and_rejects_missing_metadata(spark):
    views = {
        "data_versions": "SELECT 'v1' AS data_version, 2 AS n_prior, 2 AS n_holdout",
        "source_baskets": "SELECT explode(array('prior', 'prior', 'holdout', 'holdout')) AS split",
        "baskets": """
            SELECT 'v1' AS data_version, explode(array('prior', 'prior', 'holdout', 'holdout')) AS split, 1 AS n_items
        """,
        "transactions": """
            SELECT 'v1' AS data_version, 'prior' AS split, id AS order_id, 0 AS scale_bucket,
                   array(0) AS items, 1 AS n_items,
                   explode(array('product', 'aisle', 'department')) AS grain
            FROM range(2)
        """,
        "item_index": """
            SELECT 'v1' AS data_version, 'product' AS grain
            UNION ALL SELECT 'v1', 'department' FROM range(21)
            UNION ALL SELECT 'v1', 'aisle' FROM range(134)
        """,
        "holdout_eval": """
            SELECT 'v1' AS data_version, id AS order_id, CASE WHEN id = 0 THEN 'val' ELSE 'test' END AS eval_part,
                   array(0) AS context, array(1) AS targets
            FROM range(2)
        """,
    }
    args = {"v": "v1", **{key: f"check_{key}" for key in views}}
    try:
        for key, query in views.items():
            spark.sql(f"CREATE OR REPLACE TEMP VIEW {args[key]} AS {query}")
        run_files(spark, args, ("04_check.sql",))
        with pytest.raises(RuntimeError, match="Snapshot thiếu metadata"):
            run_files(spark, args | {"v": "missing"}, ("04_check.sql",))
    finally:
        for key in views:
            spark.catalog.dropTempView(args[key])
