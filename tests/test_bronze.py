"""Kiểm khai báo LDP và biến đổi Spark; reader local không mô phỏng checkpoint/rescue Auto Loader."""

from __future__ import annotations

import csv
import datetime as dt
import json
import shutil
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest
from py4j.protocol import Py4JJavaError
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]
CSV_NAMES = ("orders", "order_products_prior", "order_products_train", "products", "aisles", "departments")
CDC_NAMES = ("product_prices_cdc", "promotions_cdc")


class LocalReader:
    """Ghi nhận plan cloudFiles, dùng batch reader Spark thật để kiểm parsing và metadata."""

    def __init__(self, spark, plans):
        self.spark = spark
        self.plans = plans
        self.options = {}
        self.ddl = None

    def format(self, fmt):
        assert fmt == "cloudFiles"
        return self

    def option(self, key, value):
        self.options[key] = value
        return self

    def schema(self, ddl):
        self.ddl = ddl
        return self

    def load(self, path):
        self.plans.append({"path": path, "options": self.options.copy(), "schema": self.ddl})
        fmt = self.options["cloudFiles.format"]
        if fmt in ("json", "binaryFile"):
            reader = self.spark.read.format(fmt)
            for key, value in self.options.items():
                if not key.startswith("cloudFiles.") and key != "rescuedDataColumn":
                    reader = reader.option(key, value)
            result = reader.load(path)
            if fmt == "json":
                result = result.withColumn("_rescued_data", F.lit(None).cast("string"))
            return result
        if self.options["cloudFiles.format"] == "parquet":
            # Parquet tự mang schema; reader batch dùng đúng các tùy chọn không thuộc cloudFiles.
            reader = self.spark.read.format("parquet")
            for key, value in self.options.items():
                if not key.startswith("cloudFiles."):
                    reader = reader.option(key, value)
            return reader.load(path)
        assert self.options["cloudFiles.format"] == "csv"
        reader = self.spark.read.format("csv")
        for key, value in self.options.items():
            if not key.startswith("cloudFiles.") and key != "rescuedDataColumn":
                reader = reader.option(key, value)
        # CSV native không hỗ trợ rescue. Kiểm parsing bằng schema nghiệp vụ.
        native_schema = self.ddl.removesuffix(", _rescued_data STRING")
        return reader.schema(native_schema).load(path).withColumn("_rescued_data", F.lit(None).cast("string"))


@pytest.fixture
def bronze(spark, monkeypatch, tmp_path, request):
    definitions = {}

    def table(**kwargs):
        def decorate(function):
            assert kwargs["name"] not in definitions
            definitions[kwargs["name"]] = (function, kwargs)
            return function

        return decorate

    monkeypatch.setitem(sys.modules, "pyspark.pipelines", SimpleNamespace(table=table))
    settings = {"catalog": "test_catalog", "bronze": "qa-de_bronze", "landing_root": f"{tmp_path}/"}
    if getattr(request, "param", None):
        settings.update(enable_off="true", off_root=f"{tmp_path}/dev/alice/off_api")
    previous = {key: spark.conf.get(key, None) for key in settings}
    try:
        for key, value in settings.items():
            spark.conf.set(key, value)
        module = ModuleType("bronze_under_test")
        source = ROOT / "src/pipelines/batch/bronze.py"
        exec(compile(source.read_text(), str(source), "exec"), module.__dict__)
        plans = []

        class LocalSpark:
            @property
            def readStream(self):
                return LocalReader(spark, plans)

        module.spark = LocalSpark()
        yield SimpleNamespace(module=module, definitions=definitions, plans=plans, landing=tmp_path)
    finally:
        for key, value in previous.items():
            if value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, value)


def dataset(bronze, name):
    return bronze.definitions[f"`test_catalog`.`qa-de_bronze`.`{name}`"][0]()


def test_registers_csv_and_cdc_tables_with_full_refresh_protection(bronze):
    assert set(bronze.definitions) == {f"`test_catalog`.`qa-de_bronze`.`{name}`" for name in CSV_NAMES + CDC_NAMES}
    for _, definition in bronze.definitions.values():
        assert definition["table_properties"]["pipelines.reset.allowed"] == "false"
        assert definition["table_properties"]["quality"] == "bronze"
    # Đăng ký không đọc landing hoặc thực hiện Spark action.
    assert bronze.plans == []


@pytest.mark.parametrize("name", CSV_NAMES)
def test_csv_fixture_preserves_rows_types_and_metadata(bronze, name):
    folder = {"order_products_prior": "order_products__prior", "order_products_train": "order_products__train"}.get(
        name, name
    )
    source = ROOT / "tests/fixtures/instacart" / f"{folder}.csv"
    target = bronze.landing / "instacart" / folder / "release=2017" / source.name
    target.parent.mkdir(parents=True)
    shutil.copyfile(source, target)
    with source.open(newline="") as handle:
        expected = list(csv.DictReader(handle))

    output = dataset(bronze, name)
    actual = output.collect()
    assert len(actual) == len(expected)
    assert output.schema["_file_mtime"].dataType.simpleString() == "timestamp"
    assert output.schema["_ingest_ts"].dataType.simpleString() == "timestamp"
    assert all(row._source_file.endswith(f"/release=2017/{source.name}") for row in actual)
    assert all(row._file_mtime is not None and row._ingest_ts is not None for row in actual)
    assert "release" not in output.columns

    if name == "products":
        rows = {row.product_id: row for row in actual}
        assert rows[2].product_name == "Tea, Green"
        assert rows[4].product_name == 'Cookies "Classic"'
        assert isinstance(rows[2].aisle_id, int)
    elif name == "orders":
        assert {row.eval_set for row in actual} == {"prior", "train", "test"}
        assert any(row.days_since_prior_order is None for row in actual)
        assert output.schema["days_since_prior_order"].dataType.simpleString() == "double"

    plan = bronze.plans[-1]
    assert plan["options"]["cloudFiles.schemaEvolutionMode"] == "rescue"
    assert plan["schema"].endswith(", _rescued_data STRING")
    assert plan["options"]["mode"] == "FAILFAST"
    assert plan["path"] == str(target.parent.parent) + "/"


def test_malformed_csv_fails_instead_of_being_silently_skipped(bronze):
    root = bronze.landing / "instacart/products/release=2017"
    root.mkdir(parents=True)
    (root / "products.csv").write_text("product_id,product_name,aisle_id,department_id\n1,Tea\n")
    with pytest.raises(Py4JJavaError, match="(?i)malformed|failfast"):
        dataset(bronze, "products").collect()


def test_all_readers_use_immutable_landing_and_leave_state_to_ldp(bronze, monkeypatch, spark):
    # Không đọc file trong test này; vẫn để Spark dựng biểu thức metadata thật.
    frame = spark.createDataFrame(
        [("file:/landing/x", dt.datetime(2026, 3, 1))],
        "file_path STRING, file_modification_time TIMESTAMP",
    ).select(F.struct("*").alias("_metadata"))

    def load(reader, path):
        reader.plans.append({"path": path, "options": reader.options.copy(), "schema": reader.ddl})
        return frame

    monkeypatch.setattr(LocalReader, "load", load)
    for function, _ in bronze.definitions.values():
        function()
    assert len(bronze.plans) == len(CSV_NAMES) + len(CDC_NAMES)
    for plan in bronze.plans:
        options = plan["options"]
        assert options["cloudFiles.allowOverwrites"] == "false"
        assert options["cloudFiles.includeExistingFiles"] == "true"
        assert options["cloudFiles.partitionColumns"] == ""
        assert options["recursiveFileLookup"] == "true"
        assert not {"cloudFiles.schemaLocation", "checkpointLocation", "badRecordsPath"}.intersection(options)
        assert "_staging" not in plan["path"]


CDC_COLUMNS = (
    "_lsn STRING, _seqval STRING, _op INT, `__$start_lsn` BINARY, `__$seqval` BINARY, "
    "`__$operation` INT, `__$update_mask` BINARY"
)


def write_parquet(spark, path, rows, schema):
    spark.createDataFrame(rows, schema).coalesce(1).write.parquet(str(path))


def test_cdc_reads_snapshot_and_windows_without_path_columns(bronze, spark):
    """Snapshot (cột __$ NULL) và window (cột __$ có giá trị) gộp chung một schema; bronze không lọc gì."""
    schema = (
        f"{CDC_COLUMNS}, promo_id STRING, product_id INT, promo_type STRING, discount_pct INT, updated_at TIMESTAMP"
    )
    root = bronze.landing / "retail_db/promotions"
    ts = dt.datetime(2026, 3, 1)
    write_parquet(
        spark,
        root / "snapshot=0x0000000000000000000A",
        [("0x0000000000000000000A", "0x00000000000000000000", 0, None, None, None, None, "P1", 1, "bogo", 0, ts)],
        schema,
    )
    write_parquet(
        spark,
        root / "window=0x0000000000000000000B_0x0000000000000000000C",
        [
            (
                "0x0000000000000000000C",
                "0x0000000000000000000C",
                op,
                b"\x0c",
                b"\x0c",
                op,
                b"\x01",
                "P1",
                1,
                "bogo",
                0,
                ts,
            )
            for op in (1, 2, 4)
        ],
        schema,
    )
    # File phụ của Spark/ADF ngoài *.parquet không được đọc.
    (root / "snapshot=0x0000000000000000000A/_SUCCESS").write_text("")
    staging = bronze.landing / "_staging/retail_db/promotions/window=0x1_0x2"
    write_parquet(spark, staging, [("x", "x", 2, None, None, None, None, "P9", 9, "bogo", 0, ts)], schema)

    output = dataset(bronze, "promotions_cdc")
    rows = output.collect()
    assert sorted(row._op for row in rows) == [0, 1, 2, 4]
    assert {row.promo_id for row in rows} == {"P1"}
    assert not {"snapshot", "window"}.intersection(output.columns)
    assert {"__$start_lsn", "__$seqval", "__$operation", "__$update_mask"}.issubset(output.columns)
    assert all(row._source_file and "/retail_db/promotions/" in row._source_file for row in rows)
    plan = bronze.plans[-1]
    assert plan["path"] == f"{bronze.landing}/retail_db/promotions/"
    assert plan["options"]["cloudFiles.format"] == "parquet"
    assert plan["options"]["cloudFiles.schemaEvolutionMode"] == "addNewColumns"
    assert plan["options"]["pathGlobFilter"] == "*.parquet"
    assert plan["schema"] is None


@pytest.mark.parametrize("bronze", ["personal_off"], indirect=True)
def test_off_keeps_nested_json_and_crawler_fields_and_ignores_other_extensions(bronze):
    root = bronze.landing / "dev/alice/off_api/products/batch=fixture"
    root.mkdir(parents=True)
    record = {
        "code": "0012345",
        "_product_id": 7,
        "_search_query": "Green Tea",
        "nova_group": 1,
        "nutriments": {"sugars_100g": "0.2", "energy-kcal_100g": "10"},
        "countries_tags": ["en:united-states"],
        "extra": {"nested": "giữ nguyên"},
    }
    (root / "batch_1.json").write_text(json.dumps(record) + "\n" + json.dumps(record) + "\n")
    (root / "ignore.txt").write_text("không phải JSON")
    frame = dataset(bronze, "off_products")
    actual = frame.collect()
    assert len(actual) == 2  # Bronze giữ bản sao, không khử trùng theo mã vạch.
    assert actual[0].code == "0012345"
    assert actual[0]._product_id == 7 and actual[0]._search_query == "Green Tea"
    assert actual[0].extra.nested == "giữ nguyên"
    assert actual[0].countries_tags == ["en:united-states"]
    options = bronze.plans[-1]["options"]
    assert options["cloudFiles.schemaEvolutionMode"] == "addNewColumns"
    assert options["cloudFiles.inferColumnTypes"] == "true"
    assert "code STRING" in options["cloudFiles.schemaHints"]
    assert "nutriments MAP<STRING,STRING>" in options["cloudFiles.schemaHints"]


@pytest.mark.parametrize("bronze", ["personal_off"], indirect=True)
def test_images_publish_metadata_only_and_preserve_barcode_zero_prefix(bronze):
    root = bronze.landing / "dev/alice/off_api/images"
    root.mkdir(parents=True)
    (root / "0012345.jpg").write_bytes(b"\xff\xd8\xff\xd9")
    (root / "ignore.png").write_bytes(b"not ingested")
    frame = dataset(bronze, "product_images")
    actual = frame.collect()
    assert "content" not in frame.columns
    assert len(actual) == 1 and actual[0].off_code == "0012345" and actual[0].length == 4
    assert actual[0].path == actual[0]._source_file
    assert actual[0].modificationTime == actual[0]._file_mtime
    assert actual[0]._ingest_ts is not None
    assert "rescuedDataColumn" not in bronze.plans[-1]["options"]


@pytest.mark.parametrize("bronze", ["personal_off"], indirect=True)
def test_off_configuration_routes_only_to_personal_landing(bronze):
    personal = bronze.landing / "dev/alice/off_api/products"
    personal.mkdir(parents=True)
    (personal / "batch_1.json").write_text('{"code":"00123"}\n')
    shared = bronze.landing / "off_api/products"
    shared.mkdir(parents=True)
    (shared / "batch_wrong.json").write_text('{"code":"99999"}\n')
    actual = dataset(bronze, "off_products").collect()
    assert len(actual) == 1 and actual[0].code == "00123"
    assert bronze.plans[-1]["path"] == str(personal) + "/"


@pytest.mark.parametrize("bronze", ["personal_off"], indirect=True)
def test_off_opt_in_preserves_existing_csv_cdc_and_reset_protection(bronze):
    expected = (*CSV_NAMES, *CDC_NAMES, "off_products", "product_images")
    assert set(bronze.definitions) == {f"`test_catalog`.`qa-de_bronze`.`{name}`" for name in expected}
    assert all(
        definition["table_properties"]["pipelines.reset.allowed"] == "false"
        for _, definition in bronze.definitions.values()
    )
    assert not bronze.plans
