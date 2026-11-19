"""Nạp CSV, CDC và OFF vào bronze. LDP quản lý checkpoint và schema store."""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.active()
CATALOG = spark.conf.get("catalog")
BRONZE_SCHEMA = spark.conf.get("bronze")
LANDING = spark.conf.get("landing_root").rstrip("/")
OFF_ROOT = spark.conf.get("off_root", f"{LANDING}/off_api").rstrip("/")
BRONZE_PROPS = {"pipelines.reset.allowed": "false", "quality": "bronze"}

# Tên bảng UC khác tên file ở hai bảng món hàng; schema chỉ chứa cột nghiệp vụ.
CSV_TABLES = {
    "orders": (
        "orders",
        "order_id INT, user_id INT, eval_set STRING, order_number INT, order_dow INT, "
        "order_hour_of_day INT, days_since_prior_order DOUBLE",
    ),
    "order_products_prior": (
        "order_products__prior",
        "order_id INT, product_id INT, add_to_cart_order INT, reordered INT",
    ),
    "order_products_train": (
        "order_products__train",
        "order_id INT, product_id INT, add_to_cart_order INT, reordered INT",
    ),
    "products": ("products", "product_id INT, product_name STRING, aisle_id INT, department_id INT"),
    "aisles": ("aisles", "aisle_id INT, aisle STRING"),
    "departments": ("departments", "department_id INT, department STRING"),
}

# Bảng nguồn Azure SQL có capture instance dbo_<bảng> (AD-DE-04/07).
CDC_TABLES = ("product_prices", "promotions")


def table_name(table: str) -> str:
    """Quote tên catalog/schema để prefix dev có dấu gạch ngang vẫn hợp lệ."""
    return ".".join("`" + part.replace("`", "``") + "`" for part in (CATALOG, BRONZE_SCHEMA, table))


def landing_stream(fmt: str):
    """Tùy chọn Auto Loader chung: landing bất biến, đọc đệ quy, không suy cột từ tên thư mục."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", fmt)
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.allowOverwrites", "false")
        .option("cloudFiles.partitionColumns", "")
        .option("recursiveFileLookup", "true")
    )


def with_ingest_metadata(frame):
    return frame.select(
        "*",
        F.col("_metadata.file_path").alias("_source_file"),
        F.col("_metadata.file_modification_time").alias("_file_mtime"),
        F.current_timestamp().alias("_ingest_ts"),
    )


def autoload_csv(path: str, schema: str):
    """Dựng truy vấn CSV; LDP quản lý checkpoint và schema store."""
    return with_ingest_metadata(
        landing_stream("csv")
        .option("rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("header", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "FAILFAST")
        .option("pathGlobFilter", "*.csv")
        .schema(f"{schema}, _rescued_data STRING")
        .load(path)
    )


def autoload_cdc(path: str):
    """Parquet CDC mang sẵn schema; giữ nguyên mọi cột ADF ghi, kể cả __$… của hàm CDC."""
    return with_ingest_metadata(
        landing_stream("parquet")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("pathGlobFilter", "*.parquet")
        .load(path)
    )


def make_csv_table(table: str, folder: str, schema: str):
    @dp.table(
        name=table_name(table),
        comment=f"Bronze append-only từ Instacart {folder}; giữ cả prior/train/test ở nguồn.",
        table_properties=BRONZE_PROPS,
    )
    def csv_table():
        return autoload_csv(f"{LANDING}/instacart/{folder}/", schema)


for _table, (_folder, _schema) in CSV_TABLES.items():
    make_csv_table(_table, _folder, _schema)


def make_cdc_table(table: str):
    @dp.table(
        name=table_name(f"{table}_cdc"),
        comment=f"Bronze append-only từ snapshot và cửa sổ CDC của dbo.{table}; giữ _op/_lsn/_seqval, không khử trùng.",
        table_properties=BRONZE_PROPS,
    )
    def cdc_table():
        # Chỉ đọc thư mục đã công bố retail_db/<bảng>/ (snapshot=…, window=…), không đọc _staging.
        return autoload_cdc(f"{LANDING}/retail_db/{table}/")


for _table in CDC_TABLES:
    make_cdc_table(_table)


if spark.conf.get("enable_off", "false").lower() == "true":

    @dp.table(
        name=table_name("off_products"),
        comment="Bronze OFF: một dòng JSONL là một hit, giữ JSON lồng nhau và metadata crawler.",
        table_properties=BRONZE_PROPS,
    )
    def off_products():
        return with_ingest_metadata(
            landing_stream("json")
            .option("rescuedDataColumn", "_rescued_data")
            .option("pathGlobFilter", "*.json")
            .option("mode", "FAILFAST")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaHints", "code STRING, nutriments MAP<STRING,STRING>, nova_group INT")
            .load(f"{OFF_ROOT}/products/")
        )

    @dp.table(
        name=table_name("product_images"),
        comment="Bronze metadata ảnh JPG; không sao chép cột content vào Delta.",
        table_properties=BRONZE_PROPS,
    )
    def product_images():
        return with_ingest_metadata(
            landing_stream("binaryFile").option("pathGlobFilter", "*.jpg").load(f"{OFF_ROOT}/images/")
        ).select(
            F.regexp_extract("path", r"/([^/]+)\.jpg$", 1).alias("off_code"),
            "path",
            "length",
            "modificationTime",
            "_source_file",
            "_file_mtime",
            "_ingest_ts",
        )
