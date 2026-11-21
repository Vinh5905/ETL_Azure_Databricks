"""Đo ghi, quét, lọc và dung lượng dữ liệu của bốn định dạng trên cùng job cluster."""

from __future__ import annotations

import argparse
from collections.abc import Callable
from time import perf_counter
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

COLUMNS = ("order_id", "product_id", "add_to_cart_order", "reordered")
FORMATS = ("csv", "parquet", "delta", "delta_optimized")
RESULT_SCHEMA = (
    "run_id STRING, repetition INT, format STRING, n_rows LONG, filter_rows LONG, filter_product_id LONG, "
    "write_s DOUBLE, optimize_s DOUBLE, scan_s DOUBLE, filter_s DOUBLE, size_mb DOUBLE, "
    "source_table STRING, node_type_id STRING, runtime STRING, input_partitions INT"
)


def table_name(catalog: str, schema: str, table: str) -> str:
    return ".".join("`" + part.replace("`", "``") + "`" for part in (catalog, schema, table))


def timed(action: Callable):
    start = perf_counter()
    result = action()
    return result, perf_counter() - start


def fingerprint(data: DataFrame) -> tuple[int, int | None]:
    # Ép đọc giá trị cả bốn cột thay vì trả số dòng từ metadata Parquet/Delta.
    row = data.agg(F.count("*"), F.expr(f"bit_xor(xxhash64({', '.join(COLUMNS)}))")).first()
    return tuple(row)


def read_format(spark: SparkSession, storage: str, path: str, schema) -> DataFrame:
    reader = spark.read.format(storage)
    if storage == "csv":
        reader = reader.schema(schema).option("header", "true").option("mode", "FAILFAST")
    return reader.load(path)


def measure_format(spark, fs, data, path, fmt, product_id, expected_scan, expected_filter) -> tuple:
    from delta.tables import DeltaTable

    storage = "delta" if fmt == "delta_optimized" else fmt
    writer = data.write.format(storage).mode("errorifexists")
    if storage == "csv":
        writer = writer.option("header", "true")
    _, write_s = timed(lambda: writer.save(path))
    optimize_s = 0.0
    if fmt == "delta_optimized":
        _, optimize_s = timed(
            lambda: DeltaTable.forPath(spark, path).optimize().executeZOrderBy("product_id").collect()
        )

    scan, scan_s = timed(lambda: fingerprint(read_format(spark, storage, path, data.schema)))
    filtered, filter_s = timed(
        lambda: fingerprint(read_format(spark, storage, path, data.schema).where(F.col("product_id") == product_id))
    )
    if scan != expected_scan or filtered != expected_filter:
        raise ValueError(f"Số dòng hoặc nội dung khác đầu vào ở định dạng {fmt}: {scan}, {filtered}")

    if storage == "delta":
        # DESCRIBE DETAIL chỉ đếm file hoạt động, kể cả sau khi ZORDER thay file.
        size = DeltaTable.forPath(spark, path).detail().select("sizeInBytes").first()[0]
    else:
        size = sum(file.size for file in fs.ls(path) if file.name.startswith("part-"))
    if size <= 0:
        raise ValueError(f"Không đọc được dung lượng dữ liệu của {fmt}")
    return scan[0], filtered[0], product_id, write_s, optimize_s, scan_s, filter_s, size / 2**20


def remove_scratch(fs, path: str) -> None:
    if not fs.rm(path, recurse=True):
        raise OSError(f"Không dọn được thư mục benchmark {path}")


def benchmark(spark, fs, args) -> list[tuple]:
    source = table_name(args.catalog, args.silver_schema, "order_items_prior")
    root = f"/Volumes/{args.catalog}/ops/checkpoints/bench/ad-de-26/{uuid4().hex}"
    print(f"AD-DE-26: thư mục tạm {root}", flush=True)
    fs.mkdirs(root)
    rows = []
    try:
        # Chụp một bản đầu vào cho cả 12 phép đo. Thời gian staging nằm ngoài benchmark.
        snapshot = f"{root}/source"
        spark.table(source).select(*COLUMNS).repartition(args.partitions).write.parquet(snapshot)
        data = spark.read.parquet(snapshot)
        expected_scan = fingerprint(data)
        expected_filter = fingerprint(data.where(F.col("product_id") == args.product_id))
        if not expected_scan[0] or not expected_filter[0]:
            raise ValueError("Nguồn hoặc sản phẩm lọc không có dữ liệu")

        for index in range(3):
            folder = f"{root}/round-{index + 1}"
            fs.mkdirs(folder)
            try:
                # Đổi định dạng bắt đầu mỗi vòng để giảm sai lệch do thứ tự đo.
                for fmt in FORMATS[index:] + FORMATS[:index]:
                    metrics = measure_format(
                        spark, fs, data, f"{folder}/{fmt}", fmt, args.product_id, expected_scan, expected_filter
                    )
                    rows.append(
                        (args.run_id, index + 1, fmt, *metrics, source, args.node_type, args.runtime, args.partitions)
                    )
            finally:
                remove_scratch(fs, folder)
    finally:
        remove_scratch(fs, root)
    return rows


def save_results(spark: SparkSession, target: str, rows: list[tuple]) -> None:
    from delta.tables import DeltaTable

    # Một commit Delta ghi đủ 12 phép đo. Repair cùng run_id ghi lại cùng khóa.
    result = spark.createDataFrame(rows, RESULT_SCHEMA).withColumn("measured_at", F.current_timestamp())
    spark.sql(f"CREATE TABLE IF NOT EXISTS {target} ({RESULT_SCHEMA}, measured_at TIMESTAMP) USING DELTA")
    (
        DeltaTable.forName(spark, target)
        .alias("t")
        .merge(result.alias("s"), "t.run_id = s.run_id AND t.repetition = s.repetition AND t.format = s.format")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def main() -> None:
    from pyspark.dbutils import DBUtils

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--silver-schema", required=True)
    parser.add_argument("--ops-schema", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--node-type", required=True)
    parser.add_argument("--runtime", required=True)
    parser.add_argument("--product-id", type=int, default=24852)
    parser.add_argument("--partitions", type=int, default=32)
    args = parser.parse_args()
    if args.catalog in {"", ".", ".."} or "/" in args.catalog or "\\" in args.catalog:
        parser.error("catalog phải là một thành phần hợp lệ của đường dẫn Volume")
    if args.product_id <= 0 or args.partitions <= 0 or not args.run_id.strip():
        parser.error("product-id, partitions phải dương và run-id phải có giá trị")
    spark = SparkSession.builder.getOrCreate()
    rows = benchmark(spark, DBUtils(spark).fs, args)
    save_results(spark, table_name(args.catalog, args.ops_schema, "benchmark_results"), rows)
    print(f"AD-DE-26: {len(rows)} phép đo, run_id={args.run_id}, đã dọn thư mục benchmark")


if __name__ == "__main__":
    main()
