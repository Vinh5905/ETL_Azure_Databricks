"""Chọn lô từ bronze và MERGE trạng thái OFF; schema/catalog do nền tảng quản lý."""

from __future__ import annotations

import datetime as dt
import uuid

from pyspark.sql import functions as F

STATE_SCHEMA = (
    "product_id INT, status STRING, attempts INT, off_code STRING, batch_file STRING, "
    "last_error STRING, updated_at TIMESTAMP, batch_id STRING"
)


def qualified(*parts):
    return ".".join("`" + part.replace("`", "``") + "`" for part in parts)


def pick_batch(products, items, state, n=300, top=2000, max_attempts=5):
    """Top theo tần suất prior, tie-break product_id; không tra lại done/no_result/failed."""
    if not 1 <= n <= top <= 2000 or not 1 <= max_attempts <= 5:
        raise ValueError("Giới hạn lô không hợp lệ")
    popularity = items.groupBy("product_id").count().orderBy(F.desc("count"), "product_id").limit(top)
    names = (
        products.where(F.length(F.trim("product_name")) > 0)
        .groupBy("product_id")
        .agg(F.min("product_name").alias("product_name"))
    )
    return (
        popularity.join(names, "product_id")
        .join(state.select("product_id", "status", "attempts"), "product_id", "left")
        .where(F.col("status").isNull() | ((F.col("status") == "retry") & (F.col("attempts") < max_attempts)))
        .orderBy(F.desc("count"), "product_id")
        .select("product_id", "product_name", F.coalesce("attempts", F.lit(0)).alias("attempts"))
        .limit(n)
    )


def landing_frames(spark, root):
    """Bootstrap trước update LDP đầu tiên; vẫn chỉ dùng products và món prior đã công bố."""
    schemas = {
        "products": "product_id INT, product_name STRING, aisle_id INT, department_id INT",
        "order_products__prior": "order_id INT, product_id INT, add_to_cart_order INT, reordered INT",
    }
    return [
        spark.read.schema(schema)
        .options(
            header="true", quote='"', escape='"', mode="FAILFAST", recursiveFileLookup="true", pathGlobFilter="*.csv"
        )
        .csv(f"{root.rstrip('/')}/instacart/{folder}/")
        for folder, schema in schemas.items()
    ]


class SparkState:
    def __init__(self, spark, catalog, bronze_schema, state_schema, *, source="bronze", landing_root=None):
        self.spark = spark
        self.bronze = (catalog, bronze_schema)
        self.table = qualified(catalog, state_schema, "off_crawl_state")
        self.source, self.landing_root = source, landing_root
        spark.sql(f"CREATE TABLE IF NOT EXISTS {self.table} ({STATE_SCHEMA}) USING DELTA").collect()

    def pick_batch(self, n, top, max_attempts):
        frames = (
            landing_frames(self.spark, self.landing_root)
            if self.source == "landing"
            else [self.spark.table(qualified(*self.bronze, table)) for table in ("products", "order_products_prior")]
        )
        return [
            row.asDict() for row in pick_batch(*frames, self.spark.table(self.table), n, top, max_attempts).collect()
        ]

    def merge(self, updates):
        if not updates:
            return
        rows = [
            {**row, "updated_at": dt.datetime.fromisoformat(row["updated_at"]).astimezone(dt.UTC)} for row in updates
        ]
        view = "off_updates_" + uuid.uuid4().hex
        self.spark.createDataFrame(rows, STATE_SCHEMA).createOrReplaceTempView(view)
        try:
            # attempts là số tuyệt đối trong journal: replay không cộng thêm và không lùi trạng thái terminal.
            self.spark.sql(f"""
                MERGE INTO {self.table} t USING {view} s ON t.product_id = s.product_id
                WHEN MATCHED AND coalesce(t.status, 'retry') NOT IN ('done', 'no_result', 'failed')
                    AND s.attempts > coalesce(t.attempts, 0) THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """).collect()
        finally:
            self.spark.catalog.dropTempView(view)

    def batch_files(self):
        return [
            row.batch_file
            for row in self.spark.table(self.table)
            .where((F.col("status") == "done") & F.col("batch_file").isNotNull())
            .select("batch_file")
            .distinct()
            .orderBy("batch_file")
            .collect()
        ]
