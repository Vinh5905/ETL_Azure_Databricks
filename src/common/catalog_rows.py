"""Chọn bản danh mục Instacart hợp lệ và mới nhất cho bridge và dimension."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

VALID_CATALOG_PRODUCT_ID = "coalesce(try_cast(product_id AS INT) > 0, false)"


def canonical_catalog_rows(frame: DataFrame, key: str, payload: list[str], variants: str) -> DataFrame:
    """Ưu tiên bản không rescue, rồi thời gian ingest, metadata và nội dung."""
    unrescued = f"_{key}_unrescued"
    is_unrescued = F.col("_rescued_data").isNull() if "_rescued_data" in frame.columns else F.lit(True)
    values = F.struct(*(F.col(name) for name in payload))
    partition = Window.partitionBy(key)
    ordering = [
        F.col(name).desc_nulls_last() for name in ("_ingest_ts", "_file_mtime", "_source_file") if name in frame.columns
    ]
    return (
        frame.where(F.col(key) > 0)
        .withColumn(unrescued, is_unrescued)
        .withColumn(variants, F.size(F.collect_set(values).over(Window.partitionBy(key, unrescued))))
        .withColumn(
            "_rank",
            F.row_number().over(partition.orderBy(F.col(unrescued).desc(), *ordering, F.sha2(F.to_json(values), 256))),
        )
        .where("_rank = 1")
        .select(key, *payload, variants, unrescued)
    )


def canonical_products(products: DataFrame) -> DataFrame:
    """Cùng quy tắc chọn sản phẩm khi ghép tên OFF và dựng dimension."""
    payload = ["product_name", *[name for name in ("aisle_id", "department_id") if name in products.columns]]
    typed = products.withColumn("product_id", F.expr("try_cast(product_id AS INT)"))
    return canonical_catalog_rows(typed, "product_id", payload, "_products_n")
