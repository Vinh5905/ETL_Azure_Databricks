"""Làm phẳng OFF và dựng bảng cầu nối; chỉ định nghĩa truy vấn, không ghi bảng."""

from __future__ import annotations

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from rapidfuzz import fuzz

from common.catalog_rows import canonical_products
from common.dq import dq_failed
from common.text import norm

OFF_RULES = {
    "valid_product_id": "product_id IS NOT NULL AND product_id > 0",
    "valid_off_code": "off_code IS NOT NULL AND off_code RLIKE '^[0-9]{1,32}$'",
    "valid_ingest_ts": "_ingest_ts IS NOT NULL",
    "no_rescued_data": "_rescued_data IS NULL",
}
MATCH_RULES = {
    "valid_product_id": "product_id IS NOT NULL AND product_id > 0",
    "valid_score": "off_match_score IS NOT NULL AND off_match_score BETWEEN 0 AND 100",
    "valid_status": """off_match_status IS NOT NULL AND off_match_status = CASE
        WHEN off_match_score >= 85 THEN 'matched'
        WHEN off_match_score >= 70 THEN 'low_confidence' ELSE 'no_match' END""",
}
STATUSES = ("matched", "low_confidence", "no_match")


def score_names(ic_name: str | None, off_name: str | None, brand: str | None) -> float:
    """Điểm token_set_ratio +5 cho thương hiệu đầu tiên xuất hiện nguyên cụm từ."""
    a, b = norm(ic_name), norm(off_name)
    if not a or not b:
        return 0.0
    score = fuzz.token_set_ratio(a, b)
    first_brand = norm((brand or "").split(",")[0])
    if first_brand and f" {first_brand} " in f" {a} ":
        score += 5
    return float(min(score, 100))


def score_series(ic_name: pd.Series, off_name: pd.Series, brand: pd.Series) -> pd.Series:
    """Thân pandas UDF dùng cùng hàm thuần đã kiểm thử; xử lý NULL từ Arrow."""
    return pd.Series(
        [
            score_names(a, b, br)
            for a, b, br in zip(ic_name.fillna(""), off_name.fillna(""), brand.fillna(""), strict=True)
        ],
        dtype="float64",
    )


def _column(frame: DataFrame, name: str, dtype="string"):
    return F.col(name) if name in frame.columns else F.lit(None).cast(dtype)


def _strings(frame: DataFrame, name: str):
    """OFF có brands dạng chuỗi hoặc mảng; tags thiếu được giữ thành mảng rỗng."""
    dtype = frame.schema[name].dataType if name in frame.columns else T.NullType()
    if isinstance(dtype, T.ArrayType):
        values = F.transform(F.col(name), lambda x: F.trim(x.cast("string")))
    elif isinstance(dtype, T.StringType):
        value = F.trim(F.col(name))
        values = F.when(value.startswith("["), F.from_json(value, "array<string>")).otherwise(F.split(value, ","))
        values = F.transform(values, lambda x: F.trim(x))
    else:
        values = F.lit(None).cast("array<string>")
    return F.filter(F.coalesce(values, F.array().cast("array<string>")), lambda x: x.isNotNull() & (x != ""))


def _nutrient(frame: DataFrame, name: str):
    dtype = frame.schema["nutriments"].dataType if "nutriments" in frame.columns else T.NullType()
    if isinstance(dtype, T.MapType) or isinstance(dtype, T.StructType) and name in dtype.fieldNames():
        return F.col("nutriments")[name].cast("string")
    return F.lit(None).cast("string")


def assess_off(frame: DataFrame) -> DataFrame:
    """Cast an toàn ở ANSI mode, giữ nguyên raw và liệt kê mọi luật lỗi trước khử trùng."""
    flat = frame.select(
        _column(frame, "_product_id").cast("string").alias("_product_id_text"),
        F.trim(_column(frame, "code").cast("string")).alias("off_code"),
        _column(frame, "product_name").cast("string").alias("off_product_name"),
        F.concat_ws(",", _strings(frame, "brands")).alias("brand"),
        _strings(frame, "categories_tags").alias("categories_tags"),
        _strings(frame, "countries_tags").alias("countries_tags"),
        _column(frame, "nutriscore_grade").cast("string").alias("nutriscore_grade"),
        _column(frame, "nova_group").cast("string").alias("_nova_text"),
        _nutrient(frame, "energy-kcal_100g").alias("_energy_text"),
        _nutrient(frame, "sugars_100g").alias("_sugars_text"),
        _nutrient(frame, "fat_100g").alias("_fat_text"),
        _column(frame, "image_url").cast("string").alias("image_url"),
        _column(frame, "_ingest_ts").cast("string").alias("_ingest_text"),
        _column(frame, "_rescued_data").alias("_rescued_data"),
        F.struct(*[F.col(c) for c in sorted(frame.columns)]).alias("_raw"),
    )
    parsed = flat.selectExpr(
        "*",
        "try_cast(_product_id_text AS INT) AS product_id",
        "try_cast(_nova_text AS INT) AS nova_group",
        "try_cast(_energy_text AS DOUBLE) AS energy_kcal_100g",
        "try_cast(_sugars_text AS DOUBLE) AS sugars_100g",
        "try_cast(_fat_text AS DOUBLE) AS fat_100g",
        "try_cast(_ingest_text AS TIMESTAMP_LTZ) AS _ingest_ts",
    ).drop("_product_id_text", "_nova_text", "_energy_text", "_sugars_text", "_fat_text", "_ingest_text")
    return parsed.withColumn("_dq_failed", dq_failed(OFF_RULES))


def clean_off(assessed: DataFrame) -> DataFrame:
    """Mỗi cặp khóa lấy bản hợp lệ mới nhất; hash raw phá hòa, không phụ thuộc thứ tự nạp."""
    order = Window.partitionBy("product_id", "off_code").orderBy(
        F.col("_ingest_ts").desc(), F.sha2(F.to_json("_raw"), 256).asc()
    )
    return (
        assessed.where(F.size("_dq_failed") == 0)
        .withColumn("_rn", F.row_number().over(order))
        .where("_rn = 1")
        .drop("_rn", "_raw", "_dq_failed", "_rescued_data")
    )


def quarantine_off(assessed: DataFrame) -> DataFrame:
    return assessed.where(F.size("_dq_failed") > 0).select(
        "_raw", "product_id", "off_code", "_dq_failed", F.current_timestamp().alias("_quarantined_at")
    )


def product_matches(products: DataFrame, candidates: DataFrame, match_score) -> DataFrame:
    """Chỉ chấm ứng viên crawler của đúng product_id; giữ cả sản phẩm chưa được tra."""
    p = canonical_products(products).select("product_id", "product_name")
    c = candidates.where(F.array_contains("countries_tags", "en:united-states")).select(
        "product_id", "off_code", "off_product_name", "brand"
    )
    scored = p.join(c, "product_id", "left").withColumn(
        "off_match_score", match_score("product_name", "off_product_name", "brand")
    )
    best = Window.partitionBy("product_id").orderBy(F.col("off_match_score").desc(), F.col("off_code").asc_nulls_last())
    return (
        scored.withColumn("_rn", F.row_number().over(best))
        .where("_rn = 1")
        .withColumn(
            "off_match_status",
            F.when(F.col("off_match_score") >= 85, "matched")
            .when(F.col("off_match_score") >= 70, "low_confidence")
            .otherwise("no_match"),
        )
        .select("product_id", "off_code", "off_match_score", "off_match_status")
    )


def match_statistics(matches: DataFrame) -> DataFrame:
    """Ba nhãn luôn có mặt; mẫu số là toàn bộ sản phẩm, kể cả chưa crawl."""
    statuses = matches.sparkSession.createDataFrame([(s,) for s in STATUSES], "off_match_status STRING")
    counts = matches.groupBy("off_match_status").count()
    totals = matches.agg(F.count("*").alias("total_products"))
    return (
        statuses.join(counts, "off_match_status", "left")
        .fillna(0, subset=["count"])
        .crossJoin(totals)
        .withColumn(
            "pct_all_products",
            F.when(F.col("total_products") > 0, F.round(100.0 * F.col("count") / F.col("total_products"), 4)).otherwise(
                F.lit(0.0)
            ),
        )
    )
