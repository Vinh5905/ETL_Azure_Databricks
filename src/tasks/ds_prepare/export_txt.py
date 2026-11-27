"""Xuất giao dịch prior đã mã hóa cho Hadoop và công bố manifest sau cùng."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

if globals().get("__package__"):
    from .run_sql import sql_args
else:
    script_path = Path(globals().get("__file__", sys._getframe().f_code.co_filename))
    sys.path.insert(0, str(script_path.resolve().parent))
    from run_sql import sql_args

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

GRAINS = ("product", "aisle", "department")
SCALES = (10, 25, 50, 100)


def volume_path(catalog: str, ds_schema: str) -> Path:
    for name in (catalog, ds_schema):
        if name in {".", ".."} or "/" in name or "\\" in name:
            raise ValueError("Catalog và schema xuất phải là từng thành phần tên volume")
    return Path("/Volumes") / catalog / ds_schema / "exports"


def export_transactions(spark: SparkSession, args: dict[str, str], volume: Path) -> dict:
    version = args["v"]
    prior = spark.table(args["transactions"]).where((F.col("data_version") == version) & (F.col("split") == "prior"))
    grain_counts = {
        row["grain"]: row.asDict()
        for row in prior.groupBy("grain")
        .agg(
            F.count_if(F.col("scale_bucket").isNull() | ~F.col("scale_bucket").between(0, 99)).alias("invalid_bucket"),
            *(F.count_if(F.col("scale_bucket") < pct).alias(str(pct)) for pct in SCALES),
        )
        .collect()
    }
    if set(grain_counts) != set(GRAINS) or any(row["invalid_bucket"] for row in grain_counts.values()):
        raise ValueError("Giao dịch prior cần đủ ba cấp và scale_bucket hợp lệ")

    version_path = volume / version
    manifest_path = version_path / "_manifest.json"
    manifest = {"data_version": version, "format": "txt: idx cách nhau bằng dấu cách, mỗi dòng một giao dịch"}
    # Thu hồi manifest trước khi thay các file. Chỉ công bố lại khi đủ cả 12 đường dẫn.
    manifest_path.unlink(missing_ok=True)
    for grain in GRAINS:
        manifest[grain] = {}
        for pct in SCALES:
            path = str(version_path / grain / f"scale={pct}")
            part = prior.where((F.col("grain") == grain) & (F.col("scale_bucket") < pct))
            lines = part.select(F.concat_ws(" ", "items").alias("value"))
            lines.repartition(max(1, pct // 25)).write.mode("overwrite").text(path)
            expected = grain_counts[grain][str(pct)]
            actual = spark.read.text(path).count()
            if actual != expected:
                raise RuntimeError(f"{grain}, {pct}%: đã ghi {actual} dòng, kỳ vọng {expected}")
            manifest[grain][str(pct)] = actual

    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return manifest


def main() -> None:
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-version", required=True)
    parser.add_argument("--catalog", default="instacart")
    parser.add_argument("--silver-schema", default="silver")
    parser.add_argument("--ds-schema", default="ds_data")
    options = parser.parse_args()
    try:
        args = sql_args(
            options.data_version,
            catalog=options.catalog,
            silver_schema=options.silver_schema,
            ds_schema=options.ds_schema,
        )
        volume = volume_path(options.catalog, options.ds_schema)
    except ValueError as exc:
        parser.error(str(exc))
    manifest = export_transactions(SparkSession.builder.getOrCreate(), args, volume)
    print(json.dumps(manifest, ensure_ascii=False))


if __name__ == "__main__":
    main()
