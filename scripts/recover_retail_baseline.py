#!/usr/bin/env python3
"""Dựng Parquet phục hồi giá gốc từ snapshot đầu tiên bị chụp sau wave 1 (#215)."""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import sys
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from tempfile import NamedTemporaryFile

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from ingest.seed_retail_db import generate_wave, load_products  # noqa: E402

PRICE_COLUMNS = ("product_id", "unit_price", "currency", "updated_at")
SNAPSHOT_METADATA = ("_lsn", "_seqval", "_op", "__$start_lsn", "__$seqval", "__$operation", "__$update_mask")
ZERO_SEQVAL = "0x00000000000000000000"


def build_recovery(snapshot: pa.Table, products, seed: int) -> tuple[str, pa.Table]:
    """Chỉ xuất giá wave 0 của những khóa đã mất giá gốc trong snapshot đã đối chiếu."""
    initial = generate_wave(products, 0, seed)
    next_wave = generate_wave(products, 1, seed)
    baseline = {row[0]: row for row in initial.price_inserts}
    expected = baseline.copy()
    for factor, updated_at, product_id in next_wave.price_updates:
        _, price, currency, _ = baseline[product_id]
        changed_price = (price * factor).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        expected[product_id] = (product_id, changed_price, currency, updated_at)

    if set(snapshot.column_names) != {*PRICE_COLUMNS, *SNAPSHOT_METADATA}:
        raise ValueError("Schema không khớp snapshot giá của ADF")
    lsns = snapshot["_lsn"].unique().to_pylist()
    if (
        len(lsns) != 1
        or not re.fullmatch(r"0x[0-9a-fA-F]{20}", lsns[0] or "")
        or snapshot["_seqval"].unique().to_pylist() != [ZERO_SEQVAL]
        or snapshot["_op"].unique().to_pylist() != [0]
        or any(snapshot[name].null_count != snapshot.num_rows for name in SNAPSHOT_METADATA[3:])
    ):
        raise ValueError("Metadata không khớp snapshot giá của ADF")

    actual = {
        row["product_id"]: tuple(row[name] for name in PRICE_COLUMNS)
        for row in snapshot.select(PRICE_COLUMNS).to_pylist()
    }
    if len(actual) != snapshot.num_rows or actual != expected:
        raise ValueError("Snapshot không khớp toàn bộ trạng thái seed sau wave 1, dừng phục hồi")

    # Giữ schema và metadata snapshot đã công bố. Chỉ thay giá và mốc hiệu lực bằng giá wave 0.
    repair_rows = []
    for product_id in sorted(product_id for _, _, product_id in next_wave.price_updates):
        row = dict(zip(PRICE_COLUMNS, baseline[product_id], strict=True))
        row.update(zip(SNAPSHOT_METADATA, (lsns[0], ZERO_SEQVAL, 0, None, None, None, None), strict=True))
        repair_rows.append(row)
    if not repair_rows:
        raise ValueError("Kế hoạch wave 1 không có sản phẩm cần phục hồi")

    batch = hashlib.sha256(f"{lsns[0]}:{initial.fingerprint}:{next_wave.fingerprint}".encode()).hexdigest()
    return batch, pa.Table.from_pylist(repair_rows, schema=snapshot.schema.remove_metadata())


def write_recovery(path: Path, rows: pa.Table) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=path.parent, prefix=".", suffix=".tmp") as staging:
        # ADF dùng INT96; giữ định dạng timestamp tương thích với Spark khi đọc chung các file.
        pq.write_table(rows, staging.name, use_deprecated_int96_timestamps=True)
        try:
            os.link(staging.name, path)
        except FileExistsError:
            if not pq.ParquetFile(path).read().equals(pq.ParquetFile(staging.name).read()):
                raise ValueError(f"File đã tồn tại với nội dung khác: {path}") from None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--products-csv", type=Path, required=True)
    parser.add_argument("--initial-snapshot", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output-dir", type=Path, required=True, help="Thư mục staging trên máy local")
    args = parser.parse_args()
    products = load_products(args.products_csv)
    if len(products) != 49_688:
        parser.error("Cần đủ danh mục Instacart gồm 49.688 sản phẩm")
    batch, rows = build_recovery(pq.ParquetFile(args.initial_snapshot).read(), products, args.seed)
    path = args.output_dir / f"recovery={batch}" / "part-00000.parquet"
    write_recovery(path, rows)
    print(f"{rows.num_rows} dòng giá wave 0: {path}")


if __name__ == "__main__":
    main()
