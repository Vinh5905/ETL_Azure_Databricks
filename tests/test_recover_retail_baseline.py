"""Regression #215 với một snapshot ADF chụp sau wave 1."""

from decimal import ROUND_HALF_UP, Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ingest.seed_retail_db import Product, generate_wave
from recover_retail_baseline import PRICE_COLUMNS, ZERO_SEQVAL, build_recovery, write_recovery

PRODUCTS = tuple(Product(number, 4) for number in range(1, 41))


@pytest.fixture
def snapshot():
    wave0 = generate_wave(PRODUCTS, 0)
    wave1 = generate_wave(PRODUCTS, 1)
    prices = {row[0]: row for row in wave0.price_inserts}
    for factor, timestamp, product_id in wave1.price_updates:
        _, price, currency, _ = prices[product_id]
        changed_price = (price * factor).quantize(Decimal("0.01"), ROUND_HALF_UP)
        prices[product_id] = (product_id, changed_price, currency, timestamp)
    schema = pa.schema(
        [
            ("_lsn", pa.string()),
            ("_seqval", pa.string()),
            ("_op", pa.int32()),
            ("__$start_lsn", pa.binary()),
            ("__$seqval", pa.binary()),
            ("__$operation", pa.int32()),
            ("__$update_mask", pa.binary()),
            ("product_id", pa.int32()),
            ("unit_price", pa.decimal128(10, 2)),
            ("currency", pa.string()),
            ("updated_at", pa.timestamp("us")),
        ]
    )
    return pa.Table.from_pylist(
        [
            dict(zip(PRICE_COLUMNS, values, strict=True), _lsn="0x00000039000078080004", _seqval=ZERO_SEQVAL, _op=0)
            for values in prices.values()
        ],
        schema=schema,
    )


def test_recovers_only_original_values_for_affected_products(snapshot, tmp_path):
    wave0 = generate_wave(PRODUCTS, 0)
    changed = {row[2] for row in generate_wave(PRODUCTS, 1).price_updates}
    batch, rows = build_recovery(snapshot, PRODUCTS, 42)
    assert rows.schema == snapshot.schema
    assert rows.select(PRICE_COLUMNS).to_pylist() == [
        dict(zip(PRICE_COLUMNS, row, strict=True)) for row in wave0.price_inserts if row[0] in changed
    ]
    assert build_recovery(snapshot.take(list(reversed(range(snapshot.num_rows)))), PRODUCTS[::-1], 42)[0] == batch

    path = tmp_path / f"recovery={batch}" / "part-00000.parquet"
    write_recovery(path, rows)
    original = path.read_bytes()
    write_recovery(path, rows)
    assert path.read_bytes() == original
    assert pq.ParquetFile(path).read().num_rows == len(changed)
    with pytest.raises(ValueError, match="nội dung khác"):
        write_recovery(path, rows.slice(0, 1))
    assert path.read_bytes() == original


def test_rejects_partial_mismatched_or_duplicate_snapshot(snapshot):
    rows = snapshot.to_pylist()
    rows[0]["unit_price"] += Decimal("0.01")
    for candidate in (snapshot.slice(1), pa.Table.from_pylist(rows, schema=snapshot.schema), snapshot.take([0] * 40)):
        with pytest.raises(ValueError, match="không khớp toàn bộ trạng thái seed"):
            build_recovery(candidate, PRODUCTS, 42)
