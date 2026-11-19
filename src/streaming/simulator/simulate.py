"""Dựng sự kiện đơn hàng từ tập train của Instacart."""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from common.sim_time import order_ts_sim

ORDER_FIELDS = ("order_id", "user_id", "order_number", "order_dow", "order_hour_of_day")
ITEM_FIELDS = ("product_id", "add_to_cart_order", "reordered")


def _csv_rows(path: Path, required_fields: tuple[str, ...]) -> Iterator[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle, restval="")
        missing = set(required_fields) - set(reader.fieldnames or ())
        if missing:
            raise ValueError(f"{path.name}: thiếu cột {', '.join(sorted(missing))}")
        yield from reader


def build_events(data_dir: str | Path) -> list[dict[str, Any]]:
    """Ghép đơn train với các món tương ứng rồi xếp theo thời gian và mã đơn."""
    source = Path(data_dir)
    items_by_order: dict[int, list[dict[str, int]]] = defaultdict(list)
    for row in _csv_rows(source / "order_products__train.csv", ("order_id", *ITEM_FIELDS)):
        items_by_order[int(row["order_id"])].append({field: int(row[field]) for field in ITEM_FIELDS})

    events = []
    for row in _csv_rows(source / "orders.csv", (*ORDER_FIELDS, "eval_set", "days_since_prior_order")):
        if row["eval_set"] != "train":
            continue
        items = items_by_order.get(int(row["order_id"]))
        if not items:
            continue
        header = {field: int(row[field]) for field in ORDER_FIELDS}
        days = float(row["days_since_prior_order"].strip() or "nan")
        # Train là đơn cuối cùng của khách nên dùng days_back = 0 của hàm dùng chung.
        event_time = order_ts_sim(header["user_id"], header["order_hour_of_day"])
        events.append(
            {
                "event_id": f"order-{header['order_id']}",
                "event_type": "order_placed",
                "schema_version": 1,
                **header,
                "days_since_prior_order": None if math.isnan(days) else days,
                "event_time": event_time.isoformat().replace("+00:00", "Z"),
                "items": sorted(items, key=lambda item: item["add_to_cart_order"]),
            }
        )
    return sorted(events, key=lambda event: (event["event_time"], event["order_id"]))


def _fraction(value: str) -> Decimal:
    try:
        fraction = Decimal(value)
    except InvalidOperation as error:
        raise argparse.ArgumentTypeError("tỷ lệ phải là số trong khoảng 0 đến 1") from error
    if not fraction.is_finite() or not 0 <= fraction <= 1:
        raise argparse.ArgumentTypeError("tỷ lệ phải là số trong khoảng 0 đến 1")
    return fraction


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True, help="Thư mục chứa CSV đơn hàng và món train")
    parser.add_argument("--run-id", required=True, help="Mã lần phát lại")
    parser.add_argument("--location", default="laptop", help="Nơi chạy simulator")
    parser.add_argument("--from-frac", type=_fraction, default=Decimal(0), help="Tỷ lệ bắt đầu, mặc định 0")
    parser.add_argument("--to-frac", type=_fraction, default=Decimal(1), help="Tỷ lệ kết thúc, mặc định 1")
    parser.add_argument("--dry-run", type=Path, metavar="FILE", required=True, help="Ghi sự kiện dạng JSON Lines")
    args = parser.parse_args(argv)
    if args.from_frac > args.to_frac:
        parser.error("--from-frac phải nhỏ hơn hoặc bằng --to-frac")

    output = args.dry_run
    if output.resolve() in {
        (args.data_dir / "orders.csv").resolve(),
        (args.data_dir / "order_products__train.csv").resolve(),
    }:
        parser.error("--dry-run phải trỏ đến tệp khác CSV đầu vào")

    try:
        events = build_events(args.data_dir)
        start = int(len(events) * args.from_frac)
        stop = int(len(events) * args.to_frac)
        output.parent.mkdir(parents=True, exist_ok=True)
        # Chỉ thay file đích sau khi đã ghi đủ mọi sự kiện.
        with TemporaryDirectory(prefix=".simulator-", dir=output.parent) as temporary:
            staged = Path(temporary) / "events.jsonl"
            with staged.open("w", encoding="utf-8", newline="\n") as handle:
                for event in events[start:stop]:
                    payload = {
                        **event,
                        "replay_run_id": args.run_id,
                        "produced_at": datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                        "producer_location": args.location,
                    }
                    handle.write(json.dumps(payload, ensure_ascii=False, allow_nan=False) + "\n")
            staged.replace(output)
    except (OSError, ValueError, csv.Error) as error:
        parser.exit(1, f"Lỗi: {error}\n")

    print(f"events_written={stop - start} total_events={len(events)}")


if __name__ == "__main__":
    main()
