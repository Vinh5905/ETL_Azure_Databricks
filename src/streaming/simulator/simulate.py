"""Dựng sự kiện đơn hàng từ tập train của Instacart."""

from __future__ import annotations

import csv
import math
from collections import defaultdict
from collections.abc import Iterator
from pathlib import Path
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
