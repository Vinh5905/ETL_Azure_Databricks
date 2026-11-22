"""Luật chất lượng và biểu thức quarantine dùng chung cho batch/stream."""

from __future__ import annotations

from collections.abc import Mapping

from pyspark.sql import Column
from pyspark.sql import functions as F

# Giữ tên luật trong tài liệu để số liệu expectations và quarantine đối chiếu được.
# NULL là lỗi, trừ khoảng cách đơn đầu và metadata không có rescued data.
ORDER_RULES = {
    "valid_dow": "coalesce(order_dow BETWEEN 0 AND 6, false)",
    "valid_hour": "coalesce(order_hour_of_day BETWEEN 0 AND 23, false)",
    "valid_dsp": "coalesce(days_since_prior_order IS NULL OR days_since_prior_order BETWEEN 0 AND 30, false)",
    "no_rescued": "_rescued_data IS NULL",
}

ITEM_RULES = {
    "has_keys": "order_id IS NOT NULL AND product_id IS NOT NULL",
    "cart_order_pos": "coalesce(add_to_cart_order >= 1, false)",
    "reordered_flag": "coalesce(reordered IN (0, 1), false)",
    "no_rescued": "_rescued_data IS NULL",
}

PRICE_RULES = {
    "price_positive": "coalesce(unit_price > 0, false)",
    "currency_usd": "coalesce(currency = 'USD', false)",
    "has_product_id": "product_id IS NOT NULL",
}

PROMO_RULES = {
    "discount_range": "coalesce(discount_pct BETWEEN 0 AND 90, false)",
    "valid_window": "coalesce(end_date >= start_date, false)",
}

EVENT_RULES = {
    "parsed": "event_id IS NOT NULL",
    "known_type": "coalesce(event_type = 'order_placed', false)",
    "schema_v1": "coalesce(schema_version = 1, false)",
    "has_items": "coalesce(size(items) > 0, false)",
    "valid_item_keys": (
        "items IS NULL OR (forall(items, item -> item.product_id IS NOT NULL) "
        "AND size(array_distinct(transform(items, item -> item.product_id))) = size(items))"
    ),
    "id_matches": "coalesce(event_id = concat('order-', order_id), false)",
}


def dq_failed(rules: Mapping[str, str]) -> Column:
    """Tạo array<string> tên luật không đạt, theo thứ tự khai báo của dict.

    Dùng với df.withColumn("_dq_failed", dq_failed(RULES)). Dòng hợp lệ nhận [],
    dòng lỗi giữ đủ tên luật; dict rỗng cũng trả array<string> rỗng.
    Luật tùy chỉnh trả NULL được coi là không đạt để không mất dòng quarantine.
    Tên cột thiếu, SQL sai hoặc biểu thức không Boolean phải báo lỗi Spark;
    hàm không nuốt lỗi cấu hình, không lọc/ghi dữ liệu hay phụ thuộc pipelines/dp.
    """
    if not rules:
        return F.array().cast("array<string>")
    failed = F.array(
        *[F.when(~F.coalesce(F.expr(condition), F.lit(False)), F.lit(name)) for name, condition in rules.items()]
    )
    return F.filter(failed, lambda name: name.isNotNull())
