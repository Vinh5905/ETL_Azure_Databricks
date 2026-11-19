from __future__ import annotations

from streaming.simulator.simulate import build_events


def test_build_events_joins_train_orders_and_sorts_ties(tmp_path):
    (tmp_path / "orders.csv").write_text(
        "order_id,user_id,eval_set,order_number,order_dow,order_hour_of_day,days_since_prior_order\n"
        "9,31,train,2,4,8,NaN\n"
        "7,30,train,2,1,0,\n"
        "8,1,train,2,4,8,14.0\n"
        "1,1,prior,1,3,8,\n"
        "10,2,test,2,4,9,1.0\n"
        "12,3,train,2,4,9,1.0\n",
        encoding="utf-8-sig",
    )
    (tmp_path / "order_products__train.csv").write_text(
        "order_id,product_id,add_to_cart_order,reordered\n"
        "8,196,2,1\n"
        "1,1,1,0\n"
        "9,3,1,0\n"
        "7,4,1,1\n"
        "8,5,1,0\n"
        "10,2,1,1\n"
        "99,2,1,1\n",
        encoding="utf-8",
    )

    events = build_events(tmp_path)

    assert [event["event_id"] for event in events] == ["order-7", "order-8", "order-9"]
    assert [event["event_time"] for event in events] == [
        "2026-03-01T00:00:00Z",
        "2026-03-30T08:00:00Z",
        "2026-03-30T08:00:00Z",
    ]
    assert [event["days_since_prior_order"] for event in events] == [None, 14.0, None]
    assert events[1] == {
        "event_id": "order-8",
        "event_type": "order_placed",
        "schema_version": 1,
        "order_id": 8,
        "user_id": 1,
        "order_number": 2,
        "order_dow": 4,
        "order_hour_of_day": 8,
        "days_since_prior_order": 14.0,
        "event_time": "2026-03-30T08:00:00Z",
        "items": [
            {"product_id": 5, "add_to_cart_order": 1, "reordered": 0},
            {"product_id": 196, "add_to_cart_order": 2, "reordered": 1},
        ],
    }
