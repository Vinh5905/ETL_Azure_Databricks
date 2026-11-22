# Nghiệm thu Silver stream (AD-ST-05)

Nghiệm thu ngày 22/11/2026 trên một pipeline Databricks dev riêng với code ở commit `2ab1aec`. Cả ba lượt dùng cùng checkpoint và chạy update thường. [Issue #68](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/68)

Event Hubs Standard ở `koreacentral`, hub `orders-stream`. [Lượt bật Event Hubs](https://github.com/Vinh5905/ETL_Azure_Databricks/actions/runs/45000311012)

## Kết quả

| Lượt | `run-id` khi gửi | Broker xác nhận | Update hoàn tất (UTC) | Bronze `order_events` | Silver `stream_orders` | Silver `stream_order_items` | Quarantine |
| --- | --- | ---: | --- | ---: | ---: | ---: | ---: |
| Phát lần đầu | `ad-st-05-7649c8-01` | 1.000 | 08:11:32 | 1.000 | 1.000 | 10.498 | 0 |
| Phát lại cùng 1.000 sự kiện | `ad-st-05-7649c8-02` | 1.000 | 08:13:30 | 2.000 | 1.000 | 10.498 | 0 |
| Gửi một bản tin JSON thiếu dấu đóng `}` | `ad-st-05-7649c8-invalid` (Kafka key) | 1 | 08:14:37 | 2.001 | 1.000 | 10.498 | 1 |

## Đối soát

- Sau lượt phát lại, Bronze có 1.000 dòng theo từng `replay_run_id`. Cả 1.000 đơn Silver mang `replay_run_id` của lượt phát lại.
- Trong cả ba lượt, có 1.000 khóa `order_id` phân biệt và 10.498 cặp khóa `(order_id, product_id)` phân biệt.
- `quarantine_order_events` giữ nguyên `value_json` của bản tin JSON lỗi và ghi luật `parsed` trong `_dq_failed`.
