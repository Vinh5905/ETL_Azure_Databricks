# Mốc trước khi đổi thế hệ Event Hubs (AD-ST-08)

Truy vấn đối soát chạy thành công trên `wh-instacart-bi` lúc **00:19:09 ngày 26/11/2026 (UTC+7)**, tức 17:19:09 ngày 25/11/2026 (UTC). Dữ liệu lấy từ `order_events` và `stream_orders` của pipeline dev đã chạy [AD-ST-07](../ad-st-07/2026-11-25-train-replay.md). Pipeline ở trạng thái `IDLE` với `eh_generation=g20261125_6b780de`.

## Kết quả trước khi đổi

[Truy vấn đối soát](../../stream_generation_reconciliation.sql) dùng `new_generation=g20261126_st08` và `replay_run_id=ad-st-08-replay-01` làm tham số dự kiến cho lượt sau.

| Chỉ số | Kết quả |
|---|---:|
| `bronze_rows` | 262.418 |
| `previous_bronze_rows` | 262.418 |
| `previous_bronze_checksum` | `4787021701658162850035` |
| `new_generation_rows` | 0 |
| `new_replay_rows` | 0 |
| `new_kafka_keys` | 0 |
| `new_min_offset` | `NULL` |
| `silver_rows` | 131.209 |
| `silver_order_keys` | 131.209 |
| `silver_replayed_orders` | 0 |

Cùng câu lệnh SQL đối chiếu 50 mã đơn của lô dry-run với `stream_orders`. Cả **50/50** mã đều có trong Silver ở mốc này.

## Đối chiếu sau khi phát lại

Giữ nguyên pipeline và hai bảng. Dùng cùng tham số khi so với mốc trên. Nếu đổi mã thế hệ hoặc mã lượt phát dự kiến, lấy lại mốc trước bằng cặp tham số mới.

Với 50 bản tin được broker xác nhận, Bronze dự kiến tăng lên 262.468 dòng và checksum của 262.418 dòng lịch sử giữ nguyên. Silver dự kiến vẫn có 131.209 dòng với 131.209 khóa đơn, trong đó 50 dòng mang mã lượt phát lại. Kết quả trên mới ghi nhận mốc trước. Lượt đổi thế hệ, update flow mới và đối soát mốc sau được thực hiện theo [runbook ST08](../../06_van_hanh_chay_lai.md#62-đổi-thế-hệ-event-hubs-ad-st-08).
