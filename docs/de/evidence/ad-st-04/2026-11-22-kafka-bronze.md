# Nghiệm thu Kafka Bronze (AD-ST-04)

Nghiệm thu trên pipeline Databricks dev với code ở commit `a0e043c`. Cả hai update dùng flow `kafka_g1` và chế độ update thường. [Issue #67](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/67)

- Event Hubs Standard ở `koreacentral`, hub `orders-stream`. [Lượt bật Event Hubs](https://github.com/Vinh5905/ETL_Azure_Databricks/actions/runs/44985924712)
- Simulator phát một lượt `ad-st-04-dev-20261122T051701Z-98f514` từ 05:17:02 đến 05:17:22 UTC ngày 22/11/2026. Broker xác nhận `events_sent=1000`.

## Kết quả update

Số liệu trong bảng được lọc theo `replay_run_id` trong `value_json` của `bronze.order_events`.

| Update dev | Hoàn tất (UTC) | Số dòng Bronze | `event_id` phân biệt | Cặp partition–offset phân biệt |
| --- | --- | ---: | ---: | ---: |
| Lần 1 | 05:29:13 | 1.000 | 1.000 | 1.000 |
| Lần 2 | 05:30:04 | 1.000 | 1.000 | 1.000 |

## Đối soát checkpoint

Giữa hai update không gửi thêm dữ liệu. Dải offset của lượt thử giữ nguyên ở từng partition.

| Partition | Offset sau update lần 1 | Offset sau update lần 2 | Số dòng sau mỗi update |
| ---: | --- | --- | ---: |
| 0 | 0–501 | 0–501 | 502 |
| 1 | 0–497 | 0–497 | 498 |

`event_log_stream` ghi flow `kafka_g1` hoàn tất ở cả hai update. Lần 1 ghi 1.000 dòng Kafka. Lần 2 hoàn tất mà không có mẻ ghi Kafka mới. Số dòng, số `event_id` phân biệt và dải offset của lượt thử khớp sau cả hai update.
