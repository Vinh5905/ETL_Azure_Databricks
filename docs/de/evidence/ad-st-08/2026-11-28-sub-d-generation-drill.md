# Đổi thế hệ Event Hubs trên Sub D (AD-ST-08)

Mốc này thay kết quả ngày 26/11 vì catalog của workspace cũ đã bị xóa khi
chuyển hạ tầng. Lượt kiểm tra dùng pipeline dev riêng trên Sub D, prefix
`p3_st08_1128_`, ở chế độ triggered. [Nhận xét trên PR #239](https://github.com/Vinh5905/ETL_Azure_Databricks/pull/239#issuecomment-7234020353)

## Mốc trước khi đổi

- [Bật namespace đầu trên Sub B](https://github.com/Vinh5905/ETL_Azure_Databricks/actions/runs/46141718080) và deploy riêng pipeline stream với `eh_generation=g20261128_st08a`.
- Simulator phát 50 đơn đầu của tập train với `st08-subd-20261128-base`.
  Broker xác nhận **50/50** lúc **18:54:09 ngày 28/11** (11:54:09 UTC).
- Lượt đầu thiếu schema dev Ops để tạo event log. Sau khi tạo schema theo
  [runbook](../../06_van_hanh_chay_lai.md#62-đổi-thế-hệ-event-hubs-ad-st-08),
  chạy lại update thường `24a60797-41eb-4752-932a-db7def50e88d` trên cùng
  namespace và lô đã gửi. Update `COMPLETED` lúc 19:18:32 (12:18:32 UTC).

Pipeline dev `eb1105a1-2bd5-422e-ac4d-a0007629af43` dùng các bảng
`instacart.p3_st08_1128_bronze.order_events` và
`instacart.p3_st08_1128_silver.stream_orders`. Truy vấn đối soát lúc
19:18:42 (12:18:42 UTC) dùng `new_generation=g20261128_st08b` và
`replay_run_id=st08-subd-20261128-replay` để chuẩn bị so sánh hai mốc.
[Truy vấn đối soát](../../stream_generation_reconciliation.sql)

| Chỉ số | Mốc trước |
|---|---:|
| `bronze_rows` | 50 |
| `previous_bronze_rows` | 50 |
| `previous_bronze_checksum` | `-92589178091316604324` |
| `new_generation_rows` | 0 |
| `new_replay_rows` | 0 |
| `new_kafka_keys` | 0 |
| `new_min_offset` | `NULL` |
| `silver_rows` | 50 |
| `silver_order_keys` | 50 |
| `silver_replayed_orders` | 0 |
