# Đo độ trễ Event Hubs và Databricks (AD-ST-09)

So sánh `ms_eh_to_bronze` của hai lượt trên cùng Databricks `eastasia`.
Lượt A dùng Event Hubs Sub B `koreacentral`. Lượt B cần hub tạm ở
`eastasia` do P1 bàn giao. Workflow `stream-toggle` hiện chỉ có input
`action` và dựng stack Sub B. [Issue AD-ST-09](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/72)

## Chuẩn bị và bàn giao

- Chờ AD-OPS-24 (#26) xác nhận đường chạy Event Hubs. Thống nhất với P1 về
  thời gian đo, hub `orders-stream` ở region Databricks, SAS Send và tên secret
  SAS Listen riêng trong scope `kv`. Nếu namespace B chưa sẵn sàng thì chỉ
  chuẩn bị cấu hình và truy vấn.
- Kiểm tra hai hub có tier, số partition và cấu hình hoạt động tương đương.
  Dùng cùng bộ CSV, mã nguồn, máy gửi, region Databricks, policy cluster, số
  partition và tốc độ gửi. Tắt lịch stream cho buổi đo. Phối hợp P1 tạm
  dừng lịch `pl_master` và đợi các job batch chạy xong trước khi dùng quota.
- Dùng pipeline dev cách ly. Ghi cấu hình ban đầu để khôi phục. Đặt
  `stream_continuous=true`, `dbx_region=eastasia`, `eh_region` theo hub và
  `eh_listen_secret` theo tên secret Listen. Dùng mã `eh_generation` mới
  cho từng namespace và giữ checkpoint khi lặp trên cùng namespace.

## Chạy hai lượt

1. Bật hub Sub B qua `stream-toggle up` nếu cần. Kiểm tra pipeline
   continuous đang chạy và gửi một lô khởi động với run ID riêng. Sau khi
   thấy sự kiện khởi động vào Bronze, đo lô chính với run ID mới.
2. Từ gốc repo, nạp SAS Send của đúng hub theo [README simulator](../../src/streaming/simulator/README.md).
   Dùng cùng lệnh cho hai lượt, chỉ đổi `--run-id` và SAS Send.

   ```bash
   PYTHONPATH=src uv run --frozen --extra ingest python -m streaming.simulator.simulate --data-dir data/instacart --send --limit 1000 --rate 100 --run-id st09-cross-01
   unset EH_SEND_CONN
   ```

3. Ghi số broker xác nhận, giờ UTC, run ID, mã thế hệ và update ID. Đợi
   `streaming_latency` nhận đủ trước khi dừng pipeline. Dừng hoàn tất rồi
   chuyển sang hub B bằng `eh_listen_secret` riêng, region và thế hệ mới.
   Giữ nguyên bảng đích dev để truy vấn cả hai lượt. Khởi động và phát với
   run ID `st09-same-01` theo cùng quy trình.
4. Khi đo xong, dừng pipeline, đưa `stream_continuous=false` và trả lại
   cấu hình hub theo trạng thái đã thống nhất. Xác nhận chế độ triggered,
   lịch và tài nguyên đã được khôi phục. Phối hợp tắt từng namespace thử
   theo vòng đời của nó và ghi bằng chứng.

## Đối chiếu và lưu bằng chứng

Chạy [truy vấn so sánh](stream_latency_comparison.sql) với tên đầy đủ của
bảng `latency_table`, hai run ID, hai region và `expected_events` bằng số
broker xác nhận **ở mỗi lượt**. Mỗi kết quả phải có `comparable = true`.
Cờ này yêu cầu đủ số hàng và số `event_id` khác nhau, đúng nhãn region và
độ trễ từ Event Hubs đến Bronze hợp lệ. Nếu có lệch, ghi nguyên nhân và
lặp lại với run ID mới.

| Lượt | Run ID | Số sự kiện | p50 EH → Bronze (ms) | p95 EH → Bronze (ms) |
|---|---|---:|---:|---:|
| A · khác region | Chờ chạy | | | |
| B · cùng region | Chờ chạy | | | |

Đưa kết quả và ảnh vào `docs/de/evidence/ad-st-09/`. Ghi chênh lệch A trừ B
cho p50 và p95, cùng số mẫu và điều kiện chạy. Chỉ số này gồm cả thời gian
chờ micro-batch, vì thế diễn giải theo hai lượt đã đo. Kèm bằng chứng
pipeline đã về triggered và trạng thái tài nguyên sau buổi đo.
