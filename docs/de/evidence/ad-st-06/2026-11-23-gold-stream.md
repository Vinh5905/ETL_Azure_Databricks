# Nghiệm thu Gold stream (AD-ST-06)

Nghiệm thu ngày 23/11/2026 trên pipeline Databricks dev riêng với code ở commit `c9327dd` và `eh_generation=g20261123_caefb7f`. Hai lượt dùng cùng checkpoint và chạy update thường. Event Hubs ở `koreacentral`, Databricks ở `eastasia`. [Issue #69](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/69) · [Lượt bật Event Hubs](https://github.com/Vinh5905/ETL_Azure_Databricks/actions/runs/45164024481)

## Lượt chạy

| Lượt | `replay_run_id` | Broker xác nhận | Event Hubs nhận (UTC) | Update hoàn tất (UTC) | Dòng latency ghi thêm | Dòng cặp sản phẩm ghi thêm |
| --- | --- | ---: | --- | --- | ---: | ---: |
| Mẫu từ tập train, gồm một bản tin trùng | `ad-st-06-caefb7f-burst` | 5 | 11:21:14 | 11:33:32 | 5 | 0 |
| Đẩy watermark | `ad-st-06-caefb7f-watermark` | 1 | 11:33:43 | 11:37:33 | 1 | 6 |

Mốc update và số dòng ghi thêm lấy từ event log Databricks. Số liệu cuối được đối soát bằng một truy vấn trên `wh-instacart-bi`.

## Đối soát tiêu chí #69

- `stream_kpi_1m` ghi **4 đơn, 9 món** trong phút 11:21 UTC, khớp 4 đơn hợp lệ phân biệt trong 5 bản tin của lượt đầu.
- `streaming_latency` ghi **6/6 dòng** cho các bản tin parse được trong hai lượt. Cả 6 dòng có `eh_region=koreacentral`, `dbx_region=eastasia` và đủ ba giá trị độ trễ.
- `stream_pair_counts_10m` ghi **6 cặp**, khớp cả khóa và số lần xuất hiện được tính từ giỏ hàng mẫu. Cửa sổ **11:20–11:30 UTC** ghi 0 dòng trong update đầu. Bản tin nhận lúc 11:33:43 UTC đưa watermark 2 phút qua cuối cửa sổ, rồi update thứ hai ghi đủ 6 dòng.

Workflow `stream-toggle down` hoàn tất và xác nhận namespace của buổi thử đã được gỡ. [Lượt tắt Event Hubs](https://github.com/Vinh5905/ETL_Azure_Databricks/actions/runs/45167028729)
