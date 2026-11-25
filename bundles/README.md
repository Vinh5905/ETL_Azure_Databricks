# Bundle Instacart — nghiệm thu pipeline batch (AD-DE-08)

`resources/pipeline_batch.yml` khai báo `instacart_batch`: classic single-node theo G0,
edition Advanced, channel Current, chạy triggered, không Photon. Policy riêng
`instacart-pipeline-single-node` yêu cầu tag `cost_owner: p2` và biến máy phải khớp AD-OPS-21.
Runtime do pipeline quản lý, không gắn DBR của job vào pipeline. Cluster tắt sau 60 giây kể từ khi update xong;
không chạy đồng thời với job khác dùng quota 4 vCPU. SQL warehouse vẫn là serverless.

| Target | Tên pipeline | Schema mặc định | Event log |
|---|---|---|---|
| dev | `[dev <user>] instacart-batch` | `<user>_bronze` | `<catalog>.<user>_ops.event_log_batch` |
| prod | `instacart-batch` | `bronze` | `<catalog>.ops.event_log_batch` |

Dev dùng `${workspace.current_user.short_name}_`, bật development và tắt retry update/flow
để lỗi cấu hình dừng sớm. Prod giữ retry mặc định và chạy bằng SP CI như cấu hình chung.
CI deploy dev bằng SP CI nên `<user>` khi đó là short name của SP, khác prefix local.

## Phạm vi hiện tại

- Glob chỉ nạp source trong `src/pipelines/batch/`; `root_path` là `src/` để import `common`.
  Khi các task sau thêm Python/SQL/notebook vào batch, chúng nằm trong glob này.
- `bootstrap.py` tạo một materialized view **private** `_batch_bootstrap`, đúng một dòng
  cấu hình, không công bố bảng nghiệp vụ vào UC và không đọc landing. Nó giúp update smoke
  và event log có đồ thị chạy trước khi có bronze. Không dùng kết quả này để xác nhận ETL.
- `bronze.py` (AD-DE-09) nạp sáu bảng CSV Instacart từ landing.
  Xem [nghiệm thu bronze](../docs/de/02b_nghiem_thu_bronze.md).
- `silver_instacart.py` (AD-DE-12) thêm clock, header batch, AUTO CDC món prior và hai quarantine;
  pipeline đặt session UTC. Xem [nghiệm thu đơn hàng](../docs/de/03c_nghiem_thu_orders.md).
- `silver_retail.py` (AD-DE-11) thêm AUTO CDC SCD2 giá, SCD1 khuyến mãi và hai quarantine.
  Xem [nghiệm thu retail](../docs/de/03b_nghiem_thu_retail.md).
- Job batch chạy SQL vận hành bằng notebook Spark serverless. Warehouse dùng cho truy vấn nghiệm thu và BI.
- Schema prod, catalog, volume, warehouse, policy và quyền do Terraform/P1 quản lý.
  Schema dev tạo một lần bằng DDL bên dưới; bundle cleanup không sở hữu/xóa bốn schema này.

## Trước khi chạy dev

1. AD-OPS-19 (#21), AD-OPS-20 (#22), AD-OPS-21 (#23) phải được nghiệm thu: có catalog,
   managed storage, volume raw, hai policy `instacart-single-node` / `instacart-pipeline-single-node`
   và warehouse `wh-instacart-bi` (AD-OPS-22 #24) để các lookup của bundle resolve được.
   Job batch không dùng warehouse; biến `warehouse_id` phục vụ các `sql_task` của job DS.
2. Có Databricks CLI hiện hành và đăng nhập bằng **profile người chạy đã chọn**. Không tự lấy
   profile đầu tiên. Cài/đăng nhập theo `databricks-core` hoặc `/databricks:setup` nếu còn thiếu.
3. Danh tính deploy/run có `USE CATALOG`, `CREATE SCHEMA` trên catalog; trên các schema dev
   cần `USE SCHEMA`, `CREATE TABLE`, `CREATE MATERIALIZED VIEW`. Tạo schema dưới cùng danh tính
   giúp người chạy sở hữu schema. Landing cần `READ VOLUME` khi các tầng sau bắt đầu đọc nguồn.
   Nếu CI dùng prefix riêng của SP, chuẩn bị schema và quyền cho SP trước deploy CI.

## Tạo schema và kiểm tra

Các lệnh bundle dưới đây chạy trong `bundles/`. Thay `<profile>` bằng profile đã chọn:

```bash
databricks bundle validate --strict -t dev --profile <profile> -o json
```

Đọc `schema_prefix` đã resolve trong cấu hình (đúng short name của danh tính deploy).
Ví dụ prefix đã resolve là `p2_`, chạy từ gốc repo:

```bash
python3 scripts/dev_schema_sql.py --catalog instacart --schema-prefix p2_ > /tmp/dev-schemas.sql
```

Chạy nội dung SQL vừa tạo trong SQL editor trên warehouse `wh-instacart-bi`, bằng đúng
danh tính deploy. Script chỉ in bốn câu `CREATE SCHEMA IF NOT EXISTS`, không kết nối cloud,
không cấp quyền và từ chối prefix rỗng. Nếu đổi catalog/prefix qua `--var` hoặc `BUNDLE_VAR_*`,
phải dùng cùng giá trị lúc validate, tạo schema, deploy và run.

Sau khi tạo schema, quay lại `bundles/`:

```bash
databricks bundle validate --strict -t dev --profile <profile>
databricks bundle deploy -t dev --profile <profile>
databricks bundle run -t dev --profile <profile> instacart_batch
```

Kiểm tên pipeline, schema mặc định, policy, số worker và event log trong cấu hình đã resolve
trước deploy. Chỉ chạy update khi không có job/pipeline khác chiếm quota. Trong SQL editor,
thay `p2_ops` bằng schema đã resolve:

```sql
SELECT timestamp, event_type, level, message
FROM instacart.p2_ops.event_log_batch
ORDER BY timestamp DESC
LIMIT 20;
```

Ghi lại pipeline ID, update ID, trạng thái update, tên đầy đủ event log và kết quả query trên
issue #44. Event log chỉ được xác nhận sau update thành công; validate/deploy không đủ.
Quyền đọc event log cho vai khác vẫn theo AD-OPS-20; không tự mở rộng grant ở bundle.

## Trạng thái nghiệm thu

Xem kết quả validate, deploy, smoke update và đọc event log trên [issue AD-DE-08](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/44).
Smoke update kiểm đường chạy của pipeline và event log. Đối soát dữ liệu ETL thực hiện ở các task bronze, silver và gold.

AD-DE-13 bổ sung notebook `src/pipelines/batch/silver_off.py` được glob hiện tại nạp:
`off_products_clean`, `product_off_match` và quarantine trong schema silver đã resolve.
Notebook cài RapidFuzz ở ô đầu; không thêm cluster/job. Cách lấy số thật và nghiệm thu:
[03d_nghiem_thu_off.md](../docs/de/03d_nghiem_thu_off.md). Số đo theo nhóm nằm ở
[data dictionary §5.6](../docs/data/data_dictionary.md#56-cách-join-ghép-theo-tên-qua-bảng-cầu-nối).

AD-DE-14 luôn đăng ký MV `dim_product` và chỉ đọc ba nguồn OFF khi bật `enable_off`. MV dùng
`as_of_date` để chọn khuyến mãi. Giá hiện hành lấy từ SCD2, OFF/ảnh chỉ dùng matched, có fail guard
cho fanout và xung đột nguồn. Không thêm cluster/job. Cách đối soát 49.688 khóa và coverage giá thật:
[03e_nghiem_thu_dim_product.md](../docs/de/03e_nghiem_thu_dim_product.md). Đã kiểm trên dev với OFF tắt và bật.

AD-DE-17 có init_reconciliation/reconcile/reconcile_g3/reconcile_g4 trong `src/tasks/`,
chạy SQL warehouse theo profile kỳ vọng nguồn và run_id/check_date chung. Các stage đọc nguồn
batch/G3/G4 tách riêng để không resolve bảng chưa có; AD-DE-18 sẽ tích hợp điều phối, chưa tạo job mới.
Cách lấy kỳ vọng từ manifest và kiểm đủ inventory trước refresh_bi/ds-prepare:
[06c_nghiem_thu_reconciliation.md](../docs/de/06c_nghiem_thu_reconciliation.md). Hai lượt batch trên
warehouse dev đều đạt 22 check. G3/G4 còn chờ nguồn.

AD-DE-16 có `src/tasks/collect_dq.sql`, nhận catalog/ops_schema/event_log_table/update_id,
thu batch hoặc stream vào cùng bảng Delta `ops.dq_results`. Task chạy trên warehouse serverless,
do AD-DE-18 tích hợp điều phối sau; không tạo job/cluster trong thay đổi này.
Hợp đồng counters, khóa có dataset và cách nghiệm thu chạy hai lần/quarantine:
[06b_nghiem_thu_dq.md](../docs/de/06b_nghiem_thu_dq.md). Đã kiểm batch trên warehouse dev với
47 khóa expectation và bốn bảng quarantine. Còn chờ event log stream.

AD-DE-15 thêm `baskets` và views UC hợp nhất. Mặc định `enable_baskets=false`; chỉ bật
khi hai bảng stream tồn tại đúng schema. Xem [nghiệm thu baskets](../docs/de/03f_nghiem_thu_baskets.md).

## Nghiệm thu pipeline stream (AD-ST-03)

[Cấu hình stream](resources/pipeline_stream.yml) tạo `instacart_stream` và job
`stream_schedule` (tên hiển thị `instacart-stream-15m`). Pipeline dùng classic single-node,
policy `instacart-pipeline-single-node` và tag chi phí `p3`. Event log là
`<catalog>.<schema_prefix>ops.event_log_stream`. Glob chỉ nạp `src/pipelines/stream/`.
View riêng tư `_stream_bootstrap` trả một dòng cấu hình để kiểm update và event log
trước khi AD-ST-04 bổ sung nguồn Kafka.

1. Trong `bundles/`, thay `<profile>` bằng profile Databricks đã chọn và chạy validate.

   ```bash
   databricks bundle validate --strict -t dev --profile <profile> -o json
   ```

2. Lấy `schema_prefix` từ kết quả JSON và tạo các schema dev cho cùng danh tính
   theo [hướng dẫn ở trên](#tạo-schema-và-kiểm-tra). Sau đó deploy bundle.

   ```bash
   databricks bundle deploy -t dev --profile <profile>
   ```

3. Kiểm pipeline và job hiển thị với tiền tố `[dev <user>]` và lịch job ở `PAUSED`.
   Ghi kết quả validate và deploy trên [#66](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/66).

Khi nghiệm thu [AD-ST-04 (#67)](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/67),
chạy job bằng tay để kiểm update, bản ghi event log trong schema `ops` dev và dữ liệu Kafka.

```bash
databricks bundle run -t dev --profile <profile> stream_schedule
```

Dev luôn tạm dừng lịch. Prod mặc định `PAUSED`. Job lên lịch mỗi 15 phút theo
`Asia/Ho_Chi_Minh`, lượt 01:45 kế tiếp là 03:00. Job cho phép một lượt chạy tại một
thời điểm và bỏ hàng đợi để tránh chạy dồn các lượt cũ. Lượt đang chạy cần được
phối hợp với lịch batch theo quota 4 vCPU.

`eh_region` mặc định là `koreacentral`, region của Event Hubs Sub B theo
[kết quả spike](../docs/devops/spike_results.md). Khi AD-ST-04 đã có source Kafka,
cấu hình CI prod với `dbx_region` đúng region workspace và `eh_generation` ứng
với lần tạo hub. Mỗi lần hub được tạo lại, cấp mã thế hệ mới
trong lần deploy tiếp theo. Đặt `stream_schedule_pause_status=UNPAUSED` khi hub hoạt
động và pipeline chạy từng đợt. Tạm dừng lịch trước khi tắt hub hoặc trước khi CI triển khai
`stream_continuous=true` để demo. Trước demo, phối hợp dừng lịch batch và xác nhận
các lượt batch đã kết thúc.

Tham khảo: [resource bundle chính thức](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/resources),
[event log pipeline](https://learn.microsoft.com/en-us/azure/databricks/ldp/monitor-event-logs).
