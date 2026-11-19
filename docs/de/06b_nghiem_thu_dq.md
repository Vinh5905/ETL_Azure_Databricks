# Nghiệm thu collect_dq — AD-DE-16

## Phạm vi và điều kiện

[`collect_dq.sql`](../../src/tasks/collect_dq.sql) đọc event log UC, tạo bảng Delta thường
`ops.dq_results` và MERGE counters của một update. Không sửa pipeline/nguồn/quarantine hay cấp quyền.
Hợp đồng và các truy vấn điều tra nằm ở [DE/06 §3](06_van_hanh_chay_lai.md#3-event-log-và-opsdq_results).

Cần nghiệm thu #44 (pipeline/event log), #48 (silver/DQ), #24 (warehouse) và #22 (quyền UC).
Chưa có CLI/profile được chọn thì tiếp tục kiểm local, không tự dùng profile đầu tiên.
Stream chỉ thu sau AD-ST-03 publish `event_log_stream`; không tạo bảng log giả ở schema thật.
AD-DE-18 sẽ tích hợp task/parameters và lấy update ID từ đúng lần chạy, kể cả update lỗi.

## Chạy trên warehouse dev

Mở nguyên SQL file trên warehouse **serverless** `wh-instacart-bi`, chạy toàn bộ năm câu trong cùng phiên.
Danh tính cần đọc log và tạo/MODIFY bảng đích trong ops theo quyền của P1; không tự mở rộng grants.
Schema ops phải tồn tại sẵn theo bundle/infra. Truyền bốn tham số STRING, không bọc tên bằng backtick:

| Tham số | Ví dụ dev | Ghi chú |
|---|---|---|
| catalog | instacart | Catalog của bundle đã resolve |
| ops_schema | p2_ops | Schema ops dev đã resolve, không có mặc định prod |
| event_log_table | event_log_batch | Chỉ batch hoặc event_log_stream đã publish |
| update_id | ID lấy từ đúng update trên UI/job | Không truyền job run ID/ADF run ID thay pipeline update ID |

File kiểm tham số, chọn namespace, CREATE TABLE IF NOT EXISTS, MERGE và trả summary theo pipeline/update.
Nếu bước USE lỗi thì dừng, không chạy riêng câu CREATE/MERGE ở namespace đang mở.
Khi có stream: chạy lại cùng file với event_log_table=event_log_stream và update_id stream, không UNION
với bảng chưa tồn tại. Không chạy đồng thời nhiều collector ghi cùng target khi nghiệm thu.

## Kiểm chạy lại và late metrics

1. Đợi update kết thúc và event log đã emit đủ. Ghi commit, catalog/schema/log, pipeline ID/update ID,
   trạng thái update và thời điểm nguồn được chốt. Collector không có cửa sổ hai ngày cắt ngang update.
2. Chạy file lần đầu, lấy toàn bộ tám cột dq_results của update bằng query ở DE/06 §3.3.
   Kiểm khóa `(pipeline, update_id, flow, dataset, expectation)` không trùng, passed/failed không âm/NULL.
3. Chạy lần hai với cùng params và log không đổi: số dòng và payload phải không đổi, không chỉ so tổng COUNT.
   `at` là max event timestamp nên không đổi theo thời điểm collector chạy. Payload giống hệt không UPDATE lại.
4. Sau event metrics mới của **cùng update** xuất hiện, chạy collector lại: tổng được tính lại từ đầy đủ
   events còn giữ, cùng khóa không sinh hàng mới. Chạy thêm update mới: xuất hiện bộ khóa update mới.
5. Nếu log cùng/mới hơn nhưng tổng thấp hơn kết quả đã thu, MERGE fail và rollback: kiểm retention,
   log bị cắt hoặc chọn sai nguồn. Không DELETE kết quả cũ để che lỗi. Snapshot thực sự cũ (at thấp hơn)
   không ghi đè dữ liệu mới. Giữ đủ/archive log trước lần thu đầu; collector không thể dựng lại event đã mất.

Khóa được mở rộng so với ví dụ ba cột cũ vì một flow có thể có cùng tên luật ở nhiều dataset.
Pipeline dùng ID nếu có; fallback tên chỉ dành cho nguồn cũ, tên không bảo đảm duy nhất như ID.
Nếu đã có dq_results do bản khác tạo: kiểm schema, NULL và khóa trùng trước chuyển đổi; script không tự
migrate/dọn dữ liệu cũ. Kết quả trên update đang chạy là **tạm thời**, không là bằng chứng G3 hoàn chỉnh.

## Đối chiếu expectations và quarantine

- Đối chiếu mọi metrics thật đã emit theo đúng flow/dataset với dq_results. Khoanh rõ luật không emit:
  không có dữ liệu mới, view chưa được đọc, operator không hỗ trợ hoặc fail dừng update khi gặp vi phạm.
  Không coi hàng vắng là passed=failed=0; zero chỉ xuất hiện nếu event thực sự mang counter zero.
- Kiểm ERROR/violation bằng query đã lưu, đặc biệt các gate fail trong AD-DE-12. Nếu cần chỉnh cấu hình
  metrics, kiểm `pipelines.metrics.flowTimeReporter.enabled` trên workspace với P1 trước thay đổi.
- Chọn lô dữ liệu xấu **đã biết số bản ghi**, chỉ ở nguồn dev, không sửa file landing đã công bố.
  Lưu khóa/source file/input window và các `_dq_failed` tương ứng. Một dòng vi phạm hai luật làm tăng
  failed ở hai luật nhưng chỉ là một dòng quarantine; không cộng các bộ đếm luật để đếm dòng lỗi.
- So đúng input population: warn header assessed đọc raw snapshot; canonical header dùng fail gate;
  streaming quarantine giữ raw được xử lý; mỗi flow/view có thể đếm lại dữ liệu khác nhau. Không dùng
  toàn bộ quarantine lịch sử hay chỉ timestamp gần nhau làm bằng chứng bằng nhau với một update.
- Malformed JSON/thiếu key/count/timestamp, counters âm, cùng event ID khác nội dung hoặc duplicate
  expectation trong cùng event làm collector fail. Log gốc vẫn còn để điều tra, không bị bỏ im lặng.

Nghiệm thu ghi issue #52: kết quả hai lần chạy, sample key/counters/at, bằng chứng không trùng,
inventory metrics có/vắng và lý do, đối chiếu quarantine cùng lô theo từng luật, ERROR nếu có,
kết quả batch/stream khi stream đã sẵn sàng. PBI trang 5 phải giữ grain dataset/flow/pipeline, không
cộng mọi counter thành số bản ghi nguồn hoặc số dòng bị loại.

## Kiểm local và giới hạn

```bash
uv sync --frozen
uv run pytest tests/test_collect_dq.py -q
uv run ruff check .
uv run ruff format --check .
```

Dev dependencies pin Delta Spark 3.3.3 cho Spark 3.5. Fixture chung bật Delta extension/catalog và
snapshotPartitions=1 để test nhỏ; đây không phải cấu hình deploy. JAR được tải lần đầu từ Maven Central
qua `tests/fixtures/ivysettings.xml` và cache Ivy, nên lần đầu cần mạng ngoài phần uv sync.
Tests chạy nguyên file SQL, CREATE/MERGE Delta thật, JSON/STRUCT, late metrics và rollback dữ liệu lỗi.
Nguồn metrics là dữ liệu tự dựng, không phải event log của engine LDP.

## Kết quả trên dev

Ngày 2026-11-19, chạy `instacart_batch` với Bronze/Silver thực của AD-DE-11 và AD-DE-12.
Lượt dev thường hoàn tất với checkpoint cũ và chỉ phát 2 event có expectation. Lượt có số đo dùng schema dev cách ly để đọc toàn bộ nguồn mà không reset bảng đang dùng.
Event log là `23521394_dqreview_ops.event_log_batch`. Kết quả lấy trên [update batch](https://adb-7405615740735217.17.azuredatabricks.net/#joblist/pipelines/845539d8-e9cd-4ee5-b6dc-e9dd9bb5c8ff/updates/aabb2085-a4a4-4449-a192-9d593e0e5d44).

Chạy `collect_dq.sql` hai lần trên warehouse dev. Event log có **47 khóa**
expectation. `dq_results` khớp đầy đủ khóa và counter với từng event đã phát, có
**270.271.755 lượt đạt** và **0 lượt lỗi**.
Lượt hai giữ nguyên số dòng, counter, tên pipeline và timestamp của từng khóa.

| Bảng quarantine | Số dòng | Số lỗi theo `_dq_failed` | Tổng `failed` từ expectation tương ứng |
|---|---:|---:|---:|
| `quarantine_orders` | 0 | 0 | 0 (`order_headers_assessed`) |
| `quarantine_order_items_prior` | 0 | 0 | 0 (`order_items_prior_valid`) |
| `quarantine_product_prices` | 0 | 0 | 0 (`product_prices_valid`) |
| `quarantine_promotions` | 0 | 0 | 0 (`promotions_valid`) |

Bốn bảng quarantine đều rỗng và expectation tương ứng đều ghi 0 lỗi. Các flow quarantine lọc dòng lỗi nên dùng counter của view kiểm nguồn để đối chiếu.

| Flow | Dataset | Số khóa expectation |
|---|---|---:|
| `order_items_prior_scd1` | `order_items_prior_valid` | 6 |
| `product_prices_scd2` | `product_prices_valid` | 9 |
| `promotions_scd1` | `promotions_valid` | 10 |
| `order_time_sim` | `order_time_sim` | 1 |
| `order_time_sim` | `order_headers_assessed` | 10 |
| `order_time_sim` | `order_headers_canonical` | 10 |
| `orders_batch` | `orders_batch` | 1 |

Còn chờ `event_log_stream` để nghiệm thu stream và tích hợp job điều phối.

### Nâng bảng dev đã tạo từ bản thử trước review

Bảng mới tự tạo có `pipeline_name`. Nếu bảng dev cũ còn tám cột, kiểm `DESCRIBE TABLE` rồi
chạy một lần `ALTER TABLE <catalog>.<ops_schema>.dq_results ADD COLUMNS (pipeline_name STRING)`.
MERGE sau đó cập nhật tên ngoài khóa, giữ nguyên các tổng đã thu. Trang BI dùng
`coalesce(pipeline_name, pipeline)` để hiển thị các event thiếu tên.
