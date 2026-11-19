# 06 · Vận hành: điều phối, theo dõi, chạy lại khi lỗi

**Đầu vào:** các pipeline ở [01](01_ingest_landing.md)–[05](05_gold_serving.md).

**Đầu ra:**
- Job `instacart-batch-medallion` hoàn chỉnh.
- Event log của pipeline được lưu thành bảng.
- Các bảng `ops.dq_results`, `ops.reconciliation`, `ops.pipeline_runs`.
- Sổ tay chạy lại khi lỗi.

**Xong khi:** đã diễn tập đủ 4 kịch bản lỗi ở mục 7, lần nào cũng chạy lại được mà đối soát vẫn đạt.

---

## 1. Cây quyết định khi có lỗi

![Cây quyết định chạy lại khi lỗi](diagrams/03_chay_lai.png)

<sub>File gốc: [diagrams/de_diagrams.drawio](diagrams/de_diagrams.drawio), trang 3.</sub>

Ba câu hỏi theo thứ tự:
1. **Lỗi ở tầng nào?** Xem ADF Monitor (landing), rồi job run (task nào đỏ), rồi event log của pipeline (flow nào lỗi).
2. **Đã có gì được "công bố" chưa?** Ví dụ: file đã nằm ở thư mục đích, watermark đã tăng, micro-batch đã commit.
3. **Chạy lại phần lỗi có cho cùng kết quả không?** Nếu tầng đó tuân thủ [00 §4](00_tong_quan_de.md) thì có, cứ chạy lại. Nếu không chắc thì kiểm đối soát (mục 4) trước và sau khi chạy.

## 2. Job điều phối batch

ADF `pl_master` gọi job `instacart-batch-medallion` theo ID sau khi landing công bố xong
([01 §5](01_ingest_landing.md)). Bundle định nghĩa job trong
[`jobs_batch.yml`](../../bundles/resources/jobs_batch.yml). ADF truyền `adf_run_id`; khi chạy tay
giá trị mặc định là `manual`. Job chạy một lượt tại một thời điểm và xếp hàng lượt tiếp theo.

| Task | Điều kiện và đầu ra |
|---|---|
| `pipeline_batch` | Chạy update tăng dần của `instacart_batch` trên policy single-node. |
| `resolve_pipeline_update` | Tra `create_update` trong event log theo pipeline ID và task run ID. Trả đúng một `update_id`; báo lỗi khi thiếu hoặc trùng update. |
| `collect_dq` | Nhận `update_id` đã xác định và MERGE counters. Có thể thu metrics của update lỗi nếu event log đã có. |
| `init_reconciliation` | Chỉ chạy khi cả pipeline và collector thành công. Tạo bảng và seed số kỳ vọng còn thiếu. |
| `reconcile` | Chạy 22 check batch sau init. Dùng `job.run_id` và ngày UTC lúc job bắt đầu để repair trong cùng lượt giữ khóa ổn định. |

Task SQL dùng serverless warehouse `wh-instacart-bi` đã có. Tại G3 hiện tại, bundle giữ
`enable_baskets=false`. Hai view, kiểm G3/G4 và `refresh_bi` được nối vào khi có đủ nguồn và task
phụ thuộc. Chạy batch tách thời gian với crawler, seed và stream để vừa quota 4 vCPU.

Khi repair task pipeline, dùng task run ID mới và chạy lại các task phụ thuộc. Khi chỉ repair
`reconcile`, làm lại với cùng job run ID và ngày kiểm tra. Mục 4 mô tả khóa MERGE và cách đọc hàng lỗi.

Cảnh báo lỗi và vượt 60 phút nhận danh sách email từ biến bundle `batch_email_notifications`.
Ở dev, đặt giá trị trong `bundles/.databricks/bundle/dev/variable-overrides.json`. Giá trị của biến là
một object có hai mảng `on_failure` và `on_duration_warning_threshold_exceeded`. CI prod lấy object
từ secret `DATABRICKS_BATCH_EMAIL_NOTIFICATIONS` trong environment `prod`.

**Job của DS không nằm trong batch job.** Bản đầu của hình kiến trúc 2 vẽ `train_rules → evaluate_register` sau `build_gold`; kế hoạch (và hình hiện tại) tách thành job riêng, chạy tay:
- `ds-prepare` (DE sở hữu, [08 §6](08_du_lieu_cho_ds.md)).
- `ds-benchmark`, `ds-champion` (DS sở hữu, [ds/04 §5](../ds/04_thuc_nghiem_danh_gia.md)).

Lý do: dữ liệu DS là bản cố định nên không cần train lại mỗi đêm, và lưới thực nghiệm chạy hàng giờ sẽ chặn pipeline DE trên 4 vCPU. Các job này chạy ngoài giờ batch, và lịch `instacart-stream-15m` phải tạm dừng trong lúc chạy.

`warehouse_id` là biến DAB, lấy bằng lookup:

```yaml
variables:
  warehouse_id: { lookup: { warehouse: wh-instacart-bi } }
```

## 3. Event log và `ops.dq_results`

Cấu hình publish event log UC của batch đã có trong bundle AD-DE-08. Stream dùng cùng schema ops
và tên `event_log_stream` khi AD-ST-03 triển khai:

```yaml
      event_log:
        catalog: ${var.catalog}
        schema: ${var.schema_prefix}ops
        name: event_log_batch          # pipeline stream: event_log_stream
```

### 3.1 Thu metrics bằng collect_dq

[`src/tasks/collect_dq.sql`](../../src/tasks/collect_dq.sql) tạo bảng Delta thường `ops.dq_results`
và MERGE metrics của **một update_id được chỉ định**. Bốn tham số STRING bắt buộc: `catalog`,
`ops_schema`, `event_log_table` (chỉ event_log_batch hoặc event_log_stream), `update_id`.
Chạy toàn bộ file cùng phiên SQL trên warehouse serverless, dùng đúng schema dev đã resolve.

Không UNION tới bảng stream chưa có. Khi stream đã publish log, chạy lại **cùng file** với
`event_log_table=event_log_stream` và update_id của stream. Hai nguồn ghi chung bảng đích.
Job AD-DE-18 truyền update ID từ task tra event log theo đúng pipeline task run.
Collector có thể thu cả update lỗi nếu pipeline đã ghi event. Task đối soát chỉ bắt đầu khi pipeline
và collector đều thành công.

| Cột | Hợp đồng |
|---|---|
| update_id | ID update từ origin |
| pipeline | Ưu tiên origin.pipeline_id, fallback pipeline_name nếu nguồn cũ thiếu ID |
| flow | origin.flow_name |
| dataset | Tên từ chính expectation, fallback origin.dataset_name; không gộp các view trong cùng flow |
| expectation | Tên luật |
| passed / failed | BIGINT, tổng các bộ đếm quan sát trong event log cho luật đó |
| at | TIMESTAMP_LTZ, thời điểm event metrics mới nhất, không phải lúc chạy collector |

Khóa MERGE là **(pipeline, update_id, flow, dataset, expectation)**. Khóa ba cột của ví dụ cũ
không phân biệt được một luật cùng tên trên nhiều dataset trong cùng flow; cũng không tách được pipeline.
Bảng cũ nếu đã tạo theo hợp đồng khác cần audit khóa trùng trước chuyển đổi, không tự DELETE/overwrite lịch sử.

Collector đọc origin/details JSON STRING hoặc STRUCT, giữ event flow_progress có maturity STABLE hoặc NULL.
Nó cộng metrics ở **cả RUNNING lẫn COMPLETED**, vì nhiều streaming flow chỉ emit metrics trong micro-batch RUNNING.
Cùng event ID/cùng nội dung được tính một lần; xung đột ID, thiếu khóa/counter/timestamp, số âm hoặc JSON sai
làm MERGE fail, nguồn event log vẫn giữ nguyên. Không chuyển counter thiếu thành 0.

Phạm vi là toàn bộ event còn giữ cho update_id, **không cắt lookback hai ngày giữa update**.
Chạy lại không nhân dòng; event đến muộn bổ sung tổng của cùng khóa. Snapshot cũ có at nhỏ hơn được bỏ qua;
nguồn cùng/mới hơn nhưng tổng giảm sẽ báo lỗi (điều tra retention hoặc nguồn chưa đầy đủ), không ghi đè kết quả cũ.
Payload không đổi không ghi UPDATE. Đây không phải cơ chế archive: phải giữ đủ log của update khi thu lần đầu.

### 3.2 Đọc số liệu đúng phạm vi

- Metrics là số dòng đã đi qua **operator/expectation**, không phải số dòng cuối của bảng silver.
  Một dòng có thể vi phạm nhiều luật; không cộng failed của mọi luật để suy ra số dòng quarantine.
- View dùng bởi nhiều flow có thể emit nhiều bộ metrics. Đối chiếu đúng pipeline/update/flow/dataset và
  cùng lô nguồn; không cộng xuyên flow hay so một update với toàn bộ quarantine tích lũy.
- MV đánh giá snapshot có thể đếm lại toàn bộ dữ liệu ở mỗi update; streaming view đếm phần được xử lý.
  Timestamp `_quarantined_at` không tự chứng minh cùng input population với metrics.
- Khi expectation fail (`expect_or_fail` / `expect_all_or_fail`) dừng update do vi phạm,
  nền tảng **không ghi metrics đếm**; xem ERROR/violation trong event log. Flow không có dữ liệu mới, không có expectation
  hoặc operator không hỗ trợ có thể không emit metrics. Collector không dựng hàng zero giả cho các trường hợp này.
- Mục tiêu “mọi expectation” được nghiệm thu bằng đối chiếu inventory: mọi metrics đã emit phải vào bảng,
  luật fail/không emit cần được giải thích từ cấu hình và event log, không tuyên bố coverage 100% từ số hàng đếm.

### 3.3 Truy vấn điều tra lưu sẵn

Các câu sau chạy độc lập, nhận cùng tham số log/update. Chọn nguồn batch hoặc stream bằng event_log_table.

```sql
-- Lỗi và violation của update, gồm luật fail không có counter.
WITH events AS (
  SELECT timestamp, level, message, error,
         get_json_object(to_json(named_struct('value', origin)), '$.value') AS o
  FROM IDENTIFIER('`' || replace(:catalog, '`', '``') || '`.`' || replace(:ops_schema, '`', '``')
                  || '`.`' || replace(:event_log_table, '`', '``') || '`')
)
SELECT timestamp, get_json_object(o, '$.flow_name') AS flow, message, error
FROM events WHERE level = 'ERROR' AND get_json_object(o, '$.update_id') = :update_id
ORDER BY timestamp DESC;

-- Status/output mỗi event, không lọc chỉ COMPLETED vì RUNNING có metrics.
WITH events AS (
  SELECT id, timestamp,
    get_json_object(to_json(named_struct('value', origin)), '$.value') AS o,
    get_json_object(to_json(named_struct('value', details)), '$.value') AS d
  FROM IDENTIFIER('`' || replace(:catalog, '`', '``') || '`.`' || replace(:ops_schema, '`', '``')
                  || '`.`' || replace(:event_log_table, '`', '``') || '`')
  WHERE event_type = 'flow_progress'
)
SELECT id, timestamp, get_json_object(o, '$.flow_name') AS flow,
       get_json_object(d, '$.flow_progress.status') AS status,
       try_cast(get_json_object(d, '$.flow_progress.metrics.num_output_rows') AS BIGINT) AS rows_out
FROM events WHERE get_json_object(o, '$.update_id') = :update_id ORDER BY timestamp;

-- Đọc kết quả thu theo từng dataset, tránh cộng lại luật trùng tên giữa các view.
SELECT pipeline, pipeline_name, update_id, flow, dataset, expectation, passed, failed, at
FROM IDENTIFIER('`' || replace(:catalog, '`', '``') || '`.`' || replace(:ops_schema, '`', '``') || '`.`dq_results`')
WHERE update_id = :update_id ORDER BY pipeline, flow, dataset, expectation;
```

Đếm lý do quarantine bằng `explode(_dq_failed)` trên **cùng tập bản ghi nguồn cần so**. Mỗi lý do có
số riêng, một bản ghi có thể có nhiều lý do; số dòng quarantine bằng số bản ghi lỗi duy nhất, không phải
SUM failed qua các luật. Ví dụ ở AD-DE-12: warn assessed header đọc raw snapshot, canonical header có fail gate,
orders_batch drop sau canonical; không lấy một tổng metrics của ba nơi để đối chiếu quarantine raw.

Hướng dẫn chạy hai lần, kiểm dữ liệu xấu/counter thiếu, retention và chốt bằng chứng:
[06b_nghiem_thu_dq.md](06b_nghiem_thu_dq.md). **Chưa nghiệm thu trên workspace**; #44/#48/#24/#22 còn là gate.
Tham khảo [schema event log](https://learn.microsoft.com/en-us/azure/databricks/ldp/monitor-event-log-schema),
[metrics expectations](https://learn.microsoft.com/en-us/azure/databricks/ldp/expectations).

## 4. Đối soát tự động `ops.reconciliation`

Triển khai ở `src/tasks/init_reconciliation.sql`, `reconcile.sql`, `reconcile_g3.sql`,
`reconcile_g4.sql`. Các file chạy SQL trên warehouse serverless; không tự chạy pipeline hoặc cấp quyền.
Tách file theo cổng vì SQL vẫn resolve bảng trong nhánh WHERE=false; không thể dùng điều kiện tắt
để tránh lỗi khi bảng stream/gold chưa tồn tại.

### 4.1 Kỳ vọng theo manifest nguồn

`ops.expected_counts` có profile, metric, expected BIGINT, source và configured_at.
Init seed chín số tham chiếu Instacart 2017 **khi khóa chưa có**, không ghi đè cấu hình đã thay.
Số tham chiếu phải đối chiếu bản Zenodo thực tế; chưa được nghiệm thu trong phiên triển khai này.

Với dữ liệu mẫu, `scripts/expected_counts_sql.py` đọc `_manifest.json` do AD-DE-01 tạo để in SQL MERGE
chín số tổng hợp và SHA-256 manifest. Script không đọc actual từ bronze/silver, không kết nối cloud,
không xuất user_ids. Kiểm số nguyên không âm, tổng prior/train/test khớp orders và nguồn không rỗng.
Không lấy actual làm expected hoặc nhân tất cả số nguồn theo 1%, vì danh mục được giữ toàn bộ.

### 4.2 Các stage và grain kết quả

| Stage / file | Số check | Phạm vi |
|---|---|---|
| batch / reconcile.sql | 22 | Sáu bronze CSV so nguồn; split, clock, header/món prior silver, khóa, payload, orphan |
| G3 / reconcile_g3.sql | 25 | dim_product, phiên bản giá mở, stream train, view hợp nhất và baskets |
| G4 / reconcile_g4.sql | 3 | Số dòng fact, không fanout, cặp khóa gold khớp silver |

Check số dòng dùng chín kỳ vọng độc lập, không hardcode lại theo từng nguồn.
Các check payload dùng **EXCEPT ALL hai chiều** để bắt cả dữ liệu sai nhưng tổng số dòng vẫn đúng.
G3 đối chiếu header stream với bronze train, payload món stream và danh sách/split của giỏ;
G4 đối chiếu khóa fact với silver.order_items. Chỉ đếm phiên bản giá `__END_AT IS NULL` khi tìm trùng.

`ops.reconciliation` giữ check_date/check_name/expected/actual/passed/checked_at và thêm
run_id/profile/stage/config_hash để biết lần chạy/phạm vi. Khóa MERGE vẫn **(check_date, check_name)**:
chạy lại trong ngày ghi lại kết quả mới nhất, không nhân dòng. Không dùng một ops namespace cho các
profile/dataset khác nhau chạy đồng thời. Check_date ISO do caller cố định cho cả sequence, checked_at
là thời điểm kiểm, được thay đổi khi chạy lại.

Hash gồm kỳ vọng/provenance và catalog/ops/bronze/silver/gold schema. G3 yêu cầu đủ 22 check batch xanh,
G4 yêu cầu đủ 47 check trước, cùng run/profile/hash. Thay manifest hoặc namespace cần kiểm lại từ batch.
Thiếu rows/config, config âm/trùng hoặc kết quả cũ không được coi là đạt; COUNT đủ inventory chặn vacuous pass.

MERGE **lưu cả kết quả sai**, sau đó assert_true cuối file làm task fail. Kết quả đỏ còn trong ops để
BI/điều tra; đây là hai transaction, không rollback kết quả sai khi assert đỏ. Repair bằng dữ liệu/expected
đúng nguồn rồi chạy lại, không xóa lịch sử hay đổi số kỳ vọng để che sai.

Nguồn trong reconcile là manifest CSV Instacart của profile tương ứng. Đối soát Azure SQL
`retail_db` và Parquet CDC theo cửa sổ ADF thuộc nghiệm thu AD-DE-07.

Bronze append-only có thể giữ cả bản cũ và file nạp lại. Check so số khóa phân biệt theo manifest.
Check payload chọn bản hợp lệ mới nhất theo khóa rồi dùng một FULL OUTER JOIN và so null-safe.
Số dòng thô dùng để điều tra lượng replay, không quyết định qua cổng. Đo thời gian trên warehouse
khi đầy đủ nguồn G3 và lưu vào báo cáo cùng quy mô dữ liệu.

### 4.3 Chạy và xem kết quả

Chạy init với catalog/ops_schema rồi cấu hình profile. Mỗi file reconcile nhận tám tham số STRING:
`catalog`, `ops_schema`, `bronze_schema`, `silver_schema`, `gold_schema`, `profile`, `run_id`, `check_date`.
Chạy **nguyên file**, dừng ở lỗi đầu tiên. Batch không cần bảng stream/dim/gold; G3 chỉ bật sau full stream;
G4 chỉ chạy khi gold đã triển khai. G3 chưa xanh thì không chạy ds-prepare, G4 chưa kiểm thì không nhận
đã nghiệm thu gold/dashboard. AD-DE-18 tích hợp task dependencies để refresh_bi chỉ chạy khi stage cần thiết đạt.

```sql
SELECT check_name, expected, actual, passed, checked_at, run_id, profile, stage, config_hash
FROM IDENTIFIER('`' || replace(:catalog, '`', '``') || '`.`' || replace(:ops_schema, '`', '``') || '`.`reconciliation`')
WHERE check_date = CAST(:check_date AS DATE) AND run_id = :run_id AND profile = :profile
ORDER BY stage, check_name;
```

Không chỉ lọc current_date rồi nhìn mọi hàng xanh: các stage có thể thuộc các run/profile khác nhau.
Hướng dẫn manifest/params, chạy hai lần, thử sai kỳ vọng, repair và chốt bằng chứng:
[06c_nghiem_thu_reconciliation.md](06c_nghiem_thu_reconciliation.md).
**Chưa nghiệm thu trên warehouse/G3 thật**; #50/#51/#70 còn là gate hoàn tất.

## 5. Bộ công cụ chạy lại

| Công cụ | Làm gì | Lệnh / thao tác |
|---|---|---|
| ADF *Rerun from failed activity* | Chạy tiếp từ activity lỗi, **giữ output** của activity đã thành công (ví dụ `to_lsn` của Lookup) | ADF Studio → Monitor → Pipeline runs → biểu tượng rerun |
| Databricks *Repair run* | Chỉ chạy lại task lỗi và các task phụ thuộc nó; task đã thành công không chạy lại | Job run → **Repair run**; hoặc `databricks jobs repair-run <run_id> --rerun-all-failed-tasks` |
| Pipeline update thường | Xử lý dữ liệu mới từ checkpoint; MV tính lại | `databricks bundle run instacart_batch` |
| **`refresh_selection`** | Chỉ update một số bảng và các bảng phía sau | `databricks pipelines start-update <id> --json '{"refresh_selection": ["instacart.gold.product_360"]}'` |
| **`full_refresh_selection`** | Xóa checkpoint + dữ liệu của **bảng được chọn**, tính lại từ đầu nguồn | `--json '{"full_refresh_selection": ["instacart.silver.product_prices_history"]}'` |
| Đổi tên flow | Checkpoint mới cho riêng một nguồn, bảng giữ nguyên | Ví dụ `kafka_{eh_generation}` ([04 §3.2](04_streaming.md)) |
| Time travel | Đọc bảng như ở version cũ, để so sánh | `SELECT … FROM t VERSION AS OF 12` / `TIMESTAMP AS OF '…'` |
| `RESTORE` | Đưa bảng **thường** (không do pipeline quản lý) về version cũ | `RESTORE TABLE instacart.ops.reconciliation TO VERSION AS OF 3` |

> **Quan trọng:** repair hay rerun **không** tự làm task trở nên idempotent. Databricks cảnh báo: task đã ghi một phần rồi lỗi, chạy lại có thể sinh dữ liệu trùng. Chạy lại an toàn được là nhờ thiết kế từng tầng (00 §4), không nhờ nút bấm.

**Bảng do pipeline quản lý** (streaming table, MV) không sửa bằng `RESTORE` / `UPDATE` / `DELETE` tay, vì checkpoint của pipeline sẽ lệch với dữ liệu. Muốn sửa thì sửa code hoặc dữ liệu nguồn, rồi dùng `refresh_selection` / `full_refresh_selection`. Time travel vẫn dùng để **đọc** version cũ khi điều tra.

## 6. Ma trận lỗi đầy đủ

| # | Lỗi ở | Triệu chứng | Cách chạy lại | Vì sao không trùng / không mất |
|---|---|---|---|---|
| 1 | ADF copy Zenodo đứt | Activity Copy đỏ; `_staging` có file dở | Rerun from failed activity | Chưa có `_SUCCESS`; thư mục đích tất định ([01 §2](01_ingest_landing.md)) |
| 2 | ADF copy CDC lỗi | `etl.run_log` không có dòng mới | Rerun from failed activity | Watermark chưa tăng → cùng cửa sổ LSN ([01 §3.6](01_ingest_landing.md)) |
| 3 | Lỗi sau công bố CDC, trước SP watermark | File ở `window=A_B`, watermark cũ | Rerun from failed (chỉ chạy SP) | Lookup giữ `to_lsn` cũ. Rerun **cả** pipeline thì silver loại trùng ([03 §2.2](03_silver_cdc_scd_dq.md)) |
| 4 | CDC quá retention | `mode = snapshot` bất ngờ | Không cần làm gì | Snapshot lại; SCD2 coi như upsert |
| 5 | OFF bị 429 kéo dài | Nhiều dòng `retry` trong `off_crawl_state` | Chạy lại job `off_crawler` hôm sau | Trạng thái theo sản phẩm; tên file tất định |
| 6 | Pipeline batch mất cluster / OOM | Task `pipeline_batch` đỏ | Repair run | Checkpoint chỉ ghi micro-batch đã commit; MV tính lại |
| 7 | File nguồn có dòng hỏng | `_rescued_data` khác null; quarantine tăng | Không dừng; điều tra `quarantine_*` | Dòng hỏng được giữ, không lẫn vào silver |
| 8 | `expect_or_fail` dừng pipeline | Event log ERROR có tên expectation | Sửa nguồn/code → chạy lại update | Flow lỗi rollback; bảng khác giữ nguyên |
| 9 | Sai logic silver/gold đã chạy | Đối soát lệch | Sửa code → `full_refresh_selection` các bảng đó | Bronze còn đủ (reset bị chặn) → dựng lại đúng |
| 10 | Lỡ bấm *Full refresh all* | Update dài bất thường | Chờ xong | Bronze có `reset.allowed = false`, không bị xóa |
| 11 | Event Hubs bị xóa khi còn sự kiện chưa đọc | Thiếu đơn trong `stream_orders` | Tạo lại hub, `eh_generation` mới, phát lại phần thiếu | Nguồn sự thật là file train; `AUTO CDC` theo `order_id` |
| 12 | Checkpoint stream hỏng | Pipeline stream lỗi đọc checkpoint | Đổi `eh_generation` (flow mới) rồi phát lại toàn bộ | Bronze giữ dữ liệu cũ; silver upsert theo khóa |
| 13 | Đối soát không đạt | Task `reconcile` đỏ, `refresh_bi` bị bỏ | Điều tra theo `check_name`, sửa, Repair run | Dashboard vẫn giữ số của lần đạt trước |
| 14 | Ghi sai bảng thường (`ops.*`, `off_crawl_state`) | Số liệu lạ | `DESCRIBE HISTORY` → `RESTORE TABLE … TO VERSION AS OF n` | Delta giữ version cũ (mặc định 7 ngày) |
| 15 | Job `ds-prepare` lỗi giữa chừng | Task đỏ; `_manifest.json` chưa có | Repair run | Snapshot chặn ghi lần hai; index/transactions tính lại tất định; manifest ghi sau cùng ([08 §8](08_du_lieu_cho_ds.md)) |

## 7. Diễn tập lỗi (bằng chứng cho báo cáo)

Mỗi kịch bản cần ghi đủ 4 thứ: **ảnh lỗi**, **lệnh chạy lại**, **ảnh đối soát trước/sau**, **giải thích ngắn**.

| Kịch bản | Cách gây lỗi | Kỳ vọng sau khi chạy lại |
|---|---|---|
| A. CDC đứt giữa chừng | Tạm đổi tên linked service SQL sau khi Lookup chạy → Copy lỗi | Sửa lại, rerun from failed → `run_log` có đúng một dòng `window`, silver không trùng |
| B. Pipeline chết giữa update | Bấm *Stop* khi `order_items_prior` đang chạy | Repair run → `silver.order_items_prior` = 32.434.489, không trùng |
| C. Phát lại toàn bộ stream | `simulate.py --run-id run-replay` | `stream_orders` vẫn 131.209; bronze tăng theo `replay_run_id` |
| D. Sửa logic và tính lại | Đổi ngưỡng `matched` 85 → 80 | `refresh_selection` `product_off_match` + `dim_product` → số `matched` tăng, `dim_product` vẫn 49.688 |

## 8. Theo dõi hằng ngày

| Xem gì | Ở đâu | Ngưỡng cảnh báo |
|---|---|---|
| `pl_master` thành công | ADF Monitor; email khi lỗi (Web activity) | Bất kỳ lỗi nào |
| Job batch | Job runs; notification email trong `jobs_batch.yml` | Lỗi, hoặc chạy > 60 phút |
| Chất lượng | Trang 5 Power BI (`ops.dq_results`) | `failed / (passed + failed)` > 1% ở một luật |
| Đối soát | `ops.reconciliation` | Có dòng `passed = false` |
| Chi phí | Cost Management theo tag `owner` (resource Azure) và `cost_owner` (cluster, warehouse) | Budget 50% |
