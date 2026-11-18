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

ADF `pl_master` gọi job này sau khi landing đã công bố xong ([01 §5](01_ingest_landing.md)).

```yaml
# bundles/resources/job_batch.yml
resources:
  jobs:
    batch_medallion:
      name: instacart-batch-medallion
      parameters:
        - { name: adf_run_id, default: manual }
      tasks:
        - task_key: pipeline_batch
          pipeline_task:
            pipeline_id: ${resources.pipelines.instacart_batch.id}
            full_refresh: false

        - task_key: create_views                 # gộp batch ∪ stream (03 §7), idempotent
          depends_on: [{ task_key: pipeline_batch }]
          sql_task:
            file: { path: ../../src/tasks/create_views.sql }
            warehouse_id: ${var.warehouse_id}

        - task_key: collect_dq
          depends_on: [{ task_key: pipeline_batch }]
          sql_task:
            file: { path: ../../src/tasks/collect_dq.sql }
            warehouse_id: ${var.warehouse_id}

        - task_key: reconcile
          depends_on: [{ task_key: create_views }]
          sql_task:
            file: { path: ../../src/tasks/reconcile.sql }
            warehouse_id: ${var.warehouse_id}

        - task_key: refresh_bi
          depends_on: [{ task_key: reconcile }]
          run_if: ALL_SUCCESS                   # đối soát không đạt thì không đẩy số lên dashboard
          job_cluster_key: sn
          spark_python_task:
            python_file: ../../src/tasks/refresh_bi.py
            parameters: ["--adf-run-id", "{{job.parameters.adf_run_id}}"]
      job_clusters:
        - job_cluster_key: sn
          new_cluster:                          # khai đủ trường fixed của policy, không dùng apply_policy_default_values (devops/05 §3 bước 6)
            policy_id: ${var.policy_id}
            spark_version: 15.4.x-scala2.12
            node_type_id: ${var.node_type_id}
            num_workers: 0
            data_security_mode: SINGLE_USER
            spark_conf:
              spark.databricks.cluster.profile: singleNode
              spark.master: "local[*]"
            custom_tags: { ResourceClass: SingleNode, cost_owner: p2 }
```

**Vì sao các task sau pipeline là `sql_task` trên serverless warehouse:**
- Warehouse serverless **không tính vào quota vCPU**.
- Khi cluster của pipeline chưa tắt hẳn (trễ 60 giây), job vẫn chạy tiếp được mà không vượt 4 vCPU.

`refresh_bi` cần Python để gọi REST nên dùng job cluster. Nó chạy sau cùng, lúc đó cluster pipeline đã tắt.

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

Bổ sung vào cả hai pipeline (02 §2.2, 04 §3.1) để event log được lưu thành bảng UC. Người có quyền `SELECT` trên `ops` đọc được, không cần là chủ pipeline:

```yaml
      event_log:
        catalog: ${var.catalog}
        schema: ${var.schema_prefix}ops
        name: event_log_batch          # pipeline stream: event_log_stream
```

```sql
-- src/tasks/collect_dq.sql : MERGE theo (update_id, flow, expectation) → chạy lại không nhân dòng
MERGE INTO instacart.ops.dq_results t
USING (
  SELECT origin.update_id                         AS update_id,
         origin.pipeline_name                     AS pipeline,
         origin.flow_name                         AS flow,
         e.dataset, e.name                        AS expectation,
         sum(e.passed_records)                    AS passed,
         sum(e.failed_records)                    AS failed,
         max(timestamp)                           AS at
  FROM instacart.ops.event_log_batch
  LATERAL VIEW explode(from_json(details:flow_progress.data_quality.expectations,
      'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) x AS e
  WHERE event_type = 'flow_progress' AND timestamp > current_timestamp() - INTERVAL 2 DAYS
  GROUP BY ALL
) s
ON t.update_id = s.update_id AND t.flow = s.flow AND t.expectation = s.expectation
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

Bảng `ops.dq_results` tạo một lần với cột `update_id, pipeline, flow, dataset, expectation, passed, failed, at`.

Câu hỏi thường gặp khi điều tra:

```sql
-- Update gần nhất lỗi ở flow nào, vì sao
SELECT timestamp, origin.flow_name, level, message, error
FROM instacart.ops.event_log_batch
WHERE level = 'ERROR' ORDER BY timestamp DESC LIMIT 20;

-- Mỗi flow xử lý bao nhiêu dòng trong update gần nhất
SELECT origin.flow_name, details:flow_progress.metrics.num_output_rows::bigint AS rows_out
FROM instacart.ops.event_log_batch
WHERE event_type = 'flow_progress' AND details:flow_progress.status = 'COMPLETED'
  AND origin.update_id = (SELECT max_by(origin.update_id, timestamp)
                          FROM instacart.ops.event_log_batch WHERE event_type = 'create_update');
```

## 4. Đối soát tự động `ops.reconciliation`

```sql
-- src/tasks/reconcile.sql
CREATE TABLE IF NOT EXISTS instacart.ops.reconciliation
  (check_date DATE, check_name STRING, expected BIGINT, actual BIGINT, passed BOOLEAN, checked_at TIMESTAMP);

MERGE INTO instacart.ops.reconciliation t
USING (
  WITH c AS (
    SELECT 'bronze.orders' n, 3421083 e, (SELECT count(*) FROM instacart.bronze.orders) a
    UNION ALL SELECT 'bronze.order_products_prior', 32434489, (SELECT count(*) FROM instacart.bronze.order_products_prior)
    UNION ALL SELECT 'silver.order_items_prior distinct', 32434489,
                     (SELECT count(*) FROM (SELECT DISTINCT order_id, product_id FROM instacart.silver.order_items_prior))
    UNION ALL SELECT 'silver.order_items_prior no dup', 0,
                     (SELECT count(*) - count(DISTINCT order_id, product_id) FROM instacart.silver.order_items_prior)
    UNION ALL SELECT 'silver.dim_product', 49688, (SELECT count(*) FROM instacart.silver.dim_product)
    UNION ALL SELECT 'silver.stream_orders', 131209, (SELECT count(*) FROM instacart.silver.stream_orders)
    UNION ALL SELECT 'stream items = bronze train', (SELECT count(*) FROM instacart.bronze.order_products_train),
                     (SELECT count(*) FROM instacart.silver.stream_order_items)
    UNION ALL SELECT 'scd2 open versions dup', 0,
                     (SELECT count(*) FROM (SELECT product_id FROM instacart.silver.product_prices_history
                                            WHERE __END_AT IS NULL GROUP BY 1 HAVING count(*) > 1))
    UNION ALL SELECT 'gold.fact no fanout', 0,
                     (SELECT count(*) - count(DISTINCT order_id, product_id) FROM instacart.gold.fact_order_lines)
  )
  SELECT current_date() AS check_date, n AS check_name, e AS expected, a AS actual,
         (e = a) AS passed, current_timestamp() AS checked_at FROM c
) s
ON t.check_date = s.check_date AND t.check_name = s.check_name
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Làm task fail nếu có kiểm tra không đạt → refresh_bi không chạy
SELECT assert_true(count_if(NOT passed) = 0, 'reconciliation failed')
FROM instacart.ops.reconciliation WHERE check_date = current_date();
```

**Lưu ý:**
- Kiểm tra liên quan đến stream chỉ đạt sau khi đã phát đủ 100%. Trước mốc G3, đặt các dòng đó trong khối `-- G3` và comment lại.
- Nhóm có thể thay số cố định bằng bảng `ops.expected_counts` để không phải sửa SQL.

`MERGE` theo `(check_date, check_name)` làm task idempotent: chạy lại trong ngày chỉ cập nhật, không thêm dòng.

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
| Job batch | Job runs; notification email cho P2 trong `job_batch.yml` | Lỗi, hoặc chạy > 60 phút |
| Chất lượng | Trang 5 Power BI (`ops.dq_results`) | `failed / (passed + failed)` > 1% ở một luật |
| Đối soát | `ops.reconciliation` | Có dòng `passed = false` |
| Chi phí | Cost Management theo tag `owner` (resource Azure) và `cost_owner` (cluster, warehouse) | Budget 50% |
