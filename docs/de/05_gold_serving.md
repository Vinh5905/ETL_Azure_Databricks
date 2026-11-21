# 05 · Tầng gold: bảng phục vụ BI, tối ưu lưu trữ, benchmark

**Đầu vào:**
- Silver đạt G3 ([03](03_silver_cdc_scd_dq.md)).
- SQL warehouse `wh-instacart-bi` hoặc lớp serving dự phòng ([devops/06 §4](../devops/06_de_den_dashboard.md)).

**Đầu ra:**
- Các materialized view trong `gold` mà Power BI đọc.
- Bảng `ops.benchmark_results` (rubric 3.1, 3.4).

**Xong khi:**
- Mọi bảng gold Power BI dùng đều nhỏ (≤ vài triệu dòng), đã tổng hợp sẵn và có kiểm tra tổng khớp với silver.
- Benchmark có đủ 4 định dạng.

---

## 1. Nguyên tắc thiết kế gold

1. **Gold phục vụ câu hỏi, không phục vụ bảng nguồn.** Mỗi bảng gắn với một trang hoặc visual trong Power BI ([07](07_power_bi.md)).
2. **Tổng hợp sẵn.** Power BI không quét 33 triệu dòng mỗi lần mở trang. Bảng lớn nhất Power BI đọc là `department_sales` (cỡ trăm nghìn dòng).
3. **Tính lại được hoàn toàn từ silver.** Gold là materialized view, nên chạy lại bao nhiêu lần cũng ra cùng kết quả.
4. **Một bảng fact chi tiết làm nền.** `fact_order_lines` (đã gắn giá theo thời điểm) được tính một lần; các bảng tổng hợp đọc từ đó, không lặp lại phép join đắt.

| Bảng gold | Hạt (grain) | Dùng ở | Cỡ ước tính |
|---|---|---|---|
| `fact_order_lines` | 1 dòng / món trong đơn | Nền cho bảng khác (Power BI **không** đọc) | 33,8 triệu |
| `kpi_daily` | ngày × nguồn | Trang 1 | Vài nghìn |
| `kpi_by_dow_hour` | thứ × giờ | Trang 1, 3 | 168 |
| `department_sales` | ngày × aisle | Trang 4, RLS | Khoảng vài trăm nghìn |
| `product_360` | sản phẩm | Trang 2, dim sản phẩm | 49.688 |
| `product_pair_counts` | cặp sản phẩm (top) | Trang 2 | ≤ 200 nghìn |
| `dim_date`, `dim_department` | ngày / ngành | Dim chung | Nhỏ |
| `bi_user_access` | email × department | RLS | Nhỏ |
| `stream_kpi_1m`, `stream_pair_counts_10m` | phút / cửa sổ 10 phút | Trang 5, tile real-time | Nhỏ ([04](04_streaming.md)) |
| View DS: `association_rules`, `recommendations`, `experiment_metrics`, `holdout_metrics` | luật / cặp gợi ý / tổ hợp thực nghiệm | Trang 2, 6, 7 | Nhỏ. **View**, do job DS tạo ([ds/05 §4](../ds/05_mlflow_serving_bao_cao.md)), không thuộc pipeline `instacart_batch` |

## 2. Code

### 2.1 Fact có giá theo thời điểm (point-in-time join)

[`gold.py`](../../src/pipelines/batch/gold.py) khai báo MV `gold.fact_order_lines` trong
pipeline batch, phân cụm theo `order_date, department_id`.
[`build_fact_order_lines`](../../src/common/gold.py) đọc hai view hợp nhất
`silver.orders` và `silver.order_items`, khớp header theo `order_id, source` rồi lấy ngành hàng
và aisle từ `silver.dim_product`.

- Giá lấy từ `silver.product_prices_history` theo khoảng nửa mở
  `__START_AT <= order_ts_sim < __END_AT`. `__END_AT IS NULL` là phiên bản còn hiệu lực.
- Từ các ngày có header, gộp khuyến mãi SCD1 theo `product_id, order_date` và lấy mức giảm cao
  nhất trong các khoảng bao gồm ngày đó. Mỗi dòng fact vì vậy nhận tối đa một mức giảm.
- `revenue_sim` là doanh thu giả lập, tính bằng giá theo thời điểm nhân với phần trăm còn lại
  sau khi giảm, làm tròn 2 chữ số. Khi không có khuyến mãi, `discount_pct` là NULL và doanh thu
  lấy bằng giá. Lịch sử giá cố định theo SCD2, còn khuyến mãi dùng trạng thái SCD1 hiện có.
- Giữ cả món thiếu dữ liệu tham chiếu để kiểm lỗi. Thiếu header hoặc danh mục làm flow lỗi.
  Giá NULL được ghi vào metrics expectation `priced` và làm check G4 thất bại.

**Bật sau khi Silver sẵn sàng.** Mặc định `enable_gold_fact=false` để các lượt bootstrap batch
có thể chạy trước stream. Sau khi có hai view hợp nhất và giá Silver, chỉ dùng
`BUNDLE_VAR_enable_gold_fact=true` để validate và deploy target dev, rồi refresh
`instacart_batch` để tạo fact. Khi G4 sẵn sàng, mở PR đặt `enable_gold_fact: "true"`
trong `targets.prod.variables` của `bundles/databricks.yml`. CI triển khai target prod
sau khi PR cấu hình được merge.

### 2.2 Các bảng tổng hợp

Các MV phục vụ Power BI nằm trong [`gold.py`](../../src/pipelines/batch/gold.py).
`kpi_daily` nhóm theo ngày và nguồn, `kpi_by_dow_hour` dùng thứ và giờ gốc từ Instacart,
`department_sales` nhóm theo ngày, ngành và aisle. Đối chiếu tổng `items` với fact trước khi
dùng số liệu trên báo cáo.

`product_360` giữ cả sản phẩm chưa từng được đặt, với `times_ordered = 0`. Expectation
`one_row_per_product` dừng flow nếu khóa thiếu hoặc fanout. Nghiệm thu đủ 49.688 sản phẩm.

`product_pair_counts` lấy top 5.000 sản phẩm từ `product_360`, dùng mã sản phẩm để
phá hòa. Với mỗi giỏ prior, lọc về danh mục top, khử trùng và sắp tăng dần trước khi tạo cặp,
nên luôn có `p1 < p2`. Giữ cặp xuất hiện ít nhất 50 giỏ, sắp theo `pair_count` giảm dần,
rồi `p1, p2` tăng dần và giới hạn 200.000 dòng.

Support chia số giỏ có cặp cho toàn bộ số giỏ prior. Confidence/lift là **xấp xỉ**
vì `product_360.times_ordered` dùng cả prior và stream, còn `pair_count` chỉ dùng prior.
Số đo đánh giá thuật toán do DS tính trên đúng split.

MV cần cả `enable_gold_fact=true` và `enable_baskets=true`. Khi benchmark, dùng truy
vấn event log ở §4.1 với flow `product_pair_counts` và ghi update ID, số cặp, thời gian flow.

### 2.3 Dim và bảng phân quyền

`dim_date` phủ từ 01/01/2015 đến 30/04/2026 với thứ ISO (thứ Hai = 1).
`dim_department` lấy các ngành duy nhất từ Silver và kiểm xung đột theo `department_id`.
`bi_user_access` chuẩn hóa và khử trùng cặp `user_email, department_id`; các giá trị thiếu
email hoặc mã ngành hợp lệ bị loại qua expectation. Cả ba được bật cùng `enable_gold_fact`.

Trước khi bật Gold, người phụ trách BI tạo và nhập bảng seed trong SQL Editor.
Tài khoản chạy pipeline cần quyền đọc bảng này. Nhập các dòng thực ở workspace, ngoài repo.

```sql
CREATE TABLE IF NOT EXISTS instacart.landing_meta.bi_access_seed (user_email STRING, department_id INT);
```

Trong môi trường nghiệm thu cách ly, đặt cấu hình pipeline `bi_access_seed_schema` trỏ đến
schema riêng có bảng cùng cấu trúc.

## 3. Incremental hay tính lại toàn bộ

| Cách | Cơ chế | Dùng ở dự án |
|---|---|---|
| MV tính lại toàn bộ | Mỗi update chạy lại câu truy vấn | **Mặc định trên classic compute.** `fact_order_lines` mất khoảng 10–15 phút, ghi vào benchmark |
| MV tính tăng dần (incremental refresh) | LDP dùng row tracking / CDF của nguồn để chỉ tính phần đổi | Chỉ có trên **serverless**. Nếu spike cho dùng serverless pipeline thì bật, rồi so thời gian |
| **Change Data Feed (CDF)** đọc tay | Đọc các dòng insert/update/delete giữa hai version của một bảng Delta | Cho tiêu thụ **ngoài** pipeline, ví dụ task `publish_serving` (dự phòng 2) chỉ đẩy thay đổi giá sang Azure SQL |

Ví dụ CDF: đẩy thay đổi giá ra ngoài mà không đọc lại cả bảng:

```sql
-- silver.product_prices_history đã bật delta.enableChangeDataFeed (03 §2.2)
SELECT product_id, unit_price, __START_AT, __END_AT, _change_type, _commit_version
FROM table_changes('instacart.silver.product_prices_history', 12)       -- từ version 12 trở đi
WHERE _change_type IN ('insert', 'update_postimage', 'delete');
```

Task lưu `last_version` đã xử lý vào `ops.cdf_progress`. Lần sau đọc từ `last_version + 1`, chỉ cập nhật mốc sau khi ghi đích thành công. Đây là cùng nguyên tắc "watermark tăng sau cùng" như CDC ở [01 §3.6](01_ingest_landing.md).

## 4. Kiểm tra gold khớp silver

```sql
-- Fact không nhân dòng do join giá / KM
SELECT count(*) = count(DISTINCT order_id, product_id) AS no_fanout,
       count(*) AS n FROM instacart.gold.fact_order_lines;                       -- true | 33.819.106

-- Mọi món đều có giá (SCD2 phủ từ 2015-01-01)
SELECT count(*) FROM instacart.gold.fact_order_lines WHERE unit_price IS NULL;  -- 0

-- Tổng ở bảng tổng hợp = tổng ở fact
SELECT (SELECT sum(items) FROM instacart.gold.department_sales)
     = (SELECT count(*)   FROM instacart.gold.fact_order_lines) AS items_match;  -- true
```

33.819.106 là tổng 32.434.489 món prior và 1.384.617 món train từ stream.

`src/tasks/reconcile_g4.sql` ghi bốn check `g4.fact.rows`, `g4.fact.no_fanout`,
`g4.fact.keys` và `g4.fact.unpriced` vào `ops.reconciliation`, rồi báo lỗi nếu có check đỏ.
Chạy file này sau `reconcile.sql` và `reconcile_g3.sql` đã đạt với cùng
`run_id`, `check_date`, profile và schema. Đây là phần đối soát fact của AD-DE-22.
Check tổng hợp `department_sales` ở trên dùng khi bảng tổng hợp đã có.

### 4.1 Đo thời gian refresh fact

Chọn update đã `COMPLETED`, giữ lại update ID và số dòng đối soát, rồi lấy mốc thời
gian của flow `fact_order_lines` trong `ops.event_log_batch`. SQL sau nhận tham số
STRING `catalog`, `ops_schema`, `update_id` từ môi trường đang đo.

```sql
WITH flow_events AS (
  SELECT timestamp AS event_ts,
    get_json_object(to_json(named_struct('v', origin)), '$.v.flow_name') AS flow_name,
    get_json_object(to_json(named_struct('v', origin)), '$.v.update_id') AS update_id,
    get_json_object(get_json_object(to_json(named_struct('v', details)), '$.v'), '$.flow_progress.status') AS flow_status
  FROM IDENTIFIER(
    '`' || replace(:catalog, '`', '``') || '`.`' || replace(:ops_schema, '`', '``') || '`.event_log_batch'
  )
  WHERE event_type = 'flow_progress'
), fact_times AS (
  SELECT min(CASE WHEN flow_status IN ('STARTING', 'RUNNING') THEN event_ts END) AS started_at,
         max(CASE WHEN flow_status = 'COMPLETED' THEN event_ts END) AS completed_at
  FROM flow_events
  WHERE update_id = :update_id
    AND (flow_name = 'fact_order_lines' OR endswith(replace(flow_name, '`', ''), '.fact_order_lines'))
)
SELECT started_at, completed_at,
       timestampdiff(MILLISECOND, started_at, completed_at) / 1000.0 AS refresh_seconds
FROM fact_times
WHERE started_at IS NOT NULL AND completed_at >= started_at;
```

Ghi cả thời gian flow và thời gian update trong báo cáo benchmark, kèm profile dữ liệu và
loại compute. Thời gian flow lấy từ event `STARTING` hoặc `RUNNING` đến `COMPLETED`,
còn thời gian update gồm khởi tạo compute và các flow khác.
[Pipeline event log](https://learn.microsoft.com/en-us/azure/databricks/ldp/monitor-event-logs).

## 5. Tối ưu lưu trữ và đọc/ghi (rubric 3.1, 3.4)

| Kỹ thuật | Áp dụng | Hiệu quả kỳ vọng |
|---|---|---|
| Delta thay CSV | Mọi tầng từ bronze | Nén cột, có thống kê min/max, bỏ qua file không liên quan khi lọc |
| **Liquid clustering** (`cluster_by`) | `fact_order_lines (order_date, department_id)`, `department_sales`, `order_items_prior (order_id)` | Lọc theo ngày / ngành chỉ đọc một phần file |
| OPTIMIZE + VACUUM | LDP tự chạy tác vụ bảo trì cho bảng của pipeline (trong vòng 24 giờ sau update) | Gộp file nhỏ; dọn file cũ quá 7 ngày |
| Không lưu nhị phân ảnh | `bronze.product_images` chỉ có metadata | Tiết kiệm dung lượng |
| Lifecycle landing → cool | Policy ở `10-core` | Giảm tiền lưu file thô ít đọc |

> Tác vụ bảo trì của LDP trên classic compute cũng khởi động cluster, tức là cũng chiếm quota. Đặt lịch batch sao cho cửa sổ bảo trì không trùng buổi demo.

### 5.1 Job benchmark định dạng (AD-DE-26)

[`benchmark_formats.py`](../../src/tasks/benchmark_formats.py) chạy trong job
`instacart-benchmark-formats` trên `Standard_D4ds_v4`, DBR 15.4 LTS, 4 vCPU và 0 worker.
Job chạy tay với policy `instacart-single-node`. Đợi batch, crawler và stream rảnh trước khi chạy.

Trong `bundles/`:

```bash
databricks bundle validate --strict -t dev
databricks bundle deploy -t dev --select jobs.benchmark_formats
databricks bundle run -t dev benchmark_formats
```

- Chụp một bản Parquet của bốn cột từ `silver.order_items_prior` ngoài thời gian đo.
  Cả ba vòng đọc cùng đầu vào. Mỗi vòng đo CSV có header, Parquet, Delta và Delta chạy
  thêm `OPTIMIZE ZORDER BY product_id`.
- Các phép đọc chạy cùng phép đếm và checksum trên cả bốn cột, với toàn bộ dữ liệu hoặc
  sau khi lọc `product_id = 24852`. Job tắt Databricks disk cache và kiểm cả hai kết quả
  với đầu vào. Thứ tự ghi xoay vòng giữa các lượt.
- `write_s` đo ghi file; `optimize_s` đo riêng bước ZORDER. `scan_s` và `filter_s` là giây.
  `size_mb` là MiB của file dữ liệu đang dùng, bỏ qua metadata và file Delta đã bị thay thế.
- Đường dẫn tạm riêng nằm dưới `/Volumes/<catalog>/ops/checkpoints/bench/ad-de-26/`.
  Job dọn từng vòng và bản đầu vào trước khi ghi kết quả. Nếu bị hủy giữa chừng,
  dùng log để xác định đúng thư mục UUID của lượt chạy trước khi dọn thủ công.
- Đích dev là `<catalog>.<user>_ops.benchmark_results`. Mỗi job run ghi 12 dòng.
  Khóa MERGE là `(run_id, repetition, format)`, nên repair cùng run giữ nguyên số dòng.
  Kết quả lưu thêm bảng nguồn, sản phẩm lọc, số dòng và cấu hình máy để đối chiếu.

Lấy trung vị theo định dạng từ bảng đúng target. `median(write_s + optimize_s)`
là tổng thời gian ghi và tối ưu tính trong từng vòng, rồi mới lấy trung vị.

```sql
SELECT format, count(*) AS repetitions, max(n_rows) AS n_rows,
       round(median(write_s), 2) AS write_s, round(median(optimize_s), 2) AS optimize_s,
       round(median(write_s + optimize_s), 2) AS write_total_s,
       round(median(scan_s), 2) AS scan_s, round(median(filter_s), 2) AS filter_s,
       round(median(size_mb), 2) AS size_mib
FROM instacart.ops.benchmark_results
WHERE run_id = :run_id
GROUP BY format;
```

Với `order_items_prior_scd1`, `fact_order_lines` và `product_pair_counts`, lấy mốc đầu
`STARTING` hoặc `RUNNING` đến `COMPLETED` theo cùng flow và update ID trong event log.
Với SCD1, lưu thêm `num_upserted_rows` để nhận diện lượt nạp có dữ liệu. Các số đo flow
được báo cáo riêng theo update và runtime của pipeline.
[Pipeline event log](https://learn.microsoft.com/en-us/azure/databricks/ldp/monitor-event-logs).

## 6. Phục vụ Power BI

- **Mặc định:** Power BI kết nối serverless SQL warehouse `wh-instacart-bi`, chỉ đọc schema `gold` (và `ops` cho trang vận hành). Quyền đã cấp ở [devops/05 bước 5](../devops/05_databricks_platform.md).
- Thang dự phòng (gold → Azure SQL, hoặc `DeltaLake.Table`) ở [devops/06 §4](../devops/06_de_den_dashboard.md). Nếu dùng dự phòng 2, task `publish_serving` chép các bảng trong mục 1, **trừ** `fact_order_lines`, bằng JDBC `mode("overwrite")`. Bảng nhỏ nên ghi đè cả bảng là idempotent.
- Sau khi pipeline batch xong, task `refresh_bi` gọi API refresh semantic model ([07 §7](07_power_bi.md)).

## 7. Lỗi giữa chừng thì chạy lại thế nào

| Tình huống | Cách xử lý |
|---|---|
| Update gold lỗi | Chạy lại update (hoặc *Repair run* của job). MV tính lại, không có trạng thái dở |
| Sửa logic một bảng gold | Deploy rồi `refresh_selection` bảng đó. Các bảng phía sau trong đồ thị cũng tự refresh |
| Power BI đọc lúc gold đang refresh | Mỗi MV được thay bằng một commit Delta nguyên tử. Power BI thấy bản cũ hoặc bản mới, không thấy bản nửa vời |
| Benchmark lỗi giữa chừng | Repair cùng `run_id` để đo lại đủ ba vòng. Bảng kết quả chỉ nhận các phép đo sau khi cả ba vòng và dọn thư mục hoàn tất |

## 8. Checklist (phần DE của G4)

- [ ] Các truy vấn ở mục 4 đều đạt.
- [ ] `SELECT * FROM instacart.ops.benchmark_results` có 4 định dạng × 3 lần.
- [ ] P4 (vai `bi`) đọc được `gold.*` qua warehouse. Không có tài khoản chỉ mang vai `bi` (P4 còn vai `ds`, được đọc silver), nên kiểm phần chặn bằng `SHOW GRANTS ON SCHEMA instacart.silver`: không có grant nào dành riêng cho vai `bi` (bảng quyền ở devops/05 bước 5).
- [ ] `gold.bi_user_access` có ít nhất 2 người, ở 2 department khác nhau.
