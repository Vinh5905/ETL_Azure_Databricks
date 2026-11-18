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

```python
# src/pipelines/batch/gold.py
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CAT    = spark.conf.get("catalog")
SILVER = f"{CAT}.{spark.conf.get('silver')}"
GOLD   = f"{CAT}.{spark.conf.get('gold')}"
LANDING_META = f"{CAT}.landing_meta"


@dp.materialized_view(name=f"{GOLD}.fact_order_lines", cluster_by=["order_date", "department_id"],
                      comment="Mỗi món trong đơn + giá tại thời điểm đặt (SCD2) + KM đang chạy")
@dp.expect("priced", "unit_price IS NOT NULL")
def fact_order_lines():
    return spark.sql(f"""
      SELECT i.order_id, o.user_id, i.product_id, i.reordered, i.add_to_cart_order, o.source,
             o.order_dow, o.order_hour_of_day, o.order_ts_sim, to_date(o.order_ts_sim) AS order_date,
             d.department_id, d.aisle_id,
             p.unit_price,
             km.discount_pct,
             round(p.unit_price * (1 - coalesce(km.discount_pct, 0) / 100.0), 2) AS revenue_sim
      FROM {SILVER}.order_items i
      JOIN {SILVER}.orders o            ON o.order_id = i.order_id
      JOIN {SILVER}.dim_product d       ON d.product_id = i.product_id
      LEFT JOIN {SILVER}.product_prices_history p
             ON p.product_id = i.product_id
            AND o.order_ts_sim >= p.__START_AT
            AND (p.__END_AT IS NULL OR o.order_ts_sim < p.__END_AT)     -- đúng một phiên bản
      LEFT JOIN (
             SELECT product_id, start_date, end_date, max(discount_pct) AS discount_pct
             FROM {SILVER}.promotions GROUP BY ALL) km
             ON km.product_id = i.product_id
            AND to_date(o.order_ts_sim) BETWEEN km.start_date AND km.end_date
    """)
```

- **Point-in-time join:** điều kiện `__START_AT ≤ order_ts_sim < __END_AT` chọn **đúng một** phiên bản giá cho mỗi món. Đây là lý do cần SCD2.
- Join bằng giá hiện hành sẽ làm doanh thu tháng trước thay đổi mỗi khi đổi giá.
- Khi nhiều khuyến mãi cùng phủ một ngày của một sản phẩm, subquery `km` vẫn có thể ra nhiều dòng. Kiểm tra bằng `count(*) = count(DISTINCT order_id, product_id)` (mục 4). Nếu lệch thì gộp tiếp theo `(product_id, ngày)` trước khi join.
- Doanh thu là **giả lập**: ghi rõ trên báo cáo và trên Power BI.

### 2.2 Các bảng tổng hợp

```python
@dp.materialized_view(name=f"{GOLD}.kpi_daily")
def kpi_daily():
    return spark.sql(f"""
      SELECT order_date, source,
             count(DISTINCT order_id)               AS orders,
             count(*)                               AS items,
             count(DISTINCT user_id)                AS users,
             round(sum(revenue_sim), 2)             AS revenue_sim,
             round(avg(reordered), 4)               AS reorder_rate,
             round(count(*) / count(DISTINCT order_id), 2) AS avg_basket_size
      FROM {GOLD}.fact_order_lines GROUP BY order_date, source""")


@dp.materialized_view(name=f"{GOLD}.kpi_by_dow_hour")
def kpi_by_dow_hour():
    # Dùng cột gốc order_dow / order_hour_of_day, KHÔNG suy từ order_ts_sim (data_dictionary §4)
    return spark.sql(f"""
      SELECT order_dow, order_hour_of_day,
             count(DISTINCT order_id) AS orders, count(*) AS items, round(avg(reordered), 4) AS reorder_rate
      FROM {GOLD}.fact_order_lines GROUP BY ALL""")


@dp.materialized_view(name=f"{GOLD}.department_sales", cluster_by=["department_id", "order_date"])
def department_sales():
    return spark.sql(f"""
      SELECT order_date, department_id, aisle_id,
             count(DISTINCT order_id) AS orders, count(*) AS items,
             round(sum(revenue_sim), 2) AS revenue_sim,
             sum(reordered) AS reordered_items,
             sum(CASE WHEN discount_pct IS NOT NULL THEN 1 ELSE 0 END) AS promo_items
      FROM {GOLD}.fact_order_lines GROUP BY ALL""")


@dp.materialized_view(name=f"{GOLD}.product_360")
@dp.expect_or_fail("one_row_per_product", "n = 1")
def product_360():
    return spark.sql(f"""
      WITH s AS (SELECT product_id, count(*) AS times_ordered, round(avg(reordered), 4) AS reorder_rate,
                        round(sum(revenue_sim), 2) AS revenue_sim
                 FROM {GOLD}.fact_order_lines GROUP BY product_id)
      SELECT d.product_id, d.product_name, d.aisle_id, d.aisle, d.department_id, d.department,
             d.current_price, d.on_promotion, d.discount_pct,
             d.brand, d.nutriscore_grade, d.nova_group, d.energy_kcal_100g, d.sugars_100g,
             d.image_url, d.off_match_status, d.off_match_score,
             coalesce(s.times_ordered, 0) AS times_ordered, s.reorder_rate, s.revenue_sim,
             count(*) OVER (PARTITION BY d.product_id) AS n
      FROM {SILVER}.dim_product d LEFT JOIN s USING (product_id)""")


@dp.materialized_view(name=f"{GOLD}.product_pair_counts", cluster_by=["p1"])
def product_pair_counts():
    # Baseline DE cho trang 2. Luật kết hợp đầy đủ là gold.association_rules / recommendations của DS (ds/05 §4).
    return spark.sql(f"""
      WITH top AS (SELECT product_id FROM {GOLD}.product_360 ORDER BY times_ordered DESC LIMIT 5000),
           b AS (SELECT e.order_id, array_sort(collect_list(e.product_id)) AS items      -- giữ món thuộc top
                 FROM (SELECT order_id, explode(items) AS product_id
                       FROM {SILVER}.baskets WHERE split = 'prior') e
                 JOIN top USING (product_id)
                 GROUP BY e.order_id),
           n AS (SELECT count(*) AS n_baskets FROM {SILVER}.baskets WHERE split = 'prior'),
           pairs AS (SELECT p.p1, p.p2 FROM b
                     LATERAL VIEW explode(flatten(transform(items, (a, i) ->
                        transform(slice(items, i + 2, size(items)), c -> named_struct('p1', a, 'p2', c))))) t AS p),
           cnt AS (SELECT p1, p2, count(*) AS pair_count FROM pairs GROUP BY p1, p2 HAVING count(*) >= 50),
           single AS (SELECT product_id, times_ordered FROM {GOLD}.product_360)
      SELECT c.p1, c.p2, c.pair_count,
             c.pair_count / n.n_baskets                               AS support,
             c.pair_count / s1.times_ordered                          AS confidence_p1_to_p2,
             (c.pair_count * n.n_baskets) / (s1.times_ordered * s2.times_ordered) AS lift
      FROM cnt c CROSS JOIN n
      JOIN single s1 ON s1.product_id = c.p1 JOIN single s2 ON s2.product_id = c.p2""")
```

- `items` được `array_sort` lại sau khi lọc, nên luôn có `p1 < p2`, không cần `least` / `greatest`.
- `times_ordered` ở `product_360` đếm **mọi** đơn (prior + stream), còn `pair_count` chỉ đếm giỏ prior. Vì vậy `confidence` / `lift` ở đây là số **xấp xỉ** cho dashboard. Số chuẩn để đánh giá thuật toán do DS tính trên đúng split.
- Lọc top 5.000 sản phẩm và `HAVING ≥ 50` giữ phép đếm cặp trong khả năng của 4 vCPU. Ghi thời gian chạy vào benchmark.

### 2.3 Dim và bảng phân quyền

```python
@dp.materialized_view(name=f"{GOLD}.dim_date")
def dim_date():
    return spark.sql("""
      SELECT d AS date, year(d) AS year, month(d) AS month, weekofyear(d) AS week,
             date_format(d, 'yyyy-MM') AS year_month, dayofweek(d) AS iso_dow
      FROM (SELECT explode(sequence(DATE'2015-01-01', DATE'2026-04-30', INTERVAL 1 DAY)) AS d)""")


@dp.materialized_view(name=f"{GOLD}.dim_department")
def dim_department():
    return spark.sql(f"SELECT DISTINCT department_id, department FROM {SILVER}.dim_product")


@dp.materialized_view(name=f"{GOLD}.bi_user_access",
                      comment="RLS: email người xem → department được xem. Nguồn nhập tay, không nằm trong git")
@dp.expect_or_drop("valid_dept", "department_id IS NOT NULL")
def bi_user_access():
    return spark.read.table(f"{LANDING_META}.bi_access_seed")
```

**`bi_access_seed` không được commit lên git** (email là dữ liệu cá nhân). P3 nhập tay một lần trong SQL editor:

```sql
CREATE TABLE IF NOT EXISTS instacart.landing_meta.bi_access_seed (user_email STRING, department_id INT);
INSERT INTO instacart.landing_meta.bi_access_seed VALUES
  ('<email thành viên A>', 4),     -- produce
  ('<email thành viên B>', 16);    -- dairy eggs
-- Vai Executive không cần dòng ở đây: role đó không lọc (07 §5)
```

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

33.819.106 là tổng 32.434.489 + 1.384.617.

## 5. Tối ưu lưu trữ và đọc/ghi (rubric 3.1, 3.4)

| Kỹ thuật | Áp dụng | Hiệu quả kỳ vọng |
|---|---|---|
| Delta thay CSV | Mọi tầng từ bronze | Nén cột, có thống kê min/max, bỏ qua file không liên quan khi lọc |
| **Liquid clustering** (`cluster_by`) | `fact_order_lines (order_date, department_id)`, `department_sales`, `order_items_prior (order_id)` | Lọc theo ngày / ngành chỉ đọc một phần file |
| OPTIMIZE + VACUUM | LDP tự chạy tác vụ bảo trì cho bảng của pipeline (trong vòng 24 giờ sau update) | Gộp file nhỏ; dọn file cũ quá 7 ngày |
| Không lưu nhị phân ảnh | `bronze.product_images` chỉ có metadata | Tiết kiệm dung lượng |
| Lifecycle landing → cool | Policy ở `10-core` | Giảm tiền lưu file thô ít đọc |

> Tác vụ bảo trì của LDP trên classic compute cũng khởi động cluster, tức là cũng chiếm quota. Đặt lịch batch sao cho cửa sổ bảo trì không trùng buổi demo.

### 5.1 Notebook benchmark → `ops.benchmark_results`

```python
# src/tasks/benchmark_formats.py  (chạy trên job cluster single-node theo policy → cấu hình cố định)
import time, uuid
from pyspark.sql import functions as F

SRC  = "instacart.silver.order_items_prior"
BASE = "/Volumes/instacart/ops/checkpoints/bench"     # hoặc một volume riêng trong instacart_sandbox
run  = str(uuid.uuid4())[:8]
df   = spark.table(SRC).select("order_id", "product_id", "add_to_cart_order", "reordered")
rows = []


def timed(fn):
    t = time.time(); fn(); return round(time.time() - t, 2)


def size_mb(path):
    return round(sum(f.size for f in dbutils.fs.ls(path) if not f.name.startswith("_")) / 2**20, 1)


for fmt in ["csv", "parquet", "delta", "delta_optimized"]:
    path = f"{BASE}/{run}/{fmt}"
    w = "delta" if fmt.startswith("delta") else fmt
    t_write = timed(lambda: df.write.format(w).mode("overwrite").option("header", "true").save(path))
    if fmt == "delta_optimized":
        t_write += timed(lambda: spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY (product_id)"))
    reader = lambda: spark.read.format(w).option("header", "true").schema(df.schema).load(path)
    t_scan = timed(lambda: reader().agg(F.count("*")).collect())
    t_filter = timed(lambda: reader().where("product_id = 24852").agg(F.count("*")).collect())
    rows.append((run, fmt, df.count(), t_write, t_scan, t_filter, size_mb(path)))

(spark.createDataFrame(rows, "run_id string, format string, n_rows long, write_s double, "
                             "scan_s double, filter_s double, size_mb double")
      .withColumn("measured_at", F.current_timestamp())
      .write.mode("append").saveAsTable("instacart.ops.benchmark_results"))
dbutils.fs.rm(f"{BASE}/{run}", recurse=True)          # dọn dữ liệu thử
```

- Đường dẫn `delta.` cho phép `OPTIMIZE ... ZORDER BY` trên bảng không đăng ký trong catalog. Muốn thử liquid clustering thì ghi thành bảng trong `instacart_sandbox` với `CLUSTER BY (product_id)`.
- Chạy task **3 lần** (mỗi lần một `run_id`), báo cáo lấy trung vị theo định dạng. Ghi cấu hình máy (`Standard_D4ds_v4`, runtime) vào báo cáo.
- `size_mb` chỉ đếm file ở thư mục gốc. Với Delta, file dữ liệu nằm ngay ở gốc nên đếm đúng; CSV và Parquet của Spark cũng vậy.

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
| Benchmark lỗi giữa chừng | Chạy lại. Mỗi lần có `run_id` riêng; dòng của lần lỗi không được ghi vì `append` ở cuối |

## 8. Checklist (phần DE của G4)

- [ ] Các truy vấn ở mục 4 đều đạt.
- [ ] `SELECT * FROM instacart.ops.benchmark_results` có 4 định dạng × 3 lần.
- [ ] P4 (vai `bi`) đọc được `gold.*` qua warehouse. Không có tài khoản chỉ mang vai `bi` (P4 còn vai `ds`, được đọc silver), nên kiểm phần chặn bằng `SHOW GRANTS ON SCHEMA instacart.silver`: không có grant nào dành riêng cho vai `bi` (bảng quyền ở devops/05 bước 5).
- [ ] `gold.bi_user_access` có ít nhất 2 người, ở 2 department khác nhau.
