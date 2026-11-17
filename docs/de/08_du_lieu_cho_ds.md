# 08 · Chuẩn bị dữ liệu cho DS (silver → `ds_data`)

**Đầu vào:**
- Checklist G3 đã đạt ([03 §9](03_silver_cdc_scd_dq.md)): `silver.baskets` có đủ `prior` và `holdout`, stream đã phát xong.
- Schema `ds_data` và volume `ds_data.exports` đã có ([devops/05 bước 4](../devops/05_databricks_platform.md)).

**Đầu ra (schema `ds_data`, DE sở hữu, DS chỉ đọc):**

| Bảng / file | Nội dung | Ai dùng |
|---|---|---|
| `ds_data.data_versions` | Mỗi phiên bản dữ liệu một dòng: thời điểm tạo, số giỏ `prior` / `holdout` | Đối soát |
| `ds_data.baskets` | **Bản cố định** của `silver.baskets` theo `data_version` | Nguồn của mọi bảng dưới |
| `ds_data.item_index` | Mã hóa item thành `idx` (0 = phổ biến nhất) cho 3 cấp `product` / `aisle` / `department` | DS, view `gold.*` |
| `ds_data.transactions` | Giỏ hàng theo 3 cấp, `items array<int>` tăng dần, `scale_bucket` 0–99 | DApriori, DATID, FP-Growth |
| `ds_data.holdout_eval` | Đơn holdout chia thành `context` / `targets`, chia `val` / `test` | Đánh giá gợi ý (E7) |
| `/Volumes/instacart/ds_data/exports/<v>/<grain>/scale=<pct>/part-*.txt` + `<v>/_manifest.json` | File txt, mỗi dòng một giao dịch | Hadoop MapReduce / Spark ở máy local ([ds/03](../ds/03_hadoop_mapreduce_local.md)) |

**Xong khi:** checklist mục 7 đạt, và DS xác nhận đọc được (mốc G3 của DS, [ds/00 §5](../ds/00_tong_quan_ds.md)).

---

## 1. Vì sao DE làm phần này

- **Một nguồn dữ liệu cho mọi thực nghiệm.** Databricks và cụm Hadoop local đều đọc từ cùng `ds_data.transactions` (local đọc qua file txt xuất ra). Số cặp phổ biến của MapReduce vì vậy so được với số của DApriori.
- **Dữ liệu cố định.** `silver.baskets` là materialized view và có thể được tính lại khi pipeline chạy. Thực nghiệm kéo dài nhiều tuần thì cần một bản **không đổi**.
  - Không dùng `VERSION AS OF` trên MV, vì time travel trên bảng do pipeline quản lý không chắc được hỗ trợ.
  - Không dùng `CLONE`, vì clone không áp dụng cho MV / streaming table.
  - Dùng **`INSERT … SELECT` có chặn ghi lần hai**.
- **Quyền rõ ràng.** DS có `SELECT` trên `ds_data` và không sửa được dữ liệu đầu vào của chính mình.

## 2. Snapshot: `ds_data.baskets`

```sql
-- src/tasks/ds_prepare/01_snapshot.sql   (chạy bằng spark.sql(sql, args={"v": data_version}))
CREATE TABLE IF NOT EXISTS instacart.ds_data.data_versions (
  data_version STRING, created_at TIMESTAMP, n_prior BIGINT, n_holdout BIGINT, source STRING);
CREATE TABLE IF NOT EXISTS instacart.ds_data.baskets (
  data_version STRING, order_id INT, user_id INT, split STRING, items ARRAY<INT>, n_items INT)
  CLUSTER BY (data_version, split);

-- Chỉ ghi khi phiên bản này CHƯA có: chạy lại không ghi đè, không nhân dòng
INSERT INTO instacart.ds_data.baskets
SELECT :v, order_id, user_id, split, items, n_items
FROM instacart.silver.baskets
WHERE NOT EXISTS (SELECT 1 FROM instacart.ds_data.baskets WHERE data_version = :v);

MERGE INTO instacart.ds_data.data_versions t
USING (SELECT :v AS data_version, current_timestamp() AS created_at,
              count_if(split = 'prior') AS n_prior, count_if(split = 'holdout') AS n_holdout,
              'instacart.silver.baskets' AS source
       FROM instacart.ds_data.baskets WHERE data_version = :v) s
ON t.data_version = s.data_version
WHEN NOT MATCHED THEN INSERT *;
```

**Quy tắc phiên bản:**
- `v1` được tạo **một lần** sau G3 và **không bao giờ bị ghi đè**.
- Cần dữ liệu khác (ví dụ silver được sửa logic) thì tạo `v2`. DS chạy lại thực nghiệm với `data_version = 'v2'`; kết quả `v1` vẫn giữ để so.
- Muốn tạo lại `v1` thật sự (hiếm): xóa có chủ đích bằng `DELETE … WHERE data_version = 'v1'` ở **mọi** bảng `ds_data`, và báo DS vì toàn bộ kết quả `v1` mất giá trị.

## 3. Mã hóa item và giao dịch theo 3 cấp

**Mã hóa:** `idx` đánh số liên tục từ 0 theo **tần suất giảm dần** trong split `prior`; hòa thì xét khóa tăng dần. Thứ tự này **tất định**, nên tính lại vẫn ra đúng `idx` cũ. DS dựa vào đó để:
- lọc "top-N item" bằng `idx < N`;
- bỏ qua bước sắp xếp theo support của DATID ([ds/02 §3](../ds/02_thuat_toan_spark.md)).

```sql
-- src/tasks/ds_prepare/02_index_transactions.sql
CREATE TABLE IF NOT EXISTS instacart.ds_data.item_index (
  data_version STRING, grain STRING, item_key STRING, idx INT, item_name STRING, freq_prior BIGINT)
  CLUSTER BY (data_version, grain);

-- Dòng món của snapshot, gắn aisle/department (dim_product có đúng 1 dòng / sản phẩm: de/03 §6)
CREATE OR REPLACE TEMPORARY VIEW ds_lines AS
SELECT b.order_id, b.split, CAST(d.product_id AS STRING) AS product_key, d.product_name,
       CAST(d.aisle_id AS STRING) AS aisle_key, d.aisle, CAST(d.department_id AS STRING) AS department_key,
       d.department
FROM (SELECT order_id, split, explode(items) AS product_id
      FROM instacart.ds_data.baskets WHERE data_version = :v) b
JOIN instacart.silver.dim_product d USING (product_id);

CREATE OR REPLACE TEMPORARY VIEW ds_keyed AS
SELECT 'product' AS grain, order_id, split, product_key AS item_key, product_name AS item_name FROM ds_lines
UNION ALL SELECT 'aisle', order_id, split, aisle_key, aisle FROM ds_lines
UNION ALL SELECT 'department', order_id, split, department_key, department FROM ds_lines;

-- Tính lại cho cùng :v luôn ra cùng kết quả (tất định), nên REPLACE WHERE là idempotent
INSERT INTO instacart.ds_data.item_index REPLACE WHERE data_version = :v
SELECT :v, grain, item_key,
       CAST(row_number() OVER (PARTITION BY grain ORDER BY freq_prior DESC, CAST(item_key AS INT)) - 1 AS INT) AS idx,
       item_name, freq_prior
FROM (SELECT grain, item_key, any_value(item_name) AS item_name, count(DISTINCT order_id) AS freq_prior
      FROM ds_keyed WHERE split = 'prior' GROUP BY grain, item_key);

CREATE TABLE IF NOT EXISTS instacart.ds_data.transactions (
  data_version STRING, grain STRING, order_id INT, split STRING, items ARRAY<INT>, n_items INT, scale_bucket INT)
  CLUSTER BY (data_version, grain, scale_bucket);

INSERT INTO instacart.ds_data.transactions REPLACE WHERE data_version = :v
SELECT :v, grain, order_id, split, items, size(items) AS n_items, scale_bucket
FROM (SELECT k.grain, k.order_id, k.split, array_sort(collect_set(i.idx)) AS items,
             CAST(pmod(xxhash64(k.order_id), 100) AS INT) AS scale_bucket
      FROM ds_keyed k
      JOIN instacart.ds_data.item_index i
        ON i.data_version = :v AND i.grain = k.grain AND i.item_key = k.item_key
      GROUP BY k.grain, k.order_id, k.split);
```

- `collect_set` loại trùng ở cấp aisle/department: một đơn mua 3 món cùng aisle "fresh fruits" chỉ tính **một** lần aisle đó. Đây là định nghĩa giỏ hàng ở cấp aisle.
- Món chỉ xuất hiện trong `holdout` mà không có trong `prior` sẽ không có `idx`, nên bị bỏ khỏi giỏ holdout. Số món bị bỏ được ghi ở checklist mục 7.
- `scale_bucket = pmod(xxhash64(order_id), 100)` cho các tập con **lồng nhau và tất định**: 10% ⊂ 25% ⊂ 50% ⊂ 100% ([ds/01 §1](../ds/01_du_lieu_thuc_nghiem.md)).

## 4. Tập đánh giá: `ds_data.holdout_eval`

Mỗi đơn holdout (đơn `train` của Instacart, đi vào qua streaming) có ≥ 2 món được tách làm hai phần **theo thứ tự bỏ vào giỏ** (`add_to_cart_order`):
- **`context`**: nửa đầu (làm tròn lên). Mô phỏng "khách đã bỏ những món này vào giỏ".
- **`targets`**: phần còn lại. Đây là đáp án cho gợi ý.

`eval_part` chia đôi theo hash: `val` để DS chọn cấu hình, `test` để báo cáo số cuối.

```sql
-- src/tasks/ds_prepare/03_holdout.sql
CREATE TABLE IF NOT EXISTS instacart.ds_data.holdout_eval (
  data_version STRING, order_id INT, eval_part STRING, context ARRAY<INT>, targets ARRAY<INT>);

INSERT INTO instacart.ds_data.holdout_eval
WITH lines AS (
  SELECT o.order_id, o.add_to_cart_order, i.idx
  FROM instacart.silver.order_items o
  JOIN instacart.ds_data.item_index i
    ON i.data_version = :v AND i.grain = 'product' AND i.item_key = CAST(o.product_id AS STRING)
  WHERE o.source = 'stream'
    AND o.order_id IN (SELECT order_id FROM instacart.ds_data.baskets WHERE data_version = :v AND split = 'holdout')),
seq AS (
  SELECT order_id, transform(array_sort(collect_list(struct(add_to_cart_order, idx))), s -> s.idx) AS items
  FROM lines GROUP BY order_id HAVING count(*) >= 2)
SELECT :v, order_id,
       CASE WHEN pmod(xxhash64(order_id, 'eval'), 2) = 0 THEN 'val' ELSE 'test' END AS eval_part,
       array_sort(slice(items, 1, CAST(ceil(size(items) / 2.0) AS INT)))                    AS context,
       array_sort(slice(items, CAST(ceil(size(items) / 2.0) AS INT) + 1, size(items)))    AS targets
FROM seq
WHERE NOT EXISTS (SELECT 1 FROM instacart.ds_data.holdout_eval WHERE data_version = :v);
```

- Bảng này đọc thêm `silver.order_items` để lấy `add_to_cart_order`, vì `silver.baskets` không giữ thứ tự món. Vì silver có thể đổi, bảng dùng **chặn ghi lần hai** (`NOT EXISTS`) giống `ds_data.baskets`, không dùng `REPLACE WHERE`.
- `context` và `targets` được `array_sort` theo `idx`. DS so khớp vế trái của luật với tập con của `context` bằng phép `=` giữa hai mảng, nên cả hai phía phải cùng tăng dần ([ds/04 §7.2](../ds/04_thuc_nghiem_danh_gia.md)).

## 5. Xuất file cho Hadoop local

```python
# src/tasks/ds_prepare/export_txt.py  (job cluster single-node của DE)
import argparse
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

VOL = "/Volumes/instacart/ds_data/exports"
GRAINS, SCALES = ("product", "aisle", "department"), (10, 25, 50, 100)

p = argparse.ArgumentParser()
p.add_argument("--data-version", default="v1")
v = p.parse_args().data_version
spark = SparkSession.builder.getOrCreate()

tx = spark.table("instacart.ds_data.transactions").where((F.col("data_version") == v) & (F.col("split") == "prior"))
manifest = {"data_version": v, "format": "txt: idx cách nhau bằng dấu cách, mỗi dòng một giao dịch"}
for grain in GRAINS:
    manifest[grain] = {}
    for scale in SCALES:
        part = tx.where((F.col("grain") == grain) & (F.col("scale_bucket") < scale))
        path = f"{VOL}/{v}/{grain}/scale={scale}"
        (part.select(F.concat_ws(" ", F.transform("items", lambda x: x.cast("string"))).alias("value"))
             .repartition(max(1, scale // 25))                           # vài file; HDFS tự chia block 8 MB lúc put
             .write.mode("overwrite").text(path))
        n_written = spark.read.text(path).count()
        n_expected = part.count()
        assert n_written == n_expected, (grain, scale, n_written, n_expected)
        manifest[grain][str(scale)] = n_expected

# Manifest ghi SAU CÙNG: có manifest nghĩa là mọi file đã đủ (vai trò như _SUCCESS)
with open(f"{VOL}/{v}/_manifest.json", "w") as f:
    json.dump(manifest, f, indent=2)
```

- Volume `ds_data.exports` trỏ tới `abfss://ds@<lake>.dfs.core.windows.net/exports`. Đường dẫn `/Volumes/…/exports/v1/product/scale=10/` tương ứng với blob `ds/exports/v1/product/scale=10/`, nơi AzCopy ở máy local tải về ([ds/03 §3](../ds/03_hadoop_mapreduce_local.md)).
- `mode("overwrite")` trên đường dẫn tất định: chạy lại thì thay đúng thư mục đó bằng nội dung y hệt, vì `transactions` của một phiên bản không đổi.
- Volume hỗ trợ ghi file bằng `open()` của Python (FUSE), nên manifest không cần `dbutils`.

## 6. Job `ds-prepare`

```yaml
# bundles/resources/jobs_ds.yml  (phần của DE)
resources:
  jobs:
    ds_prepare:
      name: instacart-ds-prepare
      max_concurrent_runs: 1
      parameters:
        - { name: data_version, default: v1 }
      tasks:
        - task_key: prepare_tables                      # chạy 01 → 02 → 03 theo thứ tự
          job_cluster_key: sn
          spark_python_task:
            python_file: ../../src/tasks/ds_prepare/run_sql.py
            parameters: ["--data-version", "{{job.parameters.data_version}}",
                         "01_snapshot.sql", "02_index_transactions.sql", "03_holdout.sql"]
        - task_key: export_txt
          depends_on: [{ task_key: prepare_tables }]
          job_cluster_key: sn
          spark_python_task:
            python_file: ../../src/tasks/ds_prepare/export_txt.py
            parameters: ["--data-version", "{{job.parameters.data_version}}"]
        - task_key: check
          depends_on: [{ task_key: export_txt }]
          job_cluster_key: sn
          spark_python_task:
            python_file: ../../src/tasks/ds_prepare/run_sql.py
            parameters: ["--data-version", "{{job.parameters.data_version}}", "04_check.sql"]
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
            custom_tags: { ResourceClass: SingleNode, owner: p2 }
```

- `run_sql.py` đọc từng file, tách theo dấu `;` ở cuối câu, rồi chạy `spark.sql(stmt, args={"v": data_version})`. Tham số `:v` là *named parameter marker* của Spark SQL; tên bảng không đi qua tham số.
- **Không đặt lịch.** P2 chạy tay một lần sau G3 (`databricks bundle run ds_prepare`), và chạy lại khi tạo phiên bản mới.
- Dùng job cluster single-node (4 vCPU), nên **không chạy cùng lúc** với job batch hay pipeline stream.

## 7. Checklist (mốc G3 của DS)

```sql
-- src/tasks/ds_prepare/04_check.sql   (assert_true: sai thì task đỏ)
SELECT assert_true(n_prior = (SELECT count(*) FROM instacart.silver.baskets WHERE split = 'prior'),
                   'n_prior lệch silver.baskets')
FROM instacart.ds_data.data_versions WHERE data_version = :v;

SELECT assert_true(count(DISTINCT grain) = 3 AND min(n) = max(n), 'mỗi cấp phải có cùng số giỏ prior')
FROM (SELECT grain, count(*) AS n FROM instacart.ds_data.transactions
      WHERE data_version = :v AND split = 'prior' GROUP BY grain);

SELECT assert_true(sum(n_items) = (SELECT sum(n_items) FROM instacart.ds_data.baskets
                                   WHERE data_version = :v AND split = 'prior'),
                   'tổng số món cấp product phải bằng snapshot (mỗi món 1 lần / đơn)')
FROM instacart.ds_data.transactions WHERE data_version = :v AND grain = 'product' AND split = 'prior';

SELECT assert_true(count_if(grain = 'department') = 21 AND count_if(grain = 'aisle') = 134,
                   'số item cấp department / aisle')
FROM instacart.ds_data.item_index WHERE data_version = :v;

SELECT assert_true(count(*) > 0 AND abs(count_if(eval_part = 'val') - count_if(eval_part = 'test')) < 0.05 * count(*),
                   'holdout_eval rỗng hoặc chia val/test lệch')
FROM instacart.ds_data.holdout_eval WHERE data_version = :v;
```

**Ghi vào báo cáo** (chạy tay, không chặn):

| Số liệu | Truy vấn | Kỳ vọng |
|---|---|---|
| Số giỏ `prior` / `holdout` | `ds_data.data_versions` | 3.214.874 / 131.209 (khớp checklist G3) |
| Số item cấp product | `count(*) FROM item_index WHERE grain = 'product'` | Khoảng 49.677 (sản phẩm có trong prior) |
| Số đơn holdout dùng được để đánh giá | `count(*) FROM holdout_eval` | Ít hơn 131.209 một chút (bỏ đơn 1 món) |
| Số món holdout bị bỏ vì không có trong prior | Đếm `stream_order_items` không join được `item_index` | Rất nhỏ; ghi con số thật |
| Số dòng file xuất | `_manifest.json` | Bằng `transactions` theo cấp × quy mô (task tự `assert`) |

- [ ] Task `check` xanh.
- [ ] `_manifest.json` có đủ 3 cấp × 4 quy mô.
- [ ] Tài khoản vai `ds` đọc được `ds_data.*` và volume `exports`; **không** ghi được (`INSERT` báo `PERMISSION_DENIED`).
- [ ] Báo DS (P4) rằng `v1` đã sẵn sàng; ghi ngày tạo `v1` vào [ds/00 §5](../ds/00_tong_quan_ds.md).

## 8. Lỗi giữa chừng thì chạy lại thế nào

| Tình huống | Cách chạy lại | Vì sao không trùng / không mất |
|---|---|---|
| `prepare_tables` lỗi giữa 01 và 02 | Repair run | 01 có chặn `NOT EXISTS` nên không ghi lần hai; 02 dùng `REPLACE WHERE` |
| `export_txt` lỗi ở một quy mô | Repair run | `mode("overwrite")` trên đường dẫn tất định; manifest chỉ ghi khi **mọi** file đủ |
| `check` báo lệch | Điều tra theo thông báo; nếu silver sai thì sửa silver rồi tạo **`v2`** | `v1` không bị sửa ngầm; DS biết rõ đang dùng phiên bản nào |
| Lỡ chạy job với `data_version` đã có | Không cần làm gì | Snapshot và holdout bị chặn ghi; index/transactions tính lại ra y hệt |
| File trên volume bị xóa nhầm | Chạy lại riêng task `export_txt` | Tạo lại từ `ds_data.transactions` |
