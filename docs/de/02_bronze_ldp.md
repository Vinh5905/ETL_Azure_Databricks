# 02 · Tầng bronze: pipeline Lakeflow + Auto Loader (landing → bronze)

**Đầu vào:**
- Landing đã có dữ liệu ([01](01_ingest_landing.md)).
- Catalog, schema và volume `raw` đã có ([devops/05](../devops/05_databricks_platform.md)).
- Bundle `bundles/` đã có khung.

**Đầu ra:**
- Pipeline `instacart_batch` được khai báo trong DAB.
- Các streaming table trong `bronze`:
  - `orders`, `order_products_prior`, `order_products_train`, `products`, `aisles`, `departments`.
  - `product_prices_cdc`, `promotions_cdc`.
  - `off_products`, `product_images`.

**Xong khi:** số dòng bronze bằng số dòng nguồn (mục 6), và chạy pipeline lần hai không tăng số dòng.

---

## 1. Bronze là gì và không là gì

- **Là:** bản sao trung thực của landing dưới dạng Delta. Giữ nguyên mọi cột gốc và thêm cột kỹ thuật (`_source_file`, `_ingest_ts`, `_rescued_data`). **Chỉ thêm dòng mới, không bao giờ sửa hay xóa.**
- **Không là:** nơi làm sạch hay loại trùng. Bronze giữ cả bản ghi lỗi và bản sao (ví dụ CDC đọc lặp, sự kiện phát lại). Việc lọc thuộc về silver.

Nhờ vậy, khi phát hiện lỗi logic ở silver, nhóm sửa code rồi tính lại silver từ bronze mà không phải tải lại từ nguồn.

## 2. Khai báo pipeline trong DAB

### 2.1 Cluster policy cho pipeline

Pipeline dùng cluster riêng do LDP tạo, nên cần policy loại `dlt` (policy `instacart-single-node` ở devops/05 chỉ áp cho job cluster). Thêm vào stack `30-databricks`:

```hcl
resource "databricks_cluster_policy" "pipeline_single_node" {
  name = "instacart-pipeline-single-node"
  definition = jsonencode({
    "cluster_type"      = { type = "fixed", value = "dlt" }
    "node_type_id"      = { type = "fixed", value = "Standard_D4ds_v4" }
    "num_workers"       = { type = "fixed", value = 0 }
    "custom_tags.owner" = { type = "unlimited", isOptional = false }
  })
}
```

Runtime của pipeline do `channel` (`CURRENT` / `PREVIEW`) quyết định, không cố định `spark_version` trong policy.

### 2.2 Resource pipeline

```yaml
# bundles/resources/pipeline_batch.yml
variables:
  pipeline_policy_id: { lookup: { cluster_policy: instacart-pipeline-single-node } }
  schema_prefix:      { default: "" }          # target dev ghi đè thành "<user>_"

resources:
  pipelines:
    instacart_batch:
      name: instacart-batch
      catalog: ${var.catalog}
      schema: ${var.schema_prefix}bronze        # schema mặc định; bảng khác ghi tên đầy đủ
      edition: ADVANCED                         # AUTO CDC (Pro) + expectations (Advanced)
      channel: CURRENT
      continuous: false                         # triggered: mỗi lần update xử lý hết dữ liệu mới rồi dừng
      photon: false
      clusters:
        - label: default
          policy_id: ${var.pipeline_policy_id}
          num_workers: 0
          custom_tags: { owner: p2 }
      configuration:
        landing_root: /Volumes/${var.catalog}/landing_meta/raw
        catalog: ${var.catalog}
        bronze: ${var.schema_prefix}bronze
        silver: ${var.schema_prefix}silver
        gold:   ${var.schema_prefix}gold
        ops:    ${var.schema_prefix}ops
        as_of_date: "2026-03-30"                # ngày "hôm nay" của lịch giả lập (KM đang chạy)
        pipelines.clusterShutdown.delay: 60s    # mặc định dev mode giữ cluster 2 giờ → tốn tiền
      libraries:
        - file:     { path: ../../src/pipelines/batch/bronze.py }
        - file:     { path: ../../src/pipelines/batch/silver_retail.py }
        - file:     { path: ../../src/pipelines/batch/silver_instacart.py }
        - notebook: { path: ../../src/pipelines/batch/silver_off.py }   # notebook vì cần %pip rapidfuzz
        - file:     { path: ../../src/pipelines/batch/gold.py }
```

```yaml
# bundles/databricks.yml  (bổ sung vào target dev)
targets:
  dev:
    mode: development
    variables:
      schema_prefix: ${workspace.current_user.short_name}_
```

**Vì sao dev cần schema riêng cho từng người:**
- Trong Unity Catalog, mỗi bảng chỉ thuộc **một** pipeline.
- Nếu pipeline dev của P2 và pipeline prod cùng ghi `instacart.bronze.orders` thì pipeline sau sẽ lỗi "table is managed by another pipeline".
- Với tiền tố, P2 dev ghi `instacart.p2_bronze.orders`.

Muốn vậy, vai `de` cần quyền `CREATE SCHEMA` trên catalog. Đã bổ sung vào bảng quyền ở [devops/05 bước 5](../devops/05_databricks_platform.md). Mỗi người tạo một lần:

```sql
CREATE SCHEMA IF NOT EXISTS instacart.p2_bronze;
CREATE SCHEMA IF NOT EXISTS instacart.p2_silver;
CREATE SCHEMA IF NOT EXISTS instacart.p2_gold;
CREATE SCHEMA IF NOT EXISTS instacart.p2_ops;
```

Code đọc tên schema từ `configuration`, nên dev và prod chạy cùng một code.

## 3. Code bronze

### 3.1 Hàm dùng chung và các bảng CSV Instacart

```python
# src/pipelines/batch/bronze.py
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CAT     = spark.conf.get("catalog")
BRONZE  = f"{CAT}.{spark.conf.get('bronze')}"
LANDING = spark.conf.get("landing_root")

# Không cho full refresh xóa bronze: nguồn (Zenodo, CDC log, Event Hubs) có thể không còn để đọc lại.
BRONZE_PROPS = {"pipelines.reset.allowed": "false", "quality": "bronze"}


def autoload(path: str, fmt: str, schema: str | None = None, **opts):
    """Auto Loader + cột kỹ thuật. LDP tự quản lý checkpoint và schemaLocation."""
    r = (spark.readStream.format("cloudFiles")
         .option("cloudFiles.format", fmt)
         .option("rescuedDataColumn", "_rescued_data")
         .option("cloudFiles.partitionColumns", "")      # không biến release=/window= thành cột
         )
    for k, v in opts.items():
        r = r.option(k, v)
    if schema:
        r = r.schema(schema)                             # schema cố định → cột lạ vào _rescued_data
    return (r.load(path)
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_file_mtime", F.col("_metadata.file_modification_time"))
            .withColumn("_ingest_ts", F.current_timestamp()))


CSV_TABLES = {
    "orders": ("orders",
               "order_id INT, user_id INT, eval_set STRING, order_number INT, order_dow INT, "
               "order_hour_of_day INT, days_since_prior_order DOUBLE"),
    "order_products_prior": ("order_products__prior",
               "order_id INT, product_id INT, add_to_cart_order INT, reordered INT"),
    "order_products_train": ("order_products__train",
               "order_id INT, product_id INT, add_to_cart_order INT, reordered INT"),
    "products":    ("products", "product_id INT, product_name STRING, aisle_id INT, department_id INT"),
    "aisles":      ("aisles", "aisle_id INT, aisle STRING"),
    "departments": ("departments", "department_id INT, department STRING"),
}


def make_csv_table(table: str, folder: str, schema: str):
    @dp.table(name=f"{BRONZE}.{table}",
              comment=f"Bronze: landing/instacart/{folder}, append-only",
              table_properties=BRONZE_PROPS)
    def _t():
        return autoload(f"{LANDING}/instacart/{folder}/", "csv", schema,
                        header="true", pathGlobFilter="*.csv",
                        **{"cloudFiles.schemaEvolutionMode": "rescue"})


for t, (folder, schema) in CSV_TABLES.items():
    make_csv_table(t, folder, schema)      # gọi qua hàm để mỗi vòng lặp giữ đúng biến của nó
```

**Ghi chú:**
- `order_products_train` **có** nạp vào bronze, nhưng **không** đi vào silver bằng đường batch. Nó chỉ dùng để đối soát với luồng stream (131.209 đơn / 1.384.617 dòng) và để DS tham chiếu.
- `products.csv` có tên chứa dấu phẩy trong ngoặc kép. Auto Loader CSV mặc định xử lý `quote="\""`, nên không cần thêm tùy chọn. Kiểm tra lại bằng `count` và `_rescued_data IS NOT NULL` (mục 6).
- Không đặt `cloudFiles.schemaLocation` / `checkpointLocation`. LDP tự lưu những thứ này trong storage của pipeline và gắn với từng bảng.

### 3.2 Bảng CDC từ Azure SQL

```python
def make_cdc_table(table: str):
    @dp.table(name=f"{BRONZE}.{table}_cdc",
              comment=f"Bronze: thay đổi CDC (snapshot + window) của dbo.{table}",
              table_properties=BRONZE_PROPS)
    def _t():
        return autoload(f"{LANDING}/retail_db/{table}/", "parquet",
                        pathGlobFilter="*.parquet",
                        **{"cloudFiles.schemaEvolutionMode": "addNewColumns"})


for t in ["product_prices", "promotions"]:
    make_cdc_table(t)
```

- Parquet mang sẵn schema, nên dùng `addNewColumns`. Nếu nguồn SQL thêm cột, update đầu tiên sẽ dừng với lỗi đổi schema; LDP tự khởi động lại flow và nhận cột mới.
- Các cột `__$start_lsn`, `__$seqval`, `__$operation`, `__$update_mask` từ hàm CDC được giữ nguyên trong bronze. Silver dùng các cột `_lsn`, `_seqval`, `_op` đã đổi tên sẵn ở câu truy vấn ADF.
- Auto Loader mặc định đọc **đệ quy** mọi thư mục con (`snapshot=…`, `window=…`), không cần liệt kê.

### 3.3 Open Food Facts: JSON lồng nhau và ảnh

```python
@dp.table(name=f"{BRONZE}.off_products",
          comment="Bronze: kết quả search OFF (mỗi dòng = một hit), JSON lồng nhau",
          table_properties=BRONZE_PROPS)
def off_products():
    return autoload(f"{LANDING}/off_api/products/", "json",
                    **{"cloudFiles.schemaEvolutionMode": "addNewColumns",
                       "cloudFiles.inferColumnTypes": "true",
                       # nutriments có hàng trăm khóa và kiểu lẫn lộn → giữ dạng map, silver mới lấy khóa cần
                       "cloudFiles.schemaHints": "nutriments MAP<STRING,STRING>, nova_group INT"})


@dp.table(name=f"{BRONZE}.product_images",
          comment="Bronze: metadata ảnh (unstructured); không lưu nội dung nhị phân",
          table_properties=BRONZE_PROPS)
def product_images():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "binaryFile")
            .option("pathGlobFilter", "*.jpg")
            .load(f"{LANDING}/off_api/images/")
            .select(F.regexp_extract("path", r"/([^/]+)\.jpg$", 1).alias("off_code"),
                    "path", "length", "modificationTime",
                    F.current_timestamp().alias("_ingest_ts")))
```

- **Không lưu cột `content`** (byte ảnh) vào Delta: ảnh đã có trên ADLS, chép lại vào bảng chỉ tốn chỗ (rubric 3.4).
- Muốn có `width` / `height` thì thêm pandas UDF dùng Pillow ở silver (tùy chọn).

## 4. Chạy và theo dõi

```bash
cd bundles
databricks bundle deploy -t dev
databricks bundle run instacart_batch -t dev                    # một update, triggered
databricks bundle run instacart_batch -t dev --refresh bronze.orders   # chỉ refresh một bảng (tên theo pipeline)
```

- Trên UI (Jobs & Pipelines → `[dev p2] instacart-batch`) có đồ thị bảng, số dòng mỗi flow, và tab *Data quality*.
- Lần đầu cluster mất khoảng 5–7 phút để khởi động. Các update tiếp theo trong vòng 60 giây sẽ dùng lại cluster (`clusterShutdown.delay`).
- Ở prod, job `instacart-batch-medallion` gọi pipeline bằng `pipeline_task`, xem [devops/06 §2](../devops/06_de_den_dashboard.md).

## 5. Lỗi giữa chừng thì chạy lại thế nào

| Tình huống | Hành vi của Auto Loader / LDP | Việc cần làm |
|---|---|---|
| Update lỗi giữa chừng (mất cluster, OOM) | Checkpoint chỉ ghi nhận các micro-batch **đã commit**. Micro-batch dở bị bỏ, Delta không thấy commit dở | Chạy lại update. File chưa commit được đọc lại, file đã commit không bị đọc lại |
| File CSV lỗi định dạng một vài dòng | Dòng không khớp schema → cột `_rescued_data` khác null, không làm dừng pipeline | Silver đưa các dòng đó vào quarantine ([03 §4](03_silver_cdc_scd_dq.md)) |
| Nguồn thêm cột (Parquet/JSON) | `addNewColumns`: flow dừng một lần, LDP tự khởi động lại với schema mới | Không cần làm gì; kiểm tra event log |
| Cần nạp lại **một file** đã nạp | Auto Loader không đọc lại file cùng đường dẫn | Chép file sang đường dẫn mới (ví dụ thêm hậu tố `_fix1`), silver loại trùng theo khóa |
| Lỡ bấm **Full refresh all** | Bảng có `pipelines.reset.allowed = false` bị **bỏ qua**, không xóa | Chỉ silver/gold bị tính lại |
| Thật sự cần nạp lại toàn bộ bronze một bảng | Tạm bỏ thuộc tính `reset.allowed` của bảng đó, full refresh riêng bảng đó, rồi đặt lại thuộc tính | Chỉ làm khi landing **còn đủ** file. Ghi vào `ops.pipeline_runs` |

## 6. Kiểm tra (đối soát G1)

```sql
-- Số dòng bronze phải bằng nguồn (mỗi file chỉ nạp một lần)
SELECT 'orders' t, count(*) n FROM instacart.bronze.orders                         -- 3.421.083
UNION ALL SELECT 'order_products_prior', count(*) FROM instacart.bronze.order_products_prior  -- 32.434.489
UNION ALL SELECT 'order_products_train', count(*) FROM instacart.bronze.order_products_train  -- 1.384.617
UNION ALL SELECT 'products', count(*) FROM instacart.bronze.products               -- 49.688
UNION ALL SELECT 'aisles',   count(*) FROM instacart.bronze.aisles                 -- 134
UNION ALL SELECT 'departments', count(*) FROM instacart.bronze.departments         -- 21;

-- Dòng không khớp schema
SELECT count(*) FROM instacart.bronze.products WHERE _rescued_data IS NOT NULL;     -- kỳ vọng 0

-- Mỗi file chỉ một lần
SELECT _source_file, count(*) FROM instacart.bronze.orders GROUP BY 1;              -- 1 file

-- CDC: phân bố loại thao tác
SELECT _op, count(*) FROM instacart.bronze.promotions_cdc GROUP BY _op;             -- 0 (snapshot), 1, 2, 4
```

- [ ] Các số trên khớp. Chạy `bundle run` lần hai: số dòng **không đổi**, flow báo 0 dòng mới.
- [ ] Thử *Full refresh all* trên pipeline dev: bronze giữ nguyên số dòng.
- [ ] Ghi kết quả vào `ops.reconciliation` bằng task `reconcile` ([06 §4](06_van_hanh_chay_lai.md)).
