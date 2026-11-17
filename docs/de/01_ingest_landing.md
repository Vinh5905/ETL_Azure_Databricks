# 01 · Đưa dữ liệu vào Azure: nguồn → `landing` (ADF, CDC, crawler)

**Đầu vào:**
- Stack `10-core` và `40-orchestration` đã apply: ADF, linked service `ls_adls`, `ls_sql_retail`, `ls_zenodo`, `ls_kv` ([devops/06 §1](../devops/06_de_den_dashboard.md)).
- Azure SQL `retail_db` đã có. Tài khoản admin Entra của server dùng để bật CDC.
- Quy ước đường dẫn ở [00 §3](00_tong_quan_de.md).

**Đầu ra:**
- `landing/instacart/*/release=2017/` đủ 6 file CSV.
- `landing/retail_db/<bảng>/snapshot=…` và các thư mục `window=…` chứa thay đổi CDC dạng Parquet.
- `landing/off_api/products/…` và `landing/off_api/images/…`.
- Bảng điều khiển `etl.cdc_watermark`, `etl.run_log` (Azure SQL) và `landing_meta.off_crawl_state` (Delta).

**Xong khi:** đạt checklist ở mục 6.

---

## 1. Khuôn chung cho mọi ADF pipeline: staging → công bố → đánh dấu

Copy activity bị đứt giữa chừng sẽ để lại **file dở dang**. Nếu Auto Loader nạp file đó, bronze sẽ thiếu dòng. Khi chạy lại, file đầy đủ được nạp thêm và bronze trùng dòng. Vì vậy mọi pipeline đều đi theo 4 bước:

```
1. Copy  nguồn → landing/_staging/<đường dẫn đích>/        (Auto Loader không đọc _staging)
2. Copy  _staging → <đường dẫn đích tất định>/   (binary, cùng tên file → ghi đè nếu chạy lại)
3. Delete _staging/<đường dẫn đích>/
4. Ghi mốc: _SUCCESS (CSV) hoặc cập nhật watermark (CDC)   ← chỉ chạy khi 1–3 thành công
```

- Bước 2 là thao tác chép trong cùng storage account, nhanh, và gần như không lỗi giữa chừng.
- Pipeline Databricks chỉ được gọi **sau** bước 4 (ADF `pl_master` nối tiếp). Vì vậy không có chuyện bronze đọc trong lúc ADF đang ghi.
- Auto Loader chỉ trỏ vào `instacart/`, `retail_db/`, `off_api/`, **không** trỏ vào gốc `landing/`. Thư mục `_staging` vì vậy không bao giờ được đọc.

## 2. Instacart CSV từ Zenodo: full load một lần (`pl_ingest_instacart`)

**Vì sao chạy lại được an toàn:**
- Thư mục đích là `release=2017`, cố định.
- Nếu đã có `_SUCCESS` thì bỏ qua hẳn bảng đó.

```
ForEach table in ["orders","order_products__prior","order_products__train","products","aisles","departments"]
  ├─ Get Metadata  landing/instacart/<t>/release=2017/_SUCCESS  (field: exists)
  └─ If Condition  not(exists)
       ├─ Copy  HTTP (Zenodo) → _staging/instacart/<t>/  [DelimitedText → DelimitedText]
       ├─ If    rowsCopied != expected_rows[t]  → Fail activity "row count mismatch"
       ├─ Copy  _staging/... → instacart/<t>/release=2017/<t>.csv  [Binary]
       ├─ Delete _staging/instacart/<t>/
       └─ Copy  template ops/_SUCCESS (file 0 byte) → instacart/<t>/release=2017/_SUCCESS
```

Tham số pipeline `expected_rows` dùng để kiểm số dòng **trước khi công bố**:

```json
{"orders": 3421083, "order_products__prior": 32434489, "order_products__train": 1384617,
 "products": 49688, "aisles": 134, "departments": 21}
```

- Copy DelimitedText đếm số dòng **dữ liệu**, không tính header. Số trên lấy theo bản Kaggle; nếu bản Zenodo khác thì sửa tham số sau khi kiểm bằng `wc -l` ([data_dictionary](../data/data_dictionary.md)).

**Nguồn và cấu hình Copy (kết quả spike 3.5, [spike_results](../devops/spike_results.md)):**
- **URL:** record Zenodo [2654444](https://zenodo.org/records/2654444) có đủ 6 file CSV **không nén**. Dataset HTTP dùng đường dẫn tương đối `api/records/2654444/files/<t>.csv/content` trên `ls_zenodo` (base URL `https://zenodo.org/`). Không cần `ZipDeflate`.
- **Phải khai mapping cột tường minh.** Copy DelimitedText → DelimitedText cùng định dạng mà không có mapping thì ADF chép nguyên file và **không trả `rowsCopied`**, làm bước kiểm số dòng ở trên không chạy được. Mỗi bảng khai `translator` kiểu `TabularTranslator` có `mappings` liệt kê đủ cột theo [data_dictionary](../data/data_dictionary.md). Lần spike đạt có kèm `quoteAllText = true` ở sink và chưa tách riêng yếu tố nào quyết định, nên giữ cả hai.
- **Retry trên activity Copy:** Zenodo có lúc trả HTTP 503 khi bảo trì (gặp sáng 2026-11-17). Đặt `policy.retry = 3`, `retryIntervalInSeconds = 300`. Copy ghi vào `_staging`, nên chạy lại không làm bẩn landing.
- **Chưa kiểm:** thời gian copy `order_products__prior.csv` (khoảng 578 MB). Đặt `policy.timeout` của activity này rộng (ví dụ `0.02:00:00`) và ghi thời gian thật vào báo cáo.

## 3. Azure SQL: CDC thật theo LSN (`pl_ingest_retail_db`)

### 3.1 CDC là gì, khác watermark `updated_at` ở đâu

| | Watermark `updated_at` | **SQL Server CDC** |
|---|---|---|
| Đọc gì | Bảng hiện tại: `WHERE updated_at > @mốc` | **Bảng thay đổi** do SQL tự ghi từ transaction log |
| Bắt được delete | Không. Dòng đã xóa thì không còn để đọc | **Có** (`__$operation = 1`) |
| Bắt được nhiều lần sửa giữa 2 lần chạy | Không, chỉ thấy giá trị cuối | **Có**, mỗi lần sửa là một dòng |
| Phụ thuộc ứng dụng | Ứng dụng phải luôn cập nhật `updated_at` | Không phụ thuộc |
| Mốc | Thời gian (có thể lệch đồng hồ) | **LSN**: số thứ tự tăng dần của log, không trùng |

Mã `__$operation` khi đọc bằng `fn_cdc_get_all_changes_…(…, N'all')`:

| Mã | Nghĩa |
|---|---|
| 1 | delete |
| 2 | insert |
| 4 | update (giá trị **sau** khi sửa) |

Mã 3 (giá trị trước khi sửa) chỉ xuất hiện khi dùng `N'all update old'`. Nhóm không cần.

### 3.2 Thiết kế bảng nguồn để có thay đổi thật

`product_prices` trong Azure SQL là **bảng giá hiện hành** của hệ thống bán hàng (mỗi sản phẩm một dòng; đổi giá = `UPDATE`). **Lịch sử giá không lưu ở nguồn** mà được dựng ở silver bằng SCD Type 2 từ luồng CDC ([03 §2](03_silver_cdc_scd_dq.md)). Đây đúng là bài toán CDC thường gặp ngoài thực tế. [data_dictionary §3.1](../data/data_dictionary.md) đã được cập nhật theo thiết kế này.

```sql
-- src/ingest/sql/01_schema.sql
CREATE TABLE dbo.product_prices (
    product_id  int           NOT NULL PRIMARY KEY,
    unit_price  decimal(10,2) NOT NULL,
    currency    char(3)       NOT NULL DEFAULT 'USD',
    updated_at  datetime2     NOT NULL      -- thời điểm đổi giá theo lịch giả lập
);
CREATE TABLE dbo.promotions (
    promo_id     varchar(10) NOT NULL PRIMARY KEY,
    product_id   int         NOT NULL,
    promo_type   varchar(20) NOT NULL,      -- percent_off | bogo
    discount_pct int         NOT NULL,
    start_date   date        NOT NULL,
    end_date     date        NOT NULL,
    updated_at   datetime2   NOT NULL
);
```

### 3.3 Bật CDC và tạo bảng điều khiển

Chạy một lần bằng tài khoản admin Entra của server (`sqlcmd` hoặc Query editor trên Portal). Azure SQL Database có bộ lập lịch CDC riêng, **không cần SQL Agent**.

```sql
-- src/ingest/sql/02_enable_cdc.sql
EXEC sys.sp_cdc_enable_db;

EXEC sys.sp_cdc_enable_table
     @source_schema = N'dbo', @source_name = N'product_prices',
     @role_name = NULL, @supports_net_changes = 1;

EXEC sys.sp_cdc_enable_table
     @source_schema = N'dbo', @source_name = N'promotions',
     @role_name = NULL, @supports_net_changes = 1;

-- Giữ thay đổi 7 ngày (mặc định 3 ngày) để có thời gian sửa lỗi pipeline
EXEC sys.sp_cdc_change_job @job_type = N'cleanup', @retention = 10080;   -- phút

-- Kiểm tra
SELECT name, is_cdc_enabled FROM sys.databases WHERE name = DB_NAME();
SELECT capture_instance, start_lsn FROM cdc.change_tables;  -- dbo_product_prices, dbo_promotions
```

> **Spike bắt buộc:** chưa có tài liệu chính thức xác nhận CDC chạy được trên DB *free offer* (serverless General Purpose).
> Nếu `sp_cdc_enable_db` báo lỗi tier, dùng phương án dự phòng ở mục 3.7.

```sql
-- src/ingest/sql/03_etl_control.sql
CREATE SCHEMA etl;
GO
CREATE TABLE etl.cdc_watermark (
    capture_instance sysname    NOT NULL PRIMARY KEY,
    last_lsn         binary(10) NULL,           -- NULL = chưa từng chạy
    updated_at       datetime2  NOT NULL DEFAULT sysutcdatetime()
);
INSERT INTO etl.cdc_watermark(capture_instance) VALUES ('dbo_product_prices'), ('dbo_promotions');

CREATE TABLE etl.run_log (
    adf_run_id       varchar(64)  NOT NULL,
    capture_instance sysname      NOT NULL,
    mode             varchar(10)  NOT NULL,     -- snapshot | window | empty
    from_lsn         varchar(22)  NULL,
    to_lsn           varchar(22)  NOT NULL,
    rows_copied      bigint       NULL,
    finished_at      datetime2    NOT NULL DEFAULT sysutcdatetime()
);
GO

-- Tính "cửa sổ" LSN cần đọc. Trả về một dòng cho ADF Lookup.
CREATE OR ALTER PROCEDURE etl.usp_get_cdc_window @capture_instance sysname
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @last binary(10) = (SELECT last_lsn FROM etl.cdc_watermark
                                WHERE capture_instance = @capture_instance);
    DECLARE @min  binary(10) = sys.fn_cdc_get_min_lsn(@capture_instance);
    DECLARE @max  binary(10) = sys.fn_cdc_get_max_lsn();
    DECLARE @from binary(10) = CASE WHEN @last IS NULL THEN NULL
                                    ELSE sys.fn_cdc_increment_lsn(@last) END;

    SELECT
        mode = CASE
                 WHEN @last IS NULL OR @last < @min THEN 'snapshot'   -- lần đầu, hoặc đã quá retention
                 WHEN @from > @max                  THEN 'empty'      -- không có gì mới
                 ELSE 'window' END,
        from_lsn = CONVERT(varchar(22), @from, 1),                   -- dạng '0x0000002A000001F80003'
        to_lsn   = CONVERT(varchar(22), @max, 1);
END;
GO

-- Chỉ gọi sau khi dữ liệu đã được công bố lên landing.
CREATE OR ALTER PROCEDURE etl.usp_set_cdc_watermark
    @capture_instance sysname, @to_lsn varchar(22),
    @adf_run_id varchar(64), @mode varchar(10), @from_lsn varchar(22) = NULL, @rows bigint = NULL
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRAN;
      UPDATE etl.cdc_watermark
         SET last_lsn = CONVERT(binary(10), @to_lsn, 1), updated_at = sysutcdatetime()
       WHERE capture_instance = @capture_instance;
      INSERT INTO etl.run_log(adf_run_id, capture_instance, mode, from_lsn, to_lsn, rows_copied)
      VALUES (@adf_run_id, @capture_instance, @mode, @from_lsn, @to_lsn, @rows);
    COMMIT;
END;
```

LSN được truyền qua ADF dưới dạng **chuỗi hex cố định 22 ký tự** (`0x` + 20 chữ số hex). Chuỗi cùng độ dài thì so sánh theo thứ tự chữ cũng chính là so sánh theo số. Nhờ vậy ở silver có thể dùng chuỗi này trực tiếp trong `SEQUENCE BY`.

### 3.4 Script sinh dữ liệu và tạo thay đổi (`seed_retail_db.py`)

Để CDC có cả insert, update và delete, script chạy theo **đợt** (`--wave`):

| Đợt | Làm gì | `updated_at` (lịch giả lập) |
|---|---|---|
| 0 | Insert giá cho 49.688 sản phẩm và khoảng 3.000 khuyến mãi | `2015-01-01` (giá từ đầu, phủ cả đơn prior xa nhất) |
| k = 1..4 | Đổi giá khoảng 5% sản phẩm; thêm 300 khuyến mãi; gia hạn 50; **xóa** 30 (hủy khuyến mãi) | `2026-02-22 + 7·k ngày`, tức 01/03, 08/03, 15/03, 22/03, nằm trong cửa sổ replay 01–30/03 |

```python
# src/ingest/seed_retail_db.py  (rút gọn; chạy trong task Databricks hoặc local)
import argparse, datetime as dt, random
import pymssql   # %pip install pymssql

DEPT_PRICE = {4: (0.2, 4.0), 5: (8.0, 40.0), 7: (1.0, 6.0)}   # department_id -> (min, max); còn lại (1, 10)

def connect(server: str, db: str, user: str, password: str):
    return pymssql.connect(server=server, database=db, user=user, password=password, autocommit=False)

def already_applied(cur, wave: int) -> bool:
    cur.execute("IF OBJECT_ID('etl.seed_wave') IS NULL "
                "CREATE TABLE etl.seed_wave(wave int PRIMARY KEY, applied_at datetime2 DEFAULT sysutcdatetime())")
    cur.execute("SELECT 1 FROM etl.seed_wave WHERE wave = %d", (wave,))
    return cur.fetchone() is not None

def wave0(cur, products, rng):
    ts = dt.datetime(2015, 1, 1)
    rows = []
    for pid, dept in products:
        lo, hi = DEPT_PRICE.get(dept, (1.0, 10.0))
        rows.append((pid, round(rng.uniform(lo, hi), 2), "USD", ts))
    cur.executemany("INSERT INTO dbo.product_prices VALUES (%d, %s, %s, %s)", rows)
    # ... insert promotions tương tự (promo_id P00001...)

def wave_k(cur, k, products, rng):
    ts = dt.datetime(2026, 2, 22) + dt.timedelta(days=7 * k)
    changed = rng.sample([p for p, _ in products], k=len(products) // 20)
    cur.executemany(
        "UPDATE dbo.product_prices SET unit_price = ROUND(unit_price * %s, 2), updated_at = %s WHERE product_id = %d",
        [(rng.choice([0.85, 0.9, 1.05, 1.1]), ts, pid) for pid in changed])
    cur.execute("SELECT TOP 30 promo_id FROM dbo.promotions ORDER BY CHECKSUM(promo_id, %d)", (k,))
    to_delete = [r[0] for r in cur.fetchall()]
    cur.executemany("DELETE FROM dbo.promotions WHERE promo_id = %s", [(p,) for p in to_delete])
    # ... insert 300 promotions mới, gia hạn end_date của 50 promotions

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wave", type=int, required=True)
    args = ap.parse_args()
    rng = random.Random(42 + args.wave)          # cùng đợt → cùng dữ liệu trên máy 4 người
    conn = connect(*get_conn_params())           # đọc từ secret scope `kv` (Databricks) hoặc biến môi trường
    cur = conn.cursor()
    if already_applied(cur, args.wave):
        print(f"wave {args.wave} đã áp dụng, bỏ qua"); return
    products = load_products()                   # product_id, department_id từ products.csv
    (wave0 if args.wave == 0 else lambda c, p, r: wave_k(c, args.wave, p, r))(cur, products, rng)
    cur.execute("INSERT INTO etl.seed_wave(wave) VALUES (%d)", (args.wave,))
    conn.commit()                                # một transaction cho cả đợt: lỗi giữa chừng → rollback hết
```

**Chạy lại an toàn** nhờ hai điểm:
- Cả đợt nằm trong **một transaction**: lỗi giữa chừng thì rollback toàn bộ, không có nửa đợt.
- Bảng `etl.seed_wave` ngăn áp dụng một đợt hai lần.

### 3.5 ADF pipeline `pl_ingest_retail_db`

```
ForEach t in [{ci:"dbo_product_prices", table:"product_prices"}, {ci:"dbo_promotions", table:"promotions"}]   (tuần tự)
  └─ Execute pipeline pl_cdc_one_table(ci, table)

pl_cdc_one_table
  ├─ Lookup   lk_window       → etl.usp_get_cdc_window(@ci)       → mode, from_lsn, to_lsn
  ├─ Switch   mode
  │   ├─ "empty"    → SP  etl.usp_set_cdc_watermark(mode='empty')  (chỉ ghi log, LSN giữ nguyên)
  │   ├─ "snapshot" → Copy (SELECT * FROM dbo.<table>) → _staging → snapshot=<to_lsn>/   → SP set watermark
  │   └─ "window"   → Copy (fn_cdc_get_all_changes) → _staging → window=<from>_<to>/     → SP set watermark
  └─ (lỗi) → Web activity ghi cảnh báo / pipeline fail
```

Câu truy vấn nguồn của Copy ở nhánh `window` (dynamic content trong ADF):

```sql
@concat('SELECT CONVERT(varchar(22), __$start_lsn, 1) AS _lsn, ',
        'CONVERT(varchar(22), __$seqval, 1) AS _seqval, ',
        '__$operation AS _op, * ',
        'FROM cdc.fn_cdc_get_all_changes_', pipeline().parameters.ci, '(',
        'CONVERT(binary(10), ''', activity('lk_window').output.firstRow.from_lsn, ''', 1), ',
        'CONVERT(binary(10), ''', activity('lk_window').output.firstRow.to_lsn,   ''', 1), ',
        'N''all'')')
```

`SELECT *` của hàm CDC vẫn trả về các cột `__$…`. Dataset Parquet sẽ ghi cả những cột này. Bronze giữ nguyên, còn silver chỉ dùng `_lsn`, `_seqval`, `_op`.

Nhánh `snapshot`:

```sql
@concat('SELECT ''', activity('lk_window').output.firstRow.to_lsn, ''' AS _lsn, ',
        '''0x00000000000000000000'' AS _seqval, 0 AS _op, * FROM dbo.', pipeline().parameters.table)
```

- `_op = 0` nghĩa là bản ghi snapshot, silver xử lý như upsert.
- Snapshot đọc bảng tại thời điểm `to_lsn`. Nhóm chỉ chạy `seed_retail_db` khi ADF **không** chạy, nên không có thay đổi chen vào giữa. Hệ thống thật cần snapshot có khóa nhất quán hơn.

Đoạn JSON của activity gọi watermark (dán vào `adf/pipelines/pl_cdc_one_table.json`):

```json
{
  "name": "sp_set_watermark_window",
  "type": "SqlServerStoredProcedure",
  "dependsOn": [{ "activity": "del_staging_window", "dependencyConditions": ["Succeeded"] }],
  "linkedServiceName": { "referenceName": "ls_sql_retail", "type": "LinkedServiceReference" },
  "typeProperties": {
    "storedProcedureName": "etl.usp_set_cdc_watermark",
    "storedProcedureParameters": {
      "capture_instance": { "value": "@pipeline().parameters.ci", "type": "String" },
      "to_lsn":   { "value": "@activity('lk_window').output.firstRow.to_lsn", "type": "String" },
      "from_lsn": { "value": "@activity('lk_window').output.firstRow.from_lsn", "type": "String" },
      "mode":     { "value": "window", "type": "String" },
      "adf_run_id": { "value": "@pipeline().RunId", "type": "String" },
      "rows":     { "value": "@activity('cp_window_to_staging').output.rowsCopied", "type": "Int64" }
    }
  }
}
```

### 3.6 Vì sao CDC theo cách này không mất và không trùng

| Tình huống | Điều xảy ra | Kết quả |
|---|---|---|
| Copy lỗi giữa chừng | Watermark **chưa** tăng. File dở chỉ nằm trong `_staging` | Chạy lại đọc **cùng** cửa sổ từ `from_lsn` cũ → không mất |
| Lỗi sau khi công bố, trước khi gọi SP | Dữ liệu đã ở `window=A_B`, watermark chưa tăng | *Rerun from failed activity*: ADF giữ output của `lk_window` → chỉ chạy lại SP → không đọc lại |
| Chạy lại **toàn bộ** pipeline sau lỗi như trên | `lk_window` tính lại: `from` cũ, `to` mới lớn hơn → thư mục `window=A_C` chứa lại các thay đổi của `A_B` | Bronze có **bản sao** của các thay đổi. Silver loại trùng theo `(_lsn, _seqval, khóa)` trước khi `AUTO CDC` ([03 §2.2](03_silver_cdc_scd_dq.md)) |
| Pipeline dừng lâu hơn retention (7 ngày) | `last_lsn < min_lsn` → `mode = snapshot` | Chụp lại toàn bảng. Silver SCD2 xem như upsert; khoảng thay đổi bị mất được ghi nhận trong `etl.run_log` |

### 3.7 Dự phòng nếu không bật được CDC

Dùng watermark `updated_at` kèm **soft delete**:
- Thêm cột `is_deleted bit`. Script seed không `DELETE` mà đặt `is_deleted = 1, updated_at = …`.
- Copy chạy `WHERE updated_at > @last_ts AND updated_at <= @to_ts`, trong đó `@to_ts = sysutcdatetime()` được lấy một lần ở Lookup.
- Silver giữ nguyên `AUTO CDC`, chỉ đổi điều kiện:
  - `SEQUENCE BY updated_at`.
  - `APPLY AS DELETE WHEN is_deleted = 1`.
- Ghi rõ trong báo cáo vì sao không phải CDC log-based.

## 4. Open Food Facts: crawler theo lô (task `off_crawler`)

**Vì sao chạy bằng Python trên Databricks, không dùng ADF ForEach:**
- API giới hạn khoảng 10 lần tìm/phút, cần nghỉ có kiểm soát và retry khi gặp 429.
- Cần đọc độ phổ biến sản phẩm từ `bronze.order_products_prior` để chọn sản phẩm nào tra trước.
- ADF chỉ việc gọi job (`pl_ingest_off`).

**Bảng trạng thái** (Delta, quản lý bằng MERGE):

```sql
CREATE TABLE IF NOT EXISTS instacart.landing_meta.off_crawl_state (
  product_id   INT,
  status       STRING,      -- done | no_result | retry | failed
  attempts     INT,
  off_code     STRING,
  batch_file   STRING,
  last_error   STRING,
  updated_at   TIMESTAMP
);
```

```python
# src/ingest/off_crawler.py  (rút gọn)
import hashlib, json, time, requests
from pyspark.sql import SparkSession

SEARCH = "https://search.openfoodfacts.org/search"
FIELDS = "code,product_name,brands,categories_tags,countries_tags,nutriscore_grade,nova_group,nutriments,image_url"
HEADERS = {"User-Agent": "InstacartCourseProject/1.0 (student project, non-commercial)"}
VOL = "/Volumes/instacart/landing_meta/raw/off_api"

def pick_batch(spark, n=300, top=2000):
    return [r.product_id for r in spark.sql(f"""
        WITH pop AS (
          SELECT product_id, count(*) AS cnt FROM instacart.bronze.order_products_prior
          GROUP BY product_id ORDER BY cnt DESC LIMIT {top})
        SELECT p.product_id FROM pop p
        LEFT JOIN instacart.landing_meta.off_crawl_state s USING (product_id)
        WHERE s.status IS NULL OR (s.status = 'retry' AND s.attempts < 5)
        ORDER BY p.cnt DESC LIMIT {n}""").collect()]

def search(name: str, max_retry=5):
    for i in range(max_retry):
        r = requests.get(SEARCH, params={"q": name, "fields": FIELDS, "page_size": 5},
                         headers=HEADERS, timeout=30)
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(int(r.headers.get("Retry-After", 2 ** i * 10))); continue
        r.raise_for_status()
        return r.json().get("hits", [])
    raise RuntimeError("retry exhausted")

def run(spark):
    ids = pick_batch(spark)
    if not ids:
        print("hết sản phẩm cần tra"); return
    names = {r.product_id: r.product_name for r in spark.table("instacart.bronze.products")
             .where(f"product_id IN ({','.join(map(str, ids))})").collect()}
    lines, state = [], []
    for pid in ids:
        try:
            hits = search(names[pid])
            for h in hits:
                lines.append(json.dumps({**h, "_product_id": pid, "_search_query": names[pid]}))
            state.append((pid, "done" if hits else "no_result", hits[0]["code"] if hits else None, None))
        except Exception as e:                          # lỗi một sản phẩm không làm hỏng cả lô
            state.append((pid, "retry", None, str(e)[:500]))
        time.sleep(6.5)                                 # ≤ 10 request/phút

    batch_id = hashlib.sha1(",".join(map(str, sorted(ids))).encode()).hexdigest()[:12]
    day = time.strftime("%Y-%m-%d")
    final = f"{VOL}/products/ingest_date={day}/batch_{batch_id}.json"
    tmp = f"{VOL}/_staging/off/batch_{batch_id}.json"
    dbutils.fs.put(tmp, "\n".join(lines), overwrite=True)
    dbutils.fs.mv(tmp, final)                           # đổi tên trên ADLS HNS là thao tác nguyên tử
    upsert_state(spark, state, final)                   # MERGE INTO off_crawl_state, attempts = attempts + 1
```

**Ảnh sản phẩm:** task `off_images` chạy sau. Với mỗi `off_code` có `image_url`, nếu `off_api/images/<code>.jpg` **chưa có** thì tải. File đã có thì bỏ qua, nên chạy lại không tải lại.

**Chạy lại an toàn:**
- Sản phẩm `done` / `no_result` không bị tra lại.
- Tên file lô được tính từ danh sách `product_id`. Chạy lại cùng lô thì ghi đè **cùng tên**, và Auto Loader không nạp lại file đã nạp.
- Nếu crawler chết sau khi ghi file nhưng trước khi MERGE trạng thái, lần sau sẽ tra lại đúng các sản phẩm đó. Silver vẫn chỉ giữ **một** kết quả mỗi `_product_id`, lấy bản có `_ingest_ts` mới nhất ([03 §5](03_silver_cdc_scd_dq.md)).

## 5. ADF `pl_master` và nhật ký chạy

```
pl_master (trigger 02:00, tham số wave_seed: bool = false)
  ├─ Execute pl_ingest_retail_db
  ├─ Execute pl_ingest_off            (gọi job off_crawler → off_images)
  ├─ Databricks Job  instacart-batch-medallion   (job_parameters: adf_run_id = @pipeline().RunId)
  └─ On failure  → Web activity gửi Teams/email (tùy chọn)
```

- Các activity nối với nhau bằng điều kiện **Succeeded**, nên Databricks chỉ chạy khi landing đã công bố xong.
- Job Databricks ghi `adf_run_id` vào `ops.pipeline_runs`, nối được hai hệ thống khi truy vết.

## 6. Lỗi giữa chừng thì chạy lại thế nào

| Lỗi ở | Làm gì | Vì sao an toàn |
|---|---|---|
| Copy Zenodo | ADF Monitor → *Rerun from failed activity* | Chưa có `_SUCCESS` → bảng đó copy lại từ đầu vào `_staging`. Bảng đã xong thì bỏ qua |
| Kiểm số dòng Zenodo fail | Kiểm tra lại nguồn, sửa `expected_rows`, rerun | Chưa công bố, landing sạch |
| Copy CDC | *Rerun from failed activity* | Watermark chưa tăng → cùng cửa sổ, cùng thư mục |
| SP set watermark | *Rerun from failed activity* | Chỉ chạy lại SP, dùng `to_lsn` cũ |
| Crawler OFF | Chạy lại job `off_crawler` | Trạng thái theo từng sản phẩm; tên file tất định |
| Seed wave | Chạy lại cùng `--wave` | Transaction + `etl.seed_wave` |

Chi tiết từng tầng sau landing: [06_van_hanh_chay_lai.md](06_van_hanh_chay_lai.md).

## 7. Checklist

- [ ] 6 thư mục `instacart/*/release=2017/` đều có `_SUCCESS`; `rowsCopied` khớp `expected_rows`.
- [ ] `SELECT * FROM etl.run_log` có 2 dòng `snapshot`. Sau `seed --wave 1` và chạy lại: 2 dòng `window`, `rows_copied > 0`.
- [ ] Parquet ở `window=…` của `promotions` có dòng `_op = 1` (delete) và `_op = 4` (update).
- [ ] Chạy `pl_ingest_retail_db` hai lần liên tiếp không seed: lần hai ra `mode = empty`, không có thư mục mới.
- [ ] Bắn lỗi thử (đổi tạm tên bảng trong query) → watermark không đổi → sửa → rerun → không mất thay đổi.
- [ ] `off_crawl_state` có ít nhất 300 dòng `done` / `no_result` sau lô đầu.
