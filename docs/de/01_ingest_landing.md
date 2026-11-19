# 01 · Đưa dữ liệu vào Azure: nguồn → `landing` (ADF, CDC, crawler)

**Đầu vào:**
- Stack `10-core` và `40-orchestration` đã apply: ADF, linked service `ls_adls`, `ls_sql_retail`, `ls_zenodo`, `ls_kv` ([devops/06 §1](../devops/06_de_den_dashboard.md)).
- Azure SQL `retail_db` đã có. Người bật CDC cần role `db_owner` trong DB: admin Entra (P1), hoặc user Entra của P2 do P1 tạo ([devops/03 §3.6](../devops/03_terraform_azure.md)).
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

Code thực thi chuẩn bị ở [adf/README.md](../../adf/README.md), gồm mảng activities của
`pl_ingest_instacart` và pipeline con `pl_ingest_instacart_table`, settings tham số/concurrency,
cùng hợp đồng ba dataset cho P1 triển khai qua `40-orchestration`.

1. Pipeline cha ForEach tuần tự sáu bảng, Get Metadata `_SUCCESS`, If chưa có marker thì gọi pipeline con.
2. Pipeline con kiểm template marker 0 byte, Copy HTTP CSV → `_staging` với mapping tường minh.
3. If `rowsRead` và `rowsCopied` đều bằng expected_rows thì Binary publish → Delete staging → Copy `_SUCCESS`.
   Sai số dòng thì Fail; thiếu trường output cũng là lỗi, không tự coi là đạt.
4. Kiểm marker thực sự tồn tại sau Copy; pipeline cha chỉ hoàn tất khi mọi bảng đã hoàn tất/bỏ qua hợp lệ.

Không đặt If kiểm số dòng bên trong If chưa có marker: ADF không cho lồng If trong If.
Dùng Execute Pipeline để chia scope; `waitOnCompletion = true`, concurrency 1 cho cả cha/con.
Auto Loader chỉ chạy sau khi cha thành công, không đọc trong lúc công bố file.

Tham số `expected_rows` giữ các số đếm **không gồm header**:

```json
{"orders": 3421083, "order_products__prior": 32434489, "order_products__train": 1384617,
 "products": 49688, "aisles": 134, "departments": 21}
```

Provider Terraform nhận parameter map(string), nên giá trị trên được mã hóa thành String JSON;
ADF đọc bằng `json()`. Cùng cách đó cho `tables_json` và `translators_json`, settings nằm cạnh activities.

- **Đã kiểm nguồn thật 2026-11-17:** MD5/header/số bản ghi CSV và `wc -l` trừ header của sáu file
  đều khớp catalog `adf/instacart_sources.json`; số đếm không cần đổi so với data dictionary.
- **Theo spike #141:** Zenodo record 2654444 có CSV không nén, dùng đường dẫn
  `api/records/2654444/files/<file>.csv/content`, không đặt ZipDeflate cho nguồn này.
- DelimitedText → DelimitedText không mapping có thể chép nguyên file và thiếu `rowsCopied`.
  Vì vậy Copy nhận toàn object translator qua `translator_json`, đặt Expression trên `translator`
  (chứa `type`, `mappings` đủ cột và `typeConversion`) và dùng `quoteAllText = true` như spike;
  giữ các cột dạng String, quote/escape theo CSV chuẩn và UTF-8 không BOM. Nguồn HTTP tự nhận LF/CRLF,
  không ép rowDelimiter; output ADLS dùng LF.
- Binary publish/marker bật consistency verification với PreserveHierarchy; không bật kiểm này cho HTTP
  vì connector không hỗ trợ. HTTP Copy retry 3 lần/300 giây theo chốt G0 #146;
  Copy ADLS/Metadata/Delete khác retry 5 lần/60 giây;
  riêng kiểm template marker retry 0 vì thiếu template là lỗi cấu hình.
- **Chưa kiểm thời gian thực tế:** prior khoảng 578 MB; timeout Copy là 2 giờ, ghi throughput/thời gian khi debug.
- **Chưa nghiệm thu Azure:** AD-OPS-25 còn mở; JSON đã kiểm local/SDK, chưa export/debug từ Studio
  hay có sáu thư mục landing thật. Checklist chạy lần đầu/lần hai nằm ở `adf/README.md`.

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

Chạy bằng tài khoản có `db_owner` trong `retail_db` (admin Entra hoặc user Entra của P2) sau khi AD-OPS-12 cấp `retail_db`
và AD-OPS-06 chốt nhánh CDC. Azure SQL Database có scheduler CDC riêng, **không cần SQL Agent**.
Script thực thi và cách chạy ở [src/ingest/sql/README.md](../../src/ingest/sql/README.md):

1. `01_schema.sql`: tạo `product_prices`, `promotions` nếu chưa có, giữ nguyên dữ liệu cũ.
2. `02_enable_cdc.sql`: bật CDC nếu chưa bật, tạo đúng `dbo_product_prices`, `dbo_promotions`,
   đặt retention **10.080 phút (7 ngày)** và in trạng thái kiểm tra.
3. `03_etl_control.sql`: tạo `etl.cdc_watermark`, `etl.run_log` và hai procedure, không reset mốc khi chạy lại.

```sql
EXEC etl.usp_get_cdc_window @capture_instance = N'dbo_product_prices';
EXEC etl.usp_get_cdc_window @capture_instance = N'dbo_promotions';
```

Lookup trả một dòng `mode`, `from_lsn`, `to_lsn`:

- `snapshot`: lần đầu hoặc `increment(last_lsn) < min_lsn`, `from_lsn = NULL`.
- `window`: đọc inclusive từ `increment(last_lsn)` tới max LSN đã capture.
- `empty`: max LSN bằng mốc hiện tại, `from_lsn = NULL`; setter chỉ ghi log và giữ mốc.

CDC chưa sẵn sàng/không truy cập được instance phải báo lỗi để thử lại, không ghi watermark LSN 0.
`etl.usp_set_cdc_watermark` giữ hợp đồng tham số ở §3.5; kiểm định dạng, khóa mốc theo capture instance,
không cho cửa sổ cũ/bỏ khoảng trống/hạ mốc. Update mốc và log trong cùng transaction;
retry cùng `(adf_run_id, capture_instance)` và nội dung là no-op, khác nội dung báo lỗi.
Chỉ gọi setter sau khi công bố landing và xóa staging thành công.

**Spike free offer đã đạt (#8, PR #141):** P1 xác nhận CDC trên Azure SQL free offer.
AD-DE-04 đã nghiệm thu trên `retail_db` do #14 cấp qua PR #140 ngày 2026-11-18.
Nhánh §3.7 chỉ là dự phòng, không chạy cùng 02/03.

LSN được truyền qua ADF dưới dạng **chuỗi hex cố định 22 ký tự** (`0x` + 20 chữ số hex). Chuỗi cùng độ dài thì so sánh theo thứ tự chữ cũng chính là so sánh theo số. Nhờ vậy ở silver có thể dùng chuỗi này trực tiếp trong `SEQUENCE BY`.

### 3.4 Script sinh dữ liệu và tạo thay đổi (`seed_retail_db.py`)

Để CDC có cả insert, update và delete, script chạy theo **đợt** (`--wave`):

| Đợt | Làm gì | `updated_at` (lịch giả lập) |
|---|---|---|
| 0 | Insert giá cho 49.688 sản phẩm và khoảng 3.000 khuyến mãi | `2015-01-01` (giá từ đầu, phủ cả đơn prior xa nhất) |
| k = 1..4 | Đổi giá khoảng 5% sản phẩm; thêm 300 khuyến mãi; gia hạn 50; **xóa** 30 (hủy khuyến mãi) | `2026-02-22 + 7·k ngày`, tức 01/03, 08/03, 15/03, 22/03, nằm trong cửa sổ replay 01–30/03 |

Code thực thi ở [src/ingest/seed_retail_db.py](../../src/ingest/seed_retail_db.py),
job riêng ở [jobs_ingest.yml](../../bundles/resources/jobs_ingest.yml);
[hướng dẫn seed](../../src/ingest/README.md) ghi tham số, cách chạy và nghiệm thu.

- `generate_wave()` là hàm thuần, dùng `Random(42 + wave)` trên danh mục sắp theo khóa;
  giá dùng Decimal, mã khuyến mãi không tái dùng, chọn hủy/gia hạn từ các chương trình chưa hủy.
- `apply_wave()` lấy application lock theo transaction, kiểm các wave trước,
  ghi toàn bộ thay đổi và marker `etl.seed_wave` trong một transaction. Lô 500 câu chỉ giảm số lần gửi SQL,
  **không commit giữa lô**; lỗi giữa đợt rollback cả giá, khuyến mãi và marker.
- Retry cùng danh mục/seed/kế hoạch/chế độ là no-op, in `wave N đã áp dụng, bỏ qua`;
  đổi fingerprint bị từ chối. Wave 0 không tự ghi đè nguồn có dữ liệu khi chưa có marker.
- Đường chạy Databricks đọc mật khẩu bằng `dbutils.secrets.get("kv", "sql-admin-password")`, không đọc env/file.
- Job chạy tay, một run, cluster theo policy `instacart-single-node`; không chạy cùng ADF.
  CDC hủy khuyến mãi bằng DELETE; nhánh §3.7 dùng `is_deleted` và thời gian wave.
- `--dry-run` kiểm kế hoạch trên CSV thật mà không kết nối SQL/Spark hoặc đọc secret.
  Regression Docker kiểm đủ 5 wave trên 49.688 mã tự dựng, gồm CDC, rollback và chạy đồng thời.
  AD-DE-04 và AD-OPS-22 đã hoàn tất. Nghiệm thu seed còn chờ CSV landing của AD-DE-06 hoặc bản danh mục thật đã cấp trên volume.

### 3.5 ADF pipeline `pl_ingest_retail_db`

Pipeline cha lần lượt gọi `pl_cdc_one_table` cho `dbo_product_prices` và `dbo_promotions`, chờ cả hai
hoàn tất. Lookup gọi `etl.usp_get_cdc_window` bằng tham số SQL `capture_instance` và giữ nguyên đầu
ra `mode`, `from_lsn`, `to_lsn` cho toàn tuyến của bảng.

| Mode | Nguồn và đường dẫn | Ghi mốc |
|---|---|---|
| `empty` | Không đọc hay ghi ADLS | Chỉ ghi log `rows = 0`, giữ nguyên watermark |
| `snapshot` | Bảng hiện hành → Parquet `snapshot=<to_lsn>/` | Setter lấy `to_lsn` từ Lookup |
| `window` | Hàm `cdc.fn_cdc_get_all_changes_*` với `from_lsn` và `to_lsn` inclusive → Parquet `window=<from_lsn>_<to_lsn>/` | Setter nhận đúng hai LSN và `rowsCopied` |

Snapshot đặt `_op = 0`, `_lsn = to_lsn` và `_seqval` bằng 0. Window giữ `_op` 1/2/4 với tùy chọn `all`
và chuẩn hóa `_lsn`, `_seqval` thành hex 22 ký tự. Hai chế độ xuất cùng cột capture và cột kỹ thuật
cho từng bảng. Các cột CDC gốc `__$…` là NULL có kiểu trong snapshot.

Copy SQL ghi một file `part-00000.parquet` vào staging. Pipeline `pl_cdc_publish_slice` đối chiếu
số dòng, Binary Copy có kiểm toàn vẹn, xác nhận file đích rồi xóa đúng file staging. Cửa sổ hợp lệ
với 0 dòng được dọn staging và ghi log 0 dòng. Setter chỉ chạy sau khi publisher thành công.
`from_lsn` được gửi riêng ở mode `window`. `snapshot` và `empty` dùng giá trị SQL mặc định NULL.
Run ID cha dùng để truy vết và retry setter, không xuất hiện trong đường dẫn file.

Snapshot đọc bảng hiện hành. Seed cần commit và CDC bắt kịp trước Lookup, sau đó không chạy seed
đồng thời với lượt ingest. Một pipeline có concurrency 1 và là writer duy nhất cho đường dẫn này.

JSON, thứ tự nạp resource, quyền và các bước debug/rerun nằm ở
[ADF retail CDC](../../adf/README_RETAIL_CDC.md).

### 3.6 Vì sao CDC theo cách này không mất và không trùng

| Tình huống | Điều xảy ra | Kết quả |
|---|---|---|
| Copy lỗi giữa chừng | Watermark **chưa** tăng. File dở chỉ nằm trong `_staging` | Rerun dùng Lookup đã giữ lại để đọc cùng cửa sổ. Lookup mới có thể lấy `to_lsn` khác |
| Lỗi sau khi công bố, trước khi gọi SP | Dữ liệu đã ở `window=A_B`, watermark chưa tăng | Rerun dùng Lookup và ID cũ. Nếu ADF chạy lại cả nhánh Switch, đường dẫn vẫn là `window=A_B` |
| Chạy lại **toàn bộ** pipeline sau lỗi như trên | `lk_window` tính lại: `from` cũ, `to` mới lớn hơn → thư mục `window=A_C` chứa lại các thay đổi của `A_B` | Bronze có **bản sao** của các thay đổi. Silver loại trùng theo `(_lsn, _seqval, khóa)` trước khi `AUTO CDC` ([03 §2.2](03_silver_cdc_scd_dq.md)) |
| Pipeline dừng lâu hơn retention (7 ngày) | `increment(last_lsn) < min_lsn` → `mode = snapshot` | Chụp lại toàn bảng. Silver SCD2 xem như upsert; khoảng thay đổi bị mất được ghi nhận trong `etl.run_log` |

### 3.7 Dự phòng nếu không bật được CDC

Dùng `01_schema.sql` → `04_fallback_watermark.sql` **thay cho** 02/03.
Script thêm `is_deleted bit NOT NULL DEFAULT 0`, tạo `etl.ts_watermark`, `etl.ts_run_log`
và hai procedure `usp_get_ts_window`, `usp_set_ts_watermark`.
Không trộn nhánh này với CDC đang bật; không tự disable CDC hoặc reset mốc.

- Seed không `DELETE` mà đặt `is_deleted = 1, updated_at = @wave_ts`; INSERT dùng danh sách cột tường minh.
- Lookup trả `mode`, `from_ts`, `to_ts`. Snapshot đọc toàn bảng cả tombstone;
  window đọc `updated_at > @from_ts AND updated_at <= @to_ts`; empty chỉ ghi log.
- **Dùng `MAX(updated_at)` của nguồn làm mốc trên**, không dùng giờ máy vì nguồn dự án theo lịch giả lập
  2015–03/2026. Nếu lấy giờ máy sau 03/2026, các đợt seed sau sẽ bị bỏ qua.
- Seed phải chạy từng đợt nguyên tử, mốc đợt sau lớn hơn mốc đã công bố; không chạy seed cùng ADF
  từ Lookup đến khi cập nhật watermark. Backdate/thêm dòng cùng timestamp của đợt đã công bố không được phát hiện.
- Silver vẫn dùng `AUTO CDC`, đổi `SEQUENCE BY updated_at`, `APPLY AS DELETE WHEN is_deleted = 1`.
- Chỉ thấy trạng thái cuối giữa các lần Copy, không giữ mọi lần sửa. Ghi rõ vì sao không phải CDC log-based.

Hợp đồng ADF, lệnh kiểm local/CI và checklist nghiệm thu Azure nằm ở
[src/ingest/sql/README.md](../../src/ingest/sql/README.md).

## 4. Open Food Facts: crawler theo lô (task `off_crawler`)

**Vì sao chạy bằng Python trên Databricks, không dùng ADF ForEach:**
- API giới hạn khoảng 10 lần tìm/phút, cần nghỉ có kiểm soát và retry khi gặp 429.
- Cần đọc độ phổ biến sản phẩm từ `bronze.order_products_prior` để chọn sản phẩm nào tra trước.
- ADF chỉ việc gọi job (`pl_ingest_off`).

**Bản triển khai AD-DE-10:** [hướng dẫn OFF](../../src/ingest/README.md#off-crawler-ad-de-10),
[`off_crawler.py`](../../src/ingest/off_crawler.py), [`off_state.py`](../../src/ingest/off_state.py)
và [`off_images.py`](../../src/ingest/off_images.py). Job DAB nằm ở
[`jobs_ingest.yml`](../../bundles/resources/jobs_ingest.yml), hai task chạy nối tiếp, không có lịch tự động.

**Bảng trạng thái** Delta: prod dùng `instacart.landing_meta.off_crawl_state`, dev dùng
`instacart.<user>_ops.off_crawl_state`. Các cột là `product_id, status, attempts, off_code,
batch_file, last_error, updated_at, batch_id`. Code tạo bảng khi chưa có; schema và quyền do P1 cấp.

- Chọn top 2.000 theo số lần xuất hiện trong prior, mỗi lô tối đa 300; tie-break bằng product_id.
  Bỏ qua done/no_result/failed, chỉ retry khi attempts < 5. Mặc định đọc bronze; bootstrap có chủ đích
  `source=landing` đọc products và món prior từ CSV đã công bố để khởi tạo OFF trước update LDP đầu tiên.
- Search dùng endpoint Search-a-licious và User-Agent của dự án, tối đa 5 HTTP request/lần tra;
  timeout/429/5xx có backoff và Retry-After. Mọi search request, kể cả retry, cách nhau ≥ 6,5 giây.
  Không biến phản hồi lỗi hoặc sai cấu trúc thành no_result.
- Plan và kết quả từng sản phẩm nằm trong journal `_staging/off`. Batch ID từ danh sách sản phẩm,
  tên tra và attempt; JSONL có tên hash nội dung, không có ngày/run ID trong tên:
  `off_api/products/batch=<plan_hash>/batch_<content_hash>.json`.
- Publish trước MERGE, attempts là giá trị tuyệt đối để replay không cộng thêm. Chạy lại phục hồi
  active plan/journal trước, không tra lại phần đã lưu. Không ghi đè landing hay công bố JSONL rỗng.
  Vẫn có thể lặp một request nếu process chết giữa HTTP response và ghi journal.
- Ảnh lấy từ JSONL đã công bố, không cần chờ bronze.off_products; chỉ tải JPG còn thiếu vào
  `off_api/images/<code>.jpg`. Ảnh đã có không phát request; kiểm host, Content-Type, kích thước và JPEG.

**Công bố file:** dùng rename trong cùng volume, dừng nếu backend không hỗ trợ; không fallback copy.
`dbutils.fs.mv` là copy rồi xóa, không nên coi là rename nguyên tử. Test local chưa chứng minh hành vi
rename trên UC Volume Azure; phải nghiệm thu trước khi dùng ingestion song song.

Dev dùng off_root riêng dưới `raw/dev/<user>/off_api`, cùng giá trị được truyền cho bronze;
prod vẫn đọc `raw/off_api`. Không chạy đồng thời nhiều writer/crawler dùng chung state, off_root hoặc
IP gọi API; không đổi cấu hình hay xóa journal trong lúc phục hồi. Chi tiết và checklist ở hướng dẫn OFF.

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
| Copy CDC | *Rerun from failed activity* trên pipeline con, giữ output Lookup và ID lượt ingest | Watermark chưa tăng. Cùng cặp LSN thì dùng cùng thư mục |
| SP set watermark | *Rerun from failed activity* trên pipeline con | Giữ ID lượt ingest cũ và tham số từ Lookup. Setter cùng đầu vào là no-op nếu đã commit |
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
