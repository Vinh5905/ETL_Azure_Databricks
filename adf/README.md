# ADF · ingest Instacart (AD-DE-06)

[DE/01 §1–2](../docs/de/01_ingest_landing.md) là thiết kế; các JSON ở đây chuẩn bị cho
AD-OPS-25 (#27) và AD-OPS-26 (#28) triển khai qua Terraform `40-orchestration`.
**Chưa debug/export từ ADF Studio hoặc chạy trên factory thật.** Kiểm tra ngày 2026-11-18
xác nhận Data Factory đã tồn tại, còn danh sách linked service, dataset và pipeline đều rỗng.
JSON được dựng theo API và đã kiểm model SDK. Terraform phụ trách publish.

## File và ranh giới với P1

| File | Nội dung | Cách dùng |
|---|---|---|
| `instacart_sources.json` | Zenodo record 2654444, release 2017, 6 bảng, header/count/MD5 | Nguồn dựng mapping và tham số; checksum dùng đối chiếu nguồn local |
| `pipelines/pl_ingest_instacart.json` | **Mảng activities** pipeline cha | Gửi nguyên làm `properties.activities` |
| `pipelines/pl_ingest_instacart_table.json` | Mảng activities pipeline con | Stack tự tạo con trước cha theo `referenceName` `pl_*` |
| `pipeline_settings/<pipeline>.json` | `concurrency` và `parameters` mặc định | P1 phải nạp cùng activities, không bỏ qua |
| `datasets/*.json` | 5 định nghĩa dataset với linked service theo tên | Stack `40-orchestration` publish mọi file bằng `azapi_resource` (AD-OPS-25b); `name` phải trùng tên file |
| `assets/_SUCCESS` | Template 0 byte | P1 đưa đúng file vào `landing/ops/_SUCCESS` trước lần chạy đầu |

[ADF retail CDC](README_RETAIL_CDC.md) mô tả hai dataset retail SQL/Parquet và ba pipeline CDC. Copy Binary dùng chung dataset ADLS của Instacart.

Stack `40-orchestration` khai mọi tham số pipeline là **String** (`settings.parameters` là map chuỗi).
Vì vậy `tables_json`, `expected_rows`, `translators_json` là **String chứa JSON**, đọc bằng `json()` trong ADF;
không đổi thành object/array. Pipeline con nhận `table_name`, `expected_rows`, `translator_json` cũng là String.
Cha chọn object translator của từng bảng, gửi cả `type`, `mappings`, `typeConversion` sang con;
Copy đặt Expression **trên toàn bộ `translator`**, theo mẫu parameterize mapping của Microsoft.
Không đặt Expression riêng trong mảng `mappings`.

Stack `40-orchestration` (AD-OPS-25b, AD-OPS-26) nạp mọi `pipelines/*.json` bằng
`azapi_resource` và gửi:

- `properties.activities` = nguyên mảng trong `pipelines/<name>.json`
- `properties.concurrency` = `concurrency` trong `pipeline_settings/<name>.json` (**1** cho cả cha và con)
- `properties.parameters` = mỗi khóa trong `parameters` thành `{type: String, defaultValue: <giá trị>}`

Mỗi pipeline phải có đủ hai file cùng tên. Pipeline được tạo theo tầng Execute Pipeline (con trước cha),
đọc từ `referenceName` bắt đầu bằng `pl_`; lồng tối đa 4 tầng (`pl_master` ở tầng 3). Tham số job ID của
`pl_ingest_off`, `pl_master` được thay bằng ID job prod (mục "Điều phối batch").
Mọi giá trị trong `parameters` phải là chuỗi. PR làm một pipeline đổi tầng (con gọi thêm pipeline mới) cần
P1 thêm khối `moved` trong stack; plan của PR sẽ hiện cặp xóa và tạo cùng tên.

Các JSON trong `datasets/` có dạng resource API `{name, properties}`; stack gửi nguyên `properties`.

Hợp đồng linked service/dataset:

| Tên | Yêu cầu |
|---|---|
| `ls_zenodo` | **HttpServer**, Anonymous, base URL `https://zenodo.org/`, xác minh chứng chỉ TLS |
| `ls_adls` | **AzureBlobFS**, managed identity của ADF, quyền đọc/ghi/xóa đúng landing |
| `ds_instacart_http_csv` | DelimitedText HTTP; `file_name` → `api/records/2654444/files/<file>/content` |
| `ds_instacart_adls_csv` | DelimitedText ADLS; String params `file_system`, `folder_path`, `file_name` |
| `ds_instacart_adls_binary` | Binary ADLS; cùng 3 params, dùng metadata/công bố/delete/marker |

CSV dùng header, dấu phẩy, quote/escape `"`, UTF-8 không BOM. Dataset HTTP **không đặt rowDelimiter**
để ADF tự nhận LF/CRLF; dataset ghi ADLS chuẩn hóa về LF. Mọi cột được mapping tường minh dạng String
để giữ giá trị giờ có số 0 đầu và ô rỗng; kiểu nghiệp vụ được xử lý ở bronze/silver.

## Luồng chạy và retry

Pipeline cha ForEach **tuần tự** sáu tên bảng, không dùng biến chia sẻ giữa các lượt:

1. Get Metadata `landing/instacart/<bảng>/release=2017/_SUCCESS` với field `exists`.
2. Đã có marker: bỏ qua toàn bộ copy/delete của bảng.
3. Chưa có marker: If gọi pipeline con bằng Execute Pipeline, `waitOnCompletion = true`.

Pipeline con:

1. Kiểm template `landing/ops/_SUCCESS` tồn tại và **0 byte**, sai thì dừng trước HTTP Copy.
2. HTTP DelimitedText → `landing/_staging/instacart/<bảng>/release=2017/<bảng>.csv`.
   Có `TabularTranslator.mappings` tường minh và `quoteAllText = true` theo spike #141.
3. Kiểm **cả `rowsRead` và `rowsCopied` bằng expected_rows**, không tính header.
   Sai thì Fail; thiếu trường output làm expression lỗi, không có đường bỏ qua kiểm đếm để công bố.
4. Binary Copy đúng file staging sang `landing/instacart/<bảng>/release=2017/<bảng>.csv`.
5. Delete đúng thư mục staging của bảng/release.
6. Binary Copy template thành `_SUCCESS` **sau** khi copy và delete thành công.
7. Get Metadata xác nhận marker thực sự tồn tại; thiếu marker thì Fail.

Hai If kiểm template/số dòng nằm ở cấp cao nhất của pipeline con; không lồng If trong If.
Pipeline cha dùng ForEach → If → Execute Pipeline để tuân giới hạn nesting của ADF.

Theo chốt G0 #146, HTTP Copy từ Zenodo retry 3 lần, nghỉ 300 giây để chờ bảo trì/503.
Các Copy nội bộ ADLS/Metadata/Delete khác retry 5 lần, nghỉ 60 giây; timeout activity 2 giờ.
Riêng `get_marker_template` đặt retry 0: thiếu template là lỗi cấu hình cần sửa, không đợi 5 phút rồi thất bại.
Binary Copy dùng `PreserveHierarchy` + `validateDataConsistency: true`, không bỏ qua file lỗi.
HTTP không hỗ trợ data consistency verification nên dùng mapping + đếm dòng, không bật flag đó trên HTTP.

Tên file/thư mục cố định từ bảng và release, không dùng RunId/giờ chạy. Retry trước marker đọc/ghi lại
cùng đường dẫn; lỗi giữ staging hoặc file đích chưa công bố để chạy lại. Nếu đã ghi marker nhưng mất
phản hồi, lần sau bỏ qua an toàn vì dữ liệu đã kiểm và staging đã dọn trước marker.
Không xóa marker/file release để ép chạy lại một bản đã công bố.

**Điều kiện vận hành:** resource phải giữ concurrency 1, không có tiến trình khác ghi cùng release;
bronze/Databricks chỉ được gọi sau khi pipeline cha thành công toàn bộ. Copy không phải giao dịch
storage nguyên tử; không cho Auto Loader chạy trong lúc công bố. Marker là ranh giới commit theo thiết kế.

Landing giữ giá trị/cột/số dòng; quoteAllText chuẩn hóa CSV nên hash landing có thể khác file Zenodo.
Binary publish giữ nguyên byte giữa staging và landing. Catalog MD5 chỉ dùng đối chiếu CSV nguồn,
không phải kiểm MD5 HTTP trong pipeline.

## Nguồn không nén và kiểm dữ liệu thật

Record 2654444 hiện cung cấp đủ sáu **CSV không nén**, đúng spike #141. Không cấu hình ZipDeflate cho `.csv`;
spike ZIP chỉ là phép thử khả năng giải nén, không phải nguồn Instacart chính. Nếu đổi sang file nén/record khác,
cần PR riêng sửa dataset/mapping/catalog và kiểm lại số dòng trước khi công bố.

Đã kiểm ngày 2026-11-17 trên dữ liệu tải thật: MD5 khớp metadata Zenodo, header khớp catalog,
số bản ghi bằng `csv.reader` và `wc -l` (trừ 1 header) đều khớp:

| Bảng | Số dòng dữ liệu |
|---|---:|
| orders | 3.421.083 |
| order_products__prior | 32.434.489 |
| order_products__train | 1.384.617 |
| products | 49.688 |
| aisles | 134 |
| departments | 21 |

Nguồn ở `data/instacart/`, báo cáo local `data/ad_de_06_verification.json`, đều đã gitignore.
Không đưa CSV thật hoặc dữ liệu landing vào Git.

## Dựng, kiểm JSON và debug sau khi có hạ tầng

Từ gốc repo:

```bash
python3 scripts/build_instacart_adf.py
python3 scripts/build_instacart_adf.py --check
uv run --with azure-mgmt-datafactory==9.2.0 python scripts/build_instacart_adf.py --check --sdk-check
uv run pytest tests/test_adf_instacart.py -q
```

Sửa catalog hoặc script dựng, chạy lại để đồng bộ JSON; không sửa tay riêng một bản generated.
`--sdk-check` deserialize/serialize bằng SDK chính thức, không tạo Azure client hoặc gửi request.
Workflow `adf-checks` chạy kiểm này và bộ test local; không dùng secret/OIDC/quyền cloud.
Test local đọc **các JSON đã commit**, diễn giải tập biểu thức/hoạt động đang dùng, thay HTTP/ADLS bằng
fixture/file local để kiểm lỗi giữa chừng, missing count, marker, CSV dấu phẩy/ngoặc kép và retry.
Bộ mô phỏng đánh giá một tập biểu thức đang dùng; chỉ hỗ trợ dependency `Succeeded`, từ chối kiểu khác
thay vì bỏ qua. Test có nguồn CRLF và kiểm contract translator/template.
Đây là mô phỏng giới hạn, không xác nhận runtime ADF hay thông lượng file 578 MB.

Để lấy resource JSON đầy đủ nhập vào trình soạn JSON khi debug (khác mảng activities cho Terraform):

```bash
python3 scripts/build_instacart_adf.py --definition pl_ingest_instacart_table
python3 scripts/build_instacart_adf.py --definition pl_ingest_instacart
```

P1 bàn giao theo thứ tự sau:

1. **#27** tạo `ls_zenodo`, `ls_adls` và ba dataset đúng hợp đồng ở trên. Chuẩn bị template
   `landing/ops/_SUCCESS` 0 byte và xác nhận quyền đọc, ghi, xóa của managed identity ADF.
2. Nếu có storage dev riêng, chạy debug `tables_json = ["departments"]` (giá trị String JSON) ở đó.
   Kiểm `rowsRead = rowsCopied = 21`, nội dung CSV và marker 0 byte. Dùng linked service dev
   trỏ tới storage riêng vì đường dẫn release cố định trong code. Nếu chỉ có storage chính thức,
   dùng lượt chạy đủ sáu bảng sau publish để nghiệm thu.
3. **#28** nạp settings và activities của cả hai pipeline qua `40-orchestration` theo thứ tự con trước cha.
   Sau khi workflow publish, chạy pipeline cha với tham số mặc định. Ghi run ID, số `rowsRead` và
   `rowsCopied` theo bảng, đủ sáu CSV và sáu marker; ghi thời gian/throughput/retry của prior 578 MB.
4. Chạy lại pipeline cha. Ghi run ID thứ hai và xác nhận cả sáu lượt bỏ qua nhánh Copy/Delete,
   file đã công bố giữ nguyên và `landing/_staging/instacart` không còn file. Lưu bằng chứng vào #42.

Tham chiếu chính thức:
[parameterize mapping](https://learn.microsoft.com/en-us/azure/data-factory/copy-activity-schema-and-type-mapping#parameterize-mapping),
[nesting ADF](https://learn.microsoft.com/en-us/azure/data-factory/concepts-nested-activities),
[DelimitedText](https://learn.microsoft.com/en-us/azure/data-factory/format-delimited-text),
[consistency verification](https://learn.microsoft.com/en-us/azure/data-factory/copy-activity-data-consistency),
[provider pipeline](https://github.com/hashicorp/terraform-provider-azurerm/blob/v4.0.0/internal/services/datafactory/data_factory_pipeline_resource.go),
[Zenodo record](https://zenodo.org/records/2654444).

## Điều phối batch (AD-DE-19)

| Pipeline | Tham số String | Cách gán khi triển khai |
|---|---|---|
| `pl_ingest_off` | `off_job_id`, `off_source` | AD-OPS-26 lookup job prod `instacart-off-crawler` và đặt ID dạng chuỗi làm default. `off_source` mặc định `bronze`. |
| `pl_master` | `batch_job_id`, `off_source`, `failure_webhook_vault` | Lookup job prod `instacart-batch-medallion` và đặt ID dạng chuỗi làm default. Vault mặc định rỗng. |

P1 nạp mảng activities và settings tương ứng với `concurrency: 1`. Stack AD-OPS-25b đã
chừa hai pipeline này cho AD-OPS-26. Publish `pl_ingest_off` trước `pl_master`, sau khi có
`ls_dbx`, `pl_ingest_retail_db` và hai job prod. Managed identity của ADF phải có quyền
chạy cả hai job. Các ID để trống trong JSON repo và được điền lúc triển khai.

Để bật thông báo lỗi, P1 đặt tên Key Vault dự án vào `failure_webhook_vault` và lưu URL HTTPS
của relay Teams/email trong secret `adf-batch-failure-webhook`. Managed identity của ADF cần
quyền đọc secret. Relay nhận POST JSON với `pipeline`, `adf_run_id` và `text` rồi trả JSON.
URL lấy lúc chạy và được che trong Monitor. Nếu để trống tên vault, ADF bỏ qua phần gửi và
vẫn báo run thất bại. Không đặt webhook hoặc token trong Git hay tham số trigger.

### Nghiệm thu sau khi P1 publish

1. Giữ trigger tắt. Khi seed, stream và crawler khác đã dừng, chạy `pl_master` bằng
   **Trigger now**. Nếu chưa có bảng bronze nguồn cho crawler, truyền `off_source=landing`
   sau khi Instacart đã công bố đủ CSV. Giữ `enable_off=false` cho đến khi lượt crawl prod
   đầu tiên được kiểm và cấu hình được đổi qua PR riêng.
2. Trong ADF Monitor ghi RunId và xác nhận CDC, OFF, batch đều `Succeeded`. Mở
   `runPageUrl` của activity `run_batch_medallion`, lấy ID job run và đối chiếu tham số
   `adf_run_id` với RunId của `pl_master`.
3. Trong bản Debug chưa publish, đánh dấu riêng activity `ingest_retail_db` là
   **Inactive → Failed** để diễn tập. Kiểm OFF và batch đều `Skipped`, `pl_master`
   báo `Failed` và không tạo job run mới. Dùng cấu hình thông báo thử nếu cần đo đường Web.
4. Ghi hai RunId ADF, ID job run thành công và kết quả diễn tập vào #55. Sau khi lượt chạy
   tay đạt, P1 mở PR bật lịch 02:00 ở AD-OPS-26.

Tham chiếu [Databricks Job activity](https://learn.microsoft.com/en-us/azure/data-factory/transform-data-databricks-job),
[nhánh lỗi ADF](https://learn.microsoft.com/en-us/azure/data-factory/tutorial-pipeline-failure-error-handling),
[truy xuất secret trong Web activity](https://learn.microsoft.com/en-us/azure/data-factory/how-to-use-azure-key-vault-secrets-pipeline-activities)
và [giả lập activity thất bại](https://learn.microsoft.com/en-us/azure/data-factory/deactivate-activity).
