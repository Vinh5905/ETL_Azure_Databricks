# Nghiệm thu bronze CSV và CDC — AD-DE-09

`src/pipelines/batch/bronze.py` đăng ký sáu streaming table CSV và hai bảng CDC vào catalog và schema lấy từ
configuration của `instacart_batch`. Glob của bundle nạp file này. LDP quản lý schema store và
checkpoint riêng cho mỗi flow.

| Bảng bronze | Thư mục dưới landing_root | Số dòng bản đầy đủ |
|---|---|---:|
| orders | instacart/orders/release=2017/ | 3.421.083 |
| order_products_prior | instacart/order_products__prior/release=2017/ | 32.434.489 |
| order_products_train | instacart/order_products__train/release=2017/ | 1.384.617 |
| products | instacart/products/release=2017/ | 49.688 |
| aisles | instacart/aisles/release=2017/ | 134 |
| departments | instacart/departments/release=2017/ | 21 |

Mỗi bảng có `pipelines.reset.allowed=false` và metadata `_source_file`, `_file_mtime`, `_ingest_ts`.
Reader dùng schema CSV tường minh kèm `_rescued_data STRING`, giữ nguyên các split và bản ghi nguồn.
Auto Loader đọc file hiện có và file mới, bỏ qua lần ghi đè cùng đường dẫn, và đưa cột hoặc kiểu
lệch schema vào rescue. Bản ghi sai cú pháp làm update lỗi với `FAILFAST`. Reader trỏ vào từng nhánh
dữ liệu đã công bố, ngoài `_staging`, và không đưa `release` vào schema bronze.

## Hai bảng CDC retail

`product_prices_cdc` và `promotions_cdc` đọc Parquet AD-DE-07 đã công bố ở
`retail_db/<bảng>/snapshot=<lsn>/` và `retail_db/<bảng>/window=<from>_<to>/`.
Parquet mang sẵn schema nên reader không khai schema, dùng `addNewColumns` và chỉ nhận `*.parquet`.
Tên thư mục không thành cột (`partitionColumns=""`). Bronze giữ nguyên `_op`, `_lsn`, `_seqval` và
bốn cột `__$…` của hàm CDC. Ở file snapshot, bốn cột này NULL. Bronze không lọc delete, không khử trùng.
Auto Loader tự thêm `_rescued_data` cho Parquet. Cột này giữ giá trị lệch kiểu nếu nguồn đổi schema.

| Bảng bronze | Thư mục dưới landing_root | Kỳ vọng |
|---|---|---|
| product_prices_cdc | retail_db/product_prices/ | Snapshot 49.688 dòng `_op=0`, mỗi wave seed thêm 2.484 dòng `_op=4` |
| promotions_cdc | retail_db/promotions/ | `_op` có 0, 1, 2, 4 |

Nguồn JSONL và ảnh OFF đi cùng AD-DE-10 khi dữ liệu tương ứng có trên landing.

## Chuẩn bị dev

- Xác nhận cấu hình DAB, schema, policy và quyền theo [hướng dẫn bundle](../../bundles/README.md).
  Dùng cùng danh tính để deploy và chạy. Chỉ thử full refresh trên pipeline dev cá nhân.
- Kiểm tra sáu thư mục CSV đã công bố từ AD-DE-06. Chờ pipeline idle và quota single-node trống
  trước khi chạy update.
- Khi thử mẫu 1%, tạo bằng `scripts/make_sample.py` và so với `_manifest.json`. Dùng landing,
  pipeline, checkpoint và schema dev riêng cho mẫu để giữ bộ đầy đủ độc lập.

## Chạy và ghi bằng chứng

Chạy các lệnh dưới đây từ `bundles/` bằng phiên xác thực đã chọn.

```bash
databricks bundle validate --strict -t dev
databricks bundle deploy -t dev --select instacart_batch
databricks bundle run -t dev instacart_batch
```

1. Sau khi update `COMPLETED`, chạy truy vấn [02 §6](02_bronze_ldp.md#6-kiểm-tra-đối-soát-g1)
   cho sáu bảng CSV và hai bảng CDC, thay catalog/schema bằng giá trị dev đã resolve. Đối chiếu số dòng và
   xác nhận `products._rescued_data` có 0 dòng khác null. Lưu update ID và kết quả.
2. Chạy lại cùng pipeline khi landing chưa thêm file. Đối chiếu số dòng bằng lần đầu.
3. Thử *Full refresh all* trên pipeline dev bằng `databricks bundle run -t dev instacart_batch --full-refresh-all`.
   Kiểm tra trạng thái update, số dòng và dấu thời gian ingest hoặc phiên bản Delta để xác nhận
   tám bảng bronze được giữ, kể cả khi smoke materialized view được tính lại.
4. Ghi kết quả trên issue #45 và PR, gồm pipeline/schema dev, quy mô nguồn, ba update ID,
   số dòng tám bảng, phân bố `_op`, số rescue và bằng chứng bảo vệ reset.

## Kiểm tra local

`tests/test_bronze.py` dùng Spark local để kiểm schema, parsing, metadata, mapping đường dẫn,
khai báo tám bảng và cấu hình reset. Adapter local đọc CSV/Parquet bằng reader Spark native;
test CDC gộp snapshot và window, kiểm không có cột `snapshot`/`window` từ đường dẫn.
Kiểm rescue, checkpoint, chạy lại và full refresh thực hiện trên workspace.

```bash
uv run pytest tests/test_bronze.py -q
uv run ruff check .
uv run ruff format --check .
```

Tham khảo [tuỳ chọn Auto Loader](https://learn.microsoft.com/en-us/azure/databricks/spark/api-options),
[schema và rescue](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/schema)
và [metadata file](https://learn.microsoft.com/en-us/azure/databricks/ingestion/file-metadata-column).
