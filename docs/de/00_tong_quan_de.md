# 00 · Kế hoạch Data Engineering tổng quan

> Bộ tài liệu `docs/de/` hướng dẫn **xây luồng dữ liệu từ nguồn đến Power BI**: đưa dữ liệu vào Azure, xử lý qua từng tầng medallion trên Databricks, rồi phục vụ dashboard.
> Hạ tầng (Terraform, CI/CD, Unity Catalog, quyền) đã có ở [docs/devops](../devops/00_tong_quan.md). Ý nghĩa từng bảng/cột ở [data_dictionary.md](../data/data_dictionary.md).

| Thứ tự đọc | File | Trả lời câu hỏi | Ai chính |
|---|---|---|---|
| 0 | `00_tong_quan_de.md` (file này) | Luồng tổng thể, quy ước chung, nguyên tắc chạy lại an toàn | Cả nhóm |
| 1 | [01_ingest_landing.md](01_ingest_landing.md) | ADF đưa CSV, CDC từ Azure SQL, Open Food Facts vào `landing` thế nào | P2 |
| 2 | [02_bronze_ldp.md](02_bronze_ldp.md) | Dựng pipeline Lakeflow, Auto Loader nạp landing → bronze | P2 |
| 3 | [03_silver_cdc_scd_dq.md](03_silver_cdc_scd_dq.md) | CDC → SCD2, kiểm chất lượng, chuẩn hóa, dựng `baskets` | P2 |
| 4 | [04_streaming.md](04_streaming.md) | Simulator → Event Hubs → bronze/silver/gold gần thời gian thực, phát lại | P3 |
| 5 | [05_gold_serving.md](05_gold_serving.md) | Bảng gold cho BI, tối ưu lưu trữ, benchmark | P2, P3 |
| 6 | [06_van_hanh_chay_lai.md](06_van_hanh_chay_lai.md) | Lỗi ở đâu thì chạy lại thế nào để không mất, không trùng | P2, P3 |
| 7 | [07_power_bi.md](07_power_bi.md) | Dựng semantic model, 4 trang, RLS, mobile (+ trang 6–7 cho DS) | P3 (trang 6–7: P4) |
| 8 | [08_du_lieu_cho_ds.md](08_du_lieu_cho_ds.md) | Snapshot và chuẩn bị dữ liệu thực nghiệm cho DS, xuất file cho Hadoop local | P2 |

---

## 1. Luồng end-to-end

![Luồng DE end-to-end và ranh giới chạy lại an toàn](diagrams/01_luong_de.png)

<sub>File gốc: [diagrams/de_diagrams.drawio](diagrams/de_diagrams.drawio), trang 1.</sub>

| Bước | Từ → đến | Công cụ | Kiểu xử lý | Tài liệu |
|---|---|---|---|---|
| 1 | Zenodo CSV → `landing/instacart/` | ADF Copy (HTTP) | Full load một lần | 01 |
| 2 | Azure SQL `product_prices`, `promotions` → `landing/retail_db/` | ADF Copy đọc hàm CDC | **CDC tăng dần theo LSN** | 01 |
| 3 | Open Food Facts API → `landing/off_api/` | Task Python (crawler) | Lô tăng dần có bảng trạng thái | 01 |
| 4 | Simulator → Event Hubs `orders-stream` | Python Kafka producer | Sự kiện | 04 |
| 5 | `landing/*` → `bronze.*` | LDP `instacart_batch` + Auto Loader | Incremental file (streaming table) | 02 |
| 6 | Event Hubs → `bronze.order_events` | LDP `instacart_stream` + Kafka source | Streaming | 04 |
| 7 | `bronze.*` → `silver.*` | LDP: `AUTO CDC`, expectations, materialized view | CDC/SCD, DQ, chuẩn hóa | 03, 04 |
| 8 | Batch + stream **nhập lại ở silver** | View `silver.orders`, `silver.order_items` (UNION ALL) | — | 03 |
| 9 | `silver.*` → `gold.*` | Materialized view, streaming table cửa sổ | Tổng hợp | 05, 04 |
| 10 | `gold.*` → Power BI | Serverless SQL warehouse | Import / DirectQuery / push | 07 |
| 11 | `silver.baskets` → `ds_data.*` + file txt cho Hadoop | Job `ds-prepare` (chạy tay sau G3) | Snapshot có phiên bản | 08 |

**Điều phối:**
- ADF `pl_master` (02:00 hằng ngày): bước 2 → bước 3 → gọi Databricks Job `instacart-batch-medallion`. Job này chạy pipeline `instacart_batch` (bước 5, 7, 8, 9) và các task phụ trợ.
- Pipeline `instacart_stream` chạy độc lập: triggered theo lịch 15 phút, hoặc continuous lúc demo.
- Chi tiết deploy (DAB, Terraform cho ADF) ở [devops/06](../devops/06_de_den_dashboard.md).

### Vì sao chọn Lakeflow Declarative Pipelines (LDP)

LDP là tên mới của Delta Live Tables. Code khai báo **bảng nào được tạo từ đâu**, còn LDP lo phần còn lại:
- Thứ tự chạy (đồ thị phụ thuộc).
- Checkpoint của từng luồng streaming.
- Thử lại khi lỗi tạm thời.
- Event log ghi lại chất lượng dữ liệu.

| Cần làm | Viết tay bằng PySpark | Với LDP |
|---|---|---|
| CDC → SCD Type 2 | MERGE nhiều nhánh, tự xử lý dữ liệu đến sai thứ tự | `AUTO CDC ... STORED AS SCD TYPE 2` |
| Kiểm chất lượng | Tự đếm, tự ghi bảng | `@dp.expect_or_drop`, số liệu tự vào event log |
| Checkpoint | Tự quản lý đường dẫn | Tự động, gắn với từng streaming table |
| Chạy lại một bảng | Tự viết logic | `refresh_selection` / `full_refresh_selection` |

**Cái giá phải trả** (ghi vào báo cáo):
- `AUTO CDC` cần edition **Pro**.
- Expectations cần edition **Advanced**. Nhóm dùng **Advanced**, DBU đắt hơn Core.
- Mỗi bảng chỉ thuộc **một** pipeline. Vì vậy batch và stream không cùng ghi vào một bảng silver; chúng gặp nhau qua view (mục 1, bước 8).
- Mục spike bắt buộc: chạy được pipeline Advanced trên cluster `num_workers: 0` trong quota 4 vCPU. Xem [devops/01 §3.4](../devops/01_chuan_bi_va_spike.md).

API Python dùng trong tài liệu là `from pyspark import pipelines as dp`. Code cũ dùng `import dlt` có cùng hàm, chỉ đổi tên:

| `pyspark.pipelines` (mới) | `dlt` (cũ) |
|---|---|
| `@dp.table` | `@dlt.table` |
| `@dp.materialized_view` | `@dlt.table` (khi trả về DataFrame batch) |
| `dp.create_streaming_table` | `dlt.create_streaming_table` |
| `dp.create_auto_cdc_flow` | `dlt.apply_changes` |
| `@dp.append_flow` | `@dlt.append_flow` |

## 2. Kỹ thuật DE và chỗ áp dụng

| Kỹ thuật | Ý nghĩa ngắn | Áp dụng ở đâu | Tài liệu |
|---|---|---|---|
| Full load | Chép toàn bộ nguồn một lần | 6 file Instacart; snapshot đầu tiên của bảng SQL | 01 |
| Incremental load theo file | Chỉ nạp file chưa nạp | Auto Loader trên `landing/` | 02 |
| **CDC (Change Data Capture)** | Đọc **nhật ký thay đổi** của CSDL: insert, update, delete kèm thứ tự (LSN) | `product_prices`, `promotions` trên Azure SQL | 01, 03 |
| Watermark nguồn | Mốc "đã đọc tới đâu", lưu ngoài dữ liệu | `etl.cdc_watermark` (LSN), `off_crawl_state` | 01 |
| SCD Type 1 | Ghi đè bằng giá trị mới nhất | `silver.promotions`, `silver.stream_orders` | 03, 04 |
| **SCD Type 2** | Giữ mọi phiên bản, có khoảng hiệu lực | `silver.product_prices_history` | 03 |
| Schema evolution | Nguồn thêm cột thì không vỡ pipeline | Auto Loader `addNewColumns` + `_rescued_data` | 02 |
| Data quality | Luật kiểm tra; bản ghi lỗi giữ lại, không xóa im lặng | Expectations + `silver.quarantine_*` | 03 |
| Structured Streaming | Xử lý liên tục, có checkpoint | Event Hubs → bronze → silver → gold | 04 |
| Event-time watermark | Giới hạn chờ dữ liệu đến muộn | Cửa sổ 10 phút của `product_pair_counts` | 04 |
| Dedup / idempotent | Chạy lại cho cùng kết quả | `AUTO CDC` theo khóa; `event_id`; thư mục landing tất định | 01–04, 06 |
| Point-in-time join | Ghép đơn với giá **tại thời điểm** đặt | `order_items` × `product_prices_history` | 05 |
| Change Data Feed (CDF) | Delta ghi lại dòng nào đổi | Đọc thay đổi của silver khi cần (tùy chọn) | 05 |
| Time travel / RESTORE | Quay bảng về version cũ | Sửa lỗi ghi nhầm | 06 |

## 3. Quy ước chung

### 3.1 Đường dẫn landing

ADF ghi vào container `landing`. Container này là volume `raw` trong Unity Catalog, nên Databricks đọc qua `/Volumes/instacart/landing_meta/raw/`.

```
landing/
├── instacart/<bảng>/release=2017/<bảng>.csv              # một lần, tất định
├── retail_db/<bảng>/window=<from_lsn>_<to_lsn>/part-*.parquet
├── retail_db/<bảng>/snapshot=<to_lsn>/part-*.parquet     # lần đầu
├── off_api/products/batch=<plan_hash>/batch_<content_hash>.json
├── off_api/images/<code>.jpg
└── _staging/...                                          # ADF ghi tạm, Auto Loader KHÔNG đọc
```

**Tên thư mục được tính từ nội dung, không từ thời điểm chạy.** Ví dụ: `window=` là khoảng LSN, `release=` là phiên bản dữ liệu. Nhờ vậy chạy lại cùng một phần việc sẽ ghi lại **đúng chỗ cũ** với **đúng nội dung cũ**, không sinh bản sao thứ hai.

ADF `RunId` thay đổi mỗi lần rerun, nên không dùng làm tên thư mục. Chỉ ghi `RunId` vào `ops.pipeline_runs` để truy vết.

### 3.2 Cột kỹ thuật

| Cột | Có ở tầng | Ý nghĩa |
|---|---|---|
| `_source_file` | bronze | `_metadata.file_path`, biết dòng đến từ file nào |
| `_file_mtime` | bronze | `_metadata.file_modification_time` |
| `_ingest_ts` | bronze | `current_timestamp()` lúc nạp |
| `_rescued_data` | bronze | Phần dữ liệu không khớp schema (Auto Loader) |
| `_op`, `_lsn`, `_seqval` | bronze/silver CDC | Loại thao tác và thứ tự từ SQL CDC |
| `_enqueued_ts`, `_kafka_partition`, `_kafka_offset` | bronze stream | Metadata Event Hubs |
| `replay_run_id` | bronze/silver stream | Lần phát lại của simulator |
| `source` | silver | `batch` / `stream` |
| `__START_AT`, `__END_AT` | silver SCD2 | Khoảng hiệu lực do `AUTO CDC` sinh |

### 3.3 Bảng vận hành `ops.*`

| Bảng | Ai ghi | Nội dung |
|---|---|---|
| `ops.pipeline_runs` | Task cuối của job, và ADF (Stored Procedure → Azure SQL `etl.run_log`, chép sang) | Run ID ADF/Databricks, thời gian, trạng thái, số dòng |
| `ops.dq_results` | Task `collect_dq` đọc event log | Số dòng đạt / bị loại theo expectation |
| `ops.reconciliation` | Task `reconcile` | Số dòng nguồn ↔ bronze ↔ silver theo bảng, đạt/không |
| `ops.streaming_latency` | Pipeline `instacart_stream` | Độ trễ từng chặng của sự kiện |
| `ops.benchmark_results` | Notebook benchmark | Thời gian đọc/ghi CSV, Parquet, Delta |

Watermark CDC **không** nằm trong `ops`, mà nằm cạnh nguồn trong Azure SQL (`etl.cdc_watermark`). Lý do: ADF cập nhật nó bằng Stored Procedure trong cùng phiên với lúc đọc LSN.

## 4. Nguyên tắc chạy lại an toàn (idempotent) theo tầng

> **Idempotent** nghĩa là chạy một bước 1 lần hay 5 lần thì kết quả cuối vẫn như nhau. Đây là điều kiện để "lỗi giữa chừng thì cứ chạy lại", không phải dọn dẹp tay.

| Tầng | Nguyên tắc | Cơ chế cụ thể | Nếu vi phạm thì |
|---|---|---|---|
| Landing | **Bất biến** sau khi đã "công bố" | Ghi vào `_staging/`, xong mới chép sang thư mục tất định và ghi `_SUCCESS`. Watermark chỉ tăng sau khi chép xong | File dở dang bị bronze nạp → dữ liệu thiếu hoặc trùng |
| Bronze | **Chỉ append**, mỗi file nạp đúng một lần | Checkpoint của Auto Loader / Kafka. `pipelines.reset.allowed = false` | Full refresh xóa bronze; nguồn đã bị xóa thì **mất vĩnh viễn** |
| Silver | **Upsert theo khóa nghiệp vụ** | `AUTO CDC` (KEYS + SEQUENCE BY); MV tính lại từ bronze | Append thẳng → mỗi lần chạy lại nhân đôi dòng |
| Gold | **Tính lại được hoàn toàn** từ silver | Materialized view | — |
| Đầu ra ngoài (Power BI push) | Chấp nhận gửi lại, vì chỉ là số hiển thị | Gửi số tổng hợp, không gửi số cộng dồn | Tile cộng sai nếu gửi số chênh lệch |

**Một câu để nhớ:** *landing và bronze là bản ghi lịch sử, không bao giờ sửa; silver và gold là kết quả tính, lúc nào cũng tính lại được từ bronze.*

## 5. Cổng và thứ tự làm

Bám theo cổng ở [devops/00 §3](../devops/00_tong_quan.md):

| Cổng | Việc DE trước cổng | Xong khi |
|---|---|---|
| G0 | Viết code local với dữ liệu mẫu 1% (Docker Spark, hoặc `databricks-connect`); thử CDC trên SQL free offer | Spike đạt |
| G1 | 01 (Zenodo + CDC snapshot) → 02 | Số dòng bronze = file nguồn |
| G2 | 04 phần bronze | 1.000 sự kiện thử vào bronze, phát lại không nhân dòng silver |
| G3 | 03 + 04 phần silver, rồi 08 | Đối soát silver đạt → chạy `ds-prepare` → **DS bắt đầu** ([ds/00](../ds/00_tong_quan_ds.md)) |
| G4 | 05 + 07 | Dashboard đạt rubric |

## 6. Cấu trúc code trong repo

```
src/
├── ingest/
│   ├── seed_retail_db.py          # sinh dữ liệu + đợt thay đổi cho CDC
│   ├── off_crawler.py             # crawler Open Food Facts
│   └── sql/                       # bật CDC, bảng etl.*, stored procedure
├── common/                        # hàm thuần dùng chung: sim_time, text (chuẩn hóa tên)
├── pipelines/
│   ├── batch/                     # source của LDP instacart_batch
│   │   ├── bronze.py
│   │   ├── silver_retail.py       # AUTO CDC prices / promotions
│   │   ├── silver_instacart.py
│   │   ├── silver_off.py          # notebook (cần %pip rapidfuzz): OFF + fuzzy match
│   │   └── gold.py
│   └── stream/                    # source của LDP instacart_stream
│       ├── bronze_events.py
│       ├── silver_events.py
│       └── gold_stream.py
├── tasks/                         # task của job: create_views.sql, reconcile, collect_dq, refresh_bi
│   └── ds_prepare/                # job ds-prepare: snapshot, item_index, transactions, holdout, export (08)
└── streaming/simulator/           # producer chạy local
adf/pipelines/*.json               # activities_json cho Terraform
bundles/resources/                 # job + pipeline (DAB)
tests/                             # pytest cho hàm thuần (chuẩn hóa tên, chấm điểm, sim time)
```

Logic dùng chung giữa pipeline và simulator (ví dụ cách tính `order_ts_sim`) đặt trong `src/common/`. Viết hai bản (SQL và Python) thì phải có test so khớp.
