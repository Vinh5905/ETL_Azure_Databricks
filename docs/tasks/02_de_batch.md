# Phân việc · DE batch (`AD-DE-xx`)

27 task. Quy ước, trạng thái và bảng tổng ở [README](README.md). Vai ghi theo P1–P4, người thật được gán trên GitHub Project.

| Mã | Việc | Vai | Cổng | Phụ thuộc | Issue |
|---|---|---|---|---|---|
| [AD-DE-01](#ad-de-01) | Chuyển khung src sang cấu trúc đích và tạo dữ liệu mẫu 1% | P2 | G0 | Chuẩn bị ngay | [#37](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/37) |
| [AD-DE-02](#ad-de-02) | Viết hàm thuần sim_time và chuẩn hóa tên kèm test | P2 | G0 | Chuẩn bị ngay; Hoàn tất: AD-DE-01 | [#38](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/38) |
| [AD-DE-03](#ad-de-03) | Viết khuôn quarantine và bộ luật chất lượng dùng chung | P2 | G0 | Chuẩn bị ngay; Hoàn tất: AD-DE-01 | [#39](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/39) |
| [AD-DE-04](#ad-de-04) | Viết script SQL tạo bảng nguồn, bật CDC và bảng điều khiển etl | P2 | G1 | Chuẩn bị ngay; Hoàn tất: AD-OPS-12, AD-OPS-06 | [#40](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/40) |
| [AD-DE-05](#ad-de-05) | Viết seed_retail_db theo đợt và job chạy seed | P2 | G1 | Chuẩn bị ngay; Hoàn tất: AD-DE-04, AD-OPS-22 | [#41](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/41) |
| [AD-DE-06](#ad-de-06) | Dựng ADF pl_ingest_instacart chép CSV Zenodo vào landing | P2 | G1 | Chuẩn bị ngay; Hoàn tất: AD-OPS-25 | [#42](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/42) |
| [AD-DE-07](#ad-de-07) | Dựng ADF pl_ingest_retail_db đọc CDC theo LSN | P2 | G1 | Bắt đầu: AD-DE-04; Hoàn tất: AD-DE-05, AD-OPS-25 | [#43](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/43) |
| [AD-DE-08](#ad-de-08) | Khai báo pipeline instacart_batch trong DAB với schema dev riêng | P2 | G1 | Chuẩn bị ngay; Hoàn tất: AD-OPS-19, AD-OPS-21, AD-OPS-20, AD-OPS-05 | [#44](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/44) |
| [AD-DE-09](#ad-de-09) | Viết bronze.py nạp landing vào bronze bằng Auto Loader | P2 | G1 | Chuẩn bị ngay; Hoàn tất: AD-DE-06, AD-DE-07, AD-DE-08 | [#45](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/45) |
| [AD-DE-10](#ad-de-10) | Viết crawler Open Food Facts và tải ảnh theo lô | P2 | G3 | Chuẩn bị ngay; Hoàn tất: AD-DE-09 | [#46](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/46) |
| [AD-DE-11](#ad-de-11) | Dựng silver SCD2 lịch sử giá và SCD1 khuyến mãi | P2 | G3 | Chuẩn bị ngay; Hoàn tất: AD-DE-03, AD-DE-09 | [#47](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/47) |
| [AD-DE-12](#ad-de-12) | Dựng silver đơn hàng: order_time_sim, orders_batch, order_items_prior | P2 | G3 | Chuẩn bị ngay; Hoàn tất: AD-DE-02, AD-DE-03, AD-DE-09 | [#48](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/48) |
| [AD-DE-13](#ad-de-13) | Làm phẳng OFF và fuzzy match sang sản phẩm Instacart | P2 | G3 | Chuẩn bị ngay; Hoàn tất: AD-DE-02, AD-DE-10 | [#49](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/49) |
| [AD-DE-14](#ad-de-14) | Dựng silver.dim_product không nhân dòng | P2 | G3 | Chuẩn bị ngay; Hoàn tất: AD-DE-11, AD-DE-13 | [#50](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/50) |
| [AD-DE-15](#ad-de-15) | Gộp batch với stream ở silver và dựng baskets | P2 | G3 | Chuẩn bị ngay; Hoàn tất: AD-DE-12, AD-ST-05 | [#51](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/51) |
| [AD-DE-16](#ad-de-16) | Viết collect_dq ghi kết quả expectations vào ops.dq_results | P2 | G3 | Chuẩn bị ngay; Hoàn tất: AD-DE-08, AD-DE-12, AD-OPS-22, AD-OPS-20 | [#52](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/52) |
| [AD-DE-17](#ad-de-17) | Viết reconcile đối soát nguồn, bronze, silver vào ops.reconciliation | P2 | G3 | Chuẩn bị ngay; Hoàn tất: AD-DE-14, AD-DE-15, AD-ST-07 | [#53](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/53) |
| [AD-DE-18](#ad-de-18) | Hoàn thiện job instacart-batch-medallion trong DAB | P2 | G3 | Bắt đầu: AD-DE-08; Hoàn tất: AD-DE-15, AD-DE-16, AD-DE-17, AD-OPS-22, AD-OPS-20 | [#54](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/54) |
| [AD-DE-19](#ad-de-19) | Dựng ADF pl_ingest_off và pl_master điều phối hằng ngày | P2 | G3 | Bắt đầu: AD-DE-07; Hoàn tất: AD-DE-10, AD-DE-18, AD-OPS-25, AD-OPS-23 | [#55](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/55) |
| [AD-DE-20](#ad-de-20) | Viết SQL ds-prepare: snapshot, item_index, transactions, holdout_eval | P2 | G3 | Chuẩn bị ngay; Hoàn tất: AD-DE-15, AD-OPS-19, AD-OPS-20 | [#56](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/56) |
| [AD-DE-21](#ad-de-21) | Xuất file txt cho Hadoop, dựng job ds-prepare và chạy v1 | P2 | G3 | Bắt đầu: AD-DE-17, AD-DE-20; Hoàn tất: AD-OPS-19, AD-OPS-20 | [#57](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/57) |
| [AD-DE-22](#ad-de-22) | Dựng gold.fact_order_lines với giá theo thời điểm | P2 | G4 | Chuẩn bị ngay; Hoàn tất: AD-DE-11, AD-DE-14, AD-DE-15 | [#58](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/58) |
| [AD-DE-23](#ad-de-23) | Dựng gold tổng hợp, dim và bảng phân quyền bi_user_access | P2 | G4 | Chuẩn bị ngay; Hoàn tất: AD-DE-22 | [#59](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/59) |
| [AD-DE-24](#ad-de-24) | Dựng gold.product_pair_counts cho trang chi tiết sản phẩm | P2 | G4 | Chuẩn bị ngay; Hoàn tất: AD-DE-15, AD-DE-23 | [#60](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/60) |
| [AD-DE-25](#ad-de-25) | Viết refresh_bi và ghi ops.pipeline_runs | P2 | G4 | Chuẩn bị ngay; Hoàn tất: AD-DE-18, AD-BI-09, AD-BI-01 | [#61](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/61) |
| [AD-DE-26](#ad-de-26) | Chạy benchmark CSV, Parquet, Delta, Delta OPTIMIZE | P2 | G4 | Chuẩn bị ngay; Hoàn tất: AD-DE-12 | [#62](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/62) |
| [AD-DE-27](#ad-de-27) | Viết runbook chạy lại và diễn tập 4 kịch bản lỗi | P2 | G4 | Bắt đầu: AD-DE-18, AD-DE-19; Hoàn tất: AD-DE-14, AD-ST-07 | [#63](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/63) |

### AD-DE-01

**Chuyển khung src sang cấu trúc đích và tạo dữ liệu mẫu 1%**

- Vai dự kiến: **P2 · DE batch**
- Cổng: **G0**
- Issue: [#37](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/37)
- Tài liệu: `docs/de/00_tong_quan_de.md §5, §6`, `src/AGENTS.md`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** Không có.
- **Mở khóa:** [AD-DE-02](02_de_batch.md#ad-de-02) (hoàn tất), [AD-DE-03](02_de_batch.md#ad-de-03) (hoàn tất)

Cần thay khung batch và job Python tạm bằng cấu trúc đích để mọi task sau đặt code đúng chỗ,
và dữ liệu mẫu để viết code local trước G1. Job thật được khai báo ở AD-DE-18.

**Việc cần làm**

1. Tạo thư mục src/ingest/{sql}, src/pipelines/{batch,stream}, src/tasks/ds_prepare, giữ src/common, src/streaming/simulator
2. Xóa thư mục batch tạm và test liên quan, cập nhật import, đường dẫn bundle và CODEOWNERS
3. Thêm dependency vào pyproject: pandas, rapidfuzz 3.*, confluent-kafka, pymssql, requests (nhóm dev/optional nếu cần)
4. Viết script tạo mẫu 1% Instacart theo user_id (giữ trọn lịch sử mỗi khách) vào tests/fixtures/sample_1pct/ hoặc thư mục data/ đã gitignore
5. Ghi cách dùng mẫu trong src/AGENTS.md nếu cần

**Đầu ra**

- Cây thư mục src/ theo de/00 §6
- pyproject.toml + uv.lock cập nhật
- Script tạo mẫu 1% (ví dụ scripts/make_sample.py)
- Fixture nhỏ trong tests/fixtures/

**Xong khi**

- [x] uv sync và uv run pytest -q xanh
- [x] Không còn tham chiếu tới thư mục batch tạm
- [x] Mẫu 1% giữ đủ prior/train/test của mỗi user được chọn

**Kết quả local AD-DE-01 (2026-11-17)**

- Đã tạo cấu trúc nguồn, bỏ job Python tạm, cập nhật CODEOWNERS và đường dẫn trong tài liệu.
- Đã khóa dependency; Kafka và driver SQL nằm trong extra `ingest` (`uv sync --extra ingest`).
- `scripts/make_sample.py` lấy mẫu theo khách hàng, giữ nguyên lịch sử và toàn bộ danh mục;
  có hướng dẫn trong `src/AGENTS.md`, fixture nhỏ tự dựng trong `tests/fixtures/instacart/`.
- Đã kiểm `uv sync --frozen`, pytest: 35 test đạt; `ruff check` và `ruff format --check` sạch
  trên Python 3.11.9, Java 17. Spark local cần quyền mở socket loopback.
- Test mặc định 1% trên bộ 200 khách tự dựng chọn đúng 2 khách, giữ đủ mọi dòng đơn và món hàng.
  Đã kiểm seed lặp lại, CSV có dấu phẩy/ngoặc kép, không ghi đè đích cũ và không công bố mẫu dở.
- Đã tải đủ sáu CSV từ [Zenodo record 2654444](https://zenodo.org/records/2654444), kiểm MD5
  từng file khớp metadata nguồn. Số dòng nguồn đúng như data dictionary: orders 3.421.083,
  prior 32.434.489, train 1.384.617, products 49.688, aisles 134, departments 21.
- Đã chạy mẫu thật bằng `python3 scripts/make_sample.py --input-dir data/instacart
  --output-dir data/sample_1pct` (viết trên một dòng), tỷ lệ 0,01 và seed 42:
  **2.063/206.209 khách, 34.538 đơn** (32.475 prior, 1.311 train, 752 test),
  **326.492 dòng món prior, 13.665 dòng món train**; danh mục giữ nguyên toàn bộ.
- Đối soát độc lập từng dòng với nguồn: giữ đủ mọi đơn của khách đã chọn, đủ mọi món của các đơn đó,
  thứ tự và giá trị gốc không đổi; số dòng mẫu khớp `_manifest.json`.
  Nguồn ở `data/instacart/`, mẫu ở `data/sample_1pct/`; `_source.json` ghi checksum nguồn,
  `_verification.json` ghi kết quả đối soát. Toàn bộ `data/` đã gitignore.
- **Chưa kiểm bundle trên workspace:** máy chưa có Databricks CLI/cấu hình workspace cho G1.
  Chưa deploy; job `instacart-batch-medallion` thật sẽ bổ sung ở AD-DE-18.

### AD-DE-02

**Viết hàm thuần sim_time và chuẩn hóa tên kèm test**

- Vai dự kiến: **P2 · DE batch**
- Cổng: **G0**
- Issue: [#38](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/38)
- Tài liệu: `docs/de/03_silver_cdc_scd_dq.md §3.1, §5`, `docs/de/04_streaming.md §2.2`, `docs/data/data_dictionary.md §4, §5.6`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** [AD-DE-01](02_de_batch.md#ad-de-01) [#37](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/37)
- **Mở khóa:** [AD-DE-12](02_de_batch.md#ad-de-12) (hoàn tất), [AD-DE-13](02_de_batch.md#ad-de-13) (hoàn tất), [AD-ST-01](03_de_stream.md#ad-st-01) (hoàn tất)

order_ts_sim phải ra cùng giá trị ở silver (SQL) và simulator (Python). Hàm norm dùng cho fuzzy match OFF. Cả hai là hàm thuần dùng chung.

**Việc cần làm**

1. Viết src/common/sim_time.py: order_ts_sim(user_id, hour, days_back=0) và hằng SIM_END_ANCHOR
2. Đặt chuỗi SQL của order_time_sim ở một chỗ dùng chung để test so khớp
3. Viết src/common/text.py: norm() theo de/03 §5
4. Viết tests/test_sim_time.py so khớp bản Python và bản SQL trên Spark local[1] với fixture
5. Viết tests/test_norm.py với 4 ví dụ ở data_dictionary §5.6

**Đầu ra**

- src/common/sim_time.py
- src/common/text.py
- tests/test_sim_time.py
- tests/test_norm.py

**Xong khi**

- [x] uv run pytest tests/test_sim_time.py tests/test_norm.py xanh
- [x] Bản SQL và Python cho cùng order_ts_sim trên toàn bộ fixture

**Kết quả local AD-DE-02 (2026-11-17)**

- Đã có `src/common/sim_time.py`: mốc `SIM_END_ANCHOR`, hàm UTC thuần
  `order_ts_sim(user_id, hour, days_back=0)` và `order_time_sim_sql(orders_table)`.
- SQL loại trùng trước khi cộng ngày, sắp `order_number` theo số, chạy trên Spark 3.5 local;
  cast dữ liệu CSV để nhận được chuỗi số thập phân ở ANSI mode. Timestamp trả UTC bằng
  `make_timestamp_ltz`, không phụ thuộc timezone hay default timestampType của session.
- Đã có `src/common/text.py`: `norm` theo bộ từ nhiễu trong DE/03; xử lý thêm đúng số đo
  thập phân và đơn vị ghép `fl oz`. Ví dụ batch/simulator trong tài liệu import lại các hàm này.
- 47 ca kiểm thử mới: bốn cặp tên ở data dictionary, từ nhiễu/đơn vị, đầu vào rỗng,
  mốc thời gian, NULL ở đơn đầu, đơn cuối train/test, lịch sử đảo thứ tự, đơn thứ 10 trở lên,
  bản ghi cũ lặp lại và phép nhân vượt INT32. SQL/Python khớp trên toàn bộ fixture ở
  UTC, Asia/Ho_Chi_Minh, America/New_York, với ANSI bật/tắt và default TIMESTAMP_NTZ.
- Toàn bộ pytest **82 test đạt**, Ruff check/format và `git diff --check` sạch
  trên Python 3.11.9, Java 17, Spark 3.5 local[1].
- Đối soát độc lập SQL/Python trên mẫu thật của AD-DE-01: **34.538 đơn / 2.063 khách,
  0 sai lệch** ở session Asia/Ho_Chi_Minh và ANSI bật. `norm` chạy trên toàn bộ
  **49.688 tên sản phẩm**, kết quả không đổi khi chuẩn hóa lần hai.
  Bằng chứng local: `data/ad_de_02_verification.json` (đã gitignore).
- **Chưa kiểm trên Databricks workspace**; tích hợp pipeline/simulator thuộc AD-DE-12,
  AD-DE-13 và AD-ST-01. **Nghiệm thu cuối còn chờ AD-DE-01 (#37) Done**;
  nhánh AD-DE-02 đang dựa trên phần AD-DE-01 chờ merge ở PR #135.

### AD-DE-03

**Viết khuôn quarantine và bộ luật chất lượng dùng chung**

- Vai dự kiến: **P2 · DE batch**
- Cổng: **G0**
- Issue: [#39](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/39)
- Tài liệu: `docs/de/03_silver_cdc_scd_dq.md §4`, `docs/de/04_streaming.md §4.1`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** [AD-DE-01](02_de_batch.md#ad-de-01) [#37](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/37)
- **Mở khóa:** [AD-DE-11](02_de_batch.md#ad-de-11) (hoàn tất), [AD-DE-12](02_de_batch.md#ad-de-12) (hoàn tất), [AD-ST-05](03_de_stream.md#ad-st-05) (hoàn tất)

Expectations chỉ đếm dòng bị bỏ. Bảng quarantine_* giữ nội dung dòng lỗi với cùng bộ luật đảo ngược, dùng cho cả pipeline batch và stream.

**Việc cần làm**

1. Viết src/common/dq.py: hàm tạo cột _dq_failed từ dict luật (không phụ thuộc dp)
2. Khai báo ORDER_RULES, ITEM_RULES, PRICE_RULES, PROMO_RULES, EVENT_RULES ở một chỗ
3. Kiểm mọi luật không trả NULL (viết test)
4. Viết tests/test_dq.py: dòng hợp lệ có mảng rỗng, dòng lỗi liệt kê đúng tên luật

**Đầu ra**

- src/common/dq.py
- tests/test_dq.py

**Xong khi**

- [x] uv run pytest tests/test_dq.py xanh
- [x] Mỗi bộ luật có ít nhất một ca đạt và một ca lỗi trong test

**Kết quả local AD-DE-03 (2026-11-17)**

- `src/common/dq.py` tập trung 5 bộ luật (18 điều kiện), dùng chung cho expectations và quarantine.
  `dq_failed()` trả `array<string>` tên luật lỗi theo thứ tự khai báo; NULL từ luật tùy chỉnh là lỗi,
  dòng đạt hoặc dict rỗng nhận `[]`. Hàm không phụ thuộc `dp`, không chạy action hay ghi bảng.
- 15 test DQ kiểm từng luật cả đạt/lỗi, biên, NULL trên hai chế độ ANSI; kiểm cấu hình sai,
  giữ nguyên dòng/payload JSON hỏng và chạy biểu thức trên Structured Streaming local.
- Toàn bộ nhánh đạt **50 test** trên Python 3.11.9, PySpark 3.5, Java 17;
  `uv sync --frozen`, `ruff check .`, `ruff format --check .` đều đạt.
- Áp dụng ORDER/ITEM_RULES trên mẫu thật `data/sample_1pct/`: **34.538 đơn,
  326.492 dòng món prior, 13.665 dòng món train**, không có dòng vi phạm.
  Đọc CSV bằng schema tường minh, bổ sung `_rescued_data = NULL` cho nguồn CSV sạch;
  báo cáo ở `data/ad_de_03_verification.json`, toàn bộ `data/` đã gitignore.
- Đã cập nhật ví dụ batch/stream dùng cùng bộ luật và helper; quarantine sự kiện giữ `value_json`.
  Chưa triển khai pipeline silver thật (AD-DE-11/12, AD-ST-05); **chưa kiểm trên workspace**
  vì máy chưa có Databricks CLI/cấu hình workspace.
- Hoàn tất kiểm tra local G0; phụ thuộc AD-DE-01 đã được merge qua PR #135.
  Nhánh AD-DE-03 đã đồng bộ main sau #135, độc lập với AD-DE-02 (#137).

### AD-DE-04

**Viết script SQL tạo bảng nguồn, bật CDC và bảng điều khiển etl**

- Vai dự kiến: **P2 · DE batch**
- Cổng: **G1**
- Issue: [#40](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/40)
- Tài liệu: `docs/de/01_ingest_landing.md §3.2, §3.3, §3.7`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** [AD-OPS-12](01_devops.md#ad-ops-12) [#14](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/14), [AD-OPS-06](01_devops.md#ad-ops-06) [#8](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/8)
- **Mở khóa:** [AD-DE-05](02_de_batch.md#ad-de-05) (hoàn tất), [AD-DE-07](02_de_batch.md#ad-de-07) (bắt đầu)

CDC theo LSN cần bảng nguồn, CDC bật trên 2 bảng, watermark và nhật ký chạy nằm cạnh nguồn trong Azure SQL.

**Việc cần làm**

1. Viết src/ingest/sql/01_schema.sql (product_prices, promotions)
2. Viết src/ingest/sql/02_enable_cdc.sql (retention 7 ngày)
3. Viết src/ingest/sql/03_etl_control.sql (etl.cdc_watermark, etl.run_log, usp_get_cdc_window, usp_set_cdc_watermark)
4. Chuẩn bị bản dự phòng watermark updated_at + is_deleted nếu spike CDC không đạt
5. Chạy trên retail_db bằng tài khoản admin Entra

**Đầu ra**

- src/ingest/sql/01_schema.sql
- src/ingest/sql/02_enable_cdc.sql
- src/ingest/sql/03_etl_control.sql

**Xong khi**

- [ ] cdc.change_tables có dbo_product_prices và dbo_promotions
- [ ] EXEC etl.usp_get_cdc_window trả mode = snapshot cho cả 2 capture instance
- [ ] Script chạy lại lần hai không lỗi

### AD-DE-05

**Viết seed_retail_db theo đợt và job chạy seed**

- Vai dự kiến: **P2 · DE batch**
- Cổng: **G1**
- Issue: [#41](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/41)
- Tài liệu: `docs/de/01_ingest_landing.md §3.4`, `docs/devops/06_de_den_dashboard.md §2.1, §3`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** [AD-DE-04](02_de_batch.md#ad-de-04) [#40](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/40), [AD-OPS-22](01_devops.md#ad-ops-22) [#24](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/24)
- **Mở khóa:** [AD-DE-07](02_de_batch.md#ad-de-07) (hoàn tất)

CDC cần insert, update, delete thật. Script sinh giá và khuyến mãi theo đợt 0–4, tất định theo seed, chạy lại không áp dụng hai lần.

**Việc cần làm**

1. Viết src/ingest/seed_retail_db.py: wave 0 và wave k=1..4, một transaction mỗi đợt, bảng etl.seed_wave
2. Đọc mật khẩu bằng dbutils.secrets.get('kv', …) khi chạy trên Databricks
3. Tách phần sinh dữ liệu thành hàm thuần, test tính tất định (cùng wave cùng kết quả)
4. Khai báo job seed_retail_db trong bundles/resources (tham số --wave, job cluster theo policy)
5. Chạy wave 0 trên dev

**Đầu ra**

- src/ingest/seed_retail_db.py
- tests/test_seed_retail_db.py
- bundles/resources/jobs_ingest.yml (job seed_retail_db)

**Xong khi**

- [ ] dbo.product_prices có 49.688 dòng sau wave 0
- [ ] Chạy lại wave 0 in 'đã áp dụng, bỏ qua'
- [ ] Sau wave 1: có update giá và delete khuyến mãi trong cdc.fn_cdc_get_all_changes

### AD-DE-06

**Dựng ADF pl_ingest_instacart chép CSV Zenodo vào landing**

- Vai dự kiến: **P2 · DE batch**
- Cổng: **G1**
- Issue: [#42](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/42)
- Tài liệu: `docs/de/01_ingest_landing.md §1, §2`, `docs/devops/06_de_den_dashboard.md §1`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** [AD-OPS-25](01_devops.md#ad-ops-25) [#27](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/27)
- **Mở khóa:** [AD-OPS-26](01_devops.md#ad-ops-26) (hoàn tất), [AD-DE-09](02_de_batch.md#ad-de-09) (hoàn tất)

Bước 1 của luồng: 6 file Instacart vào landing/instacart/<bảng>/release=2017/, theo khuôn staging → công bố → _SUCCESS để chạy lại an toàn.

**Việc cần làm**

1. Kiểm số dòng bản Zenodo bằng wc -l, cập nhật expected_rows nếu khác Kaggle
2. Thiết kế pipeline trong ADF Studio: ForEach, Get Metadata _SUCCESS, Copy vào _staging, kiểm rowsCopied, Copy Binary, Delete, ghi _SUCCESS
3. Xử lý file nén (ZipDeflate) theo kết quả spike
4. Export activities JSON vào adf/pipelines/pl_ingest_instacart.json, mở PR
5. Chạy một lần sau khi P1 publish

**Đầu ra**

- adf/pipelines/pl_ingest_instacart.json
- 6 thư mục landing/instacart/*/release=2017/ có _SUCCESS

**Xong khi**

- [ ] rowsCopied khớp expected_rows cho 6 bảng
- [ ] Chạy lại lần hai bỏ qua cả 6 bảng, không ghi thêm file
- [ ] Không còn file trong landing/_staging/instacart

### AD-DE-07

**Dựng ADF pl_ingest_retail_db đọc CDC theo LSN**

- Vai dự kiến: **P2 · DE batch**
- Cổng: **G1**
- Issue: [#43](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/43)
- Tài liệu: `docs/de/01_ingest_landing.md §3.5, §3.6, §6, §7`
- **Bắt đầu sau:** [AD-DE-04](02_de_batch.md#ad-de-04) [#40](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/40)
- **Hoàn tất còn cần:** [AD-DE-05](02_de_batch.md#ad-de-05) [#41](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/41), [AD-OPS-25](01_devops.md#ad-ops-25) [#27](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/27)
- **Mở khóa:** [AD-OPS-26](01_devops.md#ad-ops-26) (hoàn tất), [AD-DE-09](02_de_batch.md#ad-de-09) (hoàn tất), [AD-DE-19](02_de_batch.md#ad-de-19) (bắt đầu)

Đọc thay đổi product_prices, promotions theo cửa sổ LSN vào landing/retail_db/<bảng>/snapshot=… hoặc window=…; watermark chỉ tăng sau khi công bố.

**Việc cần làm**

1. Thiết kế pl_cdc_one_table: Lookup usp_get_cdc_window, Switch empty/snapshot/window, Copy Parquet qua _staging, Delete, SP set watermark
2. Thiết kế pl_ingest_retail_db: ForEach tuần tự 2 bảng gọi pl_cdc_one_table
3. Export JSON vào adf/pipelines/, mở PR
4. Chạy snapshot, seed wave 1, chạy lại ra window, chạy lần ba ra empty
5. Thử lỗi giữa chừng (đổi tạm tên bảng) và rerun from failed

**Đầu ra**

- adf/pipelines/pl_cdc_one_table.json
- adf/pipelines/pl_ingest_retail_db.json
- landing/retail_db/<bảng>/snapshot=…, window=…

**Xong khi**

- [ ] etl.run_log có 2 dòng snapshot, sau wave 1 có 2 dòng window với rows_copied > 0
- [ ] Parquet window của promotions có _op = 1 và _op = 4
- [ ] Chạy hai lần liên tiếp không seed: lần hai mode = empty, không có thư mục mới
- [ ] Bắn lỗi rồi rerun: watermark không đổi trước khi sửa, không mất thay đổi

### AD-DE-08

**Khai báo pipeline instacart_batch trong DAB với schema dev riêng**

- Vai dự kiến: **P2 · DE batch**
- Cổng: **G1**
- Issue: [#44](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/44)
- Tài liệu: `docs/de/02_bronze_ldp.md §2.2`, `docs/de/06_van_hanh_chay_lai.md §3`, `bundles/AGENTS.md`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** [AD-OPS-19](01_devops.md#ad-ops-19) [#21](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/21), [AD-OPS-21](01_devops.md#ad-ops-21) [#23](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/23), [AD-OPS-20](01_devops.md#ad-ops-20) [#22](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/22), [AD-OPS-05](01_devops.md#ad-ops-05) [#7](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/7)
- **Mở khóa:** [AD-DE-09](02_de_batch.md#ad-de-09) (hoàn tất), [AD-DE-16](02_de_batch.md#ad-de-16) (hoàn tất), [AD-DE-18](02_de_batch.md#ad-de-18) (bắt đầu), [AD-ST-03](03_de_stream.md#ad-st-03) (hoàn tất)

LDP instacart_batch chạy bronze → silver → gold. Dev cần tiền tố schema để không tranh bảng với prod.

**Việc cần làm**

1. Thêm biến pipeline_policy_id, schema_prefix, warehouse_id vào bundles/databricks.yml; target dev đặt schema_prefix
2. Viết bundles/resources/pipeline_batch.yml (edition ADVANCED, num_workers 0, configuration, clusterShutdown 60s, libraries)
3. Thêm event_log vào ops (event_log_batch)
4. Tạo schema dev <user>_bronze/silver/gold/ops
5. databricks bundle validate -t dev

**Đầu ra**

- bundles/resources/pipeline_batch.yml
- bundles/databricks.yml cập nhật biến

**Xong khi**

- [ ] bundle validate -t dev đạt
- [ ] bundle deploy -t dev tạo được pipeline [dev …] instacart-batch
- [ ] Event log ghi thành bảng <user>_ops.event_log_batch

### AD-DE-09

**Viết bronze.py nạp landing vào bronze bằng Auto Loader**

- Vai dự kiến: **P2 · DE batch**
- Cổng: **G1**
- Issue: [#45](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/45)
- Tài liệu: `docs/de/02_bronze_ldp.md §3, §5, §6`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** [AD-DE-06](02_de_batch.md#ad-de-06) [#42](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/42), [AD-DE-07](02_de_batch.md#ad-de-07) [#43](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/43), [AD-DE-08](02_de_batch.md#ad-de-08) [#44](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/44)
- **Mở khóa:** [AD-DE-10](02_de_batch.md#ad-de-10) (hoàn tất), [AD-DE-11](02_de_batch.md#ad-de-11) (hoàn tất), [AD-DE-12](02_de_batch.md#ad-de-12) (hoàn tất)

Bronze là bản sao append-only của landing: 6 bảng CSV, 2 bảng CDC, off_products, product_images; chặn full refresh.

**Việc cần làm**

1. Viết src/pipelines/batch/bronze.py: hàm autoload, CSV_TABLES, make_cdc_table, off_products, product_images
2. Đặt pipelines.reset.allowed = false cho mọi bảng bronze
3. Chạy thử trên mẫu 1% (dev) rồi trên dữ liệu đủ
4. Chạy bundle run lần hai và thử Full refresh all trên dev
5. Chạy truy vấn đối soát G1

**Đầu ra**

- src/pipelines/batch/bronze.py
- Bảng bronze.orders, order_products_prior, order_products_train, products, aisles, departments, product_prices_cdc, promotions_cdc, off_products, product_images

**Xong khi**

- [ ] Số dòng: orders 3.421.083; order_products_prior 32.434.489; order_products_train 1.384.617; products 49.688; aisles 134; departments 21
- [ ] bronze.products có 0 dòng _rescued_data
- [ ] Chạy lần hai không tăng số dòng; Full refresh all giữ nguyên bronze
- [ ] promotions_cdc có _op 0, 1, 2, 4

### AD-DE-10

**Viết crawler Open Food Facts và tải ảnh theo lô**

- Vai dự kiến: **P2 · DE batch**
- Cổng: **G3**
- Issue: [#46](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/46)
- Tài liệu: `docs/de/01_ingest_landing.md §4`, `docs/data/data_dictionary.md §5.2–5.5`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** [AD-DE-09](02_de_batch.md#ad-de-09) [#45](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/45)
- **Mở khóa:** [AD-DE-13](02_de_batch.md#ad-de-13) (hoàn tất), [AD-DE-19](02_de_batch.md#ad-de-19) (hoàn tất)

Làm giàu danh mục bằng OFF. Crawler chọn sản phẩm phổ biến từ bronze, tôn trọng giới hạn 10 request/phút, ghi file tất định và bảng trạng thái.

**Việc cần làm**

1. Viết src/ingest/off_crawler.py: pick_batch, search có retry 429, ghi _staging rồi mv, MERGE off_crawl_state
2. Viết task off_images: chỉ tải ảnh chưa có
3. Tách hàm thuần (batch_id, dựng dòng JSON) và viết test
4. Khai báo job off_crawler → off_images trong bundles/resources/jobs_ingest.yml
5. Chạy lô đầu, rồi bundle run instacart_batch để nạp off_products, product_images

**Đầu ra**

- src/ingest/off_crawler.py
- tests/test_off_crawler.py
- Job off_crawler trong DAB
- Bảng landing_meta.off_crawl_state

**Xong khi**

- [ ] off_crawl_state có ≥ 300 dòng done/no_result sau lô đầu
- [ ] Chạy lại không tra lại sản phẩm done, không tải lại ảnh đã có
- [ ] bronze.off_products và bronze.product_images có dữ liệu

### AD-DE-11

**Dựng silver SCD2 lịch sử giá và SCD1 khuyến mãi**

- Vai dự kiến: **P2 · DE batch**
- Cổng: **G3**
- Issue: [#47](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/47)
- Tài liệu: `docs/de/03_silver_cdc_scd_dq.md §2`, `docs/de/01_ingest_landing.md §3.6`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** [AD-DE-03](02_de_batch.md#ad-de-03) [#39](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/39), [AD-DE-09](02_de_batch.md#ad-de-09) [#45](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/45)
- **Mở khóa:** [AD-DE-14](02_de_batch.md#ad-de-14) (hoàn tất), [AD-DE-22](02_de_batch.md#ad-de-22) (hoàn tất)

AUTO CDC biến luồng CDC thành silver.product_prices_history (SCD2, phục vụ join giá theo thời điểm) và silver.promotions (SCD1, áp dụng delete).

**Việc cần làm**

1. Viết src/pipelines/batch/silver_retail.py: view product_prices_changes, promotions_changes có expectations và loại trùng
2. create_auto_cdc_flow SCD2 (sequence_by updated_at, track history unit_price, currency)
3. create_auto_cdc_flow SCD1 (sequence_by struct(_lsn, _seqval), apply delete)
4. Thêm quarantine_product_prices bằng khuôn dq dùng chung
5. Seed wave 1–3, chạy ADF và pipeline, chạy truy vấn §2.4

**Đầu ra**

- src/pipelines/batch/silver_retail.py
- Bảng silver.product_prices_history, silver.promotions, silver.quarantine_product_prices

**Xong khi**

- [ ] Không product_id nào có > 1 phiên bản đang mở
- [ ] Sản phẩm đổi giá ở wave 1 và 3 có 3 phiên bản đúng khoảng hiệu lực
- [ ] Khuyến mãi bị delete ở nguồn không còn ở silver.promotions
- [ ] Đọc lặp cùng window không sinh phiên bản mới

### AD-DE-12

**Dựng silver đơn hàng: order_time_sim, orders_batch, order_items_prior**

- Vai dự kiến: **P2 · DE batch**
- Cổng: **G3**
- Issue: [#48](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/48)
- Tài liệu: `docs/de/03_silver_cdc_scd_dq.md §3, §4`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** [AD-DE-02](02_de_batch.md#ad-de-02) [#38](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/38), [AD-DE-03](02_de_batch.md#ad-de-03) [#39](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/39), [AD-DE-09](02_de_batch.md#ad-de-09) [#45](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/45)
- **Mở khóa:** [AD-DE-15](02_de_batch.md#ad-de-15) (hoàn tất), [AD-DE-16](02_de_batch.md#ad-de-16) (hoàn tất), [AD-DE-26](02_de_batch.md#ad-de-26) (hoàn tất)

Silver phần batch của đơn: thời gian giả lập tất định, header đơn prior/test, món hàng prior upsert theo khóa để nạp lại file không nhân dòng.

**Việc cần làm**

1. Viết src/pipelines/batch/silver_instacart.py: order_time_sim (expect_or_fail), orders_batch (ORDER_RULES)
2. order_items_prior bằng AUTO CDC SCD1 theo (order_id, product_id), cluster_by order_id
3. Thêm quarantine_order_items_prior, quarantine_orders
4. Đo thời gian MERGE lần đầu, ghi lại để đưa vào benchmark

**Đầu ra**

- src/pipelines/batch/silver_instacart.py
- Bảng silver.order_time_sim, orders_batch, order_items_prior, quarantine_*

**Xong khi**

- [ ] order_items_prior: count(*) = count(DISTINCT order_id, product_id) = 32.434.489
- [ ] orders_batch có số đơn = 3.421.083 − 131.209 (kiểm lại theo số thật)
- [ ] order_time_sim không có order_ts_sim NULL
- [ ] test_sim_time vẫn khớp với SQL trong pipeline

### AD-DE-13

**Làm phẳng OFF và fuzzy match sang sản phẩm Instacart**

- Vai dự kiến: **P2 · DE batch**
- Cổng: **G3**
- Issue: [#49](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/49)
- Tài liệu: `docs/de/03_silver_cdc_scd_dq.md §5`, `docs/data/data_dictionary.md §5.6`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** [AD-DE-02](02_de_batch.md#ad-de-02) [#38](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/38), [AD-DE-10](02_de_batch.md#ad-de-10) [#46](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/46)
- **Mở khóa:** [AD-DE-14](02_de_batch.md#ad-de-14) (hoàn tất)

Instacart và OFF không có khóa chung. Bảng cầu nối product_off_match chọn ứng viên tốt nhất theo điểm rapidfuzz và gắn nhãn matched/low_confidence/no_match.

**Việc cần làm**

1. Viết notebook src/pipelines/batch/silver_off.py (%pip rapidfuzz) import norm từ src/common/text.py
2. MV off_products_clean: một dòng mới nhất mỗi (product_id, off_code)
3. MV product_off_match: lọc en:united-states, pandas UDF match_score, ngưỡng 85/70
4. Ghi tỉ lệ khớp thật vào data_dictionary §5.6

**Đầu ra**

- src/pipelines/batch/silver_off.py (phần OFF)
- Bảng silver.off_products_clean, silver.product_off_match
- Tỉ lệ khớp ghi trong docs/data/data_dictionary.md

**Xong khi**

- [ ] product_off_match có đúng một dòng mỗi product_id
- [ ] Có thống kê off_match_status theo 3 nhãn
- [ ] test_norm xanh với hàm notebook đang dùng

### AD-DE-14

**Dựng silver.dim_product không nhân dòng**

- Vai dự kiến: **P2 · DE batch**
- Cổng: **G3**
- Issue: [#50](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/50)
- Tài liệu: `docs/de/03_silver_cdc_scd_dq.md §6`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** [AD-DE-11](02_de_batch.md#ad-de-11) [#47](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/47), [AD-DE-13](02_de_batch.md#ad-de-13) [#49](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/49)
- **Mở khóa:** [AD-DE-17](02_de_batch.md#ad-de-17) (hoàn tất), [AD-DE-22](02_de_batch.md#ad-de-22) (hoàn tất), [AD-DE-27](02_de_batch.md#ad-de-27) (hoàn tất)

dim_product gộp products, aisle, department, giá hiện hành, khuyến mãi đang chạy, OFF và ảnh. Mọi bảng phải duy nhất theo khóa trước LEFT JOIN.

**Việc cần làm**

1. Viết MV dim_product trong silver_off.py với as_of_date từ configuration
2. Gộp khuyến mãi theo product_id trước khi join; lọc __END_AT IS NULL cho giá
3. Đặt expect_or_fail no_fanout và expect has_price
4. Chạy pipeline và kiểm số dòng

**Đầu ra**

- MV silver.dim_product

**Xong khi**

- [ ] silver.dim_product đúng 49.688 dòng
- [ ] Không có product_id lặp
- [ ] Tỉ lệ has_price đạt gần 100% (ghi số thật)

### AD-DE-15

**Gộp batch với stream ở silver và dựng baskets**

- Vai dự kiến: **P2 · DE batch**
- Cổng: **G3**
- Issue: [#51](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/51)
- Tài liệu: `docs/de/03_silver_cdc_scd_dq.md §7`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** [AD-DE-12](02_de_batch.md#ad-de-12) [#48](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/48), [AD-ST-05](03_de_stream.md#ad-st-05) [#68](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/68)
- **Mở khóa:** [AD-DE-17](02_de_batch.md#ad-de-17) (hoàn tất), [AD-DE-18](02_de_batch.md#ad-de-18) (hoàn tất), [AD-DE-20](02_de_batch.md#ad-de-20) (hoàn tất), [AD-DE-22](02_de_batch.md#ad-de-22) (hoàn tất), [AD-DE-24](02_de_batch.md#ad-de-24) (hoàn tất)

Mỗi bảng chỉ thuộc một pipeline, nên batch và stream gặp nhau qua view UC silver.orders, silver.order_items. baskets là đầu vào DS.

**Việc cần làm**

1. Viết src/tasks/create_views.sql: view silver.orders và silver.order_items (UNION ALL, cột source)
2. Tham số hóa tên schema để chạy được trên dev
3. Viết MV baskets ở cuối silver_instacart.py (split prior/holdout, expect_or_fail non_empty_basket)
4. Chạy trên warehouse và pipeline dev

**Đầu ra**

- src/tasks/create_views.sql
- View silver.orders, silver.order_items
- MV silver.baskets

**Xong khi**

- [ ] Chạy create_views.sql hai lần không lỗi
- [ ] baskets chỉ có split prior và holdout, không có đơn test
- [ ] Khi stream đã phát đủ: baskets prior = 3.214.874, holdout = 131.209 (kiểm lại số thật)

### AD-DE-16

**Viết collect_dq ghi kết quả expectations vào ops.dq_results**

- Vai dự kiến: **P2 · DE batch**
- Cổng: **G3**
- Issue: [#52](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/52)
- Tài liệu: `docs/de/06_van_hanh_chay_lai.md §3`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** [AD-DE-08](02_de_batch.md#ad-de-08) [#44](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/44), [AD-DE-12](02_de_batch.md#ad-de-12) [#48](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/48), [AD-OPS-22](01_devops.md#ad-ops-22) [#24](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/24), [AD-OPS-20](01_devops.md#ad-ops-20) [#22](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/22)
- **Mở khóa:** [AD-DE-18](02_de_batch.md#ad-de-18) (hoàn tất), [AD-BI-07](04_power_bi.md#ad-bi-07) (hoàn tất)

Số dòng đạt/bị loại theo từng expectation lấy từ event log, phục vụ trang 5 Power BI và theo dõi chất lượng.

**Việc cần làm**

1. Tạo bảng ops.dq_results (update_id, pipeline, flow, dataset, expectation, passed, failed, at)
2. Viết src/tasks/collect_dq.sql MERGE theo (update_id, flow, expectation) cho event_log_batch
3. Thêm nhánh cho event_log_stream khi pipeline stream có event log
4. Lưu sẵn các truy vấn điều tra trong de/06 §3 vào runbook

**Đầu ra**

- src/tasks/collect_dq.sql
- Bảng ops.dq_results

**Xong khi**

- [ ] Chạy collect_dq hai lần không nhân dòng
- [ ] ops.dq_results có dòng cho mọi expectation của silver
- [ ] Số dòng quarantine_* giải thích được bằng ops.dq_results

### AD-DE-17

**Viết reconcile đối soát nguồn, bronze, silver vào ops.reconciliation**

- Vai dự kiến: **P2 · DE batch**
- Cổng: **G3**
- Issue: [#53](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/53)
- Tài liệu: `docs/de/06_van_hanh_chay_lai.md §4`, `docs/de/03_silver_cdc_scd_dq.md §9`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** [AD-DE-14](02_de_batch.md#ad-de-14) [#50](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/50), [AD-DE-15](02_de_batch.md#ad-de-15) [#51](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/51), [AD-ST-07](03_de_stream.md#ad-st-07) [#70](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/70)
- **Mở khóa:** [AD-DE-18](02_de_batch.md#ad-de-18) (hoàn tất), [AD-DE-21](02_de_batch.md#ad-de-21) (bắt đầu), [AD-BI-07](04_power_bi.md#ad-bi-07) (hoàn tất), [AD-DS-17](05_data_science.md#ad-ds-17) (hoàn tất)

Đối soát tự động là điều kiện mở G3 và chặn refresh_bi khi số lệch.

**Việc cần làm**

1. Tạo bảng ops.reconciliation; cân nhắc bảng ops.expected_counts thay số cố định
2. Viết src/tasks/reconcile.sql MERGE theo (check_date, check_name), assert_true ở cuối
3. Tách khối -- G3 (kiểm stream) và -- G4 (gold), bật dần theo cổng
4. Chạy đủ checklist G3 sau khi stream phát đủ

**Đầu ra**

- src/tasks/reconcile.sql
- Bảng ops.reconciliation

**Xong khi**

- [ ] Chạy lại trong ngày không thêm dòng
- [ ] Mọi check G3 passed = true (orders 3.421.083, dim_product 49.688, stream 131.209 / 1.384.617, SCD2 không trùng phiên bản mở)
- [ ] Đổi tạm một số kỳ vọng thì task đỏ

### AD-DE-18

**Hoàn thiện job instacart-batch-medallion trong DAB**

- Vai dự kiến: **P2 · DE batch**
- Cổng: **G3**
- Issue: [#54](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/54)
- Tài liệu: `docs/de/06_van_hanh_chay_lai.md §2, §8`, `docs/devops/06_de_den_dashboard.md §2.1`, `bundles/AGENTS.md`
- **Bắt đầu sau:** [AD-DE-08](02_de_batch.md#ad-de-08) [#44](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/44)
- **Hoàn tất còn cần:** [AD-DE-15](02_de_batch.md#ad-de-15) [#51](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/51), [AD-DE-16](02_de_batch.md#ad-de-16) [#52](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/52), [AD-DE-17](02_de_batch.md#ad-de-17) [#53](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/53), [AD-OPS-22](01_devops.md#ad-ops-22) [#24](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/24), [AD-OPS-20](01_devops.md#ad-ops-20) [#22](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/22)
- **Mở khóa:** [AD-OPS-23](01_devops.md#ad-ops-23) (hoàn tất), [AD-OPS-26](01_devops.md#ad-ops-26) (hoàn tất), [AD-DE-19](02_de_batch.md#ad-de-19) (hoàn tất), [AD-DE-25](02_de_batch.md#ad-de-25) (hoàn tất), [AD-DE-27](02_de_batch.md#ad-de-27) (bắt đầu)

Thay khung spark_python_task bằng job thật: pipeline_task → create_views → collect_dq ∥ reconcile. ADF pl_master gọi job này theo job ID.

**Việc cần làm**

1. Viết lại bundles/resources/jobs_batch.yml theo de/06 §2 (giữ name instacart-batch-medallion, tham số adf_run_id)
2. sql_task trên warehouse_id lookup wh-instacart-bi
3. Thêm notification email khi lỗi hoặc chạy > 60 phút
4. Deploy dev và chạy trọn job

**Đầu ra**

- bundles/resources/jobs_batch.yml

**Xong khi**

- [ ] bundle validate -t dev đạt
- [ ] Job dev chạy xanh từ pipeline_batch tới reconcile
- [ ] Repair run sau khi dừng giữa pipeline vẫn cho đối soát đạt

### AD-DE-19

**Dựng ADF pl_ingest_off và pl_master điều phối hằng ngày**

- Vai dự kiến: **P2 · DE batch**
- Cổng: **G3**
- Issue: [#55](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/55)
- Tài liệu: `docs/de/01_ingest_landing.md §5`, `docs/devops/06_de_den_dashboard.md §1, §3`
- **Bắt đầu sau:** [AD-DE-07](02_de_batch.md#ad-de-07) [#43](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/43)
- **Hoàn tất còn cần:** [AD-DE-10](02_de_batch.md#ad-de-10) [#46](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/46), [AD-DE-18](02_de_batch.md#ad-de-18) [#54](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/54), [AD-OPS-25](01_devops.md#ad-ops-25) [#27](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/27), [AD-OPS-23](01_devops.md#ad-ops-23) [#25](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/25)
- **Mở khóa:** [AD-OPS-26](01_devops.md#ad-ops-26) (hoàn tất), [AD-DE-27](02_de_batch.md#ad-de-27) (bắt đầu)

pl_master 02:00 chạy CDC → OFF → job Databricks; Databricks chỉ chạy khi landing đã công bố, adf_run_id nối được hai hệ thống.

**Việc cần làm**

1. Thiết kế pl_ingest_off gọi job off_crawler
2. Thiết kế pl_master: Execute retail_db → OFF → Databricks Job (adf_run_id = RunId), Web activity khi lỗi
3. Export JSON vào adf/pipelines/, mở PR; trigger để tắt (triggers_on do P1 bật)
4. Trigger now trên prod sau khi P1 apply 40-orchestration

**Đầu ra**

- adf/pipelines/pl_ingest_off.json
- adf/pipelines/pl_master.json

**Xong khi**

- [ ] pl_master chạy xanh trọn chuỗi
- [ ] ops.pipeline_runs hoặc job run có adf_run_id khớp RunId của ADF
- [ ] Activity Databricks không chạy khi CDC lỗi

### AD-DE-20

**Viết SQL ds-prepare: snapshot, item_index, transactions, holdout_eval**

- Vai dự kiến: **P2 · DE batch**
- Cổng: **G3**
- Issue: [#56](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/56)
- Tài liệu: `docs/de/08_du_lieu_cho_ds.md §2, §3, §4, §6`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** [AD-DE-15](02_de_batch.md#ad-de-15) [#51](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/51), [AD-OPS-19](01_devops.md#ad-ops-19) [#21](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/21), [AD-OPS-20](01_devops.md#ad-ops-20) [#22](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/22)
- **Mở khóa:** [AD-DE-21](02_de_batch.md#ad-de-21) (bắt đầu)

DS cần bản dữ liệu cố định data_version v1, mã hóa item tất định ở 3 cấp và tập holdout chia context/targets.

**Việc cần làm**

1. Viết src/tasks/ds_prepare/01_snapshot.sql (chặn ghi lần hai, data_versions)
2. Viết 02_index_transactions.sql (item_index, transactions, scale_bucket, REPLACE WHERE)
3. Viết 03_holdout.sql (context/targets theo add_to_cart_order, val/test)
4. Viết src/tasks/ds_prepare/run_sql.py tách câu theo ';' và truyền :v
5. Test run_sql (tách câu) và chạy 3 file trên Spark local với fixture nhỏ

**Đầu ra**

- src/tasks/ds_prepare/01_snapshot.sql
- src/tasks/ds_prepare/02_index_transactions.sql
- src/tasks/ds_prepare/03_holdout.sql
- src/tasks/ds_prepare/run_sql.py
- tests/test_ds_prepare.py

**Xong khi**

- [ ] Test local xanh: chạy hai lần cho cùng kết quả, không nhân dòng
- [ ] idx tất định: tính lại ra cùng idx

### AD-DE-21

**Xuất file txt cho Hadoop, dựng job ds-prepare và chạy v1**

- Vai dự kiến: **P2 · DE batch**
- Cổng: **G3**
- Issue: [#57](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/57)
- Tài liệu: `docs/de/08_du_lieu_cho_ds.md §5, §6, §7, §8`
- **Bắt đầu sau:** [AD-DE-17](02_de_batch.md#ad-de-17) [#53](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/53), [AD-DE-20](02_de_batch.md#ad-de-20) [#56](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/56)
- **Hoàn tất còn cần:** [AD-OPS-19](01_devops.md#ad-ops-19) [#21](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/21), [AD-OPS-20](01_devops.md#ad-ops-20) [#22](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/22)
- **Mở khóa:** [AD-OPS-29](01_devops.md#ad-ops-29) (hoàn tất), [AD-DS-07](05_data_science.md#ad-ds-07) (hoàn tất), [AD-DS-11](05_data_science.md#ad-ds-11) (bắt đầu)

Chốt G3 phía DE: ds_data v1 và file txt cho cụm Hadoop local sẵn sàng thì DS bắt đầu trên cloud.

**Việc cần làm**

1. Viết src/tasks/ds_prepare/export_txt.py (3 cấp × 4 quy mô, assert số dòng, manifest ghi sau cùng)
2. Viết src/tasks/ds_prepare/04_check.sql
3. Viết bundles/resources/jobs_ds.yml phần ds_prepare (không lịch, max_concurrent_runs 1)
4. Chạy job với data_version v1 khi checklist G3 đạt
5. Ghi số liệu vào bảng ở de/08 §7, báo P4 và ghi ngày tạo v1 vào ds/00 §5

**Đầu ra**

- src/tasks/ds_prepare/export_txt.py
- src/tasks/ds_prepare/04_check.sql
- bundles/resources/jobs_ds.yml
- Bảng ds_data.* v1
- /Volumes/instacart/ds_data/exports/v1/ + _manifest.json

**Xong khi**

- [ ] Task check xanh
- [ ] _manifest.json đủ 3 cấp × 4 quy mô
- [ ] Vai ds đọc được ds_data và exports; INSERT báo PERMISSION_DENIED
- [ ] Chạy lại job với v1 không đổi dữ liệu

### AD-DE-22

**Dựng gold.fact_order_lines với giá theo thời điểm**

- Vai dự kiến: **P2 · DE batch**
- Cổng: **G4**
- Issue: [#58](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/58)
- Tài liệu: `docs/de/05_gold_serving.md §1, §2.1, §4`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** [AD-DE-11](02_de_batch.md#ad-de-11) [#47](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/47), [AD-DE-14](02_de_batch.md#ad-de-14) [#50](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/50), [AD-DE-15](02_de_batch.md#ad-de-15) [#51](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/51)
- **Mở khóa:** [AD-DE-23](02_de_batch.md#ad-de-23) (hoàn tất)

Fact chi tiết là nền cho mọi bảng gold; point-in-time join với SCD2 để doanh thu không đổi khi giá đổi.

**Việc cần làm**

1. Viết src/pipelines/batch/gold.py: fact_order_lines (cluster_by order_date, department_id)
2. Gộp khuyến mãi theo (product_id, ngày) nếu phát hiện nhân dòng
3. Bật khối -- G4 trong reconcile.sql (gold.fact no fanout)
4. Ghi thời gian chạy MV để đưa vào benchmark

**Đầu ra**

- src/pipelines/batch/gold.py (fact)
- MV gold.fact_order_lines

**Xong khi**

- [ ] count(*) = count(DISTINCT order_id, product_id) = 33.819.106
- [ ] 0 dòng unit_price NULL
- [ ] Check gold trong ops.reconciliation đạt

### AD-DE-23

**Dựng gold tổng hợp, dim và bảng phân quyền bi_user_access**

- Vai dự kiến: **P2 · DE batch**
- Cổng: **G4**
- Issue: [#59](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/59)
- Tài liệu: `docs/de/05_gold_serving.md §2.2, §2.3, §4, §8`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** [AD-DE-22](02_de_batch.md#ad-de-22) [#58](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/58)
- **Mở khóa:** [AD-DE-24](02_de_batch.md#ad-de-24) (hoàn tất), [AD-BI-02](04_power_bi.md#ad-bi-02) (hoàn tất), [AD-BI-06](04_power_bi.md#ad-bi-06) (hoàn tất)

Power BI chỉ đọc bảng nhỏ đã tổng hợp: kpi_daily, kpi_by_dow_hour, department_sales, product_360, dim_date, dim_department, bi_user_access.

**Việc cần làm**

1. Thêm các MV vào gold.py theo de/05 §2.2–2.3
2. product_360 có expect_or_fail one_row_per_product
3. bi_user_access đọc landing_meta.bi_access_seed (P3 nhập tay, không commit email)
4. Chạy kiểm tổng items khớp fact

**Đầu ra**

- MV gold.kpi_daily, kpi_by_dow_hour, department_sales, product_360, dim_date, dim_department, bi_user_access

**Xong khi**

- [ ] sum(items) của department_sales = count(*) của fact_order_lines
- [ ] product_360 đúng 49.688 dòng
- [ ] Vai bi đọc được gold.* qua warehouse, đọc silver.* bị PERMISSION_DENIED

### AD-DE-24

**Dựng gold.product_pair_counts cho trang chi tiết sản phẩm**

- Vai dự kiến: **P2 · DE batch**
- Cổng: **G4**
- Issue: [#60](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/60)
- Tài liệu: `docs/de/05_gold_serving.md §2.2`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** [AD-DE-15](02_de_batch.md#ad-de-15) [#51](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/51), [AD-DE-23](02_de_batch.md#ad-de-23) [#59](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/59)
- **Mở khóa:** [AD-BI-03](04_power_bi.md#ad-bi-03) (hoàn tất)

Baseline cặp sản phẩm mua kèm (support, confidence, lift xấp xỉ) cho trang 2, trong khả năng 4 vCPU.

**Việc cần làm**

1. Thêm MV product_pair_counts vào gold.py (top 5.000 sản phẩm, HAVING ≥ 50)
2. Đo thời gian chạy và ghi vào benchmark
3. Ghi chú trên tài liệu rằng confidence/lift là xấp xỉ

**Đầu ra**

- MV gold.product_pair_counts

**Xong khi**

- [ ] Mọi dòng có p1 < p2
- [ ] Số dòng ≤ 200 nghìn
- [ ] Thời gian chạy đã ghi lại

### AD-DE-25

**Viết refresh_bi và ghi ops.pipeline_runs**

- Vai dự kiến: **P2 · DE batch**
- Cổng: **G4**
- Issue: [#61](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/61)
- Tài liệu: `docs/de/07_power_bi.md §7`, `docs/de/06_van_hanh_chay_lai.md §2`, `docs/de/00_tong_quan_de.md §3.3`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** [AD-DE-18](02_de_batch.md#ad-de-18) [#54](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/54), [AD-BI-09](04_power_bi.md#ad-bi-09) [#82](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/82), [AD-BI-01](04_power_bi.md#ad-bi-01) [#74](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/74)
- **Mở khóa:** Không có.

Task cuối của job: gọi API refresh semantic model khi đối soát đạt, ghi một dòng vận hành có adf_run_id.

**Việc cần làm**

1. Viết src/tasks/refresh_bi.py: gọi POST refreshes nếu tenant cho phép, nếu không ghi 'skipped'
2. Tạo bảng ops.pipeline_runs và ghi run ID ADF/Databricks, trạng thái, số dòng
3. Thêm task refresh_bi (run_if ALL_SUCCESS sau reconcile) vào jobs_batch.yml
4. Test hàm dựng bản ghi pipeline_runs

**Đầu ra**

- src/tasks/refresh_bi.py
- tests/test_refresh_bi.py
- Bảng ops.pipeline_runs
- jobs_batch.yml thêm task refresh_bi

**Xong khi**

- [ ] Reconcile đỏ thì refresh_bi không chạy
- [ ] ops.pipeline_runs có dòng với adf_run_id sau mỗi lần job chạy
- [ ] Semantic model được refresh hoặc có dòng skipped rõ lý do

### AD-DE-26

**Chạy benchmark CSV, Parquet, Delta, Delta OPTIMIZE**

- Vai dự kiến: **P2 · DE batch**
- Cổng: **G4**
- Issue: [#62](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/62)
- Tài liệu: `docs/de/05_gold_serving.md §5, §5.1`
- **Bắt đầu sau:** Không có.
- **Hoàn tất còn cần:** [AD-DE-12](02_de_batch.md#ad-de-12) [#48](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/48)
- **Mở khóa:** [AD-BI-12](04_power_bi.md#ad-bi-12) (hoàn tất), [AD-RP-03](06_bao_cao_demo.md#ad-rp-03) (bắt đầu)

Bằng chứng rubric 3.1 và 3.4: tốc độ đọc ghi và dung lượng theo định dạng trên cùng cấu hình máy.

**Việc cần làm**

1. Viết src/tasks/benchmark_formats.py ghi ops.benchmark_results
2. Khai báo job benchmark trong DAB (job cluster theo policy)
3. Chạy 3 lần, lấy trung vị theo định dạng
4. Gom thêm thời gian MERGE order_items_prior, fact_order_lines, product_pair_counts

**Đầu ra**

- src/tasks/benchmark_formats.py
- Job benchmark trong bundles/resources
- Bảng ops.benchmark_results

**Xong khi**

- [ ] ops.benchmark_results có 4 định dạng × 3 lần
- [ ] Thư mục thử đã được dọn sau mỗi lần
- [ ] Có bảng trung vị kèm cấu hình máy cho báo cáo

### AD-DE-27

**Viết runbook chạy lại và diễn tập 4 kịch bản lỗi**

- Vai dự kiến: **P2 · DE batch**
- Cổng: **G4**
- Issue: [#63](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/63)
- Tài liệu: `docs/de/06_van_hanh_chay_lai.md §1, §5, §6, §7, §8`
- **Bắt đầu sau:** [AD-DE-18](02_de_batch.md#ad-de-18) [#54](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/54), [AD-DE-19](02_de_batch.md#ad-de-19) [#55](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/55)
- **Hoàn tất còn cần:** [AD-DE-14](02_de_batch.md#ad-de-14) [#50](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/50), [AD-ST-07](03_de_stream.md#ad-st-07) [#70](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/70)
- **Mở khóa:** [AD-RP-03](06_bao_cao_demo.md#ad-rp-03) (hoàn tất)

Bằng chứng ETL tự động và chạy lại an toàn: mỗi kịch bản có ảnh lỗi, lệnh chạy lại, đối soát trước/sau.

**Việc cần làm**

1. Diễn tập A: CDC đứt giữa chừng → rerun from failed
2. Diễn tập B: dừng pipeline khi order_items_prior đang chạy → Repair run
3. Diễn tập C: phối hợp P3 phát lại toàn bộ stream
4. Diễn tập D: đổi ngưỡng matched 85 → 80, refresh_selection product_off_match + dim_product
5. Ghi lại vào runbook kèm truy vấn điều tra event log

**Đầu ra**

- Thư mục ảnh bằng chứng cho báo cáo
- Runbook (cập nhật de/06 nếu thực tế khác tài liệu)

**Xong khi**

- [ ] Đủ 4 kịch bản, mỗi kịch bản có đủ 4 thứ
- [ ] Sau mỗi kịch bản ops.reconciliation vẫn passed = true
- [ ] Kịch bản D: matched tăng, dim_product vẫn 49.688
