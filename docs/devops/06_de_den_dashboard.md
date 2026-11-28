# 06 · Từ pipeline DE đến dashboard Power BI (giai đoạn P6–P7 → cổng G3, G4)

**Đầu vào:**
- G1 đã mở ([05](05_databricks_platform.md)).
- G2 đã mở ([03](03_terraform_azure.md) mục 4) cho phần streaming.
- Quy tắc dữ liệu trong [data_dictionary.md](../data/data_dictionary.md).

**Đầu ra:**
- ADF pipeline và trigger được quản lý bằng code.
- Job batch và job streaming được deploy bằng DAB.
- `silver`/`gold` đạt đối soát.
- Power BI Service có 4 report kèm điều hướng, RLS, bố cục mobile và cập nhật gần real-time.

**Xong khi:** đạt checklist G3 (mục 6.3) và G4 (mục 6.4).

> Tài liệu này chỉ nói phần **triển khai và vận hành** (đặt code ở đâu, deploy thế nào, nghiệm thu ra sao). Logic transform chi tiết nằm ở bộ tài liệu DE [docs/de/](../de/00_tong_quan_de.md): CDC, Lakeflow Declarative Pipelines, streaming, chạy lại khi lỗi, dựng Power BI.

---

## 1. Stack `40-orchestration`: ADF bằng code

**Vì sao dùng Terraform cho ADF thay vì ADF Git integration:**
- ADF Git integration tạo nhánh `adf_publish` và ARM template riêng, dễ xung đột với Terraform (cả hai cùng sở hữu factory).
- Dùng Terraform thì một PR thấy được cả linked service lẫn pipeline, và `drift-check` phát hiện được việc sửa tay trong Studio.

**Cách làm việc của P2:**
1. Thiết kế và debug pipeline trong ADF Studio. Studio cho phép chạy debug mà không cần publish.
2. Chọn *Export JSON* phần `activities` của pipeline, lưu vào `adf/pipelines/<tên>.json`. Tham số và `concurrency` lưu ở `adf/pipeline_settings/<tên>.json` (dạng `{concurrency, parameters}`, mọi giá trị là String).
3. Mở PR. Merge xong thì `tf-apply` publish pipeline lên factory.
4. Từ lúc này coi Studio là chỉ đọc. Mọi sửa đổi đều đi qua bước 2.

| Resource Terraform | Đối tượng ADF | Ghi chú |
|---|---|---|
| `azurerm_data_factory_linked_service_data_lake_storage_gen2` | `ls_adls` | Xác thực bằng managed identity của ADF |
| `azurerm_data_factory_linked_service_azure_sql_database` | `ls_sql_retail` | SQL auth: login `sql_admin_login` trong `infra/envs/40-orchestration.tfvars`, mật khẩu là secret `sql-admin-password` lấy qua `ls_kv` |
| `azurerm_data_factory_linked_service_key_vault` | `ls_kv` | |
| `azurerm_data_factory_linked_custom_service` kiểu `HttpServer` | `ls_zenodo`, `ls_off_api` | Anonymous. Không dùng `azurerm_data_factory_linked_service_web` vì resource đó tạo kiểu `Web` (Web Table), còn dataset `HttpServerLocation` cần `HttpServer` |
| `azurerm_data_factory_linked_custom_service` kiểu `AzureDatabricks` | `ls_dbx` | Xác thực `MSI` (managed identity của ADF có role `Contributor` trên workspace), không khai cluster. Pipeline dùng activity **Databricks Job** gọi job theo ID. Đã kiểm ở AD-OPS-25: `azurerm_data_factory_linked_service_azure_databricks` bắt buộc một trong `existing_cluster_id`, `instance_pool`, `new_cluster_config`, nên dùng custom service giống spike 3.5 |
| `azapi_resource` (`Microsoft.DataFactory/factories/datasets`) | Mọi dataset trong `adf/datasets/*.json` | P2 soạn hợp đồng dataset (file dạng `{name, properties}`, `name` phải trùng tên file). Stack đọc cả thư mục bằng `fileset()` và gửi nguyên `properties`, nên thêm hay sửa dataset chỉ cần sửa JSON. Không dùng `azurerm_data_factory_custom_dataset` vì resource đó gán `defaultValue` cho mọi tham số, làm tham số bắt buộc thành tùy chọn |
| `azapi_resource` (`Microsoft.DataFactory/factories/pipelines`) | Mọi `adf/pipelines/*.json` | Gửi nguyên mảng activities. `concurrency` và `parameters` lấy từ `adf/pipeline_settings/<tên>.json`; tham số giữ kiểu String và `defaultValue` như file. ADF từ chối pipeline gọi pipeline chưa có, nên stack đọc các `referenceName` dạng `pl_*` trong JSON và xếp pipeline vào 4 tầng (`pipeline_tier0` không gọi pipeline nào, tầng sau chỉ gọi tầng trước; `pl_master` ở tầng 3). Pipeline lồng quá 4 tầng làm plan lỗi. Thêm pipeline ingest mới chỉ cần thêm 2 file JSON (AD-OPS-25b) |
| `data "databricks_job"` | Job ID cho `pl_ingest_off`, `pl_master` | Tra theo tên job prod (`instacart-off-crawler`, `instacart-batch-medallion`) rồi điền vào `defaultValue` của `off_job_id`, `batch_job_id` (AD-OPS-26). Chưa có job prod thì plan lỗi, nên stack chỉ apply sau `bundle deploy -t prod` |
| `azurerm_data_factory_trigger_schedule` | `tr_master_daily` | `pl_master` 02:00 hằng ngày, `SE Asia Standard Time`. `activated = var.triggers_on` (mặc định `false` để không tốn tiền) |

**Các pipeline:**

| Pipeline | Làm gì | Lịch |
|---|---|---|
| `pl_ingest_instacart` | HTTP Zenodo → `_staging` → `landing/instacart/<bảng>/release=2017/` + `_SUCCESS` (một lần, chạy lại được) | Chạy tay |
| `pl_ingest_retail_db` | **SQL CDC**: Lookup cửa sổ LSN → Copy `fn_cdc_get_all_changes` → Parquet `landing/retail_db/<bảng>/window=…/` → SP cập nhật watermark ([de/01 §3](../de/01_ingest_landing.md)) | Hằng ngày |
| `pl_ingest_off` | Gọi job Databricks `off_crawler`. Giới hạn 10 request/phút nên crawler nằm trong Python, không dùng ADF ForEach | Hằng ngày, theo lô khoảng 300 sản phẩm |
| `pl_master` | Execute pipeline retail → OFF → **Databricks Job** `instacart-batch-medallion` | Hằng ngày 02:00 |

`pl_master` gọi job theo **job ID**. Job ID lấy bằng data source `databricks_job` (theo tên job prod do DAB tạo). Vì vậy phần `pl_ingest_off`, `pl_master` và trigger (AD-OPS-26) phải apply **sau** lần `bundle deploy -t prod` đầu tiên. Linked service, dataset (AD-OPS-25) và các pipeline ingest không gọi Databricks (`pl_ingest_instacart`, `pl_ingest_retail_db` và pipeline con của chúng, AD-OPS-25b) không cần job ID nên apply được trước.

## 2. Job Databricks (DAB)

### 2.1 Job batch `instacart-batch-medallion`

Nhóm đã chọn **Lakeflow Declarative Pipelines** (LDP, edition Advanced) cho bronze → silver → gold. Job chỉ còn điều phối pipeline và các task phụ trợ:

`pipeline_batch → resolve_pipeline_update → collect_dq → init_reconciliation → reconcile`

Bốn bước SQL chạy bằng notebook Spark serverless. `resolve_pipeline_update` dùng event log và Jobs task value
để truyền đúng update ID cho collector, kể cả khi repair sinh task run ID mới. Job gọi pipeline theo ID,
giữ tham số `adf_run_id` cho ADF và chạy đối soát batch. Phần hợp nhất view, kiểm G3/G4 và refresh BI
được nối sau khi có đủ nguồn stream/Gold và task AD-DE-25. Job DS vẫn chạy riêng.
Xem [cấu hình job](../../bundles/resources/jobs_batch.yml) và [runbook batch](../de/06_van_hanh_chay_lai.md#2-job-điều-phối-batch).

### 2.2 Pipeline streaming `instacart_stream`

LDP `instacart_stream`: Kafka → `bronze.order_events` → `silver.stream_orders` / `stream_order_items` (`AUTO CDC` theo `order_id`) → `gold.stream_kpi_1m`, `stream_pair_counts_10m`, `ops.streaming_latency`. Chi tiết ở [de/04](../de/04_streaming.md).

**Ràng buộc quota:** chỉ có 4 vCPU, nên **không chạy cùng lúc** với job batch trên Sub A.
- **Mặc định:** pipeline ở chế độ *triggered* và lịch job ở `PAUSED`. Khi hub hoạt động và lịch được bật, job `instacart-stream-15m` chạy mỗi 15 phút ngoài khung 02:00–03:00. Mỗi update đọc hết sự kiện mới rồi dừng, giữ checkpoint và upsert theo khóa.
- **Lúc demo:** tạm dừng lịch batch và stream, đợi các lượt batch kết thúc rồi dùng CI prod với `stream_continuous=true` để chạy pipeline liên tục trong 30–60 phút.
- **Mỗi lần Event Hubs được tạo lại:** dùng mã `eh_generation` mới khi deploy để LDP dùng checkpoint mới cho flow Kafka. Xem [hướng dẫn bundle](../../bundles/README.md#nghiệm-thu-pipeline-stream-ad-st-03).
- **Đo độ trễ (rubric 3.3):** bảng `ops.streaming_latency` gồm:
  - `event_ts` (simulator).
  - `enqueued_ts` (Event Hubs, region B).
  - `bronze_ts`.
  - `silver_ts` (Databricks, region A).
  - So sánh với lần chạy simulator **cùng region** để thấy chênh lệch.

### 2.3 Simulator

Chạy ở máy local hoặc GitHub Codespace. Đọc `order_products__train` rồi phát sự kiện JSON tới Kafka endpoint. Connection string lấy từ Key Vault (`az keyvault secret show`), không lưu trong file. Đây là script Python trong `src/streaming/simulator/`, không phải tài nguyên Terraform.

## 3. Thứ tự triển khai lần đầu

| # | Việc | Ai | Lệnh / workflow |
|---|---|---|---|
| 1 | Deploy bundle prod (tạo pipeline + job) | P2 | merge PR → `de-deploy` |
| 2 | Bật CDC trên Azure SQL, chạy `seed_retail_db --wave 0` | P2 | `src/ingest/sql/*.sql`; `databricks bundle run seed_retail_db -t prod` |
| 3 | Apply `40-orchestration` (trigger ở trạng thái tắt) | P1 | merge PR → `tf-apply` |
| 4 | Chạy `pl_ingest_instacart` một lần | P2 | ADF Studio → Trigger now |
| 5 | Chạy `pl_master` → bronze/silver/gold | P2 | Trigger now; kiểm tra G3 |
| 6 | Bật Event Hubs, chạy simulator khoảng 95% tập train | P3 | `stream-toggle up`, simulator |
| 7 | Chạy pipeline streaming tới silver/gold | P3 | `bundle run instacart_stream` |
| 8 | Bật trigger hằng ngày | P1 | `triggers_on = true` qua PR |
| 9 | Dựng Power BI (mục 5) | P3 | — |

## 4. Lớp phục vụ (serving) cho Power BI: thang dự phòng

| Ưu tiên | Phương án | Khi nào dùng | Hạ tầng thêm |
|---|---|---|---|
| **1 (mặc định)** | Serverless SQL warehouse `wh-instacart-bi` → connector *Azure Databricks* (DirectQuery cho bảng streaming, Import cho bảng lớn ít đổi) | Spike 3.4 đạt serverless | Không (đã có ở `30-databricks`) |
| 2 | Task `publish_serving` ghi các bảng gold nhỏ (KPI, top N) sang **Azure SQL** `serving_db` qua JDBC → Power BI DirectQuery/Import Azure SQL | Không có serverless | Thêm DB `serving_db` ở `10-core` (dùng chung server) |
| 3 | Power BI đọc thẳng Delta trên ADLS bằng hàm `DeltaLake.Table` của Power Query (Import) | Hai phương án trên đều không dùng được | Quyền Storage Blob Data Reader cho tài khoản Power BI |

Bảng gold dành cho Power BI phải **nhỏ và đã tổng hợp sẵn**. Không cho Power BI quét `silver.order_items` (32 triệu dòng) mỗi lần mở trang.

## 5. Power BI Service: đáp ứng rubric mục 4 "Trực quan dữ liệu"

### 5.1 Workspace, chia sẻ, giấy phép

- Workspace `instacart-bi`: P3 là Admin, P1, P2, P4 là Member.
- Phân phối cho người xem bằng **Power BI App** "Instacart Analytics", chia thành audience theo nhóm người xem.
- **Mọi người xem** (kể cả giảng viên) cần giấy phép Pro hoặc trial, vì workspace không nằm trên Premium/Fabric capacity. Gửi lời mời trước buổi demo.
- **Không** dùng *Publish to web*: tính năng này công khai link và bỏ qua RLS, trái với yêu cầu bảo mật.
- Nguồn report lưu dạng **PBIP** trong `powerbi/` (text, review được qua PR). File `.pbix` không commit.

### 5.2 Bốn report và điều hướng (1 điểm)

| Trang | Nội dung | Bảng gold |
|---|---|---|
| 1 · Tổng quan | KPI: số đơn, số sản phẩm/giỏ, tỷ lệ mua lại, doanh thu giả lập; xu hướng theo `order_ts_sim` | `kpi_daily`, `kpi_by_dow_hour` |
| 2 · Chi tiết sản phẩm | Drill-through từ một sản phẩm: giá hiện hành, khuyến mãi, Nutri-Score, ảnh OFF, sản phẩm hay mua kèm | `product_360`, `product_pair_counts` |
| 3 · Bộ lọc 1: thời gian mua | Heatmap thứ × giờ (`order_dow`, `order_hour_of_day` gốc), slicer khoảng ngày | `kpi_by_dow_hour` |
| 4 · Bộ lọc 2: danh mục | Slicer department/aisle → doanh thu, mua lại, top sản phẩm | `department_sales` |
| (5 · Vận hành streaming) | Số sự kiện/phút, độ trễ p50/p95, đối soát 131.209 đơn | `ops.streaming_latency`, `ops.dq_results` |

Điều hướng gồm:
- Thanh *Page navigator* trên mọi trang.
- Nút *Back*.
- Nút bookmark ("Xem theo tuần / theo giờ").
- Drill-through từ trang 1, 4 sang trang 2.

### 5.3 Cập nhật tức thời từ mô hình dữ liệu (0.5 điểm)

| Cơ chế | Độ trễ thấy được | Giới hạn |
|---|---|---|
| DirectQuery tới bảng gold streaming | Mỗi lần mở hoặc tương tác trang là dữ liệu mới nhất | — |
| Automatic page refresh (APR) | Trên workspace Pro (shared capacity), **tối thiểu khoảng 30 phút**; trong Desktop thì xuống được vài giây | Không đủ "tức thời" khi chỉ dùng APR trên Service |
| **Push semantic model (streaming tile)** | Vài giây | Microsoft đã thông báo ngừng *tạo mới* real-time semantic model từ **31/10/2027**, nên đồ án vẫn dùng được. Cần kiểm tra lại ở spike 3.6 |

**Cách triển khai push:**
1. P3 tạo *streaming semantic model* kiểu API và lấy **push URL**. URL này chứa key, nên lưu vào Key Vault với tên `pbi-push-url`.
2. Task `pair_counts_gold` trong `foreachBatch` gửi POST các KPI tổng hợp của micro-batch (số đơn, doanh thu giả lập, top cặp) tới push URL.
3. Ghim tile streaming lên **dashboard** Power BI cạnh các visual của report.

Trong buổi demo, khi phát nốt khoảng 5% đơn còn lại, tile sẽ nhảy số sau vài giây. Các trang DirectQuery thì cập nhật khi bấm *Refresh visuals*.

### 5.4 Bảo mật: chia sẻ và RLS (1 điểm)

- **RLS động** trong semantic model:
  - Bảng `gold.bi_user_access(user_email, department_id)` do job DE seed.
  - Role `DeptManager` có điều kiện lọc `[user_email] = USERPRINCIPALNAME()`, lan qua quan hệ tới `department_sales`.
  - Role `Executive` không lọc.
- Gán thành viên hoặc nhóm vào role trong Service (Semantic model → Security).
- Kiểm tra bằng *Test as role / View as*. Chụp màn hình hai người dùng thấy hai bộ số khác nhau để đưa vào báo cáo.
- **Hai lớp bảo vệ:**
  - Power BI RLS lọc dòng theo người xem.
  - Unity Catalog chỉ cho tài khoản kết nối của Power BI đọc schema `gold` (tài liệu 05, bước 5). Người xem không bao giờ chạm được tới `silver`/`bronze`.

### 5.5 Responsive mobile (0.5 điểm)

Mỗi trang có **Mobile layout** riêng (View → Mobile layout): sắp lại KPI card dạng dọc, ẩn visual phụ. Kiểm tra trên app Power BI Mobile, chụp màn hình cho báo cáo.

### 5.6 Làm mới dữ liệu

| Phần | Chế độ | Lịch |
|---|---|---|
| Bảng streaming / KPI | DirectQuery | Theo tương tác, có thêm APR |
| Dim sản phẩm, bảng lớn ít đổi | Import | Scheduled refresh 1 lần/ngày, sau `pl_master` (Pro cho tối đa 8 lần/ngày) |
| Tile real-time | Push | Liên tục khi job streaming chạy |

Thông tin đăng nhập nguồn trên Service: OAuth bằng tài khoản P3, hoặc một tài khoản kỹ thuật chỉ có quyền đọc `gold`. Ghi rõ lựa chọn trong báo cáo.

## 6. Checklist nghiệm thu từng cổng

### 6.1 G1: DE trên cloud (lặp lại từ 05 để tiện theo dõi)
- [ ] ADF chạy `pl_ingest_instacart`: đủ 6 file CSV trong `landing/instacart/`.
- [ ] `bronze.*` có số dòng **bằng** file nguồn: `orders` 3.421.083; `order_products__prior` 32.434.489; `products` 49.688; `aisles` 134; `departments` 21.

### 6.2 G2: streaming
- [ ] Simulator phát 1.000 sự kiện thử → `bronze.order_events` có đủ 1.000 bản ghi, không trùng `event_id`.
- [ ] Chạy lại cùng 1.000 sự kiện → silver **không** tăng số dòng (MERGE idempotent).

### 6.3 G3: silver đạt đối soát → **DS bắt đầu**
- [ ] `silver.orders`: đơn prior + test lấy từ batch, đơn train lấy từ stream. Tổng = 3.421.083 sau khi replay xong.
- [ ] Phần stream: **131.209 đơn**, **1.384.617 dòng** `order_items` có `source = 'stream'`.
- [ ] `silver.dim_product` = **49.688 dòng** (LEFT JOIN không làm mất hay nhân dòng).
- [ ] `silver.baskets` chỉ có split `prior | holdout`, không có đơn test (đơn test không có món).
- [ ] `silver.quarantine` có số dòng được giải thích được (ghi trong `ops.dq_results`).
- [ ] Chạy job `ds-prepare`: `ds_data.*` với `data_version = 'v1'` và file txt cho Hadoop đã sẵn sàng, task `check` xanh ([de/08 §7](../de/08_du_lieu_cho_ds.md)).

Đạt hết thì DS bắt đầu ([ds/00 §5](../ds/00_tong_quan_ds.md)): chạy trên `90-sandbox` ở Sub C (bắt buộc, [03 §5](03_terraform_azure.md)), song song với DE.

### 6.4 G4: dashboard đạt rubric
- [ ] App Power BI có ≥ 4 trang, điều hướng bằng nút và page navigator.
- [ ] Hai tài khoản ở hai role RLS thấy số liệu khác nhau (có ảnh chụp).
- [ ] Mobile layout cho đủ 4 trang (có ảnh chụp trên điện thoại).
- [ ] Phát nốt khoảng 5% đơn → tile streaming tăng trong ≤ 1 phút; trang DirectQuery cập nhật sau khi refresh.
- [ ] Bảng `ops.streaming_latency` có số liệu cùng region và khác region (rubric 3.3).
- [ ] Bảng `ops.benchmark_results`: CSV, Parquet, Delta, Delta có OPTIMIZE (rubric 3.1, 3.4).
- [ ] Bảng chi phí theo tag `owner`/`stack` (cluster và warehouse: `cost_owner`) trong Cost Management + ảnh chụp quota vCPU (rubric 5.4).

## 7. Sau G3/G4: phần Data Science (cổng G5)

Kế hoạch chi tiết nằm ở [docs/ds](../ds/00_tong_quan_ds.md). Hạ tầng DS đã được thêm vào các tài liệu DevOps:

| Thành phần | Ở đâu | Khi nào |
|---|---|---|
| Container `ds-data` (exports, inbox, serving) | [03 bước 3.2](03_terraform_azure.md) | Cùng `10-core` (không tốn thêm tiền) |
| Danh tính `id-func-recommend` | [03 bước 3.4](03_terraform_azure.md) | Cùng `10-core` |
| Schema `ds_data`, volume `ds_data.exports`, `ml.inbox`, quyền | [05 bước 3–5](05_databricks_platform.md) | Cùng `30-databricks` |
| Policy `instacart-ds-benchmark` (cho chọn `local[n]`) | [05 bước 6](05_databricks_platform.md) | Trước G3 |
| Job `ds-prepare` (DE), `ds-benchmark`, `ds-champion` (DS); MLflow experiment; serving endpoint | [05 §4](05_databricks_platform.md), [de/08](../de/08_du_lieu_cho_ds.md), [ds/04](../ds/04_thuc_nghiem_danh_gia.md), [ds/05](../ds/05_mlflow_serving_bao_cao.md) | Từ G3 |
| Azure Function làm API (stack `50-serving`) | [03 §5.1](03_terraform_azure.md), [04 §3.8](04_github_actions_cicd.md) | G5, tùy chọn |
| Cụm Hadoop HDFS + YARN (Docker, máy local) | [ds/03](../ds/03_hadoop_mapreduce_local.md) | Từ G0 (dựng thử), đo ở G5 |

### 7.1 Checklist G5: phần DS đạt

Checklist đầy đủ ở [ds/05 §6](../ds/05_mlflow_serving_bao_cao.md). Tóm tắt:
- [ ] E1–E7 có đủ kết quả trong `ml.experiment_runs` / `ml.holdout_metrics`; E1 khớp ở cả 3 cấp item.
- [ ] Kết quả MapReduce và Spark (E6) từ máy local đã nạp lên, từ cùng một máy.
- [ ] Model `instacart.ml.basket_rules@champion`; API trả lời được (Model Serving, hoặc Function theo phương án B).
- [ ] Power BI trang 6–7 đã publish.
