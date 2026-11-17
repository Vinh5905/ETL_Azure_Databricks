# Kết quả spike (P0 → G0)

> Điền theo [01_chuan_bi_va_spike.md](01_chuan_bi_va_spike.md). **Không** ghi subscription ID, object ID hay email vào file này.

Ngày làm spike: `<yyyy-mm-dd>` · Người tổng hợp: P1

## 1. Công cụ đã cài

| Thành viên | az | terraform | tflint | databricks | gh | uv | Docker | Ghi chú |
|---|---|---|---|---|---|---|---|---|
| P1 | 2.91.0 | 1.16.5 | 0.66.0 | 1.26.0 | 2.104.0 | 0.9.21 (Python 3.11.14) | 29.1.3 | macOS arm64, kiểm ngày 2026-11-16. Thêm: trivy 0.76.2, pre-commit 4.7.0, AzCopy 10.34.2, Infracost 2.19.1. tflint cài từ GitHub release. Docker cấp 10 CPU, 15,6 GB RAM, thiếu một chút so với mốc 16 GB cho benchmark DS. `pre-commit run --all-files` chạy được |
| P2 | | | | | | | | |
| P3 | | | | | | | | |
| P4 | | | | | | | | |

## 2. Kết quả phép thử

| # | Phép thử | Sub | Kết quả (Đạt / Không đạt) | Ghi chú / phương án dự phòng đã chọn |
|---|---|---|---|---|
| 3.1 | Region được phép | A, B, C, D | Đạt (Sub A) · Sub B, C, D hoãn sang #131 | Sub A, kiểm 2026-11-16: policy "Allowed resource deployment regions" chỉ cho `malaysiawest`, `eastasia`, `koreacentral`, `japaneast`, `indiasouthcentral`. Giao với Databricks: `eastasia`, `koreacentral`, `japaneast` (Event Hubs, SQL, ADF, Key Vault, Functions có ở cả 5). Ứng viên Sub A: `eastasia`. Region khác cho Event Hubs nếu Sub B cùng danh sách: `japaneast` hoặc `koreacentral`. Mỗi chủ Sub B/C/D tự chạy lệnh 3.1 |
| 3.2 | Quota vCPU (tổng / theo họ máy) | A | Đạt | Giống nhau ở `eastasia`, `koreacentral`, `japaneast`: tổng regional 6 vCPU (low-priority 3); DSv2 4, DSv3 4, Dv3 4, BS 4; DDSv5, DSv5, EDv4 bằng 0. Một `Standard_DS3_v2` (4 vCPU) dùng gần hết quota, còn 2 vCPU tổng nhưng không đủ cho node DSv2 thứ hai |
| 3.3 | Tạo UAMI | A | Đạt | P1 là Owner của Sub A. Tạo `id-spike` trong `rg-spike` (`eastasia`) được |
| 3.3 | Federated credential cho UAMI | A | Đạt | Subject `repo:<owner>/<repo>:ref:refs/heads/main`. Tên credential phải dài 3–120 ký tự: tên `gh` trong lệnh cũ bị từ chối, dùng `gh-main` |
| 3.3 | Gán role chéo subscription (Sub B → UAMI) | B | Hoãn (#131) | Chưa cần Sub B trước G2. Áp dụng dự phòng cho tới khi P3 thử: CI chỉ apply Sub A, Sub B do P3 apply từ máy |
| 3.3 | Tạo App Registration | — | Đạt | Tạo `spike-app` được. Thiết kế vẫn dùng UAMI |
| 3.3 | Tạo nhóm Entra ID | — | Đạt | Tạo `spike-grp` được, nên có thể dùng nhóm Entra cho quyền |
| 3.4 | Tạo workspace Premium | A | Đạt | Kiểm 2026-11-17: `dbw-spike` (Premium) trong `rg-spike`, `eastasia`, tạo bằng `az resource create` (ARM) mất khoảng 4,5 phút. Lệnh `az databricks` không dùng được vì extension cài lỗi (pip) |
| 3.4 | UC tự gán metastore | A | Đạt | Tự gán `metastore_azure_eastasia`, owner là "System user", metastore **không** có storage root. Có sẵn catalog workspace `dbw_spike` (managed, lưu ở storage account managed của workspace) |
| 3.4 | Quyền account admin / metastore admin | — | P1 là workspace admin, không là metastore admin, gần như chắc không là account admin | P1 thuộc nhóm `admins` của workspace và có `CREATE CATALOG`, `CREATE EXTERNAL LOCATION`, `CREATE STORAGE CREDENTIAL` trên metastore (quyền mặc định của workspace admin). Metastore admin là "System user". Menu người dùng trên workspace không có mục "Manage account". Chưa đăng nhập được `accounts.azuredatabricks.net` để xác nhận hẳn |
| 3.4 | Nhóm dùng trong GRANT (05 bước 2) | A | Đạt (account group do workspace admin tạo) | Nhóm workspace-local (`spike-grp`, kể cả gắn `externalId` của nhóm Entra) báo `PRINCIPAL_DOES_NOT_EXIST` khi GRANT. *Automatic identity management* **không** bật: hộp "Add group" không tìm thấy nhóm Entra. Nhưng workspace admin tạo được **account group**: qua UI ("Add new group to account and workspace", tạo `spike-acct-grp`, Source = Account) và qua API `POST /api/2.0/account/scim/v2/Groups` gọi vào workspace. ``GRANT USE CATALOG … TO `spike-acct-grp` `` thành công. Chọn nhánh account group không cần account admin. Cách khai báo trong Terraform cần kiểm ở AD-OPS-18/20; dự phòng vẫn là GRANT theo từng user |
| 3.4 | Tạo storage credential từ Access Connector | A | Đạt | Tạo Access Connector `ac-spike` (system-assigned identity) rồi `databricks storage-credentials create` thành công (`sc_spike`). Giữ nhánh external location |
| 3.4 | Serverless SQL warehouse, `SELECT 1` | A | Đạt | Workspace có sẵn "Serverless Starter Warehouse" (Small). Tạo thêm `wh-spike` 2X-Small, serverless, tự tắt 5 phút: lên `RUNNING` sau 6 giây, `SELECT 1` trả kết quả trong 7 giây (DBSQL 2026.45) |
| 3.4 | Serverless jobs / notebook | A | Đạt | `jobs submit` một notebook không khai compute thì chạy serverless, xong trong 36 giây. Lưu ý: serverless chặn đọc config `spark.databricks.clusterUsageTags.*` (`CONFIG_NOT_AVAILABLE`) |
| 3.4 | Cluster single-node DS3_v2, DBR 15.4 LTS | A | Không đạt với DS3_v2 · Đạt với `Standard_D4ds_v4` | `DS3_v2` hỏng sau 174 giây với `CLOUD_PROVIDER_RESOURCE_STOCKOUT` / `SkuNotAvailable`: Sub A không được bán `DS3_v2`, `D4s_v3`, `D4_v3`, `F4s_v2` ở cả `eastasia`, `koreacentral`, `japaneast`. Họ v5 (`D4ds_v5`, `D4as_v5`) có bán nhưng quota bằng 0. Ở `eastasia` có bán và có quota 4 vCPU: `D4ds_v4`, `D4s_v4`, `D4as_v4`, `D4_v4`, `D4d_v4`. Dự phòng đã thử: single-node `Standard_D4ds_v4` (4 vCPU, 16 GB), DBR 15.4 LTS, `SINGLE_USER`, lên `RUNNING` sau 453 giây (khoảng 7,5 phút), chạy notebook thử xong trong 52 giây. Phải đổi `node_type_id` trong policy ở 05 §3 bước 6 khi chốt G0 |
| 3.4 | Ngày hết trial Premium | A | Không có trial | Sửa kết quả AD-OPS-04: `dbw-spike` tạo với SKU `premium`, **không** phải `trial`, nên không có 14 ngày miễn DBU; DBU tính vào credit ngay từ đầu. Muốn có trial phải tạo workspace với SKU `trial` (ARM `sku.name = "trial"`). Ngoài ra workspace tạo bằng ARM mặc định `enableNoPublicIp = true`, Azure tự dựng **NAT gateway + public IP** trong managed RG, tính tiền theo giờ kể cả khi không chạy gì (theo giá niêm yết khoảng 1,2 USD/ngày, chưa đối chiếu hóa đơn) và trái quy tắc "không NAT gateway". 10-core bước 3.5 phải đặt `no_public_ip = false` |
| 3.4 | Pipeline Lakeflow edition Advanced, classic single-node | A | Đạt | Pipeline Python (`from pyspark import pipelines as dp`), `edition = ADVANCED`, `num_workers = 0`, `Standard_D4ds_v4`, `spark.databricks.cluster.profile = singleNode`: 1 bảng Auto Loader (JSON) có `expect_or_drop` (3 dòng vào, 1 dòng bị loại), 1 `create_auto_cdc_flow` SCD1 có `apply_as_deletes` (4 sự kiện I/U/D, còn đúng 1 khách sau xóa). Update `COMPLETED` sau 686 giây: chờ cluster 9 phút, chạy 2 phút. **Lưu ý:** pipeline ở `development = true` giữ cluster sống sau khi update xong (mặc định 2 giờ) và chiếm hết 4 vCPU, làm job khác lỗi `AZURE_QUOTA_EXCEEDED`. Pipeline dev phải đặt `pipelines.clusterShutdown.delay` ngắn (ví dụ `60s`) hoặc dừng tay |
| 3.4 | Serverless pipeline | A | Đạt | Cùng code, `serverless = true`: update đầu `COMPLETED` sau 93 giây, các lần sau khoảng 1 phút. Không chiếm quota vCPU |
| 3.4 | Đổi tên flow (`append_flow`) | A | Đạt với nguồn file · Kafka hoãn sang AD-OPS-06 | Thử trên pipeline serverless: `append_flow` Auto Loader vào `flow_target`, tên flow lấy từ cấu hình. Chạy với `flow_v1` (3 dòng) rồi đổi sang `flow_v2`: bảng giữ 3 dòng cũ và flow mới đọc lại từ đầu, thành 6 dòng. Đúng như tài liệu: đổi tên flow là đọc lại nguồn từ đầu, nên với Kafka phải giữ namespace và tên flow cố định. Phần đọc Kafka từ Event Hubs chưa thử vì chưa có namespace (làm ở AD-OPS-06) |
| 3.4 | ForEachBatch sink POST tới URL | A | Đạt | `@dp.foreach_batch_sink` + `@dp.append_flow(target=...)` gửi POST JSON tới một URL thử (httpbin.org) bằng `urllib`, nhận HTTP 200, chạy được cả trên classic lẫn serverless. Cluster đi ra internet được (qua NAT gateway nói ở dòng trial) |
| 3.4 | Policy DS: `spark.master` = `local[1]` / `local[2]` | A | Đạt | Policy thử `allowlist` `local[1]`, `local[2]`, `local[*]`: tạo cluster với `local[4]` bị chặn đúng ("the value needs to be one of…"). Job 2 job cluster chạy lần lượt: `local[1]` → `defaultParallelism = 1`, `local[2]` → `2`, mỗi task khởi động khoảng 2–3 phút. **Phải sửa policy ở 05 §3 bước 6:** job cluster báo "Automated clusters do not support autotermination" nếu policy có `autotermination_minutes` (kể cả kiểu `range` có `defaultValue`) và job đặt `apply_policy_default_values = true`. Cách đã chạy được: không dùng `apply_policy_default_values`, job tự khai đủ các trường `fixed`. Ngoài ra workspace không cho access mode mặc định: cluster phải khai `data_security_mode = SINGLE_USER` |
| 3.4 | Job `for_each_task` + task value | A | Đạt | Job serverless 2 task: `plan` đặt task value `combos` là list 3 dict, `loop` dùng `for_each_task` với `inputs = {{tasks.plan.values.combos}}`, `concurrency = 1`, truyền `{{input.algo}}`, `{{input.n}}` vào tham số notebook. Lặp 3/3 thành công, cả job 51 giây |
| 3.4 | Model Serving (serverless) | A | Đạt | Đăng ký model `pyfunc` thuần Python (tra luật theo vế trái) vào UC `dbw_spike.spike_ldp.spike_rules` từ notebook serverless, tạo endpoint `spike-rules` (`Small`, `scale_to_zero_enabled = true`), gọi `invocations` với `dataframe_records` trả đúng món gợi ý. Lần deploy đạt mất khoảng 20–25 phút; gọi khi endpoint đang chạy mất vài giây (đo qua CLI, gồm cả đăng nhập). Hai lần đầu hỏng vì lỗi đóng gói, không phải do subscription: (1) `pip_requirements` chỉ có `mlflow-skinny` → "conda.yaml does not specify mlflow"; (2) ghim `pandas` mà không ghim `numpy` → model server crash "numpy.dtype size changed". Code thật ở ds/05 phải ghim `mlflow`, `pandas`, `numpy` đúng phiên bản lúc log. Chưa đo thời gian khởi động lại sau khi scale về 0 |
| 3.5 | Azure SQL free offer | A | | |
| 3.5 | Event Hubs Standard + Kafka `:9093` | B | | |
| 3.5 | ADF activity "Databricks Job" | A | | |
| 3.5 | ADF tải ZIP Zenodo (ZipDeflate) | A | | |
| 3.5 | Azure Functions Flex Consumption | A | | |
| 3.6 | Giấy phép Power BI Pro / trial | — | | |
| 3.6 | Workspace `instacart-bi` + chia sẻ | — | | |
| 3.6 | Connector Azure Databricks trong Desktop | — | | |
| 3.6 | Push / streaming semantic model | — | | |
| 3.6 | Auto page refresh trên Pro | — | | |
| 3.6 | Service principal gọi Power BI REST API | — | | |

> **Ghi chú 3.4 (AD-OPS-04, AD-OPS-05):**
> - Account admin: chưa xác nhận hẳn vì `accounts.azuredatabricks.net` cần đăng nhập bằng email. Kết quả nhóm ở trên không phụ thuộc vào việc này.
> - Quyền UC khi chạy trên serverless: notebook serverless đăng ký model báo `PERMISSION_DENIED: User does not have USE CATALOG on Catalog 'dbw_spike'` dù P1 thuộc nhóm admin đang là owner của catalog. Cấp `USE CATALOG` và quyền trên schema **trực tiếp cho user** thì chạy được. Ở G1, quyền cho người và service principal phải cấp rõ qua account group (05 bước 5), không dựa vào nhóm admin workspace.
> - Tài nguyên spike thêm (dọn ở AD-OPS-07): trong `rg-spike` có workspace `dbw-spike` kèm managed RG `rg-spike-dbw-managed`, Access Connector `ac-spike`, storage credential `sc_spike`, warehouse `wh-spike` (đã dừng), cluster `spike-single-node` (đã tắt), nhóm workspace-local `spike-grp`. Thêm từ AD-OPS-05: schema `dbw_spike.spike_ldp`, `dbw_spike.spike_ldp_sl` (kèm volume `raw`, model `spike_rules`), pipeline `spike_ldp_classic`, `spike_ldp_serverless`, job `spike_for_each`, `spike_local_n`, policy `spike-ds-local-n`, endpoint `spike-rules` (scale về 0 khi không gọi). **Ở cấp account** có nhóm `spike-acct-grp`: không mất khi xóa workspace, phải xóa riêng.

## 3. Quyết định G0

| Quyết định | Chốt | Lý do (phép thử) |
|---|---|---|
| Region Sub A | | 3.1, 3.5 |
| Region Sub B (khác Sub A) | | 3.1, 3.5 |
| Region sandbox Sub C/D | | 3.1, metastore |
| Cách CI đăng nhập | UAMI + OIDC / chỉ check tĩnh | 3.3 |
| Unity Catalog | External location / managed storage | 3.4 |
| Lớp serving Power BI | Serverless warehouse / Azure SQL / `DeltaLake.Table` | 3.4, 3.6 |
| Compute | Single-node DS3_v2 / máy khác 4 vCPU | 3.2, 3.4 |
| Pipeline DE | Đề xuất: LDP Advanced classic single-node (đạt, đúng mặc định tài liệu); serverless pipeline (đạt, khởi động nhanh hơn nhiều) làm dự phòng khi quota vCPU đang bận. Chốt ở AD-OPS-07 | 3.4 |
| Nơi chạy job DS | Sub C (cùng region) / Sub A ngoài giờ batch | 3.1, 3.4 |
| API model | Đề xuất: Model Serving + Function (Model Serving đạt; phần Function kiểm ở AD-OPS-06). Chốt ở AD-OPS-07 | 3.4, 3.5 |

- [ ] Đã xóa `rg-spike` và workspace thử.
