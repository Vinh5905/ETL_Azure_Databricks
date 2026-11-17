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
| 3.4 | Ngày hết trial Premium | A | 2026-12-01 (tính, chưa xem banner) | Workspace tạo 2026-11-17, trial 14 ngày. Chưa mở UI workspace để xem banner. Sau ngày này DBU bắt đầu tính tiền, cần đưa vào budget |
| 3.4 | Policy DS: `spark.master` = `local[1]` / `local[2]` | A | | |
| 3.4 | Job `for_each_task` + task value | A | | |
| 3.4 | Model Serving (serverless) | A | | |
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

> **Ghi chú 3.4 (AD-OPS-04):**
> - Account admin: chưa xác nhận hẳn vì `accounts.azuredatabricks.net` cần đăng nhập bằng email. Kết quả nhóm ở trên không phụ thuộc vào việc này.
> - Tài nguyên spike thêm (dọn ở AD-OPS-07): trong `rg-spike` có workspace `dbw-spike` kèm managed RG `rg-spike-dbw-managed`, Access Connector `ac-spike`, storage credential `sc_spike`, warehouse `wh-spike` (đã dừng), cluster `spike-single-node` (đã tắt), nhóm workspace-local `spike-grp`. **Ở cấp account** có nhóm `spike-acct-grp`: không mất khi xóa workspace, phải xóa riêng.

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
| Nơi chạy job DS | Sub C (cùng region) / Sub A ngoài giờ batch | 3.1, 3.4 |
| API model | Model Serving + Function / Function tự tra luật | 3.4, 3.5 |

- [ ] Đã xóa `rg-spike` và workspace thử.
