# 01 · Chuẩn bị và spike (giai đoạn P0 → cổng G0)

**Đầu vào:** 4 subscription Azure for Students cùng tenant trường; repo GitHub của nhóm.
**Đầu ra:** máy của 4 người đã cài đủ công cụ; file `docs/devops/spike_results.md` ghi kết quả từng phép thử; các quyết định đã chốt (region, cách auth, lớp serving cho Power BI).
**Xong khi:** mọi dòng trong bảng mục 3 có kết quả *Đạt* hoặc đã chọn phương án dự phòng.

> Vì sao spike trước khi viết Terraform: tài khoản Students có nhiều giới hạn mà tài liệu Microsoft không công bố đầy đủ. Các giới hạn này gồm policy region, quota vCPU, quyền trong tenant trường và tính năng serverless. Viết Terraform xong mới phát hiện thì phải sửa lại cả kiến trúc.

---

## 1. Cài công cụ (cả 4 người)

| Công cụ | Phiên bản gợi ý | Dùng để | Cài trên macOS |
|---|---|---|---|
| Azure CLI | mới nhất | Đăng nhập, kiểm tra policy/quota, backend Terraform | `brew install azure-cli` |
| Terraform | `~> 1.9` (pin trong `required_version`) | IaC | `brew install hashicorp/tap/terraform` |
| TFLint + ruleset azurerm | mới nhất | Lint Terraform | Homebrew core đã bỏ `tflint`. Tải từ GitHub release: `gh release download -R terraform-linters/tflint -p 'tflint_darwin_arm64.zip' -p checksums.txt`, kiểm `shasum -a 256 -c`, giải nén vào `~/.local/bin`. Sau đó chạy `tflint --init` |
| Trivy (hoặc Checkov) | mới nhất | Quét cấu hình sai về bảo mật | `brew install trivy` |
| Databricks CLI (bản mới, có `bundle`) | `>= 0.230` | Databricks Asset Bundles, kiểm tra workspace | `brew tap databricks/tap && brew install databricks` |
| GitHub CLI | mới nhất | PR, secret, environment | `brew install gh` |
| pre-commit | mới nhất | Chạy fmt/lint trước khi commit | `uv tool install pre-commit --python 3.11` rồi `pre-commit install` trong repo. Bản `brew install pre-commit` chạy trên Python 3.15 của Homebrew và có thể lỗi `pyexpat` khi tạo virtualenv |
| Python 3.11 + uv | 3.11 (khớp Databricks Runtime) | Code PySpark, test | `brew install uv && uv python install 3.11` |
| Docker Desktop | mới nhất; máy chạy benchmark DS cấp cho Docker ≥ 8 CPU, ≥ 16 GB RAM | Spark/Kafka local; cụm Hadoop HDFS + YARN cho MapReduce ([ds/03](../ds/03_hadoop_mapreduce_local.md)) | — |
| AzCopy | 10.x | Tải file thực nghiệm từ container `ds`, upload kết quả benchmark (đăng nhập bằng `az login`) | `brew install azcopy` |
| Power BI Desktop | mới nhất | Làm report (chỉ chạy Windows: máy Windows của P4 hoặc VM) | Microsoft Store |
| Infracost | mới nhất | Ước tính chi phí trên PR (tùy chọn) | `brew install infracost` |

Một người chạy, những người còn lại kiểm tra lại trên máy mình:

```bash
az version && terraform version && tflint --version && databricks --version && gh --version && uv --version
```

## 2. Thông tin cần thu thập (P1 tổng hợp vào `spike_results.md`)

| Thông tin | Lệnh | Ghi chú |
|---|---|---|
| Tenant ID | `az account show --query tenantId -o tsv` | Phải giống nhau cho cả 4 subscription (đã xác nhận: cùng tenant) |
| Subscription ID của Sub A–D | `az account list -o table` | Gán Sub A/B/C/D cho từng người theo mục 4 của tài liệu 00 |
| Object ID của từng thành viên | `az ad signed-in-user show --query id -o tsv` | Dùng cho RBAC trong Terraform (`members.auto.tfvars`) |
| Region được phép | Xem lệnh ở mục 3.1 | Mỗi tài khoản Students có danh sách riêng |

> **Không** commit Subscription ID hay Object ID vào repo public. Lưu chúng trong `*.auto.tfvars` đã có trong `.gitignore`, và trong GitHub Variables.

## 3. Các phép thử spike

Mỗi phép thử có: lệnh/thao tác, kết quả mong đợi, phương án dự phòng nếu không đạt. Tài nguyên tạo tay khi spike sẽ **xóa hết** trước khi chạy Terraform.

### 3.1 Region được phép (làm trên cả 4 subscription)

```bash
az policy assignment list --query "[?contains(displayName,'location') || contains(displayName,'region')].{name:displayName, params:parameters}" -o jsonc
```

- **Mong đợi:** có một danh sách vài region. Lấy **giao** của danh sách đó với các region hỗ trợ Databricks, Event Hubs và Azure SQL free offer.
- **Chốt:** region chính cho Sub A, và một region **khác** cho Sub B để làm thí nghiệm độ trễ (rubric 3.3).
- **Dự phòng:** nếu Sub B không có region khác Sub A, đặt simulator ở region khác (ví dụ chạy trên máy local) và đo độ trễ client → Event Hubs.

### 3.2 Quota vCPU

```bash
az vm list-usage --location <region> -o table | grep -Ei "total regional|DSv2|DSv3|Ddsv5|EDv4"
```

- **Mong đợi:** biết chính xác tổng vCPU của region và vCPU theo họ máy. Theo diễn đàn, con số thường gặp là 4. Tài khoản Students **không xin tăng quota được**.
- **Hệ quả cho thiết kế:**
  - Một cluster single-node `Standard_DS3_v2` (4 vCPU) chiếm hết quota.
  - Không chạy song song batch và streaming trên Sub A, nên streaming phải chạy theo kiểu lô (`availableNow`) xen kẽ với batch.
  - SQL warehouse **classic** (máy chạy trong subscription của mình) không khởi động được.

### 3.3 Quyền trong tenant trường

| Thử | Lệnh / thao tác | Nếu bị chặn |
|---|---|---|
| Tạo user-assigned managed identity | `az identity create -g rg-spike -n id-spike` | Không có phương án dự phòng tốt; báo giảng viên. Nếu là Owner của subscription thì thường làm được |
| Thêm federated credential cho UAMI | `az identity federated-credential create --identity-name id-spike -g rg-spike --name gh-main --issuer https://token.actions.githubusercontent.com --subject repo:<org>/<repo>:ref:refs/heads/main --audiences api://AzureADTokenExchange` | CI chỉ chạy kiểm tra tĩnh; `apply` chạy từ máy P1 |
| Gán role chéo subscription (chủ Sub B gán Contributor cho UAMI của Sub A) | `az role assignment create --assignee <uami-principal-id> --role Contributor --scope /subscriptions/<subB>` | CI chỉ apply Sub A; Sub B do P3 apply từ máy |
| Tạo App Registration (chỉ để biết) | `az ad app create --display-name spike-app` | Không ảnh hưởng, vì thiết kế dùng UAMI |
| Tạo nhóm Entra ID | `az ad group create --display-name spike-grp --mail-nickname spikegrp` | Quản lý nhóm ở mức Databricks (account/workspace group) thay cho nhóm Entra |

### 3.4 Databricks và Unity Catalog

Tạo tay **một** workspace Premium ở region đã chốt (Portal, mất khoảng 5 phút), rồi kiểm tra các mục sau:

| Kiểm tra | Cách làm | Kết quả mong đợi | Dự phòng |
|---|---|---|---|
| Workspace tạo được (policy + quota) | Portal hoặc `az databricks workspace create` | Thành công | Đổi region; dùng Sub khác |
| UC tự gán metastore | Workspace → Catalog: thấy catalog trùng tên workspace | Có metastore của region | Nếu không có: cần account admin tạo metastore; nếu không ai có quyền thì chỉ dùng `hive_metastore` (mất UC) |
| Ai là account admin / metastore admin | `databricks current-user me`, vào thử accounts.azuredatabricks.net | Biết được P1 có quyền gì | — |
| Tạo storage credential từ Access Connector | Catalog → External data → Credentials → Create | Thành công | Dùng managed storage của catalog workspace, không có external location (ghi vào tài liệu 05) |
| Serverless SQL warehouse | SQL Warehouses → Create → kiểu Serverless | Tạo và chạy được `SELECT 1` | Lớp serving dự phòng (tài liệu 06, mục 4) |
| Serverless jobs / notebook | Chạy notebook với compute "Serverless" | Chạy được | Không sao; dùng job cluster single-node |
| Cluster single-node DS3_v2 | Tạo cluster, Runtime 15.4 LTS | Khởi động trong khoảng 5 phút | Thử máy 4 vCPU khác mà region có bán **và** họ máy có quota (`az vm list-skus` + `az vm list-usage`), ví dụ `Standard_D4ds_v4` / `Standard_D4s_v4`. Sub A không bán `DS3_v2` ở eastasia, còn họ v5 có quota 0 |
| Thời hạn trial Premium (14 ngày, miễn DBU) | Xem banner trên workspace | Ghi lại ngày hết hạn | Lên kế hoạch DBU sau trial vào budget |
| Pipeline Lakeflow edition **Advanced** chạy single-node | Tạo pipeline thử, `num_workers: 0`, 1 bảng Auto Loader + 1 `AUTO CDC` + 1 expectation | Update xong, trong quota 4 vCPU | Edition Pro (bỏ expectations, dùng bảng quarantine thủ công); hoặc serverless pipeline nếu có |
| Serverless pipeline | Tạo pipeline với *Serverless* | Chạy được | Classic như trên; MV tính lại toàn bộ thay vì tăng dần |
| Kafka (Event Hubs) trong LDP + đổi tên flow | `append_flow` đọc Kafka, sau đó đổi tên flow và chạy lại | Bảng giữ dữ liệu, flow mới đọc từ đầu | Giữ namespace cố định suốt giai đoạn demo ([de/04 §3.2](../de/04_streaming.md)) |
| ForEachBatch sink của LDP | Sink POST tới một URL thử | Chạy được | Power BI DirectQuery + auto page refresh ([de/04 §4.5](../de/04_streaming.md)) |
| Cluster đặt `spark.master = local[1]` / `local[2]` qua policy DS | Tạo job cluster với policy thử có `allowlist` cho `spark.master`; chạy `sc.defaultParallelism` | Trả về đúng 1 / 2 | Đo speedup chỉ ở local Docker (E6); E5 chỉ còn n = 4 ([ds/04](../ds/04_thuc_nghiem_danh_gia.md)) |
| Job `for_each_task` đọc task value | Job 2 task: task 1 đặt task value là list, task 2 lặp `concurrency: 1` | Lặp đủ phần tử | Task `plan` sinh nhiều job run bằng SDK, mỗi tổ hợp một run |
| **Model Serving** (serverless) | Đăng ký một model `pyfunc` nhỏ vào UC, tạo endpoint `scale_to_zero`, gọi `invocations` | Trả kết quả | Phương án B: Azure Function tự tra luật ([ds/05 §3.3](../ds/05_mlflow_serving_bao_cao.md)) |

### 3.5 Dịch vụ còn lại

| Kiểm tra | Cách làm | Dự phòng |
|---|---|---|
| Azure SQL free offer (100k vCore-giây/tháng) | Portal → SQL Database → "Apply offer"; ghi lại region | Dùng Basic DTU (khoảng 5 USD/tháng) |
| **CDC** bật được trên DB free offer | `EXEC sys.sp_cdc_enable_db` rồi `sp_cdc_enable_table`, sửa 1 dòng, đọc `cdc.fn_cdc_get_all_changes_…` | Watermark `updated_at` + soft delete ([de/01 §3.7](../de/01_ingest_landing.md)) |
| Event Hubs Standard + Kafka endpoint | Tạo namespace Standard, dùng `kcat` gửi 1 message vào `:9093` | Basic không có Kafka → bắt buộc dùng Standard |
| ADF có activity "Databricks Job" | ADF Studio → Activities → Databricks | Dùng activity Notebook để gọi notebook điều phối job |
| ADF tải ZIP từ Zenodo qua HTTP | Copy activity: HTTP → ADLS, kiểm tra xem có dùng ZipDeflate được không | Tải về rồi giải nén trong Databricks (`unzip` trên driver) |
| Azure Functions **Flex Consumption** (G5) | Portal → Function App → Flex Consumption, Python 3.11, ở region Sub A | Gói Consumption cũ (`Y1`); không được nữa thì bỏ Function, gọi endpoint trực tiếp khi demo |

### 3.6 Power BI (P4)

| Kiểm tra | Cách làm | Dự phòng |
|---|---|---|
| Có giấy phép Pro (qua M365 trường hoặc trial 60 ngày) | app.powerbi.com → Account → View account | Cả nhóm cùng bật trial vào đúng giai đoạn P7 để đủ thời gian demo |
| Tạo workspace và chia sẻ cho 3 người còn lại | Tạo workspace `instacart-bi`, thêm thành viên | — |
| Connector Azure Databricks trong Desktop | Get data → Azure Databricks → nhập host + HTTP path của warehouse đã spike | Lớp serving dự phòng |
| Tạo streaming / push semantic model | Workspace → New → Streaming semantic model → API | Chỉ còn DirectQuery + auto page refresh |
| Auto page refresh trong workspace Pro | Bật APR trên report DirectQuery, publish | Giới hạn tối thiểu khoảng 30 phút trên shared capacity, nên tile real-time phải dùng push model |
| Tenant có cho service principal gọi API Power BI | Hỏi admin, hoặc thử REST API bằng token của UAMI | Publish tay theo checklist (tài liệu 06) |

## 4. Quyết định cần chốt ở G0

| Quyết định | Lựa chọn | Phụ thuộc vào phép thử |
|---|---|---|
| Region Sub A / Sub B | `<…>` / `<…>` | 3.1, 3.5 |
| Cách CI đăng nhập | UAMI + OIDC (mặc định) / chỉ check tĩnh | 3.3 |
| Unity Catalog | External location (mặc định) / managed storage | 3.4 |
| Lớp serving cho Power BI | Serverless warehouse (mặc định) / gold → Azure SQL / `DeltaLake.Table` | 3.4, 3.6 |
| Compute | Single-node DS3_v2 (mặc định) / máy khác cùng 4 vCPU | 3.2, 3.4 |
| Pipeline DE | LDP Advanced classic single-node (mặc định) / LDP serverless / LDP Pro | 3.4 |
| Nguồn SQL | CDC theo LSN (mặc định) / watermark `updated_at` + soft delete | 3.5 |
| Nơi chạy job DS | Sub C sandbox nếu cùng region với Sub A (mặc định) / Sub A ngoài giờ batch | 3.1, 3.4 |
| API model (G5) | Model Serving + Function (mặc định) / Function tự tra luật | 3.4, 3.5 |

Sau khi chốt: **xóa resource group spike** bằng lệnh `az group delete -n rg-spike` và xóa workspace thử, để Terraform dựng lại từ đầu.
