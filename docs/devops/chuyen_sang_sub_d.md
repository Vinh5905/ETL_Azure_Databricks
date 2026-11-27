# Runbook: chuyển hạ tầng từ Sub A sang Sub D khi sắp hết credit

**Dùng khi:** budget alert của Sub A báo 75% hoặc 90% (`budget-sub-a`, stack `10-core`), hoặc Sub A đã bị vô hiệu vì hết credit.
**Người chạy chính:** P1. Chủ Sub B (P3), chủ Sub C (P4) và chủ Sub D làm các bước có ghi tên.
**Thời gian ước tính:** 1–2 ngày, nếu Sub D có `eastasia` và Sub A còn chạy lúc bắt đầu. Phần lớn thời gian là chờ apply và chạy lại pipeline.

> **Nguyên tắc quan trọng nhất: chuyển khi Sub A còn sống.**
> - Khi Sub A còn chạy, SP CI cũ còn đăng nhập được, nên vẫn dọn được Unity Catalog và sao được dữ liệu.
> - Khi Sub A đã tắt thì mất cả hai khả năng này. Bước 4 và mục 6 sẽ khó hơn nhiều.
> - Theo tài liệu Microsoft về spending limit, subscription hết credit sẽ bị vô hiệu và tài nguyên bị dừng. Nhóm **chưa kiểm** cụ thể gói Azure for Students còn cho đọc dữ liệu tới đâu, nên coi như mất quyền truy cập.

## 1. Cái gì gắn với Sub A

| Thành phần | Ở đâu | Ảnh hưởng khi Sub A tắt |
|---|---|---|
| Storage `tfstate` và UAMI `id-github-ci` (`bootstrap`) | Sub A | CI không đăng nhập được Azure. Không stack nào đọc được state, kể cả stack có tài nguyên ở Sub B, Sub C |
| `10-core`: lake, Key Vault, workspace, Azure SQL `retail_db`, ADF, budget | Sub A | Mất hết. Dữ liệu lake và `retail_db` mất theo |
| `30-databricks`: catalog `instacart`, `cred-instacart-lake`, các `el-*` | Metastore `metastore_azure_eastasia`, **cấp account, dùng chung** | **Không mất.** Các đối tượng vẫn nằm trong metastore, trỏ về lake đã chết, và chặn tên khi tạo lại cùng region (mục 4) |
| `40-orchestration`: linked service, dataset, pipeline ADF | Sub A | Mất hết |
| `20-stream`: Event Hubs | Sub B | Tài nguyên còn, nhưng state và secret Key Vault nằm ở Sub A |
| `90-sandbox`: workspace sandbox của P4 | Sub C | Tài nguyên còn, nhưng state nằm ở Sub A |
| Bằng chứng: link run, pipeline, MLflow trong `docs/de`, `docs/ds` | Workspace ở Sub A | Link chết hết. Chỉ ảnh chụp lưu trong repo là còn |

**Không phải đổi khi chuyển:**
- Object ID và UPN của thành viên: Sub D cùng tenant trường.
- Tên tài nguyên có hậu tố ngẫu nhiên (lake, Key Vault, Event Hubs, storage state): state mới sinh hậu tố mới.
- Code `src/`, `bundles/`: bundle đọc host từ `DATABRICKS_HOST`, catalog từ biến `catalog`.

## 2. Chuẩn bị sẵn (làm ngay, không tốn credit)

1. **Chủ Sub D**
   - Tạo Sub D bằng **email trường**, để cùng tenant với Sub A và dùng chung metastore.
   - Báo danh sách region được phép lên #131.
   - Gán cho P1 quyền **Owner** trên Sub D. Cần quyền này vì `bootstrap` tạo role assignment.
2. **Cả nhóm:** chụp ảnh bằng chứng cho các link workspace đang có trong `docs/`, lưu vào `docs/*/evidence/`. Số liệu đối soát đã ghi trong tài liệu thì giữ nguyên.
3. **P1:** theo dõi số chi phí mỗi tuần ở [chi_phi_quota.md](chi_phi_quota.md). Từ đó ước lượng ngày chạm 90% để lên lịch chuyển.

## 3. Khi budget báo 75–90% (Sub A còn chạy)

1. **Đóng băng `main`:** chỉ merge các PR phục vụ việc chuyển.
2. **Tắt stream:** chạy `stream-toggle` với `down`. Lịch đêm cũng tự tắt lúc 00:00. Sau đó kiểm Sub B không còn `rg-instacart-stream`. Khi đó state `20-stream` gần như trống, dựng lại ở Sub D không phải import gì.
3. **Sandbox (nếu đã apply):** P4 destroy `90-sandbox` trên Sub C, rồi apply lại sau khi có state mới. Nếu không, state ở Sub A mất trong khi workspace sandbox vẫn chạy ở Sub C, và apply lại sẽ trùng tên.
4. **Sao dữ liệu cần giữ** về máy hoặc ra ngoài Sub A, theo thứ tự ưu tiên:
   1. Kết quả thực nghiệm DS và file xuất trong `ds-data`.
   2. Kết quả MLflow cần cho báo cáo.
   3. Các bảng gold dùng cho dashboard, nếu muốn so số sau khi chuyển.

   4. **Container `landing`.** Đây là thứ đáng giữ nhất. Landing bất biến, nên nạp lại đúng các file cũ thì pipeline cho ra đúng số đối soát cũ. Không phải tải lại Zenodo, crawl lại Open Food Facts hay sinh lại CDC.

   Bronze không cần sao: dựng lại được từ landing.
5. **Unity Catalog:** kiểm P1 đã có `MANAGE` theo mục 4 bước 1, khi SP CI cũ còn đăng nhập được.
6. **P1 tự sao container `tfstate`** sang Sub D để dự phòng. Không bắt buộc nếu các bước 2–3 đã xong. Agent không đụng file `*.tfstate`.

## 4. Unity Catalog trong metastore dùng chung

Metastore `metastore_azure_eastasia` thuộc cấp account. Nó không mất khi Sub A tắt.
- SP CI cũ là owner của catalog `instacart`, `cred-instacart-lake` và các `el-*`.
- Theo spike 3.4, metastore admin là "System user" và không ai trong nhóm là metastore admin.
- Nếu Sub D ở `eastasia` và cùng tenant, workspace mới dùng lại metastore này. `30-databricks` sẽ báo trùng tên nếu các đối tượng cũ còn đó.

**Cách chính (khuyên dùng): cấp `MANAGE` cho P1 khi Sub A còn chạy, xóa sau khi chuyển.**

Theo tài liệu Databricks, người có `MANAGE` xóa được đối tượng như owner. MANAGE đặt ở catalog thì áp xuống mọi schema, bảng, volume bên trong. Cách này giữ prod chạy tới khi hết credit. SP CI vẫn là owner, nên CI vẫn sửa được grant.

1. **Khi Sub A còn chạy:**
   - Biến `uc_managers` ở `infra/envs/30-databricks.tfvars` cấp `MANAGE` cho P1 trên catalog, credential và từng external location (PR #256).
   - Sau khi `tf-apply` xong, kiểm (chỉ đọc API, không cần compute):

     ```bash
     databricks grants get-effective catalog instacart --profile <profile Sub A>
     databricks grants get-effective storage-credential cred-instacart-lake --profile <profile Sub A>
     databricks grants get-effective external-location el-landing --profile <profile Sub A>
     ```

     Kết quả phải có `MANAGE` cho tài khoản của P1.
2. **Ở Sub D,** sau khi `10-core` tạo workspace và **trước khi** apply `30-databricks`:
   - Đăng nhập CLI vào workspace mới, rồi kiểm workspace dùng metastore nào:

     ```bash
     databricks metastores current --profile <profile Sub D>
     ```

   - Nếu là `metastore_azure_eastasia`, P1 xóa theo thứ tự catalog → external location → credential:

     ```bash
     databricks catalogs delete instacart --force --profile <profile Sub D>
     for c in landing bronze silver gold checkpoints mlflow ds-data; do databricks external-locations delete "el-$c" --force --profile <profile Sub D>; done
     databricks storage-credentials delete cred-instacart-lake --force --profile <profile Sub D>
     ```

   - Sau đó mới cho CI apply `30-databricks`.
3. **Chưa kiểm:**
   - Xóa catalog có vướng các bảng do pipeline tạo (owner SP CI) không.
   - Xóa có cần truy cập lake cũ (đã chết) không.
   - Quyền xóa storage credential: chưa đọc trang tài liệu riêng.

   Lỗi ở bước nào thì chuyển sang cách B hoặc C dưới đây.

**Cách dự phòng.** Ghi lựa chọn vào #131.

| Cách | Khi nào | Phải làm | Chưa kiểm |
|---|---|---|---|
| **A. Cho CI xóa ngay** | Sub A còn chạy, muốn chắc chắn trước khi chuyển | Một PR vào `30-databricks` cho CI xóa catalog, external location, credential. Catalog còn bảng thì phải đặt `force_destroy = true` trước. Prod ngừng ngay, chỉ làm khi đã sao dữ liệu xong | Thứ tự xóa của provider và thời gian chạy |
| **B. Sub D ở region khác `eastasia`, và khác region của Sub B** | Không xóa được đối tượng cũ, hoặc Sub D không có `eastasia` | Đổi `location` trong tfvars. Workspace mới nhận metastore của region đó, không trùng tên | Databricks có tự tạo và gán metastore cho region mới không (spike 3.4 mới kiểm `eastasia`) |
| **C. Đổi tên catalog** | Không xóa được đối tượng cũ và vẫn phải ở `eastasia` | Đổi tên ở `30-databricks`, biến `catalog` của bundle, các file `src/` và `powerbi/` đang ghi cứng `instacart.`, rồi đổi tên credential và external location | Số chỗ ghi cứng tên catalog ngoài code (tài liệu, Power BI) |

## 5. Dựng lại trên Sub D

### 5.1 Bootstrap (P1, từ máy, như [02 bước 6](02_bootstrap_tfstate.md))

```bash
az login
az account set --subscription "<Sub D>"
cd infra/bootstrap
# bootstrap.auto.tfvars: đổi subscription_id_a sang Sub D (file không commit)
# versions.tf: tạm bỏ khối backend "azurerm" {} để apply với local state
terraform init -reconfigure
terraform apply
terraform output
```

- Nếu nhờ agent chạy từ máy P1 thì cần `ALLOW_TF_APPLY=1` trong `.claude/settings.local.json`. Hoặc P1 tự gõ lệnh trong terminal.
- Sau khi apply, trả lại khối backend và chuyển state lên blob như 02 bước 6 (`-migrate-state`), dùng tên storage mới.
- **Chủ Sub B (P3)** gán Contributor trên Sub B cho `ci_principal_id` mới, theo lệnh ghi trong [infra/bootstrap/rbac.tf](../../infra/bootstrap/rbac.tf). Gán cho SP cũ thì bỏ.

### 5.2 Sửa repo (một PR)

| File | Sửa |
|---|---|
| [infra/backend.hcl](../../infra/backend.hcl) | `storage_account_name` theo output `tfstate_account` mới |
| [infra/envs/10-core.tfvars](../../infra/envs/10-core.tfvars) | `tfstate_account` theo tên mới. Đổi `location` nếu đi cách B |
| `infra/envs/90-sandbox-p4.tfvars` | `location_c` nếu đổi region |
| [spike_results.md](spike_results.md), [00_tong_quan.md](00_tong_quan.md) | Ghi Sub D đã thành subscription chính, kèm ngày chuyển |

### 5.3 GitHub Secrets và Variables (P1)

```bash
gh secret set AZURE_SUBSCRIPTION_A --body "<Sub D>"
gh secret set AZURE_CLIENT_ID      --body "<ci_client_id mới>"
gh secret set TFSTATE_ACCOUNT      --body "<tfstate_account mới>"
```

- `vars.DATABRICKS_HOST` chỉ đổi sau khi `10-core` đã tạo workspace mới (5.4 bước 2).
- Giữ nguyên: `AZURE_TENANT_ID`, `AZURE_SUBSCRIPTION_B`, `TF_VAR_MEMBER_*`, `TF_VAR_DATABRICKS_APP_OBJECT_ID`.

### 5.4 Apply theo thứ tự (qua CI)

1. Merge PR ở 5.2. `tf-apply` chạy liền `10-core` → `30-databricks` → `40-orchestration`, cần duyệt ở environment `prod`.
   - Riêng workspace mới mất khoảng 10–20 phút.
   - `30-databricks` lấy host từ output của `10-core`, nên không phụ thuộc `vars.DATABRICKS_HOST`.
2. Ghi workspace URL mới vào `vars.DATABRICKS_HOST`.
3. Chạy tay `de-deploy` (workflow_dispatch) để deploy bundle prod. PR 5.2 không đụng `src/**`, `bundles/**` nên không tự kích hoạt workflow này.
4. `20-stream`: chạy `stream-toggle` với `up` khi cần stream. Lệnh này cũng ghi secret Event Hubs vào Key Vault mới.
5. P4 apply lại `90-sandbox` với state mới, theo comment hướng dẫn ở #31.

Sau mỗi stack, chạy plan lại phải ra `No changes`.

## 6. Nạp lại dữ liệu và nghiệm thu lại

1. Nạp landing theo một trong hai cách:
   - **Có bản sao landing (mục 9):** chép nguyên cây thư mục lên container `landing` mới bằng `azcopy copy <bản sao>/landing/* "https://<lake mới>.blob.core.windows.net/landing" --recursive`. Số đối soát sẽ giữ như cũ.
   - **Không có bản sao:** chạy pipeline ADF cho Zenodo, Open Food Facts và CDC `retail_db`.
     - `seed_retail_db` dùng seed cố định nên dữ liệu sinh lại giống cũ. Nhưng LSN của database mới khác, nên tên thư mục `window=` và `snapshot=` sẽ khác.
     - Kiểm lại số đối soát CDC và gold. Nếu lệch thì ghi bộ số mới, không sửa số cũ.
2. Chạy job batch, rồi pipeline stream khi cần. Đối soát theo [06_de_den_dashboard.md](06_de_den_dashboard.md) §6.
3. **Power BI:** đổi server hostname và HTTP path của warehouse trong semantic model, publish lại, rồi kiểm làm mới dữ liệu.
4. **DS:** chép kết quả đã sao ở bước 3.4 vào `ds-data`. Chạy lại thực nghiệm nếu cần số trên cloud mới.
5. Lặp lại checklist các cổng đã qua (G1–G4) ở mức rút gọn. Ghi kết quả vào #131 hoặc một issue mới.

## 7. Dọn sau khi chuyển

- **Chủ Sub B:** bỏ role Contributor của SP CI cũ trên Sub B.
- Xóa federated credential và UAMI cũ nếu Sub A còn truy cập được. Không thì để hết hạn cùng Sub A.
- Cập nhật bảng phân subscription ở [00_tong_quan.md](00_tong_quan.md) §4 và tài liệu tổng quan dự án.

## 8. Chi phí và rủi ro

- Việc dựng lại và chạy lại pipeline dùng credit 100 USD của Sub D. Workspace Premium không có trial, nên DBU tính từ ngày đầu (spike 3.4).
- Sub D là đường lui cuối cùng. Khi đã chuyển sang Sub D thì không còn subscription dự phòng nào khác.
- **Các giả định chưa kiểm:**
  - Gói Students còn cho đọc dữ liệu sau khi bị vô hiệu hay không.
  - Region mới có tự nhận metastore hay không (cách B).
  - Thứ tự xóa đối tượng UC của provider (cách A).

## 9. Bản sao ngày 2026-11-27

Ngày 2026-11-27 Sub A đã dùng khoảng 97/100 USD, mỗi ngày tốn khoảng 8 USD. P1 đã sao các thứ dưới đây về máy. Bản sao **không nằm trong repo** vì chứa ID subscription và dữ liệu lớn. Ai cần thì hỏi P1.

| Phần | Nội dung | Dùng ở |
|---|---|---|
| Lake `landing` | Toàn bộ cây thư mục, khoảng 1,06 GB: Instacart `release=2017`, Open Food Facts, `retail_db` (snapshot, window, recovery) | §6 bước 1, giữ nguyên số đối soát |
| Lake `silver`, `gold` | Bảng Delta, khoảng 225 MB | Đối chiếu số liệu, đọc bằng Spark ở máy khi chưa dựng lại xong |
| Databricks | Định nghĩa và lịch sử chạy của job, pipeline (kèm event log), notebook trong thư mục home, metadata UC (schema, bảng, volume, external location), warehouse, policy | Bằng chứng cho rubric mục 3, đối chiếu cấu hình sau khi dựng lại |
| Azure | Chi phí theo ngày (theo dịch vụ, resource group, tag), budget, danh sách tài nguyên, lịch sử chạy ADF, quota vCPU | Bằng chứng rubric mục 5.4 và AD-OPS-28 |

**Không sao, có chủ ý:**
- Container `bronze` (khoảng 4,9 GB): dựng lại được từ landing.
- Bacpac `retail_db`: cần đọc secret Key Vault. Dữ liệu sinh từ seed cố định và đã có trong landing.
- Secret Key Vault: tạo mới ở Sub D.
- Storage quản lý của workspace (DBFS root): Azure chặn truy cập trực tiếp. Không có MLflow experiment nào cần giữ.

**tfstate:** P1 tự sao container `tfstate` ra ngoài thư mục đồng bộ. Chỉ dùng để tra cứu ID cũ, không nạp lại cho Sub D.
