# 05 · Nền tảng Databricks: stack `30-databricks` + Databricks Asset Bundles (giai đoạn P4 → cổng G1)

**Đầu vào:** `10-core` đã apply (workspace, Access Connector, ADLS, Key Vault); kết quả spike 3.4 (UC có tự gán metastore không, có quyền tạo storage credential không, serverless có dùng được không).
**Đầu ra:**
- Unity Catalog đã có catalog `instacart` và đủ schema theo tầng.
- Quyền đã cấp theo vai trò.
- Có cluster policy tiết kiệm, secret scope và SQL warehouse.
- Có khung `bundles/` để DE deploy job.

**Xong khi:** đạt checklist ở mục 9. Đạt được thì mở **cổng G1: DE bắt đầu trên cloud**.

---

## 1. Ranh giới: Terraform làm gì, DAB làm gì

| Thuộc về **Terraform `30-databricks`** (nền, ít đổi, cần quyền admin) | Thuộc về **DAB `bundles/`** (việc, đổi hằng ngày, do DE/DS sở hữu) |
|---|---|
| Storage credential, external location, volume | Job batch, job streaming, task, dependencies |
| Catalog, schema, grants | Notebook, file `.py`, wheel |
| Nhóm / service principal, quyền workspace | Tham số job theo môi trường (dev/prod) |
| Cluster policy, instance pool | Job cluster (tham chiếu policy theo tên) |
| Secret scope | Lakeflow Declarative Pipeline (nếu dùng) |
| SQL warehouse | Dashboard vận hành AI/BI (tùy chọn) |

Lý do tách: khi DE sửa một transform, họ không cần quyền admin và không phải đi qua `terraform apply`. Ngược lại, khi DAB deploy nhầm thì cũng không xóa được catalog hay quyền.

## 2. Provider và xác thực

```hcl
terraform {
  required_providers {
    databricks = { source = "databricks/databricks", version = "~> 1.60" }  # kiểm tra lại bản mới nhất
    azurerm    = { source = "hashicorp/azurerm",     version = "~> 4.0" }
  }
  backend "azurerm" { key = "databricks.tfstate" }
}

locals { core = data.terraform_remote_state.core.outputs }

provider "databricks" {
  host                        = local.core.databricks_workspace_url
  azure_workspace_resource_id = local.core.databricks_workspace_id
  auth_type                   = "azure-cli"     # local: az login của P1; CI: phiên của azure/login (UAMI)
}
```

> **Account-level** (`accounts.azuredatabricks.net`) chỉ cần đến nếu phải tự tạo/gán metastore hoặc tạo *account group*. Cả hai việc đều cần **account admin**, mà sinh viên thường không có. Thiết kế dưới đây vì vậy chạy hoàn toàn ở **workspace-level**, và đi nhánh dự phòng khi không có quyền.

## 3. Các bước trong stack (làm tuần tự)

### Bước 1 · Xác nhận metastore

**Vì sao:** mọi thứ bên dưới (catalog, grants, external location) đều nằm trong metastore Unity Catalog. Workspace mới trên Azure thường được **tự gán** metastore của region.

```hcl
data "databricks_current_metastore" "this" {}
output "metastore_id" { value = data.databricks_current_metastore.this.id }
```

Nếu data source này lỗi (không có metastore), dừng lại và xử lý theo nhánh dự phòng của spike 3.4.

### Bước 2 · Danh tính: service principal cho CI và các thành viên

**Vì sao:**
- UAMI của GitHub Actions phải "là ai đó" trong workspace thì mới deploy bundle được.
- Job prod chạy bằng service principal (`run_as`), không chạy bằng tài khoản cá nhân. Nhờ vậy job không hỏng khi một thành viên rời nhóm.

```hcl
resource "databricks_service_principal" "ci" {
  # client id của id-github-ci, đọc từ output ci_client_id của state bootstrap
  application_id = sensitive(data.terraform_remote_state.bootstrap.outputs.ci_client_id)
  display_name   = "sp-github-ci"
  force          = true   # UAMI CI đã được tự thêm vào workspace lần chạy CI đầu → nhận vào state
}

# G5: danh tính của Azure Function gợi ý (tạo ở 10-core). Chỉ cần quyền gọi endpoint (CAN_QUERY,
# cấp trong DAB serving_ds.yml); không cấp cluster, không cấp quyền dữ liệu.
resource "databricks_service_principal" "func" {
  application_id             = sensitive(local.core.func_identity_client_id)
  display_name               = "sp-func-recommend"
  allow_cluster_create       = false
  databricks_sql_access      = false
  workspace_access           = false
  force                      = true
}
```

**Nhóm:** Unity Catalog chỉ nhận **account group**. Group tạo ở workspace-level (workspace-local) **không** dùng được trong lệnh GRANT. Có ba phương án, chọn theo kết quả spike:

| Trường hợp | Cách làm |
|---|---|
| Có account admin | Provider `databricks.account` tạo `de_team`, `ds_team`, `bi_readers` và gán vào workspace |
| Workspace bật *automatic identity management* (nhóm Entra tự đồng bộ) | Tạo 3 nhóm Entra (nếu tenant cho phép), rồi tham chiếu theo tên |
| Workspace admin tạo account group từ workspace (chốt G0, spike 3.4 đạt; **bỏ ở AD-OPS-18**, xem dưới) | Tạo `de_team`, `ds_team`, `bi_readers` bằng API SCIM account gọi qua workspace (`/api/2.0/account/scim/v2/Groups`) hoặc UI "Add new group to account and workspace". Không cần account admin. GRANT theo tên nhóm |
| Không có ba điều trên (dự phòng; **đang dùng từ AD-OPS-18**) | GRANT trực tiếp cho **từng user** (email trường) bằng `for_each` trên một map vai trò → danh sách người |

**Chốt ở G0 (spike 3.4):**
- Spike chọn account group do workspace admin tạo. Sub A không có automatic identity management, nên không tham chiếu được nhóm Entra.
- **Đổi ở AD-OPS-18: GRANT theo từng user.** Provider Terraform chỉ tạo account group qua host `accounts.azuredatabricks.net` (cần account admin). Với provider cấp workspace, `databricks_group` tạo nhóm workspace-local, mà nhóm này không dùng được trong GRANT. Tạo nhóm bằng script API thì thành viên nhóm nằm ngoài state, `drift-check` không thấy. Nhóm 4 người nên cấp theo từng user vẫn gọn, và mọi quyền nằm trong Terraform.
- **Cấp quyền rõ cho từng principal** (nhóm thành viên, SP CI, SP của Function), không dựa vào nhóm `admins` của workspace. Lúc spike, notebook serverless của P1 báo `PERMISSION_DENIED` trên catalog dù nhóm `admins` là owner; cấp trực tiếp mới chạy được.

```hcl
variable "member_upns" {    # secret TF_VAR_MEMBER_UPNS: { p1 = "<mssv>@ms.uit.edu.vn", … }, tài khoản đăng nhập Entra
  type      = map(string)
  sensitive = true
}
variable "roles" {          # chỉ chứa key, không chứa email → để trong infra/envs/30-databricks.tfvars
  type = map(list(string))  # { de = ["p1","p2","p3"], ds = ["p1","p4"], bi = ["p4"] }; giảng viên: thêm key "gv"
}
resource "databricks_user" "members" {
  for_each  = toset(flatten(values(var.roles)))   # lặp theo key (không nhạy cảm)
  user_name = var.member_upns[each.key]
  force     = true          # thành viên có role Azure trên workspace nên có thể đã được tự thêm
}
```

> `for_each` không nhận giá trị sensitive, nên các bước sau luôn lặp theo **key** trong `var.roles` rồi tra UPN bằng `var.member_upns[<key>]`.

> **UPN, không phải email.** Databricks trên Azure đăng nhập qua Entra, nên `user_name` phải là UPN (`<mssv>@ms.uit.edu.vn`). AD-OPS-18 lần đầu dùng hộp thư `gm` của `TF_VAR_MEMBER_EMAILS` và tạo ra 4 user không đăng nhập được, cạnh user Entra thật (Source = External). `member_emails` chỉ dùng cho cảnh báo budget ở 10-core.

### Bước 3 · Storage credential và external location

**Vì sao:** để UC đọc/ghi ADLS bằng **Access Connector** (managed identity), không dùng key. Mỗi container một external location, nên có thể cấp quyền đọc/ghi file khác nhau cho từng tầng.

```hcl
resource "databricks_storage_credential" "lake" {
  name = "cred-instacart-lake"
  azure_managed_identity { access_connector_id = local.core.access_connector_id }
}

resource "databricks_external_location" "layer" {
  for_each        = toset(["landing", "bronze", "silver", "gold", "checkpoints", "mlflow", "ds-data"])
  name            = "el-${each.key}"
  url             = "${local.lake_url[each.key]}/"   # lake_url: abfss://<container>@<account>.dfs.core.windows.net
  credential_name = databricks_storage_credential.lake.name
}
```

**Nhánh dự phòng (không có quyền `CREATE STORAGE CREDENTIAL`):**
- Bỏ bước 3.
- Dùng catalog mặc định của workspace (managed storage do Databricks quản lý) và tạo các schema bên trong catalog đó.
- ADF vẫn ghi `landing/` trên ADLS. Auto Loader đọc `landing` bằng cấu hình Spark dùng managed identity của Access Connector, **hoặc** ADF ghi thẳng vào UC Volume qua REST.
- Ghi rõ lựa chọn này trong báo cáo.

### Bước 4 · Catalog, schema, volume

**Vì sao:**
- Catalog `instacart` gom toàn bộ dự án.
- Mỗi tầng medallion là một schema, mỗi schema có `storage_root` riêng trỏ vào container tương ứng. Nhờ vậy trên ADLS dữ liệu bronze/silver/gold nằm tách bạch, và có thể đo dung lượng từng tầng (rubric 3.4).

```hcl
resource "databricks_catalog" "instacart" {
  name         = "instacart"
  storage_root = "${local.lake_url["bronze"]}/_catalog"   # mặc định; schema ghi đè bên dưới
  comment      = "Instacart Market Basket + Open Food Facts + retail_db"
}

locals {
  schemas = {
    landing_meta      = "bronze"       # bảng off_crawl_state; volume raw chiếm cả container landing
    bronze            = "bronze"
    silver            = "silver"
    gold              = "gold"
    ml                = "mlflow"       # model registry, bảng kết quả thực nghiệm (docs/ds)
    ds_data           = "ds-data"      # dữ liệu thực nghiệm DE chuẩn bị cho DS (de/08)
    instacart_sandbox = "silver"       # dữ liệu tổng hợp để thử tải (is_synthetic)
    ops               = "gold"         # benchmark_results, streaming_latency, dq_results
  }
}
resource "databricks_schema" "s" {
  for_each     = local.schemas
  catalog_name = databricks_catalog.instacart.name
  name         = each.key
  storage_root = "${local.lake_url[each.value]}/${each.key}"
}

resource "databricks_volume" "raw" {               # Auto Loader đọc /Volumes/instacart/landing_meta/raw/...
  name             = "raw"
  catalog_name     = databricks_catalog.instacart.name
  schema_name      = databricks_schema.s["landing_meta"].name
  volume_type      = "EXTERNAL"
  storage_location = "${local.lake_url["landing"]}/"
}
resource "databricks_volume" "checkpoints" {        # checkpoint của streaming
  name             = "checkpoints"
  catalog_name     = databricks_catalog.instacart.name
  schema_name      = databricks_schema.s["ops"].name
  volume_type      = "EXTERNAL"
  storage_location = "${local.lake_url["checkpoints"]}/"
}
resource "databricks_volume" "ds_exports" {         # file txt cho Hadoop local (de/08 §5) → blob ds-data/exports/
  name             = "exports"
  catalog_name     = databricks_catalog.instacart.name
  schema_name      = databricks_schema.s["ds_data"].name
  volume_type      = "EXTERNAL"
  storage_location = "${local.lake_url["ds-data"]}/exports"
}
resource "databricks_volume" "ml_inbox" {           # kết quả benchmark local upload lên (ds/03 §7) → blob ds-data/inbox/
  name             = "inbox"
  catalog_name     = databricks_catalog.instacart.name
  schema_name      = databricks_schema.s["ml"].name
  volume_type      = "EXTERNAL"
  storage_location = "${local.lake_url["ds-data"]}/inbox"
}
```

Đường dẫn của hai volume (`ds-data/exports`, `ds-data/inbox`) và `storage_root` của schema `ds_data` (`ds-data/ds_data`) **không chồng nhau**. Unity Catalog từ chối volume ngoài nằm trong vùng lưu trữ của một schema hay bảng.

Unity Catalog cũng từ chối volume ngoài **bao trùm** vùng lưu managed. Vì vậy:
- Volume `raw` trỏ cả container `landing`, nên `storage_root` của `landing_meta` đặt ở `bronze/landing_meta`, không đặt trong `landing`.
- `storage_root` của catalog là `bronze/_catalog`, không phải gốc container `bronze`, để không bao lên `bronze/bronze` và `bronze/landing_meta`.
- Đường dẫn con dựng từ `local.lake_url` cố định, không nối vào `.url` của external location: `.url` trong state là giá trị API trả về, nếu API bỏ `/` cuối thì `storage_root` (ForceNew) đổi và plan đòi thay cả catalog lẫn schema. Vì không còn tham chiếu, catalog, schema, volume khai `depends_on = [databricks_external_location.layer]`.

### Bước 5 · Phân quyền Unity Catalog (đặc quyền tối thiểu)

**Vì sao:** rubric yêu cầu "ràng buộc từng nhóm người chỉ được xem một vài loại dữ liệu nhất định". Phần này là lớp kiểm soát **ở tầng dữ liệu**. Lớp RLS trong Power BI là **ở tầng báo cáo** (tài liệu 06).

| Đối tượng | DE (`de`) | DS (`ds`) | BI viewer (`bi`) | SP CI / job |
|---|---|---|---|---|
| catalog `instacart` | USE CATALOG, CREATE SCHEMA (schema dev `<user>_*`, xem [de/02 §2.2](../de/02_bronze_ldp.md)) | USE CATALOG | USE CATALOG | USE CATALOG, CREATE SCHEMA |
| `landing_meta`, `bronze` | ALL PRIVILEGES | — | — | ALL PRIVILEGES |
| `silver` | ALL PRIVILEGES | SELECT | — | ALL PRIVILEGES |
| `gold` | ALL PRIVILEGES | SELECT | **SELECT** | ALL PRIVILEGES |
| `ml` | SELECT | ALL PRIVILEGES | — | ALL PRIVILEGES |
| `ds_data` ([de/08](../de/08_du_lieu_cho_ds.md)) | ALL PRIVILEGES | SELECT | — | ALL PRIVILEGES |
| `ops` | ALL PRIVILEGES | SELECT | SELECT (bảng vận hành) | ALL PRIVILEGES |
| `instacart_sandbox` | ALL | ALL | — | — |
| volume `raw`, `checkpoints` | READ/WRITE VOLUME | — | — | READ/WRITE VOLUME |
| volume `ds_data.exports` | READ/WRITE VOLUME | READ VOLUME | — | READ/WRITE VOLUME |
| volume `ml.inbox` | — | READ/WRITE VOLUME | — | READ/WRITE VOLUME |

```hcl
resource "databricks_grants" "gold" {
  schema = "${databricks_catalog.instacart.name}.gold"
  dynamic "grant" {
    for_each = toset(var.roles.bi)
    content {
      principal  = var.member_upns[grant.value]
      privileges = ["USE_SCHEMA", "SELECT"]
    }
  }
  grant {
    principal  = databricks_service_principal.ci.application_id
    privileges = ["ALL_PRIVILEGES"]
  }
  # ... de, ds
}
```

> **Một người có nhiều vai** (ví dụ `p1` thuộc `de` và `ds`, `p4` thuộc `ds` và `bi`): không viết một dynamic block cho mỗi vai, vì cùng một principal sẽ xuất hiện hai lần trong một securable, API trả về một mục đã gộp và plan không bao giờ về *No changes*. Dựng `local` dạng `securable → { key người → hợp privilege của mọi vai người đó có }`, rồi mỗi securable chỉ một dynamic `grant` lặp theo key người, `principal = var.member_upns[grant.key]`.

> Code thật ở `infra/stacks/30-databricks/main.tf` (bước 5): bảng trên viết thành `local.role_privileges` (securable → vai → privilege), `local.grants_by_member` gộp theo người và rút `ALL_PRIVILEGES` thành một mục. Có ba nhóm `databricks_grants`: catalog, mỗi schema (`for_each`), mỗi volume (`for_each`). Volume nằm trong schema mà vai đã có `ALL_PRIVILEGES` thì quyền đã kế thừa, khai lại chỉ để bảng quyền đọc được trong code.

> `databricks_grants` là **có thẩm quyền tuyệt đối**: nó ghi đè mọi grant không khai báo. Vì vậy mọi quyền trên một securable phải nằm trong **một** khối duy nhất.

**Bảo mật mức dòng ngay trong UC (tùy chọn, bổ trợ cho Power BI):** có thể đặt row filter trên `gold.department_sales` theo nhóm, ví dụ hàm `is_account_group_member('bi_produce')` chỉ cho xem department `produce`. Hàm SQL được tạo bằng job DE (DAB), vì hàm là "việc" chứ không phải "nền". Terraform chỉ cấp quyền `EXECUTE`.

### Bước 6 · Cluster policy (+ instance pool)

**Vì sao:**
- Quota chỉ khoảng 4 vCPU, nên phải **cưỡng chế** cluster single-node và tự tắt. Không tin vào việc mọi người tự nhớ.
- Policy cố định cấu hình máy và runtime, nên benchmark đọc/ghi (rubric 3.1) lặp lại được.
- Tag `cost_owner` bắt buộc trên cluster giúp chi phí DBU và VM tách được theo người.
  - Không dùng khóa `owner`: workspace đã có Azure tag `owner = p1` (module `naming_tags`). Databricks chép mọi tag của workspace xuống cluster làm tag mặc định, nên tag cluster trùng tên bị đổi tên và policy báo thiếu tag (lỗi `de-deploy` prod của job seed ngày 2026-11-18).
  - VM của cluster vì vậy mang cả `owner = p1` (của workspace) và `cost_owner` (người dùng thật). Lọc chi phí Databricks theo người thì dùng `cost_owner`.

```hcl
resource "databricks_cluster_policy" "single_node" {
  name = "instacart-single-node"
  definition = jsonencode({
    "spark_version"           = { type = "fixed", value = "15.4.x-scala2.12" }   # LTS, chốt ở spike
    "node_type_id"            = { type = "fixed", value = "Standard_D4ds_v4" }   # Sub A không mua được DS3_v2 (spike 3.4)
    "num_workers"             = { type = "fixed", value = 0 }
    "spark_conf.spark.databricks.cluster.profile" = { type = "fixed", value = "singleNode" }
    "spark_conf.spark.master"                     = { type = "fixed", value = "local[*]" }
    "custom_tags.ResourceClass"                   = { type = "fixed", value = "SingleNode" }
    "autotermination_minutes" = { type = "range", minValue = 10, maxValue = 20, defaultValue = 15 }
    "custom_tags.cost_owner"  = { type = "unlimited", isOptional = false }
    "data_security_mode"      = { type = "fixed", value = "SINGLE_USER" }        # bắt buộc với UC
  })
}
resource "databricks_permissions" "policy_use" {
  cluster_policy_id = databricks_cluster_policy.single_node.id
  dynamic "access_control" {
    for_each = toset(flatten(values(var.roles)))
    content {
      user_name        = var.member_upns[access_control.value]
      permission_level = "CAN_USE"
    }
  }
}
```

Chặn tạo cluster tự do: mọi principal do Terraform tạo đặt `allow_cluster_create = false` (bước 2), để ai không phải admin buộc phải dùng policy. Không quản entitlement của nhóm `users`: workspace của dự án từ chối sửa entitlement nhóm `users` và `admins` (lỗi khi apply AD-OPS-21), và nhóm này vốn không có entitlement nào, nên không cấp quyền tạo cluster cho ai.

**Job cluster dùng policy thế nào (kết quả spike 3.4):**
- `autotermination_minutes` có cả `minValue = 10` và `maxValue = 20`. Thiếu `minValue` thì ai tạo cluster qua API hay CLI với giá trị `0` có cluster không bao giờ tự tắt, chiếm hết 4 vCPU. Thử ở AD-OPS-21b ngày 2026-11-18 trên policy tạm:
  - Cluster tương tác với `autotermination_minutes = 0` bị từ chối ("the value needs to be in range [10, 20]").
  - Job one-time không khai trường này vẫn chạy xong. Job cluster mang giá trị `0` và tắt khi job xong, policy không kiểm khoảng giá trị cho job cluster.
- Job cluster **không** đặt `apply_policy_default_values: true`. Policy có `autotermination_minutes`, mà job cluster gặp trường này qua giá trị mặc định thì báo "Automated clusters do not support autotermination". Job cluster tự tắt khi job xong, nên không cần trường này.
- Vì không lấy mặc định từ policy, job cluster phải **tự khai đủ** các trường `fixed`: `spark_version`, `node_type_id`, `num_workers: 0`, `data_security_mode: SINGLE_USER`, `spark_conf` (`spark.databricks.cluster.profile`, `spark.master`), `custom_tags.ResourceClass`. Khai sai thì policy từ chối lúc tạo cluster, nên vẫn không lách được.
- Spike cho thấy workspace không tự chọn access mode: cluster nào (job hay tương tác) cũng phải khai `data_security_mode = SINGLE_USER`.
- Mẫu khai báo ở [bundles/AGENTS.md](../../bundles/AGENTS.md) và [de/06 §2](../de/06_van_hanh_chay_lai.md). `node_type_id` lấy từ biến bundle `${var.node_type_id}` để chỉ sửa một chỗ khi đổi máy.

Pipeline Lakeflow (LDP) dùng cluster riêng nên cần thêm policy loại `dlt` (`instacart-pipeline-single-node`, `num_workers = 0`), code ở [de/02 §2.1](../de/02_bronze_ldp.md).

Mọi pipeline LDP classic phải đặt `pipelines.clusterShutdown.delay: 60s` trong `configuration`. Spike 3.4: pipeline ở chế độ development giữ cluster sống khoảng 2 giờ sau khi update xong, chiếm hết 4 vCPU và làm job khác lỗi `AZURE_QUOTA_EXCEEDED`.

**Policy cho thực nghiệm DS** (`instacart-ds-benchmark`): cùng máy, cùng runtime với `instacart-single-node`, chỉ khác là cho chọn `spark.master` trong `local[1]` / `local[2]` / `local[4]`. Đo speedup theo số core (E5, [ds/04](../ds/04_thuc_nghiem_danh_gia.md)) cần điều này, mà policy chung lại khóa cứng `local[*]`.

```hcl
resource "databricks_cluster_policy" "ds_benchmark" {
  name = "instacart-ds-benchmark"
  definition = jsonencode({
    "spark_version"           = { type = "fixed", value = "15.4.x-scala2.12" }
    "node_type_id"            = { type = "fixed", value = "Standard_D4ds_v4" }  # cố định máy để số đo so được
    "num_workers"             = { type = "fixed", value = 0 }
    "spark_conf.spark.databricks.cluster.profile" = { type = "fixed", value = "singleNode" }
    "spark_conf.spark.master" = { type = "allowlist", values = ["local[1]", "local[2]", "local[4]", "local[*]"],
                                  defaultValue = "local[*]" }
    "custom_tags.ResourceClass" = { type = "fixed", value = "SingleNode" }
    "autotermination_minutes" = { type = "range", minValue = 10, maxValue = 20, defaultValue = 15 }
    "custom_tags.cost_owner"  = { type = "unlimited", isOptional = false }
    "data_security_mode"      = { type = "fixed", value = "SINGLE_USER" }
  })
}
# databricks_permissions: CAN_USE cho var.roles.ds và SP CI (giống policy_use ở trên)
```

**Code thật trong stack** (`infra/stacks/30-databricks/main.tf`, AD-OPS-21) gọn hơn mẫu trên ở mấy chỗ:
- Phần chung của `instacart-single-node` và `instacart-ds-benchmark` nằm trong `local.single_node_policy`; hai policy chỉ thêm `spark.master` riêng. Máy và runtime nằm ở `local.node_type_id`, `local.spark_version`.
- Một `databricks_permissions.policy_use` lặp theo 3 policy: `single_node` và `pipeline` cho mọi thành viên, `ds_benchmark` cho vai `ds`; cả ba thêm SP CI. `user_name` lấy từ `databricks_user.members`, giống grant ở bước 5.
- Cả ba policy cấm `instance_pool_id`. Policy `dlt` cố định thêm `spark.databricks.cluster.profile = singleNode` (spike 3.4 cần profile này), nên pipeline trong bundle phải khai `spark_conf` như mẫu ở de/02 §2.2.

> Instance pool chỉ có lợi khi chạy nhiều job ngắn liên tiếp. Với quota 4 vCPU, VM idle trong pool vẫn **chiếm quota**. Mặc định **không** dùng pool.

### Bước 7 · Secret scope dựa trên Key Vault

**Vì sao:** notebook đọc `dbutils.secrets.get("kv", "eh-listen-connection-string")`. Bí mật nằm ở Key Vault, có thể xoay vòng mà không sửa code, và Databricks tự che giá trị trong log.

```hcl
resource "databricks_secret_scope" "kv" {
  name = "kv"
  keyvault_metadata {
    resource_id = local.core.key_vault_id
    dns_name    = local.core.key_vault_uri
  }
}
resource "databricks_secret_acl" "kv_read" {        # nếu không cấp, chỉ người tạo đọc được
  for_each   = toset(concat(var.roles.de, var.roles.ds, ["ci"]))
  scope      = databricks_secret_scope.kv.name
  principal  = each.value == "ci" ? databricks_service_principal.ci.application_id : var.member_upns[each.value]
  permission = "READ"
}
```

> **Lưu ý đã biết:** API tạo secret scope **dựa trên Key Vault** yêu cầu token Entra của **người dùng**; service principal và managed identity không gọi được. Vì vậy resource này do **P1 apply từ máy local** (auth `azure-cli` bằng tài khoản người). Trong CI thì thêm `lifecycle { ignore_changes = all }` hoặc tách resource ra một thư mục riêng. Ngoài ra, Key Vault phải cấp role **Key Vault Secrets User** cho ứng dụng *AzureDatabricks* (application ID cố định `2ff814a6-3304-4ab8-85cb-cd0e6f879c1d`) thì Databricks mới đọc được bí mật.

**Code thật trong stack** (AD-OPS-22):
- Role Key Vault Secrets User cho ứng dụng AzureDatabricks nằm ở `10-core` (`azurerm_role_assignment.kv_databricks`). Object ID của ứng dụng khác nhau theo tenant nên đi qua biến `databricks_app_object_id` (GitHub Secret `TF_VAR_DATABRICKS_APP_OBJECT_ID`, xem [04 §4](04_github_actions_cicd.md)).
- ACL tách hai resource: `databricks_secret_acl.kv_member` cho vai `de` và `ds` (lấy `user_name` từ `databricks_user.members`), `databricks_secret_acl.kv_ci` cho SP CI.
- ACL của `p1` là MANAGE (người tạo scope), của các thành viên khác là READ.
- Thứ tự làm (một PR gồm cả `10-core` và `30-databricks`):
  1. Đặt GitHub Secret `TF_VAR_DATABRICKS_APP_OBJECT_ID` trước khi PR chạy `tf-plan`. Biến bắt buộc, thiếu thì plan `10-core` lỗi. P1 thêm biến này vào `terraform.auto.tfvars` của `10-core` ở máy.
  2. **Ngay trước khi merge**, P1 tạo scope từ máy (lệnh dưới). Từ lúc này tới lúc merge, không merge PR `infra/**` hay `adf/**` nào khác: state đã có scope mà config trên `main` chưa có, `tf-apply` của PR khác sẽ xóa scope (`ignore_changes` không chặn được destroy).
  3. Chạy lại `tf-plan` của PR, xác nhận `databricks_secret_scope.kv` không còn là `create`, rồi merge. `tf-apply` gán role ở `10-core` trước, rồi tạo ACL và warehouse ở `30-databricks`.

  ```bash
  cd infra/stacks/30-databricks
  terraform init -backend-config=../../backend.hcl
  cp terraform.auto.tfvars.example terraform.auto.tfvars   # rồi điền UPN thật, giống GitHub Secret TF_VAR_MEMBER_UPNS
  ALLOW_TF_APPLY=1 terraform apply -target=databricks_secret_scope.kv -var-file=../../envs/30-databricks.tfvars
  ```

### Bước 8 · SQL warehouse (lớp phục vụ Power BI)

**Vì sao:** Power BI cần một endpoint SQL ổn định (host + HTTP path) để DirectQuery hoặc import từ `gold`. **Serverless** khởi động trong vài giây, tự tắt và **không tính vào quota vCPU** của subscription. Warehouse classic/pro thì chạy VM trong Sub A, cỡ nhỏ nhất đã vượt 4 vCPU.

```hcl
resource "databricks_sql_endpoint" "bi" {
  name                      = "wh-instacart-bi"
  cluster_size              = "2X-Small"
  min_num_clusters          = 1
  max_num_clusters          = 1
  auto_stop_mins            = 2 # 2X-Small khoảng 2,8 USD/giờ, mỗi phút rảnh đều tính tiền
  enable_serverless_compute = true
  warehouse_type            = "PRO"
  tags {
    custom_tags {
      key   = "cost_owner"
      value = "p3"
    }
  }
}
resource "databricks_permissions" "wh_use" {
  sql_endpoint_id = databricks_sql_endpoint.bi.id
  # CAN_USE: bi + de + ds; CAN_MANAGE: P1
}
output "warehouse_http_path" { value = databricks_sql_endpoint.bi.odbc_params[0].path }
```

**Code thật trong stack** (AD-OPS-22): tag `cost_owner = p3` vì P3 lo Power BI. `databricks_permissions.warehouse_use` cho mọi thành viên có vai CAN_USE, riêng `p1` CAN_MANAGE; SP CI tạo warehouse nên provider tự giữ CAN_MANAGE của nó. Workspace còn có sẵn "Serverless Starter Warehouse" (Small) do Databricks tạo, không nằm trong Terraform.

Nếu serverless **không** dùng được thì bỏ resource này và dùng lớp serving dự phòng (tài liệu 06, mục 4).

### Bước 9 · Output cho DAB và Power BI

```hcl
output "catalog"              { value = databricks_catalog.instacart.name }
output "cluster_policy_id"    { value = databricks_cluster_policy.single_node.id }
output "ci_sp_application_id" { value = databricks_service_principal.ci.application_id }  # stack thật thêm sensitive = true
output "warehouse_id"         { value = databricks_sql_endpoint.bi.id }
```

## 4. Khung Databricks Asset Bundles (`bundles/`)

**Vì sao cần khung ngay ở G1:** DE bắt đầu viết job từ G1. Khung bundle chuẩn bảo đảm mọi job dùng đúng policy, đúng catalog, và có target dev/prod tách biệt ngay từ đầu.

```yaml
# bundles/databricks.yml
bundle: { name: instacart }
include: [resources/*.yml]

variables:
  catalog:   { default: instacart }
  policy_id: { lookup: { cluster_policy: instacart-single-node } }   # lấy theo tên, không hardcode ID

targets:
  dev:
    mode: development                 # tiền tố [dev <user>], tạm dừng schedule
    default: true
    workspace: { host: https://<workspace_url> }
    variables: { catalog: instacart }   # có thể đổi sang schema dev_<user> nếu muốn tách dữ liệu
  prod:
    mode: production
    workspace: { host: https://<workspace_url>, root_path: /Workspace/Shared/.bundle/instacart/prod }
    run_as: { service_principal_name: <ci_sp_application_id> }
    permissions:
      - { level: CAN_VIEW, group_name: users }
```

Pipeline LDP (`resources.pipelines`) khai báo ở [de/02 §2.2](../de/02_bronze_ldp.md) và [de/04 §3.1](../de/04_streaming.md); job đầy đủ ở [de/06 §2](../de/06_van_hanh_chay_lai.md).
AD-DE-01 đã bỏ các task Python tạm. Đến AD-DE-08 khai báo pipeline, rồi AD-DE-18 khai báo job
`instacart-batch-medallion` dùng `pipeline_task`; job cluster phụ trợ phải theo `policy_id`.

**Phần DS trong cùng bundle** (G3 → G5):

| File | Tài nguyên DAB | Chi tiết |
|---|---|---|
| `resources/jobs_ds.yml` | Job `ds_prepare` (DE), `ds_benchmark`, `ds_champion` (DS) | [de/08 §6](../de/08_du_lieu_cho_ds.md), [ds/04 §5](../ds/04_thuc_nghiem_danh_gia.md) |
| `resources/mlflow_ds.yml` | `experiments.instacart_ds` (`/Shared/instacart-ds`) | [ds/05 §1](../ds/05_mlflow_serving_bao_cao.md) |
| `resources/serving_ds.yml` | `model_serving_endpoints.basket_rules`, biến `champion_version` | [ds/05 §3.1](../ds/05_mlflow_serving_bao_cao.md) |

Thêm biến vào `databricks.yml`:

```yaml
variables:
  ds_policy_id: { lookup: { cluster_policy: instacart-ds-benchmark } }
  func_sp_application_id: { description: "client id của id-func-recommend (output 10-core), truyền qua --var" }
```

Target `sandbox` (host của workspace Sub C) **chỉ thêm khi Sub C cùng region với Sub A**, vì khi đó hai workspace dùng chung metastore ([03 §5](03_terraform_azure.md)). Target này chỉ deploy job DS.

```yaml
# bundles/resources/jobs_batch.yml ở G0; nội dung job thật bổ sung ở AD-DE-18
resources:
  jobs: {}
```

## 5. Checklist (cổng G1)

- [ ] `terraform plan` của `30-databricks` báo *No changes* sau khi apply.
- [ ] `SHOW SCHEMAS IN instacart` liệt kê đủ 8 schema (thêm `ds_data`); `DESCRIBE EXTERNAL LOCATION el-bronze` trả về đúng URL.
- [ ] P2 (vai `de`) tạo được bảng trong `bronze`. P4 (vai `ds` + `bi`, không thuộc `de`) **không** đọc được `bronze` (lỗi PERMISSION_DENIED) nhưng đọc được `gold`. Không thử bằng P3 vì P3 thuộc `de`.
- [ ] Không chọn được cấu hình cluster nào ngoài policy single-node; cluster tự tắt sau 20 phút. Thử bằng tài khoản **không** phải admin: thành viên có Contributor trên RG core là workspace admin nên bỏ qua được policy.
- [ ] `dbutils.secrets.get("kv","sql-admin-password")` trả về `[REDACTED]` trong notebook.
- [ ] `SELECT 1` chạy được trên `wh-instacart-bi` (hoặc đã chốt lớp serving dự phòng).
- [ ] `databricks bundle deploy -t dev` từ máy P2 tạo được job `[dev p2] instacart-batch-medallion`.

Đạt hết các mục trên thì **mở G1**: P2 chạy ADF → landing → bronze trên cloud, P3 chuẩn bị streaming (cần thêm G2 = `20-stream` + secret `eh-*`).
