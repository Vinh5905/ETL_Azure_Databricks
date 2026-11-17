# 03 · Dựng hạ tầng Azure bằng Terraform (giai đoạn P2 và P5)

**Đầu vào:** bootstrap đã xong ([02](02_bootstrap_tfstate.md)); kết quả spike ([01](01_chuan_bi_va_spike.md)).
**Đầu ra:** stack `10-core` (Sub A), `20-stream` (Sub B), `90-sandbox` (Sub C/D) đã apply; `50-serving` (Sub A, tùy chọn ở G5). Các output (URL workspace, tên storage, ID Key Vault) được những stack sau đọc.
**Xong khi:** đạt checklist ở cuối mỗi stack. Từ thời điểm đó mọi thay đổi chỉ đi qua PR ([04](04_github_actions_cicd.md)).

> Ở giai đoạn đầu, P1 chạy `plan/apply` của `10-core` từ máy local để gỡ lỗi nhanh. Khi CI xong (P3 của tài liệu 00), apply chuyển hẳn sang GitHub Actions.

---

## 1. Bố cục code

![Bố cục Terraform: stack, subscription, state](diagrams/02_bo_cuc_terraform.png)

<sub>File gốc: [diagrams/devops_diagrams.drawio](diagrams/devops_diagrams.drawio), trang 2.</sub>

```
infra/
  backend.hcl
  modules/
    naming_tags/        # tên + tag chuẩn, dùng ở mọi stack
    lakehouse_storage/  # wrapper AVM storage: ADLS HNS + container + lifecycle
    dbx_workspace/      # wrapper AVM databricks + access connector (dùng lại cho sandbox)
  stacks/
    10-core/            # Sub A — nền tảng chính
    20-stream/          # Sub B — Event Hubs (bật/tắt theo buổi)
    30-databricks/      # bên trong workspace (tài liệu 05)
    40-orchestration/   # ADF linked service, dataset, pipeline, trigger (tài liệu 06)
    50-serving/         # Sub A — Azure Function làm API gợi ý (tùy chọn, G5, docs/ds/05)
    90-sandbox/         # Sub C/D — workspace #2 cho DS
  envs/
    10-core.tfvars  20-stream.tfvars  30-databricks.tfvars  40-orchestration.tfvars  50-serving.tfvars  90-sandbox-p4.tfvars
```

**Vì sao tách stack (mỗi stack một state):**

| Lý do | Ví dụ |
|---|---|
| Khác subscription / khác người giữ quyền | `20-stream` nằm ở Sub B do P3 giữ |
| Khác vòng đời | `20-stream` được `destroy` mỗi đêm; `10-core` gần như không đổi |
| Khác provider, phụ thuộc thứ tự | Provider `databricks` cần `workspace_url`, mà URL này chỉ có sau khi `10-core` apply |
| Giảm phạm vi rủi ro | Lỗi khi sửa pipeline ADF (`40`) không làm `plan` đụng tới workspace (`10`) |

**Cách các stack nói chuyện với nhau:** stack sau đọc output của stack trước qua `terraform_remote_state`.

```hcl
data "terraform_remote_state" "core" {
  backend = "azurerm"
  config = {
    resource_group_name  = "rg-tfstate"
    storage_account_name = var.tfstate_account
    container_name       = "tfstate"
    key                  = "core.tfstate"
    use_azuread_auth     = true
  }
}
# dùng: data.terraform_remote_state.core.outputs.key_vault_id
```

**Module lấy từ Terraform Registry:** ưu tiên **Azure Verified Modules (AVM)**, là module chính thức do Microsoft duy trì.

| Module | Dùng cho | Ghi chú |
|---|---|---|
| `Azure/naming/azurerm` | Sinh tên theo quy ước CAF (`st…`, `kv-…`, `dbw-…`) | Bọc trong `modules/naming_tags` |
| `Azure/avm-res-storage-storageaccount/azurerm` | ADLS Gen2 (HNS) + container + role | |
| `Azure/avm-res-keyvault-vault/azurerm` | Key Vault RBAC mode | |
| `Azure/avm-res-databricks-workspace/azurerm` | Databricks workspace Premium | |
| `Azure/avm-res-sql-server/azurerm` | Azure SQL server + database | Kiểm tra module có hỗ trợ free offer không; nếu không thì dùng `azurerm_mssql_database` |
| `Azure/avm-res-datafactory-factory/azurerm` | Data Factory + managed identity | |
| `Azure/avm-res-eventhub-namespace/azurerm` | Event Hubs namespace + hub + consumer group | |

> **Quy tắc:**
> - Pin `version = "x.y.z"` chính xác cho mọi module, và **kiểm tra số phiên bản cùng tên tham số trong README của module lúc viết code**. Các đoạn code dưới đây chỉ minh họa ý, tên tham số có thể khác theo phiên bản.
> - Nếu module AVM thiếu một tính năng cần dùng thì dùng resource `azurerm_*` thô, không fork module.
> - Mọi module AVM có biến `enable_telemetry`. Đặt bằng `false`.

Phần dùng **resource thô** (không có AVM hoặc AVM quá nặng):
- `azurerm_databricks_access_connector`
- `azurerm_role_assignment`
- `azurerm_storage_management_policy`
- `azurerm_consumption_budget_subscription`
- `azurerm_monitor_action_group`
- `azurerm_key_vault_secret`
- `random_password`

## 2. Module nội bộ `naming_tags`

**Vì sao cần:** 4 người đặt tên và gắn tag thống nhất, để Cost Management lọc chi phí theo `owner` và `stack`.

```hcl
# modules/naming_tags/main.tf
module "naming" {
  source  = "Azure/naming/azurerm"
  version = "0.4.x"                        # pin chính xác khi code
  suffix  = [var.project, var.env]         # vd: st + instacart + prod + random
}
output "tags" {
  value = merge({ project = var.project, env = var.env, owner = var.owner,
                  stack = var.stack, managed_by = "terraform" }, var.extra_tags)
}
output "names" { value = module.naming }
```

---

## 3. Stack `10-core` (Sub A): làm tuần tự từng bước

Mỗi bước là **một PR nhỏ**, `plan` sạch rồi mới làm bước kế tiếp. Nhờ vậy khi lỗi thì biết ngay bước nào gây ra.

### Bước 3.1 · Provider, backend, resource group

**Vì sao:** khung chung cho stack. Provider pin phiên bản, backend trỏ state `core.tfstate`.

```hcl
terraform {
  required_version = "~> 1.11"                   # AVM Key Vault từ 0.11 cần 1.11
  required_providers {
    azurerm = { source = "hashicorp/azurerm", version = "~> 4.0" }
  }
  backend "azurerm" { key = "core.tfstate" }      # phần còn lại lấy từ backend.hcl
}
provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy = false
    }
  }
  subscription_id     = var.subscription_id_a
  storage_use_azuread = true
}
resource "azurerm_resource_group" "core" {
  name     = "rg-instacart-core"
  location = var.location
  tags     = module.nt.tags
}
```

### Bước 3.2 · ADLS Gen2: lakehouse

**Vì sao:** đây là nơi lưu mọi tầng dữ liệu. Bật HNS (hierarchical namespace) để có thư mục thật, ACL và hiệu năng tốt cho Spark/Delta. Mỗi tầng một container để phân quyền theo container trong Unity Catalog.

```hcl
# infra/modules/lakehouse_storage/main.tf (rút gọn): wrapper quanh AVM storage 0.11.0, chỉ dùng provider azapi
module "this" {
  source  = "Azure/avm-res-storage-storageaccount/azurerm"
  version = "0.11.0"                         # cần Terraform >= 1.10
  name             = var.name
  parent_id        = var.resource_group_id   # AVM 0.11 nhận ID của RG, không nhận tên
  location         = var.location
  account_kind     = "StorageV2"
  account_sku_name = "Standard_LRS"          # mặc định của AVM là Standard_ZRS
  is_hns_enabled   = true
  shared_access_key_enabled       = false    # chỉ dùng Entra ID / managed identity
  default_to_oauth_authentication = true
  public_network_access_enabled   = true     # mặc định AVM tắt public network; dự án không dùng private endpoint
  network_rules = { default_action = "Allow", bypass = ["AzureServices"] }
  containers = { for c in var.containers : c => { name = c } }   # 7 container, xem dưới
  storage_management_policy_rule = {         # rubric 3.4: tối ưu lưu trữ
    landing_to_cool = {
      name    = "landing-to-cool"
      enabled = true
      filters = { blob_types = ["blockBlob"], prefix_match = ["landing/"] }
      actions = { base_blob = { tier_to_cool_after_days_since_modification_greater_than = 30 } }
    }
  }
  enable_telemetry = false
  tags             = var.tags
}

# infra/stacks/10-core/main.tf
module "lake" {
  source            = "../../modules/lakehouse_storage"
  name              = module.nt.names.storage_account.name_unique
  resource_group_id = azurerm_resource_group.core.id
  location          = azurerm_resource_group.core.location
  tags              = module.nt.tags
}
```

Lifecycle rule khai báo qua biến `storage_management_policy_rule` của AVM, không dùng `azurerm_storage_management_policy` riêng, để cả storage account chỉ đi qua một provider (`azapi`).

7 container: `landing`, `bronze`, `silver`, `gold`, `checkpoints`, `mlflow`, `ds-data`. Container của DS tên `ds-data`, không phải `ds`, vì Azure bắt tên container dài 3–63 ký tự.

**Kiểm tra:** `az storage fs list --account-name <st> --auth-mode login` thấy đủ 7 container.

Container `ds-data` dành cho phần Data Science ([docs/ds](../ds/00_tong_quan_ds.md)):
- `ds-data/exports/`: DE xuất file txt cho cụm Hadoop local.
- `ds-data/inbox/`: kết quả benchmark từ máy local upload lên.
- `ds-data/serving/`: luật của model champion, dùng cho phương án B của API.

Các thành viên tải/upload bằng AzCopy với quyền *Storage Blob Data Contributor* đã cấp ở bước 3.8, không cần SAS hay key.

### Bước 3.3 · Key Vault

**Vì sao:** chứa bí mật như mật khẩu SQL, SAS của Event Hubs và push URL của Power BI. Databricks đọc bí mật qua *secret scope* dựa trên Key Vault, nên trong code không có bí mật nào. Dùng **RBAC mode** để phân quyền bằng role Azure, không dùng access policy.

```hcl
data "azurerm_client_config" "current" {}

module "kv" {
  source  = "Azure/avm-res-keyvault-vault/azurerm"
  version = "0.13.1"                         # cần Terraform >= 1.11, azurerm >= 4.81
  name      = module.nt.names.key_vault.name_unique
  parent_id = azurerm_resource_group.core.id # AVM nhận ID của RG, không nhận tên
  location  = azurerm_resource_group.core.location
  tenant_id = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"    # mặc định của AVM là premium
  purge_protection_enabled   = false         # để destroy/dựng lại được khi làm đồ án
  soft_delete_retention_days = 7
  # Mặc định của AVM là chặn mọi IP; dự án không dùng private endpoint nên mở và kiểm bằng RBAC.
  public_network_access_enabled = true
  network_acls = { bypass = "AzureServices", default_action = "Allow" }
  enable_telemetry = false
  tags             = module.nt.tags
}
```

AVM mặc định bật RBAC mode (`legacy_access_policies_enabled = false`), nên không cần khai báo thêm. Key Vault chưa gán role nào ở bước này. Bảng quyền ở bước 3.8 cho P1 và UAMI CI role *Key Vault Secrets Officer*, nhưng bước 3.6 đã ghi secret `sql-admin-password`, nên người chạy apply cần role này trước bước 3.6.

### Bước 3.4 · Access Connector (managed identity cho Unity Catalog)

**Vì sao:** Unity Catalog cần một danh tính để đọc/ghi ADLS **thay cho** người dùng. Không dùng account key hay SAS. Access Connector chính là managed identity đó. Nó được gán role trên storage, sau đó stack `30-databricks` tạo *storage credential* trỏ vào nó.

```hcl
resource "azurerm_databricks_access_connector" "uc" {
  name                = "dbac-instacart"
  resource_group_name = azurerm_resource_group.core.name
  location            = var.location
  identity { type = "SystemAssigned" }
  tags = module.nt.tags
}
resource "azurerm_role_assignment" "uc_lake" {
  scope                = module.lake.resource_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.uc.identity[0].principal_id
  principal_type       = "ServicePrincipal"
}

# Danh tính cho Azure Function gợi ý (G5). Tạo sẵn ở đây (không tốn tiền) để stack 30-databricks
# thêm nó vào workspace trước khi stack 50-serving tồn tại. User-assigned nên có client_id ngay,
# không cần quyền đọc Microsoft Graph để tra application ID.
resource "azurerm_user_assigned_identity" "func" {
  name                = "id-func-recommend"
  resource_group_name = azurerm_resource_group.core.name
  location            = var.location
  tags                = module.nt.tags
}
```

### Bước 3.5 · Databricks workspace (Premium)

**Vì sao Premium:** Unity Catalog, phân quyền bảng/cột, SQL warehouse và secret scope dựa trên Key Vault đều cần Premium. SKU `premium` **không** có trial: DBU tính vào credit ngay từ ngày đầu (spike 3.4). Chỉ SKU `trial` mới có 14 ngày miễn DBU, sau đó phải đổi sang `premium`; nhóm không dùng vì chưa thử đổi SKU tại chỗ.

**Vì sao không bật VNet injection / Secure Cluster Connectivity + NAT:** NAT Gateway và public IP tốn tiền, đồng thời tăng rủi ro gặp lỗi quota trên Students. Đồ án không yêu cầu cô lập mạng.

```hcl
module "dbw" {
  source  = "Azure/avm-res-databricks-workspace/azurerm"
  version = "0.6.0"
  name                        = "dbw-instacart"
  resource_group_name         = azurerm_resource_group.core.name
  location                    = azurerm_resource_group.core.location
  sku                         = "premium"
  managed_resource_group_name = "rg-instacart-dbw-managed"
  custom_parameters = {
    no_public_ip             = false          # khớp kết quả spike: không NAT gateway
    storage_account_sku_name = "Standard_LRS" # DBFS root; mặc định AVM là Standard_GRS
  }
  enable_telemetry = false
  tags             = module.nt.tags
}
output "databricks_workspace_url" { value = module.dbw.databricks_workspace_url }
output "databricks_workspace_id"  { value = module.dbw.resource_id }
```

Lưu ý khi dùng AVM 0.6.0:
- Module tạo workspace bằng `azapi`, cần Terraform `>= 1.11`.
- Output `databricks_workspace_id` của AVM là **ID số** trong Databricks. Provider `databricks` ở `30-databricks` cần **resource ID trên Azure** (`azure_workspace_resource_id`), nên output của stack lấy `module.dbw.resource_id`.
- Không đặt `default_catalog`: spike 3.4 cho thấy metastore `eastasia` tự gán và workspace có sẵn catalog cùng tên.
- Không truyền `access_connector_id`: biến đó chỉ dùng khi bật firewall cho storage mặc định. Access Connector cho UC đã có ở bước 3.4.

**Kiểm tra:** mở `https://<workspace_url>`, thấy catalog mặc định (UC tự gán) nếu spike 3.4 đạt.

> Phần bên trong workspace (catalog, nhóm, policy, warehouse) **không** nằm ở đây mà ở stack `30-databricks` ([05](05_databricks_platform.md)).

### Bước 3.6 · Azure SQL (nguồn `product_prices`, `promotions`)

**Vì sao:** đề bài 08 yêu cầu nhiều nguồn, nhiều định dạng. Bảng giá và khuyến mãi tự sinh nằm trong một CSDL quan hệ thật, để ADF copy tăng dần theo watermark. Dùng **free offer** (serverless GP, tự pause khi hết hạn mức) để gần như không tốn tiền.

```hcl
resource "random_password" "sql" {
  length  = 24
  special = true
}

module "sql" {
  source  = "Azure/avm-res-sql-server/azurerm"
  version = "x.y.z"
  name                         = module.nt.names.mssql_server.name_unique
  resource_group_name          = azurerm_resource_group.core.name
  location                     = var.location
  administrator_login          = "sqladmin"
  administrator_login_password = random_password.sql.result
  databases = { retail = { name = "retail_db", sku_name = "GP_S_Gen5_2" } }
  # Chốt G0: SQL auth (cho seed job và ls_sql_retail) + Entra admin (cho người đăng nhập tay).
  # Không bật azuread_authentication_only. Tên tham số lấy theo README của version AVM đã pin.
  azuread_administrator = {
    login_username = var.sql_entra_admin_login      # tên hiển thị, ví dụ nhóm Entra của nhóm dự án
    object_id      = var.sql_entra_admin_object_id  # qua TF_VAR_*, không commit
  }
  # free offer: kiểm tra module / azurerm_mssql_database có thuộc tính dạng use_free_limit,
  # free_limit_exhaustion_behavior = "AutoPause". Nếu chưa có thì bật free offer bằng az CLI rồi import.
  enable_telemetry = false
  tags = module.nt.tags
}

resource "azurerm_mssql_firewall_rule" "azure" {           # cho ADF / Databricks (dịch vụ Azure)
  name             = "allow-azure"
  server_id        = module.sql.resource_id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}
resource "azurerm_mssql_firewall_rule" "members" {          # IP của 4 thành viên để chạy seed
  for_each = var.member_ips
  name             = "member-${each.key}"
  server_id        = module.sql.resource_id
  start_ip_address = each.value
  end_ip_address   = each.value
}

resource "azurerm_key_vault_secret" "sql_pw" {
  name         = "sql-admin-password"
  value        = random_password.sql.result
  key_vault_id = module.kv.resource_id
}
```

**Vì sao giữ mật khẩu SQL (chốt G0):** spike 3.5 dựng server chỉ Entra ID (`--enable-ad-only-auth`) và chạy được. Nhưng job `seed_retail_db` trên Databricks kết nối bằng `pymssql` + mật khẩu, mà lấy token Entra từ cluster thì cần thêm service principal có secret hoặc service credential của UC (chưa thử). Vì vậy server bật **cả hai**: login `sqladmin` với mật khẩu trong Key Vault cho seed job và `ls_sql_retail`, Entra admin cho thành viên đăng nhập tay (`sqlcmd --authentication-method ActiveDirectoryAzCli`). Lần đầu lệnh này có thể hỏng vì lấy token chậm; chạy `az account get-access-token` trước rồi thử lại.

Script seed sinh dữ liệu giá/khuyến mãi (có seed cố định) **không** nằm trong Terraform. Nó là job DE chạy một lần (tài liệu 06), để Terraform chỉ quản lý hạ tầng.

### Bước 3.7 · Azure Data Factory (factory + danh tính)

**Vì sao:** ADF đảm nhận phần copy từ nguồn ngoài (HTTP Zenodo, SQL, REST OFF) vào `landing/` và lên lịch gọi job Databricks (rubric 3.2). Stack này chỉ tạo **factory + managed identity + quyền**. Linked service, pipeline và trigger nằm ở stack `40-orchestration`, vì chúng thay đổi thường xuyên.

```hcl
module "adf" {
  source  = "Azure/avm-res-datafactory-factory/azurerm"
  version = "x.y.z"
  name                = module.nt.names.data_factory.name_unique
  resource_group_name = azurerm_resource_group.core.name
  location            = var.location
  managed_identities  = { system_assigned = true }
  enable_telemetry    = false
  tags                = module.nt.tags
}
```

Quyền của ADF managed identity:

| Scope | Role | Để làm gì |
|---|---|---|
| ADLS `lake` | Storage Blob Data Contributor | Ghi `landing/` |
| Key Vault | Key Vault Secrets User | Đọc mật khẩu SQL trong linked service |
| Databricks workspace | Contributor | Linked service Databricks dùng MSI để gọi Job |

### Bước 3.8 · RBAC cho 4 thành viên

**Vì sao:** rubric 5.4 yêu cầu "các thành viên cùng làm trên một resource". Quyền được cấp **theo vai trò**, đủ để làm việc nhưng không đủ để xóa nền tảng.

| Thành viên | Scope | Role |
|---|---|---|
| Cả 4 | Sub A | Reader (xem chi phí, xem tài nguyên) |
| Cả 4 | RG `rg-instacart-core` | Contributor |
| Cả 4 | ADLS `lake` | Storage Blob Data Contributor (xem/khám phá dữ liệu bằng Storage Explorer) |
| Cả 4 | Key Vault | Key Vault Secrets User |
| P1 | Key Vault | Key Vault Secrets Officer |
| UAMI CI | Key Vault | Key Vault Secrets Officer (ghi SAS Event Hubs từ stack 20) |

```hcl
locals {
  member_roles = flatten([for m, oid in var.member_object_ids : [
    { k = "${m}-rg",   scope = azurerm_resource_group.core.id, role = "Contributor",                   oid = oid },
    { k = "${m}-lake", scope = module.lake.resource_id,        role = "Storage Blob Data Contributor", oid = oid },
    { k = "${m}-kv",   scope = module.kv.resource_id,          role = "Key Vault Secrets User",        oid = oid },
  ]])
}
resource "azurerm_role_assignment" "members" {
  for_each             = { for r in local.member_roles : r.k => r }
  scope                = each.value.scope
  role_definition_name = each.value.role
  principal_id         = each.value.oid
}
```

Quyền **bên trong Databricks** (ai đọc/ghi schema nào) được cấp bằng Unity Catalog ở tài liệu 05, không cấp bằng Azure RBAC.

### Bước 3.9 · Budget và cảnh báo chi phí

**Vì sao:** credit Students có hạn (khoảng 100 USD). Hết credit thì subscription bị vô hiệu hóa và tài nguyên dừng. Budget gửi email sớm cho cả nhóm. Đây cũng là bằng chứng "thông tin chi phí, quota" cho rubric 5.4.

```hcl
resource "azurerm_monitor_action_group" "team" {
  name                = "ag-instacart-team"
  resource_group_name = azurerm_resource_group.core.name
  short_name          = "instacart"
  dynamic "email_receiver" {
    for_each = var.member_emails            # đặt trong *.auto.tfvars (gitignore)
    content {
      name          = email_receiver.key
      email_address = email_receiver.value
    }
  }
}
resource "azurerm_consumption_budget_subscription" "a" {
  name            = "budget-sub-a"
  subscription_id = "/subscriptions/${var.subscription_id_a}"
  amount          = 100
  time_grain      = "Monthly"
  time_period { start_date = "2026-11-01T00:00:00Z" }
  dynamic "notification" {
    for_each = [25, 50, 75, 90]
    content {
      threshold      = notification.value
      operator       = "GreaterThanOrEqualTo"
      threshold_type = "Actual"
      contact_groups = [azurerm_monitor_action_group.team.id]
    }
  }
}
```

> Budget chỉ **cảnh báo**, không tự dừng tài nguyên. Biện pháp chặn chi phí thật sự là auto-terminate của cluster (tài liệu 05) và workflow tự hủy stream mỗi đêm (tài liệu 04).

### Bước 3.10 · Output của `10-core`

```hcl
output "location"                 { value = var.location }
output "rg_name"                  { value = azurerm_resource_group.core.name }
output "lake_account_name"        { value = module.lake.name }
output "lake_account_id"          { value = module.lake.resource_id }
output "lake_dfs_endpoint"        { value = "abfss://{container}@${module.lake.name}.dfs.core.windows.net" }
output "key_vault_id"             { value = module.kv.resource_id }
output "key_vault_uri"            { value = module.kv.uri }
output "access_connector_id"      { value = azurerm_databricks_access_connector.uc.id }
output "databricks_workspace_url" { value = module.dbw.databricks_workspace_url }
output "databricks_workspace_id"  { value = module.dbw.resource_id }
output "adf_id"                   { value = module.adf.resource_id }
output "sql_server_fqdn"          { value = module.sql.resource.fully_qualified_domain_name }
output "func_identity_id"         { value = azurerm_user_assigned_identity.func.id }
output "func_identity_client_id"  { value = azurerm_user_assigned_identity.func.client_id }
```

**Checklist `10-core`:**
- [ ] `terraform plan` lần hai báo *No changes*. Nếu vẫn còn thay đổi thì cấu hình không ổn định.
- [ ] 7 container có trong ADLS; shared key bị tắt.
- [ ] Mở được workspace; P2–P4 đăng nhập được workspace bằng tài khoản trường.
- [ ] `sqlcmd` (hoặc Azure Data Studio) kết nối `retail_db` từ IP thành viên được.
- [ ] Email test của action group đến được cả 4 người.

---

## 4. Stack `20-stream` (Sub B): Event Hubs theo buổi

**Vì sao ở Sub B:**
- Chia chi phí: Event Hubs Standard tính theo giờ TU.
- **Cố ý đặt ở region khác** Sub A để đo độ trễ liên region (rubric 3.3).

**Vì sao Standard:** chỉ từ tier Standard mới có Kafka endpoint (`:9093`), mà Spark Structured Streaming dùng Kafka source.

```hcl
provider "azurerm" {                       # Sub B — nơi tạo Event Hubs
  features {}
  subscription_id = var.subscription_id_b
}
provider "azurerm" {                       # Sub A — chỉ để ghi secret vào Key Vault
  alias           = "core"
  features {}
  subscription_id = var.subscription_id_a
}

module "eh" {
  source  = "Azure/avm-res-eventhub-namespace/azurerm"
  version = "x.y.z"
  name                = "evhns-instacart-${random_string.sfx.result}"
  resource_group_name = azurerm_resource_group.stream.name
  location            = var.location_b                  # KHÁC region của Sub A
  sku                 = "Standard"
  capacity            = 1                               # 1 TU là đủ cho replay khoảng 131k đơn
  event_hubs = {
    orders-stream = {
      name              = "orders-stream"
      partition_count   = 2
      message_retention = 1   # ngày
      consumer_groups = { dbx = { name = "dbx-bronze" } }
    }
  }
  enable_telemetry = false
  tags = module.nt.tags
}

resource "azurerm_eventhub_authorization_rule" "listen" {   # Databricks chỉ được đọc
  name                = "dbx-listen"
  namespace_name      = module.eh.resource.name
  eventhub_name       = "orders-stream"
  resource_group_name = azurerm_resource_group.stream.name
  listen              = true
  send                = false
  manage              = false
}
# tương tự: "sim-send" (send = true, listen = false) cho simulator

resource "azurerm_key_vault_secret" "eh_listen" {        # Databricks đọc qua secret scope
  provider     = azurerm.core
  name         = "eh-listen-connection-string"
  value        = azurerm_eventhub_authorization_rule.listen.primary_connection_string
  key_vault_id = data.terraform_remote_state.core.outputs.key_vault_id
}
# tương tự: eh-send-connection-string cho simulator, eh-bootstrap-server = "<ns>.servicebus.windows.net:9093"
```

**Vì sao dùng SAS mà không dùng managed identity:** Databricks classic compute không có sẵn danh tính Entra để gọi Event Hubs. Kafka client dùng SASL/PLAIN với connection string là cách đơn giản nhất. Hai rule SAS **tách quyền** send và listen, và chỉ được lưu trong Key Vault.

**Bật/tắt:** workflow `stream-toggle.yml` ([04](04_github_actions_cicd.md)) chạy `terraform apply`/`destroy` riêng stack này. Khi destroy, secret trong Key Vault bị xóa theo. Lần apply sau tạo SAS mới, và job streaming đọc lại secret khi khởi động.

> Vì `message_retention` ngắn và namespace bị hủy hằng đêm, **tính lặp lại của demo** không dựa vào Event Hubs. Nó dựa vào simulator replay deterministic, cộng với `event_id` + MERGE ở silver (đã thống nhất trong data dictionary).

**Checklist `20-stream`:**
- [ ] `kcat -b <ns>.servicebus.windows.net:9093 -X security.protocol=SASL_SSL -X sasl.mechanisms=PLAIN -X sasl.username='$ConnectionString' -X sasl.password='<send>' -t orders-stream -P` gửi được 1 message.
- [ ] Secret `eh-*` xuất hiện trong Key Vault Sub A.
- [ ] `destroy` rồi `apply` lại chạy được trong vòng 5 phút.

---

## 5. Stack `90-sandbox` (Sub C/D): workspace #2 cho DS

**Vì sao:** quota vCPU tính **theo subscription**. Train FP-Growth / DApriori với nhiều cấu hình (rubric "trực quan kết quả trên các tham số") cần chạy song song với pipeline DE mà không giành 4 vCPU của Sub A.

Stack này dùng lại module nội bộ `modules/dbx_workspace` (workspace + access connector + storage nhỏ), mỗi thành viên một state `sandbox-<member>.tfstate`:

```hcl
module "ws2" {
  source   = "../../modules/dbx_workspace"
  name     = "dbw-instacart-sbx-${var.member}"
  location = var.location_c
  sku      = "premium"
  tags     = module.nt.tags
}
```

Workspace sandbox **gắn vào cùng metastore** Unity Catalog. Metastore tính theo region, nên điều này chỉ đúng khi Sub C ở **cùng region** với Sub A. Khi đó DS đọc thẳng `instacart.silver.baskets` mà không phải sao chép dữ liệu. Nếu khác region thì phải dùng Delta Sharing hoặc sao chép bảng `baskets` sang storage của sandbox. Ghi lựa chọn này vào kết quả spike.

Stack này **chỉ cần dựng ở G3**, khi DS bắt đầu.

**DS chạy ở đâu** ([ds/04 §5.2](../ds/04_thuc_nghiem_danh_gia.md)):
- **Sub C cùng region với Sub A:** job `ds-benchmark` deploy với target `sandbox` lên workspace này, đọc `ds_data` qua metastore chung, chạy song song với DE.
- **Khác region:** không dựng Delta Sharing hay sao chép bảng (thêm việc mà không thêm điểm). Job DS chạy ở Sub A ngoài giờ batch, và `90-sandbox` chỉ dùng làm workspace thử nghiệm.
- Cụm **Hadoop/MapReduce chạy bằng Docker ở máy local**, không cần tài nguyên Azure nào ([ds/03](../ds/03_hadoop_mapreduce_local.md)). Đây là quyết định có chủ đích: HDInsight vượt quota 4 vCPU, còn Sub D để dành cho trường hợp dựng lại hạ tầng.

### 5.1 Stack `50-serving` (Sub A, tùy chọn ở G5): Azure Function làm API gợi ý

**Vì sao:** trang web hoặc demo gọi một HTTP endpoint đơn giản, không giữ token Databricks. Function dùng danh tính `id-func-recommend` (tạo ở bước 3.4) để lấy token Entra rồi gọi Model Serving. Đây cũng là thành phần **FaaS** của rubric 1.4. Code của Function ở [ds/05 §3.2](../ds/05_mlflow_serving_bao_cao.md).

| Tài nguyên | Ghi chú |
|---|---|
| Storage account nhỏ cho Function | Chỉ chứa gói code; truy cập bằng danh tính, tắt shared key |
| Function App **Flex Consumption** (Linux, Python 3.11) | Trả theo lần gọi, về 0 khi rảnh. Gói *Linux Consumption* cũ đã được Microsoft thông báo ngừng, nên không chọn |
| Gán `id-func-recommend` cho Function | App setting `AZURE_CLIENT_ID` = client ID của danh tính |
| Role *Storage Blob Data Reader* trên **container** `ds-data` (không phải cả account) | Chỉ cần cho phương án B: Function tự đọc luật từ `ds-data/serving/` |

```hcl
# infra/stacks/50-serving/main.tf  (minh họa; kiểm tên tham số theo đúng version provider azurerm đã pin)
resource "azurerm_service_plan" "func" {
  name                = "asp-instacart-func"
  resource_group_name = local.core.rg_name
  location            = local.core.location
  os_type             = "Linux"
  sku_name            = "FC1"                         # Flex Consumption
}

resource "azurerm_function_app_flex_consumption" "recommend" {
  name                        = "func-instacart-recommend"
  resource_group_name         = local.core.rg_name
  location                    = local.core.location
  service_plan_id             = azurerm_service_plan.func.id
  storage_container_type      = "blobContainer"
  storage_container_endpoint  = "${azurerm_storage_account.func.primary_blob_endpoint}${azurerm_storage_container.pkg.name}"
  storage_authentication_type = "UserAssignedIdentity"
  storage_user_assigned_identity_id = local.core.func_identity_id
  runtime_name                = "python"
  runtime_version             = "3.11"
  maximum_instance_count      = 40
  instance_memory_in_mb       = 2048
  identity {
    type         = "UserAssigned"
    identity_ids = [local.core.func_identity_id]
  }
  app_settings = {
    AZURE_CLIENT_ID = local.core.func_identity_client_id
    DATABRICKS_HOST = local.core.databricks_workspace_url
    ENDPOINT_NAME   = "instacart-basket-rules"
  }
  site_config {}
  tags = module.nt.tags
}

resource "azurerm_role_assignment" "func_ds_reader" {          # phương án B
  scope                = "${local.core.lake_account_id}/blobServices/default/containers/ds"
  role_definition_name = "Storage Blob Data Reader"
  principal_id         = local.core.func_identity_principal_id
}
```

- `local.core` đọc từ `terraform_remote_state` của `10-core`. Cần thêm output `lake_account_id` và `func_identity_principal_id` vào `10-core` khi làm stack này.
- **Spike:** Flex Consumption có ở region đã chốt và subscription Students có cho tạo hay không ([01 §3.5](01_chuan_bi_va_spike.md)). Không được thì dùng gói Consumption cũ (`sku_name = "Y1"`, resource `azurerm_linux_function_app`), và ghi lý do vào báo cáo.
- Code Function deploy bằng workflow `func-deploy` ([04 §3.8](04_github_actions_cicd.md)), không đi qua Terraform.

---

## 6. Thứ tự apply và phụ thuộc

| Thứ tự | Stack | Cần có trước | Ai apply (giai đoạn đầu → ổn định) |
|---|---|---|---|
| 0 | bootstrap | — | P1 local (một lần duy nhất) |
| 1 | 10-core | bootstrap | P1 local → CI (`tf-apply`) |
| 2 | 30-databricks | 10-core | P1 local → CI |
| 3 | 20-stream | 10-core (Key Vault) | P3 hoặc CI `stream-toggle` |
| 4 | 40-orchestration | 10-core, 30-databricks (job ID) | CI |
| 5 | 90-sandbox | bootstrap (+ metastore) | P4 local |
| 6 | 50-serving (tùy chọn) | 10-core, 30-databricks (endpoint đã có) | CI |

`terraform destroy` đi theo **thứ tự ngược lại**. Lưu ý: muốn destroy `10-core` thì phải destroy `30` và `40` trước. Nếu không, provider databricks sẽ báo lỗi vì không còn workspace.
