# Stack 10-core (Sub A): nền tảng chính. Xem docs/devops/03, mục 3.
# Mỗi bước là một PR nhỏ; `plan` sạch rồi mới sang bước kế tiếp.
#
# [x] 3.1  Provider, backend, resource group
# [x] 3.2  ADLS Gen2 (HNS) + 7 container + lifecycle landing → cool
# [x] 3.3  Key Vault (RBAC mode)
# [x] 3.4  Access Connector + Storage Blob Data Contributor trên ADLS
# [x] 3.5  Databricks workspace Premium
# [x] 3.6  Azure SQL (free offer) + firewall + secret mật khẩu
# [x] 3.7  Data Factory + managed identity + quyền
# [ ] 3.8  RBAC cho 4 thành viên
# [ ] 3.9  Action group + budget 25/50/75/90%

module "nt" {
  source = "../../modules/naming_tags"
  env    = "prod"
  owner  = "p1"
  stack  = "10-core"
}

resource "azurerm_resource_group" "core" {
  name     = "rg-instacart-core"
  location = var.location
  tags     = module.nt.tags
}

# Bước 3.2: ADLS Gen2 cho mọi tầng dữ liệu và container ds-data (docs/devops/03, bước 3.2).
module "lake" {
  source = "../../modules/lakehouse_storage"

  name              = module.nt.names.storage_account.name_unique
  resource_group_id = azurerm_resource_group.core.id
  location          = azurerm_resource_group.core.location
  tags              = module.nt.tags
}

data "azurerm_client_config" "current" {}

# Bước 3.3: Key Vault giữ mọi bí mật (mật khẩu SQL, SAS Event Hubs, push URL Power BI).
# RBAC mode: phân quyền bằng role Azure, không dùng access policy.
module "kv" {
  source  = "Azure/avm-res-keyvault-vault/azurerm"
  version = "0.13.1"

  name      = module.nt.names.key_vault.name_unique
  parent_id = azurerm_resource_group.core.id
  location  = azurerm_resource_group.core.location
  tenant_id = data.azurerm_client_config.current.tenant_id

  sku_name                   = "standard" # mặc định của AVM là premium
  purge_protection_enabled   = false      # để destroy/dựng lại được khi làm đồ án
  soft_delete_retention_days = 7

  # Mặc định của AVM là chặn mọi IP. Dự án không dùng private endpoint hay VNet injection,
  # nên mở mạng và kiểm quyền bằng RBAC (giống storage ở bước 3.2).
  public_network_access_enabled = true
  network_acls = {
    bypass         = "AzureServices"
    default_action = "Allow"
  }

  enable_telemetry = false
  tags             = module.nt.tags
}

# Bước 3.4: Access Connector là managed identity để Unity Catalog đọc/ghi ADLS thay cho người dùng.
# Stack 30-databricks tạo storage credential trỏ vào nó.
resource "azurerm_databricks_access_connector" "uc" {
  name                = "dbac-instacart"
  resource_group_name = azurerm_resource_group.core.name
  location            = azurerm_resource_group.core.location
  tags                = module.nt.tags

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_role_assignment" "uc_lake" {
  scope                = module.lake.resource_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.uc.identity[0].principal_id
  principal_type       = "ServicePrincipal"
}

# Bước 3.5: workspace Premium (Unity Catalog, SQL warehouse serverless, secret scope Key Vault đều cần).
# Không VNet injection. Bên trong workspace (catalog, nhóm, policy) do stack 30-databricks lo.
module "dbw" {
  source  = "Azure/avm-res-databricks-workspace/azurerm"
  version = "0.6.0"

  name                        = "dbw-instacart"
  resource_group_name         = azurerm_resource_group.core.name
  location                    = azurerm_resource_group.core.location
  sku                         = "premium" # không có trial: DBU tính vào credit từ ngày đầu (spike 3.4)
  managed_resource_group_name = "rg-instacart-dbw-managed"

  custom_parameters = {
    # Spike 3.4: workspace tạo bằng ARM mặc định bật no_public_ip, Azure tự dựng NAT gateway
    # + public IP trong managed RG và tính tiền theo giờ. Tắt để không có NAT.
    no_public_ip = false
    # Storage DBFS root trong managed RG. Mặc định của AVM là Standard_GRS; LRS đủ cho đồ án và rẻ hơn.
    storage_account_sku_name = "Standard_LRS"
  }

  enable_telemetry = false
  tags             = module.nt.tags
}

# Bước 3.6: Azure SQL retail_db, nguồn CDC cho product_prices và promotions (docs/devops/03, bước 3.6).

# Hai dòng "Key Vault Secrets Officer" trong bảng quyền bước 3.8 được làm sớm ở đây:
# P1 cần quyền này để apply ghi secret sql-admin-password, CI cần nó để plan đọc lại secret.
# Quyền của 4 thành viên (Secrets User) vẫn để bước 3.8.
data "terraform_remote_state" "bootstrap" {
  backend = "azurerm"
  config = {
    resource_group_name  = "rg-tfstate"
    storage_account_name = var.tfstate_account
    container_name       = "tfstate"
    key                  = "bootstrap.tfstate"
    use_azuread_auth     = true
  }
}

resource "azurerm_role_assignment" "kv_officer" {
  for_each = {
    p1 = { principal_id = var.member_object_ids["p1"], principal_type = "User" }
    ci = { principal_id = data.terraform_remote_state.bootstrap.outputs.ci_principal_id, principal_type = "ServicePrincipal" }
  }

  scope                = module.kv.resource_id
  role_definition_name = "Key Vault Secrets Officer"
  principal_id         = each.value.principal_id
  principal_type       = each.value.principal_type
}

# Mật khẩu login sqladmin. Ký tự đặc biệt giới hạn ở nhóm an toàn cho connection string
# (ADF linked service, pymssql), tránh ; { } = ' ".
resource "random_password" "sql" {
  length           = 32
  special          = true
  override_special = "-_.!#*"
  min_lower        = 2
  min_upper        = 2
  min_numeric      = 2
  min_special      = 2
}

module "sql" {
  source  = "Azure/avm-res-sql-server/azurerm"
  version = "0.2.1"

  name                = module.nt.names.mssql_server.name_unique
  resource_group_name = azurerm_resource_group.core.name
  location            = azurerm_resource_group.core.location
  server_version      = "12.0"

  # Chốt G0: SQL auth cho seed job (pymssql) và linked service ls_sql_retail,
  # cộng Entra admin cho người đăng nhập tay. Không bật Entra-only.
  administrator_login          = "sqladmin"
  administrator_login_password = random_password.sql.result
  azuread_administrator = {
    azuread_authentication_only = false
    login_username              = "p1-entra-admin" # chỉ là tên hiển thị; Azure xác thực theo object_id
    object_id                   = var.member_object_ids["p1"]
  }

  # Mặc định của AVM là tắt public network. Dự án không dùng private endpoint,
  # nên mở mạng và chặn bằng firewall: dịch vụ Azure (ADF, Databricks) + IP thành viên.
  public_network_access_enabled = true
  firewall_rules = merge(
    {
      allow-azure = { start_ip_address = "0.0.0.0", end_ip_address = "0.0.0.0" }
    },
    { for k, ip in var.member_ips : "member-${k}" => { start_ip_address = ip, end_ip_address = ip } },
  )

  enable_telemetry = false
  tags             = module.nt.tags
}

# Không dùng administrator_login_password_key_vault_configuration của AVM: count của secret trong
# module phụ thuộc mật khẩu (chưa biết lúc plan) nên plan lỗi "Invalid count argument".
resource "azurerm_key_vault_secret" "sql_pw" {
  name         = "sql-admin-password"
  value        = random_password.sql.result
  key_vault_id = module.kv.resource_id
  content_type = "password của login sqladmin trên ${module.sql.resource_name}"

  # Role Secrets Officer phải có trước khi ghi secret.
  depends_on = [azurerm_role_assignment.kv_officer]
}

# DB tạo bằng azapi vì azurerm 4.x và AVM 0.2.1 chưa có thuộc tính free offer (useFreeLimit).
# Free offer: serverless GP_S_Gen5_2 (GP_S_Gen5, 2 vCore), 100.000 vCore-giây và 32 GB mỗi tháng, hết hạn mức thì tự pause.
# Mỗi subscription chỉ có 1 DB free offer (spike 3.5).
resource "azapi_resource" "retail_db" {
  type      = "Microsoft.Sql/servers/databases@2023-08-01"
  name      = "retail_db"
  parent_id = module.sql.resource_id
  location  = azurerm_resource_group.core.location
  tags      = module.nt.tags

  body = {
    # Azure lưu tên SKU là GP_S_Gen5, số vCore nằm ở capacity. Ghi GP_S_Gen5_2 thì lần plan sau luôn đòi đổi.
    sku = {
      name     = "GP_S_Gen5"
      tier     = "GeneralPurpose"
      family   = "Gen5"
      capacity = 2
    }
    properties = {
      useFreeLimit                = true
      freeLimitExhaustionBehavior = "AutoPause"
    }
  }
}

# Bước 3.7: Data Factory chỉ gồm factory + system-assigned identity + quyền.
# Linked service, dataset, pipeline, trigger nằm ở stack 40-orchestration vì đổi thường xuyên.
module "adf" {
  source  = "Azure/avm-res-datafactory-factory/azurerm"
  version = "0.3.0"

  name                = module.nt.names.data_factory.name_unique
  resource_group_name = azurerm_resource_group.core.name
  location            = azurerm_resource_group.core.location
  managed_identities  = { system_assigned = true }

  enable_telemetry = false
  tags             = module.nt.tags
}

# Ba role của ADF, đều nằm trong ci_assignable_role_ids của bootstrap nên CI apply được.
resource "azurerm_role_assignment" "adf" {
  for_each = {
    # Ghi file vào landing/.
    lake = { scope = module.lake.resource_id, role = "Storage Blob Data Contributor" }
    # Đọc sql-admin-password cho linked service ls_sql_retail.
    kv = { scope = module.kv.resource_id, role = "Key Vault Secrets User" }
    # Linked service Databricks xác thực bằng MSI để gọi Job.
    dbw = { scope = module.dbw.resource_id, role = "Contributor" }
  }

  scope                = each.value.scope
  role_definition_name = each.value.role
  principal_id         = module.adf.resource.identity[0].principal_id
  principal_type       = "ServicePrincipal"
}

# Danh tính cho Azure Function gợi ý (G5). Tạo sẵn ở đây (không tốn tiền) để 30-databricks
# thêm nó vào workspace trước khi stack 50-serving tồn tại. User-assigned nên có client_id ngay.
resource "azurerm_user_assigned_identity" "func" {
  name                = "id-func-recommend"
  resource_group_name = azurerm_resource_group.core.name
  location            = azurerm_resource_group.core.location
  tags                = module.nt.tags
}
