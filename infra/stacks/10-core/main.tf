# Stack 10-core (Sub A): nền tảng chính. Xem docs/devops/03, mục 3.
# Mỗi bước là một PR nhỏ; `plan` sạch rồi mới sang bước kế tiếp.
#
# [x] 3.1  Provider, backend, resource group
# [x] 3.2  ADLS Gen2 (HNS) + 7 container + lifecycle landing → cool
# [x] 3.3  Key Vault (RBAC mode)
# [x] 3.4  Access Connector + Storage Blob Data Contributor trên ADLS
# [ ] 3.5  Databricks workspace Premium
# [ ] 3.6  Azure SQL (free offer) + firewall + secret mật khẩu
# [ ] 3.7  Data Factory + managed identity + quyền
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

# Danh tính cho Azure Function gợi ý (G5). Tạo sẵn ở đây (không tốn tiền) để 30-databricks
# thêm nó vào workspace trước khi stack 50-serving tồn tại. User-assigned nên có client_id ngay.
resource "azurerm_user_assigned_identity" "func" {
  name                = "id-func-recommend"
  resource_group_name = azurerm_resource_group.core.name
  location            = azurerm_resource_group.core.location
  tags                = module.nt.tags
}
