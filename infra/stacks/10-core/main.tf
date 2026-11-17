# Stack 10-core (Sub A): nền tảng chính. Xem docs/devops/03, mục 3.
# Mỗi bước là một PR nhỏ; `plan` sạch rồi mới sang bước kế tiếp.
#
# [x] 3.1  Provider, backend, resource group
# [x] 3.2  ADLS Gen2 (HNS) + 7 container + lifecycle landing → cool
# [ ] 3.3  Key Vault (RBAC mode)
# [ ] 3.4  Access Connector + Storage Blob Data Contributor trên ADLS
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
