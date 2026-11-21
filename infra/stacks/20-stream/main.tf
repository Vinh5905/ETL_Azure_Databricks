# Stack 20-stream (Sub B): Event Hubs theo buổi. Xem docs/devops/03, mục 4.
# Bật/tắt bằng workflow stream-toggle; destroy mỗi đêm.
#
# [x] Provider Sub B + alias Sub A, remote state core, resource group
# [x] Event Hubs namespace Standard (Kafka :9093), hub orders-stream, consumer group dbx-bronze
# [x] SAS dbx-listen (listen) và sim-send (send)
# [x] Secret eh-listen-connection-string, eh-send-connection-string, eh-bootstrap-server vào Key Vault core

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

module "nt" {
  source = "../../modules/naming_tags"
  env    = "prod"
  owner  = "p3"
  stack  = "20-stream"
}

resource "random_string" "sfx" {
  length  = 5
  special = false
  upper   = false
}

resource "azurerm_resource_group" "stream" {
  name     = "rg-instacart-stream"
  location = var.location_b
  tags     = module.nt.tags
}

locals {
  eh_hub = "orders-stream"
}

# Namespace và hub. Region KHÁC Sub A để đo độ trễ liên region (rubric 3.3).
# Chỉ tier Standard trở lên mới có Kafka endpoint :9093.
module "eh" {
  source  = "Azure/avm-res-eventhub-namespace/azurerm"
  version = "0.2.0"

  name                = "evhns-instacart-${random_string.sfx.result}"
  resource_group_name = azurerm_resource_group.stream.name
  location            = var.location_b
  sku                 = "Standard"
  capacity            = 1 # 1 TU đủ cho replay khoảng 131k đơn

  # Mặc định của AVM tắt SAS và tắt mạng public. Kafka client (simulator, Databricks)
  # đăng nhập bằng SASL/PLAIN với connection string và không có VNet, nên phải bật cả hai.
  # Quyền vẫn tách bằng hai rule SAS send/listen, và connection string chỉ nằm trong Key Vault.
  local_authentication_enabled  = true
  public_network_access_enabled = true

  event_hubs = {
    (local.eh_hub) = {
      namespace_name      = "evhns-instacart-${random_string.sfx.result}"
      resource_group_name = azurerm_resource_group.stream.name
      partition_count     = 2
      message_retention   = 1 # ngày
    }
  }

  enable_telemetry = false
  tags             = module.nt.tags
}

# AVM 0.2.0 không có consumer group, nên khai báo bằng resource thô.
resource "azurerm_eventhub_consumer_group" "dbx_bronze" {
  name                = "dbx-bronze"
  namespace_name      = module.eh.resource_eventhubs[local.eh_hub].namespace_name
  eventhub_name       = module.eh.resource_eventhubs[local.eh_hub].name
  resource_group_name = azurerm_resource_group.stream.name
}

# Hai rule SAS tách quyền: Databricks chỉ đọc, simulator chỉ gửi.
resource "azurerm_eventhub_authorization_rule" "listen" {
  name                = "dbx-listen"
  namespace_name      = module.eh.resource_eventhubs[local.eh_hub].namespace_name
  eventhub_name       = module.eh.resource_eventhubs[local.eh_hub].name
  resource_group_name = azurerm_resource_group.stream.name
  listen              = true
  send                = false
  manage              = false
}

resource "azurerm_eventhub_authorization_rule" "send" {
  name                = "sim-send"
  namespace_name      = module.eh.resource_eventhubs[local.eh_hub].namespace_name
  eventhub_name       = module.eh.resource_eventhubs[local.eh_hub].name
  resource_group_name = azurerm_resource_group.stream.name
  listen              = false
  send                = true
  manage              = false
}

# Secret vào Key Vault core (Sub A). Databricks đọc qua secret scope, simulator nạp vào EH_SEND_CONN.
# Mỗi đêm stream-toggle destroy thì secret bị xóa mềm; lần apply sau provider khôi phục rồi ghi giá trị mới.
resource "azurerm_key_vault_secret" "eh_listen" {
  provider     = azurerm.core
  name         = "eh-listen-connection-string"
  value        = azurerm_eventhub_authorization_rule.listen.primary_connection_string
  key_vault_id = data.terraform_remote_state.core.outputs.key_vault_id
}

resource "azurerm_key_vault_secret" "eh_send" {
  provider     = azurerm.core
  name         = "eh-send-connection-string"
  value        = azurerm_eventhub_authorization_rule.send.primary_connection_string
  key_vault_id = data.terraform_remote_state.core.outputs.key_vault_id
}

resource "azurerm_key_vault_secret" "eh_bootstrap" {
  provider     = azurerm.core
  name         = "eh-bootstrap-server"
  value        = "${module.eh.resource_eventhubs[local.eh_hub].namespace_name}.servicebus.windows.net:9093"
  key_vault_id = data.terraform_remote_state.core.outputs.key_vault_id
}
