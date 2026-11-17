# Các stack sau đọc output qua terraform_remote_state (key = core.tfstate).
# Thêm dần theo docs/devops/03, bước 3.10.

output "location" {
  value = azurerm_resource_group.core.location
}

output "rg_name" {
  value = azurerm_resource_group.core.name
}

output "lake_account_name" {
  value = module.lake.name
}

output "lake_account_id" {
  value = module.lake.resource_id
}

output "lake_dfs_endpoint" {
  value = "abfss://{container}@${module.lake.name}.dfs.core.windows.net"
}

output "key_vault_id" {
  value = module.kv.resource_id
}

output "key_vault_uri" {
  value = module.kv.uri
}

output "access_connector_id" {
  value = azurerm_databricks_access_connector.uc.id
}

output "func_identity_id" {
  value = azurerm_user_assigned_identity.func.id
}

output "func_identity_client_id" {
  value = azurerm_user_assigned_identity.func.client_id
}
