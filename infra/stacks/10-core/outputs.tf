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

# URL dạng adb-<id>.<n>.azuredatabricks.net, provider databricks của 30-databricks dùng làm host.
output "databricks_workspace_url" {
  value = module.dbw.databricks_workspace_url
}

# Resource ID trên Azure (không phải workspace ID số), dùng cho azure_workspace_resource_id
# của provider databricks và cho role assignment của ADF ở bước 3.7.
output "databricks_workspace_id" {
  value = module.dbw.resource_id
}

output "func_identity_id" {
  value = azurerm_user_assigned_identity.func.id
}

output "func_identity_client_id" {
  value = azurerm_user_assigned_identity.func.client_id
}

# FQDN của Azure SQL, dùng cho linked service ls_sql_retail (40-orchestration) và seed job.
# Ghép từ tên server vì output resource của AVM bị đánh dấu sensitive.
output "sql_server_fqdn" {
  value = "${module.sql.resource_name}.database.windows.net"
}

output "sql_database_name" {
  value = azapi_resource.retail_db.name
}

# Stack 40-orchestration khai báo linked service, dataset, pipeline vào factory này.
output "adf_id" {
  value = module.adf.resource_id
}

output "adf_name" {
  value = module.adf.name
}

# Principal ID của managed identity ADF, để kiểm role bằng az role assignment list --assignee.
output "adf_principal_id" {
  value = module.adf.resource.identity[0].principal_id
}
