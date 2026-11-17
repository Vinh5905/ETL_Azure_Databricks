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
