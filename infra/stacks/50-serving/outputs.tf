# Workflow func-deploy (AD-OPS-32) và báo cáo đọc các output này.

output "function_app_name" {
  value = module.func.name
}

output "function_app_id" {
  value = module.func.resource_id
}

# Host dạng func-instacart-recommend-<hash>.<region>.azurewebsites.net; API ở /api/recommend.
output "function_app_hostname" {
  value = module.func.resource_uri
}

output "func_storage_account_name" {
  value = module.func_storage.name
}
