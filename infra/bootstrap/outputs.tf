# Đặt các giá trị này vào GitHub Secrets (docs/devops/02, bước 7) và infra/backend.hcl.

output "tfstate_resource_group" {
  value = azurerm_resource_group.tfstate.name
}

output "tfstate_account" {
  description = "→ GitHub Secret TFSTATE_ACCOUNT và storage_account_name trong infra/backend.hcl"
  value       = azurerm_storage_account.tfstate.name
}

output "ci_client_id" {
  description = "→ GitHub Secret AZURE_CLIENT_ID; cũng là application_id của SP CI trong Databricks"
  value       = azurerm_user_assigned_identity.ci.client_id
}

output "ci_principal_id" {
  description = "Object ID của UAMI, dùng khi chủ Sub B gán quyền Contributor"
  value       = azurerm_user_assigned_identity.ci.principal_id
}

output "tenant_id" {
  description = "→ GitHub Secret AZURE_TENANT_ID"
  value       = azurerm_user_assigned_identity.ci.tenant_id
}
