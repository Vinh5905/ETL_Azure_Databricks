locals {
  # Các role mà CI được phép gán (role "Role Based Access Control Administrator" có điều kiện).
  # Không có Owner / User Access Administrator, nên CI không tự nâng quyền được.
  ci_assignable_role_ids = [
    "b24988ac-6180-42a0-ab88-20f7382dd24c", # Contributor (thành viên trên RG core, ADF trên workspace)
    "acdd72a7-3385-48ef-bd42-f606fba81ae7", # Reader
    "ba92f5b4-2d11-453d-a403-e96b0029c9fe", # Storage Blob Data Contributor
    "2a2b9908-6ea1-4ae2-8e65-a410df84e7d1", # Storage Blob Data Reader
    "4633458b-17de-408a-b874-0445c86b69e6", # Key Vault Secrets User
    "b86a8fe4-44ce-4948-aee5-eccb2c155cd7", # Key Vault Secrets Officer
    "de139f84-1756-47ae-9be6-808fbbe84772", # Website Contributor (UAMI CI trên Function App, 50-serving)
  ]

  ci_assignable_role_list = join(", ", local.ci_assignable_role_ids)
}

# 4 thành viên đọc/ghi/khóa state khi chạy plan/apply từ máy.
resource "azurerm_role_assignment" "state_members" {
  for_each = var.member_object_ids

  scope                = azurerm_storage_account.tfstate.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = each.value
}

# CI đọc/ghi/khóa state.
resource "azurerm_role_assignment" "ci_state" {
  scope                = azurerm_storage_account.tfstate.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_user_assigned_identity.ci.principal_id
  principal_type       = "ServicePrincipal"
}

# CI tạo tài nguyên trong Sub A.
resource "azurerm_role_assignment" "ci_sub_a_contrib" {
  scope                = "/subscriptions/${var.subscription_id_a}"
  role_definition_name = "Contributor"
  principal_id         = azurerm_user_assigned_identity.ci.principal_id
  principal_type       = "ServicePrincipal"
}

# CI tạo role assignment trong stack core, nhưng chỉ cho các role ở danh sách trên.
resource "azurerm_role_assignment" "ci_rbac_admin" {
  scope                = "/subscriptions/${var.subscription_id_a}"
  role_definition_name = "Role Based Access Control Administrator"
  principal_id         = azurerm_user_assigned_identity.ci.principal_id
  principal_type       = "ServicePrincipal"
  condition_version    = "2.0"
  condition            = <<-EOT
    (
      (
        !(ActionMatches{'Microsoft.Authorization/roleAssignments/write'})
      )
      OR
      (
        @Request[Microsoft.Authorization/roleAssignments:RoleDefinitionId] ForAnyOfAnyValues:GuidEquals {${local.ci_assignable_role_list}}
      )
    )
    AND
    (
      (
        !(ActionMatches{'Microsoft.Authorization/roleAssignments/delete'})
      )
      OR
      (
        @Resource[Microsoft.Authorization/roleAssignments:RoleDefinitionId] ForAnyOfAnyValues:GuidEquals {${local.ci_assignable_role_list}}
      )
    )
  EOT
}

# Quyền Contributor của CI trên Sub B do chủ Sub B (P3) tự gán, vì P1 không có quyền ở đó:
#   az role assignment create --assignee-object-id <ci_principal_id> --assignee-principal-type ServicePrincipal \
#     --role Contributor --scope /subscriptions/<SUB_B_ID>
