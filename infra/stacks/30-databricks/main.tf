# Stack 30-databricks: bên trong workspace (Unity Catalog, quyền, policy, warehouse).
# Xem docs/devops/05. Cần 10-core đã có workspace (bước 3.5) trước khi plan.
#
# [x] 1. Xác nhận metastore
# [x] 2. Service principal cho CI (application_id = ci_client_id), user theo vai trò
# [ ] 3. Storage credential + external location theo container
# [ ] 4. Catalog instacart, 7 schema, volume raw + checkpoints
# [ ] 5. Grants Unity Catalog (một khối databricks_grants cho mỗi securable)
# [ ] 6. Cluster policy single-node + tắt unrestricted cluster creation
# [ ] 7. Secret scope dựa trên Key Vault (P1 apply từ máy local)
# [ ] 8. Serverless SQL warehouse wh-instacart-bi
# [ ] 9. Output cho DAB và Power BI

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

# Đọc client ID của UAMI id-github-ci từ state của bootstrap, khỏi thêm biến riêng.
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

locals {
  core = data.terraform_remote_state.core.outputs

  # Mọi người có ít nhất một vai trong var.roles (key của member_emails, ví dụ "p2").
  members = toset(flatten(values(var.roles)))
}

# Lỗi ở đây nghĩa là workspace chưa được gán metastore → đi nhánh dự phòng của spike 3.4.
data "databricks_current_metastore" "this" {}

# ---------------------------------------------------------------------------
# Bước 2 · Danh tính (docs/devops/05 bước 2)
# ---------------------------------------------------------------------------
# Provider cấp workspace không tạo được account group, còn nhóm workspace-local thì GRANT báo
# PRINCIPAL_DOES_NOT_EXIST (spike 3.4). Vì vậy quyền Unity Catalog ở bước 5 cấp thẳng cho từng
# user theo var.roles, cộng SP CI và SP Function.
#
# force = true: danh tính đã có sẵn thì nhận vào state thay vì báo lỗi. UAMI CI và các thành viên
# có role Azure trên workspace nên được Databricks tự thêm (làm admin) ngay lần đăng nhập đầu.
# Application ID bọc sensitive() để plan in ra log công khai không lộ client ID. Thuộc tính computed
# home/acl_principal_id vẫn chứa client ID; chấp nhận vì client ID không phải bí mật, và của CI
# đã được che qua ARM_CLIENT_ID.

# Danh tính của GitHub Actions: deploy bundle và chạy job prod (run_as).
resource "databricks_service_principal" "ci" {
  application_id        = sensitive(data.terraform_remote_state.bootstrap.outputs.ci_client_id)
  display_name          = "sp-github-ci"
  workspace_access      = true
  databricks_sql_access = true
  allow_cluster_create  = false
  force                 = true

  # Đây là chính danh tính đang chạy apply và là run_as của job prod. Hủy stack (AD-OPS-34) thì
  # gỡ dòng này trước, và hủy bundle prod trước 30-databricks.
  lifecycle {
    prevent_destroy = true
  }
}

# G5: danh tính của Azure Function gợi ý (tạo ở 10-core). Chỉ cần quyền gọi endpoint (CAN_QUERY,
# cấp trong DAB serving_ds.yml); không cấp cluster, không cấp quyền dữ liệu.
resource "databricks_service_principal" "func" {
  application_id        = sensitive(local.core.func_identity_client_id)
  display_name          = "sp-func-recommend"
  workspace_access      = false
  databricks_sql_access = false
  allow_cluster_create  = false
  force                 = true
}

# Thành viên nhóm. allow_cluster_create = false chỉ có tác dụng với người không phải admin: hiện
# mọi thành viên có Contributor trên RG core nên là workspace admin và bỏ qua được entitlement
# lẫn cluster policy (đánh đổi đã chấp nhận ở 10-core). Kỷ luật chi phí dựa vào policy + budget.
resource "databricks_user" "members" {
  for_each = local.members

  user_name             = var.member_emails[each.key]
  workspace_access      = true
  databricks_sql_access = true
  allow_cluster_create  = false
  force                 = true
}
