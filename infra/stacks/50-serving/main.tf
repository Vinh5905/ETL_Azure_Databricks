# Stack 50-serving (Sub A, tùy chọn ở G5): Azure Function làm API gợi ý.
# Xem docs/devops/03 §5.1 và docs/ds/05 §3. Code Function deploy bằng workflow func-deploy
# (AD-OPS-32), không đi qua Terraform.
#
# [x] Remote state core + bootstrap
# [x] Storage account chứa gói code (LRS, tắt shared key) + container deploy
# [x] Service plan FC1 (Flex Consumption)
# [x] Function App func-instacart-recommend (Python 3.11) gắn UAMI id-func-recommend
# [x] Role cho id-func-recommend: Blob Data Contributor trên storage gói code,
#     Blob Data Reader trên container ds-data của lake (phương án B)
# [x] Role Website Contributor cho UAMI CI trên Function App (func-deploy)
#
# Vì sao Flex (FC1), không dùng Y1 + azurerm_linux_function_app: gói Linux Consumption (Y1) đã được
# Microsoft thông báo ngừng. Chỉ khi region hoặc subscription Students không cho tạo FC1 mới chuyển
# sang Y1 và ghi lý do vào báo cáo (docs/devops/03 §5.1).
#
# Vì sao dùng AVM avm-res-web-site, không dùng azurerm_function_app_flex_consumption (azurerm 4.81.0):
# resource azurerm luôn ghi app setting AzureWebJobsStorage dạng connection string với AccountKey rỗng.
# Host Function đọc chuỗi này trước AzureWebJobsStorage__accountName nên không chạy được bằng danh tính,
# còn đặt AzureWebJobsStorage = "" thì plan không bao giờ hết thay đổi (hashicorp/terraform-provider-azurerm
# issue 33211, còn mở). AVM 0.25.0 dựng site bằng azapi và chỉ ghi bộ AzureWebJobsStorage__* khi
# storage_uses_managed_identity = true.

module "nt" {
  source     = "../../modules/naming_tags"
  env        = "prod"
  owner      = "p1"
  stack      = "50-serving"
  extra_tags = { cost_owner = "p4" } # API gợi ý phục vụ demo của phần DS (AD-DS-24)
}

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

# Đọc principal ID của UAMI id-github-ci từ state của bootstrap, khỏi thêm biến riêng.
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

data "azurerm_resource_group" "core" {
  name = data.terraform_remote_state.core.outputs.rg_name
}

locals {
  core = data.terraform_remote_state.core.outputs

  # Container chứa gói zip do func-deploy đẩy lên. Flex đọc gói từ đây bằng UAMI.
  pkg_container = "func-pkg"
}

# Storage nhỏ riêng cho Function: chứa gói code và làm host storage (AzureWebJobsStorage).
# Không dùng lake để Function không có quyền ghi vào dữ liệu. Chỉ xác thực bằng Entra ID.
module "func_storage" {
  source  = "Azure/avm-res-storage-storageaccount/azurerm"
  version = "0.11.0"

  name      = module.nt.names.storage_account.name_unique
  parent_id = data.azurerm_resource_group.core.id
  location  = data.azurerm_resource_group.core.location

  # AVM 0.11.0 mặc định Standard_ZRS; LRS là mức rẻ nhất.
  account_kind     = "StorageV2"
  account_sku_name = "Standard_LRS"
  access_tier      = "Hot"
  is_hns_enabled   = false

  shared_access_key_enabled       = false
  default_to_oauth_authentication = true
  allow_nested_items_to_be_public = false
  min_tls_version                 = "TLS1_2"

  # Không dùng private endpoint (quy tắc chi phí). Function Flex không gắn VNet nên đi vào bằng
  # IP public; quyền chặn bằng RBAC.
  public_network_access_enabled = true
  network_rules = {
    default_action = "Allow"
    bypass         = ["AzureServices"]
  }

  containers = { (local.pkg_container) = { name = local.pkg_container } }

  enable_telemetry = false
  tags             = module.nt.tags
}

# UAMI của Function đọc gói code và dùng làm host storage (blob lease, khóa của host).
# Function chỉ có trigger HTTP nên không cần quyền Queue/Table. Blob Data Contributor nằm trong
# ci_assignable_role_ids nên CI gán được (Blob Data Owner thì không).
resource "azurerm_role_assignment" "func_pkg" {
  scope                = module.func_storage.resource_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = local.core.func_identity_principal_id
  principal_type       = "ServicePrincipal"
}

module "func_plan" {
  source  = "Azure/avm-res-web-serverfarm/azurerm"
  version = "2.1.0"

  name      = "asp-instacart-func"
  parent_id = data.azurerm_resource_group.core.id
  location  = data.azurerm_resource_group.core.location
  os_type   = "Linux"
  sku_name  = "FC1" # Flex Consumption: trả theo lần gọi, về 0 khi rảnh

  # Mặc định của AVM là zone redundant; tắt để giữ mức rẻ nhất và tránh lỗi ở region không hỗ trợ.
  zone_balancing_enabled = false

  enable_telemetry = false
  tags             = module.nt.tags
}

module "func" {
  source  = "Azure/avm-res-web-site/azurerm"
  version = "0.25.0"

  name                     = "func-instacart-recommend"
  parent_id                = data.azurerm_resource_group.core.id
  location                 = data.azurerm_resource_group.core.location
  service_plan_resource_id = module.func_plan.resource_id
  kind                     = "functionapp"
  os_type                  = "Linux"

  function_app_uses_fc1  = true
  fc1_runtime_name       = "python"
  fc1_runtime_version    = "3.11"
  instance_memory_in_mb  = var.instance_memory_in_mb
  maximum_instance_count = var.maximum_instance_count
  # always_ready để trống: không giữ instance nào chạy sẵn, rảnh thì về 0.

  # Gói code: Flex đọc container func-pkg bằng UAMI.
  storage_container_type            = "blobContainer"
  storage_container_endpoint        = "https://${module.func_storage.name}.blob.core.windows.net/${local.pkg_container}"
  storage_authentication_type       = "UserAssignedIdentity"
  storage_user_assigned_identity_id = local.core.func_identity_id

  # Host storage (AzureWebJobsStorage__accountName/credential/clientId) cũng bằng UAMI, không connection string.
  storage_account_name                     = module.func_storage.name
  storage_uses_managed_identity            = true
  storage_user_assigned_identity_client_id = local.core.func_identity_client_id

  managed_identities = {
    user_assigned_resource_ids = [local.core.func_identity_id]
  }

  # Web demo gọi thẳng API qua Internet (có function key), nên mở mạng public.
  public_network_access_enabled = true
  https_only                    = true
  # func-deploy đăng nhập bằng OIDC (UAMI CI + Website Contributor), không dùng publish profile.
  ftp_publish_basic_authentication_enabled = false
  scm_publish_basic_authentication_enabled = false

  # Biến mà src/serving_func/function_app.py đọc.
  app_settings = {
    AZURE_CLIENT_ID      = local.core.func_identity_client_id
    DATABRICKS_HOST      = local.core.databricks_workspace_url
    ENDPOINT_NAME        = var.endpoint_name
    SERVING_MODE         = var.serving_mode
    STORAGE_ACCOUNT_NAME = local.core.lake_account_name
    DS_CONTAINER_NAME    = "ds-data"
  }

  enable_telemetry = false
  tags             = module.nt.tags

  # Host phải đọc được gói code và host storage ngay khi khởi động.
  depends_on = [azurerm_role_assignment.func_pkg]
}

# Phương án B (docs/ds/05 §3.3): Function tự đọc luật champion trong ds-data/serving/.
# Chỉ cấp trên container ds-data, không cấp trên cả account lake.
resource "azurerm_role_assignment" "func_ds_reader" {
  scope                = "${local.core.lake_account_id}/blobServices/default/containers/ds-data"
  role_definition_name = "Storage Blob Data Reader"
  principal_id         = local.core.func_identity_principal_id
  principal_type       = "ServicePrincipal"
}

# func-deploy (AD-OPS-32) đăng nhập bằng UAMI CI và đẩy gói code lên Function App.
# Website Contributor phải có trong ci_assignable_role_ids của bootstrap (P1 apply tay trước).
resource "azurerm_role_assignment" "ci_website" {
  scope                = module.func.resource_id
  role_definition_name = "Website Contributor"
  principal_id         = data.terraform_remote_state.bootstrap.outputs.ci_principal_id
  principal_type       = "ServicePrincipal"
}
