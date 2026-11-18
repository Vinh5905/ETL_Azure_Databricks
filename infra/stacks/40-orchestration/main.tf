# Stack 40-orchestration: linked service, dataset, pipeline, trigger của ADF.
# Xem docs/devops/06, mục 1. Pipeline gọi job theo ID nên phần pipeline/trigger apply SAU
# lần `bundle deploy -t prod` đầu tiên. Linked service và dataset không cần job ID.
#
# [x] Remote state core + resource group core
# [x] Linked service: ls_adls, ls_kv, ls_sql_retail, ls_zenodo, ls_off_api, ls_dbx (MSI)
# [x] Dataset: publish mọi file adf/datasets/*.json (P2 thêm file ở #144, #183)
# [ ] Pipeline pl_ingest_instacart, pl_ingest_retail_db, pl_ingest_off, pl_master
#     (activities_json = file("${path.module}/../../../adf/pipelines/<tên>.json"))
# [ ] Trigger schedule, activated = var.triggers_on (mặc định false)

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

data "azurerm_resource_group" "core" {
  name = data.terraform_remote_state.core.outputs.rg_name
}

locals {
  core    = data.terraform_remote_state.core.outputs
  adf_id  = local.core.adf_id
  dbw_url = startswith(local.core.databricks_workspace_url, "https://") ? local.core.databricks_workspace_url : "https://${local.core.databricks_workspace_url}"

  # Mỗi file adf/datasets/<tên>.json là resource API {name, properties} do P2 dựng bằng script
  # (adf/README.md). Key lấy từ tên file để for_each biết key lúc plan; precondition bắt name khớp.
  dataset_dir = "${path.module}/../../../adf/datasets"
  datasets = {
    for f in fileset(local.dataset_dir, "*.json") :
    trimsuffix(f, ".json") => jsondecode(file("${local.dataset_dir}/${f}"))
  }
}

# ---------------------------------------------------------------------------
# Linked service. Mọi xác thực Azure đi qua managed identity của ADF; quyền cấp ở
# 10-core bước 3.7 (Storage Blob Data Contributor, Key Vault Secrets User, Contributor workspace).
# ---------------------------------------------------------------------------

resource "azurerm_data_factory_linked_service_key_vault" "kv" {
  name            = "ls_kv"
  data_factory_id = local.adf_id
  key_vault_id    = local.core.key_vault_id
  description     = "Key Vault của dự án, ADF đọc secret bằng managed identity."
}

resource "azurerm_data_factory_linked_service_data_lake_storage_gen2" "adls" {
  name                 = "ls_adls"
  data_factory_id      = local.adf_id
  url                  = "https://${local.core.lake_account_name}.dfs.core.windows.net"
  use_managed_identity = true
  description          = "ADLS Gen2 lakehouse (landing và các tầng), xác thực bằng managed identity."
}

# SQL auth vì server không bật Entra-only (spike_results, quyết định xác thực Azure SQL).
# Connection string không chứa mật khẩu; mật khẩu lấy từ secret sql-admin-password qua ls_kv.
resource "azurerm_data_factory_linked_service_azure_sql_database" "sql_retail" {
  name              = "ls_sql_retail"
  data_factory_id   = local.adf_id
  connection_string = "Integrated Security=False;Encrypt=True;Connection Timeout=60;Data Source=${local.core.sql_server_fqdn};Initial Catalog=${local.core.sql_database_name};User ID=${var.sql_admin_login}"
  description       = "Azure SQL retail_db (nguồn CDC), mật khẩu lấy từ Key Vault."

  key_vault_password {
    linked_service_name = azurerm_data_factory_linked_service_key_vault.kv.name
    secret_name         = "sql-admin-password"
  }
}

# HTTP dùng linked_custom_service kiểu HttpServer: dataset DelimitedText với HttpServerLocation
# cần đúng kiểu này, còn azurerm_data_factory_linked_service_web tạo kiểu Web (Web Table).
resource "azurerm_data_factory_linked_custom_service" "http" {
  for_each = {
    ls_zenodo  = { url = "https://zenodo.org/", desc = "Zenodo record 2654444 (CSV Instacart), anonymous." }
    ls_off_api = { url = "https://world.openfoodfacts.org/", desc = "API Open Food Facts, anonymous. Crawler chính chạy bằng job Databricks (pl_ingest_off)." }
  }

  name            = each.key
  data_factory_id = local.adf_id
  type            = "HttpServer"
  description     = each.value.desc
  type_properties_json = jsonencode({
    url                               = each.value.url
    authenticationType                = "Anonymous"
    enableServerCertificateValidation = true
  })
}

# azurerm_data_factory_linked_service_azure_databricks bắt buộc một trong existing_cluster_id,
# instance_pool, new_cluster_config. Pipeline chỉ dùng activity Databricks Job (job serverless do
# DAB tạo), không cần cluster, nên khai bằng custom service giống spike 3.5 (MSI, không cluster).
resource "azurerm_data_factory_linked_custom_service" "dbx" {
  name            = "ls_dbx"
  data_factory_id = local.adf_id
  type            = "AzureDatabricks"
  description     = "Workspace Databricks, MSI của ADF (Contributor trên workspace), gọi job theo ID."
  type_properties_json = jsonencode({
    domain              = local.dbw_url
    authentication      = "MSI"
    workspaceResourceId = local.core.databricks_workspace_id
  })
}

# ---------------------------------------------------------------------------
# Dataset. Nội dung nằm ở adf/datasets/*.json; thêm/sửa dataset là sửa JSON, không sửa file này.
# ---------------------------------------------------------------------------

# Dùng azapi thay azurerm_data_factory_custom_dataset: azurerm chỉ nhận parameters map(string) nên
# mọi tham số bị gán defaultValue, làm tham số bắt buộc của P2 (folder_path, file_name) thành tùy chọn.
# azapi gửi nguyên properties trong JSON, giữ đúng kiểu và default như hợp đồng.
resource "azapi_resource" "dataset" {
  for_each = local.datasets

  type      = "Microsoft.DataFactory/factories/datasets@2018-06-01"
  name      = each.key
  parent_id = local.adf_id
  body      = { properties = each.value.properties }

  lifecycle {
    precondition {
      condition     = each.value.name == each.key
      error_message = "Trường name trong adf/datasets/${each.key}.json phải trùng tên file."
    }
  }

  # Dataset tham chiếu linked service theo tên, ADF từ chối nếu linked service chưa có.
  depends_on = [
    azurerm_data_factory_linked_service_key_vault.kv,
    azurerm_data_factory_linked_service_data_lake_storage_gen2.adls,
    azurerm_data_factory_linked_service_azure_sql_database.sql_retail,
    azurerm_data_factory_linked_custom_service.http,
    azurerm_data_factory_linked_custom_service.dbx,
  ]
}
