# Stack 40-orchestration: linked service, dataset, pipeline, trigger của ADF.
# Xem docs/devops/06, mục 1. Pipeline gọi job theo ID nên phần pipeline/trigger apply SAU
# lần `bundle deploy -t prod` đầu tiên. Linked service và dataset không cần job ID.
#
# [x] Remote state core + resource group core
# [x] Linked service: ls_adls, ls_kv, ls_sql_retail, ls_zenodo, ls_off_api, ls_dbx (MSI)
# [x] Dataset: publish mọi file adf/datasets/*.json (P2 thêm file ở #144, #183)
# [x] Pipeline ingest không gọi Databricks: mọi adf/pipelines/*.json trừ pl_ingest_off, pl_master,
#     kèm adf/pipeline_settings/<tên>.json; xếp tầng theo Execute Pipeline (AD-OPS-25b)
# [ ] Pipeline pl_ingest_off, pl_master (gọi job theo ID) và trigger schedule,
#     activated = var.triggers_on, mặc định false (AD-OPS-26)

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

  # Pipeline ingest (AD-OPS-25b): adf/pipelines/<tên>.json là mảng activities, adf/pipeline_settings/<tên>.json
  # là {concurrency, parameters}. pl_ingest_off, pl_master gọi job Databricks theo ID nên để AD-OPS-26.
  pipeline_dir      = "${path.module}/../../../adf/pipelines"
  pipeline_settings = "${path.module}/../../../adf/pipeline_settings"
  pipeline_excluded = ["pl_ingest_off", "pl_master"]
  pipeline_names = [
    for f in fileset(local.pipeline_dir, "*.json") : trimsuffix(f, ".json")
    if !contains(local.pipeline_excluded, trimsuffix(f, ".json"))
  ]
  pipeline_text = { for n in local.pipeline_names : n => file("${local.pipeline_dir}/${n}.json") }

  # Tham số ADF đều là String (adf/README.md); giữ đúng giá trị mặc định trong settings.
  pipelines = {
    for n in local.pipeline_names : n => {
      activities  = jsondecode(local.pipeline_text[n])
      concurrency = jsondecode(file("${local.pipeline_settings}/${n}.json")).concurrency
      parameters = {
        for k, v in jsondecode(file("${local.pipeline_settings}/${n}.json")).parameters :
        k => { type = "String", defaultValue = tostring(v) }
      }
    }
  }

  # ADF từ chối pipeline tham chiếu pipeline chưa có, nên xếp tầng theo Execute Pipeline: tầng 0 không gọi
  # pipeline nào, tầng k chỉ gọi pipeline ở tầng < k. Tên pipeline theo quy ước bắt đầu bằng "pl_", còn
  # dataset là "ds_", linked service là "ls_", nên đọc referenceName "pl_*" là đủ.
  pipeline_refs = {
    for n, t in local.pipeline_text : n => distinct(flatten(regexall("\"referenceName\"\\s*:\\s*\"(pl_[A-Za-z0-9_]+)\"", t)))
  }
  pipeline_tier0 = [for n in local.pipeline_names : n if length(local.pipeline_refs[n]) == 0]
  pipeline_tier1 = [
    for n in local.pipeline_names : n
    if !contains(local.pipeline_tier0, n) && length(setsubtract(local.pipeline_refs[n], local.pipeline_tier0)) == 0
  ]
  pipeline_tier2 = [
    for n in local.pipeline_names : n
    if !contains(concat(local.pipeline_tier0, local.pipeline_tier1), n)
    && length(setsubtract(local.pipeline_refs[n], concat(local.pipeline_tier0, local.pipeline_tier1))) == 0
  ]
  # Pipeline gọi pipeline bị loại, pipeline không có trong repo, tự gọi/gọi vòng, hoặc lồng sâu hơn 3 tầng.
  # Khi một pipeline đổi tầng (vd. con gọi thêm pipeline mới), địa chỉ resource đổi theo: PR đó phải kèm
  # moved { from = azapi_resource.pipeline_tier0["x"]  to = azapi_resource.pipeline_tier1["x"] }, nếu không
  # plan sẽ xóa rồi tạo lại cùng tên.
  pipeline_unplaced = setsubtract(local.pipeline_names, concat(local.pipeline_tier0, local.pipeline_tier1, local.pipeline_tier2))
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

# ---------------------------------------------------------------------------
# Pipeline ingest không gọi Databricks (AD-OPS-25b). Thêm/sửa pipeline là sửa JSON trong adf/pipelines và
# adf/pipeline_settings, không sửa file này. Ba resource ứng với ba tầng Execute Pipeline ở locals.
# Dùng azapi như dataset: gửi nguyên activities, tham số giữ kiểu String và defaultValue như settings.
# ---------------------------------------------------------------------------

resource "azapi_resource" "pipeline_tier0" {
  for_each = toset(local.pipeline_tier0)

  type      = "Microsoft.DataFactory/factories/pipelines@2018-06-01"
  name      = each.key
  parent_id = local.adf_id
  body      = { properties = local.pipelines[each.key] }

  # Schema nhúng của azapi lệch API ADF (bắt fileExtension khi dataset đã có tên file, không biết
  # httpRequestTimeout của HttpReadSettings). ADF vẫn kiểm activities phía server lúc apply.
  schema_validation_enabled = false

  # Activity tham chiếu dataset và linked service theo tên.
  depends_on = [azapi_resource.dataset]
}

resource "azapi_resource" "pipeline_tier1" {
  for_each = toset(local.pipeline_tier1)

  type                      = "Microsoft.DataFactory/factories/pipelines@2018-06-01"
  name                      = each.key
  parent_id                 = local.adf_id
  body                      = { properties = local.pipelines[each.key] }
  schema_validation_enabled = false

  depends_on = [azapi_resource.dataset, azapi_resource.pipeline_tier0]
}

resource "azapi_resource" "pipeline_tier2" {
  for_each = toset(local.pipeline_tier2)

  type                      = "Microsoft.DataFactory/factories/pipelines@2018-06-01"
  name                      = each.key
  parent_id                 = local.adf_id
  body                      = { properties = local.pipelines[each.key] }
  schema_validation_enabled = false

  depends_on = [azapi_resource.dataset, azapi_resource.pipeline_tier1]
}
