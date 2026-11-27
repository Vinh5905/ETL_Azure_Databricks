# Stack 40-orchestration: linked service, dataset, pipeline, trigger của ADF.
# Xem docs/devops/06, mục 1. Pipeline gọi job theo ID nên stack chỉ plan được SAU lần
# `bundle deploy -t prod` đầu tiên: data source databricks_job báo lỗi khi chưa có job prod.
#
# [x] Remote state core + resource group core
# [x] Linked service: ls_adls, ls_kv, ls_sql_retail, ls_zenodo, ls_off_api, ls_dbx (MSI)
# [x] Dataset: publish mọi file adf/datasets/*.json (P2 thêm file ở #144, #183)
# [x] Pipeline ingest không gọi Databricks: mọi adf/pipelines/*.json kèm adf/pipeline_settings/<tên>.json;
#     xếp tầng theo Execute Pipeline (AD-OPS-25b)
# [x] Pipeline pl_ingest_off, pl_master: job ID tra bằng data source databricks_job theo tên job prod,
#     điền vào default của off_job_id, batch_job_id; pl_master ở tầng 3 (AD-OPS-26)
# [x] Trigger tr_master_daily: pl_master 02:00 hằng ngày (SE Asia Standard Time),
#     activated = var.triggers_on, mặc định false (AD-OPS-26)
# [ ] PR đặt triggers_on = true trong infra/envs/40-orchestration.tfvars, chỉ mở sau khi pl_master
#     chạy tay đạt (adf/README.md "Nghiệm thu sau khi P1 publish", docs/devops/06 mục 3 bước 8)

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

  # Pipeline (AD-OPS-25b, AD-OPS-26): adf/pipelines/<tên>.json là mảng activities,
  # adf/pipeline_settings/<tên>.json là {concurrency, parameters}.
  pipeline_dir      = "${path.module}/../../../adf/pipelines"
  pipeline_settings = "${path.module}/../../../adf/pipeline_settings"
  pipeline_names    = [for f in fileset(local.pipeline_dir, "*.json") : trimsuffix(f, ".json")]
  pipeline_text     = { for n in local.pipeline_names : n => file("${local.pipeline_dir}/${n}.json") }

  # Pipeline gọi job Databricks nhận job ID qua tham số String, repo để trống (adf/README.md, mục
  # "Điều phối batch"). Khóa "<pipeline>.<tham số>" => tên job prod. Target prod của DAB là mode
  # production nên không thêm tiền tố: tên prod trùng `name` trong bundles/resources/*.yml. Job dev
  # mang tiền tố "[dev <user>]" nên không bị tra nhầm. Đổi `name` job prod thì sửa map này cùng PR.
  pipeline_job_names = {
    "pl_ingest_off.off_job_id" = "instacart-off-crawler"     # bundles/resources/jobs_ingest.yml
    "pl_master.batch_job_id"   = "instacart-batch-medallion" # bundles/resources/jobs_batch.yml
  }
  pipeline_job_ids = { for k, j in data.databricks_job.prod : k => tostring(j.id) }

  # Tham số ADF đều là String (adf/README.md); giữ đúng giá trị mặc định trong settings, trừ tham số
  # job ID ở trên được thay bằng ID tra từ workspace.
  pipelines = {
    for n in local.pipeline_names : n => {
      activities  = jsondecode(local.pipeline_text[n])
      concurrency = jsondecode(file("${local.pipeline_settings}/${n}.json")).concurrency
      parameters = {
        for k, v in jsondecode(file("${local.pipeline_settings}/${n}.json")).parameters :
        k => { type = "String", defaultValue = lookup(local.pipeline_job_ids, "${n}.${k}", tostring(v)) }
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
  # pl_master gọi pl_ingest_retail_db (tầng 2) nên nằm ở tầng 3.
  pipeline_tier3 = [
    for n in local.pipeline_names : n
    if !contains(concat(local.pipeline_tier0, local.pipeline_tier1, local.pipeline_tier2), n)
    && length(setsubtract(local.pipeline_refs[n], concat(local.pipeline_tier0, local.pipeline_tier1, local.pipeline_tier2))) == 0
  ]
  # Pipeline gọi pipeline không có trong repo, tự gọi/gọi vòng, hoặc lồng sâu hơn 4 tầng.
  # Khi một pipeline đổi tầng (vd. con gọi thêm pipeline mới), địa chỉ resource đổi theo: PR đó phải kèm
  # moved { from = azapi_resource.pipeline_tier0["x"]  to = azapi_resource.pipeline_tier1["x"] }, nếu không
  # plan sẽ xóa rồi tạo lại cùng tên.
  pipeline_unplaced = setsubtract(local.pipeline_names, concat(local.pipeline_tier0, local.pipeline_tier1, local.pipeline_tier2, local.pipeline_tier3))
}

# Chỉ đọc job prod do DAB tạo (de-deploy prod). Chưa có job prod thì plan lỗi ở đây: chạy
# `bundle deploy -t prod` (merge PR bundles → de-deploy) trước khi apply stack này.
data "databricks_job" "prod" {
  for_each = local.pipeline_job_names

  job_name = each.value

  lifecycle {
    precondition {
      condition     = contains(keys(try(jsondecode(file("${local.pipeline_settings}/${split(".", each.key)[0]}.json")).parameters, {})), split(".", each.key)[1])
      error_message = "adf/pipeline_settings/${split(".", each.key)[0]}.json phải có tham số ${split(".", each.key)[1]} để nhận job ID của ${each.value}."
    }
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

# ---------------------------------------------------------------------------
# Pipeline (AD-OPS-25b, AD-OPS-26). Thêm/sửa pipeline là sửa JSON trong adf/pipelines và
# adf/pipeline_settings, không sửa file này. Bốn resource ứng với bốn tầng Execute Pipeline ở locals.
# pl_ingest_off không gọi pipeline nào nên ở tầng 0; pl_master ở tầng 3.
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

resource "azapi_resource" "pipeline_tier3" {
  for_each = toset(local.pipeline_tier3)

  type                      = "Microsoft.DataFactory/factories/pipelines@2018-06-01"
  name                      = each.key
  parent_id                 = local.adf_id
  body                      = { properties = local.pipelines[each.key] }
  schema_validation_enabled = false

  depends_on = [azapi_resource.dataset, azapi_resource.pipeline_tier2]
}

# ---------------------------------------------------------------------------
# Trigger (AD-OPS-26). pl_master chạy 02:00 hằng ngày giờ Việt Nam (docs/devops/06 mục 1), dùng
# default của tham số (job ID đã điền ở trên). Tạo ở trạng thái tắt; bật bằng PR đặt triggers_on = true
# sau khi pl_master chạy tay đạt. start_time để provider lấy thời điểm tạo (giờ UTC), lịch tính theo time_zone.
# ---------------------------------------------------------------------------

resource "azurerm_data_factory_trigger_schedule" "master_daily" {
  name            = "tr_master_daily"
  data_factory_id = local.adf_id
  description     = "pl_master hằng ngày 02:00 (SE Asia Standard Time). Bật/tắt qua var.triggers_on."
  # Tra theo tên trong mọi tầng để trigger vẫn trỏ đúng khi pl_master đổi tầng.
  pipeline_name = merge(
    azapi_resource.pipeline_tier0,
    azapi_resource.pipeline_tier1,
    azapi_resource.pipeline_tier2,
    azapi_resource.pipeline_tier3,
  )["pl_master"].name
  activated = var.triggers_on

  frequency = "Day"
  interval  = 1
  time_zone = "SE Asia Standard Time"
  schedule {
    hours   = [2]
    minutes = [0]
  }
}
