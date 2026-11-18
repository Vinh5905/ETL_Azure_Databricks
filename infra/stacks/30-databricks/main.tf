# Stack 30-databricks: bên trong workspace (Unity Catalog, quyền, policy, warehouse).
# Xem docs/devops/05. Cần 10-core đã có workspace (bước 3.5) trước khi plan.
#
# [x] 1. Xác nhận metastore
# [x] 2. Service principal cho CI (application_id = ci_client_id), user theo vai trò
# [x] 3. Storage credential + external location theo container
# [x] 4. Catalog instacart, 8 schema, volume raw + checkpoints
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

  # Mọi người có ít nhất một vai trong var.roles (key của member_upns, ví dụ "p2").
  members = toset(flatten(values(var.roles)))

  # Container của lake (10-core bước 3.2), mỗi container một external location.
  lake_containers = toset(["landing", "bronze", "silver", "gold", "checkpoints", "mlflow", "ds-data"])

  # URL gốc mỗi container (không có "/" cuối). Đường dẫn con dựng từ đây chứ không từ .url của
  # external location, vì .url trong state là giá trị API trả về: API mà bỏ "/" cuối thì storage_root
  # (ForceNew) đổi theo và plan đòi thay cả catalog lẫn schema.
  lake_url = { for c in local.lake_containers : c => "abfss://${c}@${local.core.lake_account_name}.dfs.core.windows.net" }

  # Schema → container chứa storage_root. Đường dẫn là <container>/<schema>.
  # landing_meta KHÔNG đặt trong landing: volume raw chiếm cả container landing, mà Unity Catalog
  # từ chối volume ngoài bao trùm vùng lưu managed của schema. Bảng của landing_meta (off_crawl_state)
  # nhỏ, nên để chung container bronze.
  schemas = {
    landing_meta      = "bronze"
    bronze            = "bronze"
    silver            = "silver"
    gold              = "gold"
    ml                = "mlflow"  # model registry, bảng kết quả thực nghiệm (docs/ds)
    ds_data           = "ds-data" # dữ liệu DE chuẩn bị cho DS (de/08)
    instacart_sandbox = "silver"  # dữ liệu tổng hợp để thử tải (is_synthetic)
    ops               = "gold"    # benchmark_results, streaming_latency, dq_results
  }
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
  # display_name: SP này do Databricks tự thêm từ Entra (Source = External), workspace giữ tên
  # "Unnamed service principal" và không nhận tên mới, nên bỏ qua để plan về No changes.
  lifecycle {
    prevent_destroy = true
    ignore_changes  = [display_name]
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

  user_name             = var.member_upns[each.key]
  workspace_access      = true
  databricks_sql_access = true
  allow_cluster_create  = false
  force                 = true
}

# ---------------------------------------------------------------------------
# Bước 3 · Storage credential và external location (docs/devops/05 bước 3)
# ---------------------------------------------------------------------------
# Access Connector (10-core bước 3.4) đã có Storage Blob Data Contributor trên lake. Spike 3.4 xác
# nhận workspace admin tạo được storage credential, nên không cần nhánh dự phòng managed storage.
resource "databricks_storage_credential" "lake" {
  name    = "cred-instacart-lake"
  comment = "Access Connector của 10-core, đọc/ghi ADLS lake"

  azure_managed_identity {
    access_connector_id = local.core.access_connector_id
  }
}

# Mỗi container một external location để cấp quyền file theo tầng (bước 5).
resource "databricks_external_location" "layer" {
  for_each = local.lake_containers

  name            = "el-${each.key}"
  url             = "${local.lake_url[each.key]}/"
  credential_name = databricks_storage_credential.lake.name
  comment         = "Container ${each.key} của lake"
}

# ---------------------------------------------------------------------------
# Bước 4 · Catalog, schema, volume (docs/devops/05 bước 4)
# ---------------------------------------------------------------------------
# Metastore không có storage root (spike 3.4) nên catalog phải có storage_root. Đặt ở thư mục riêng
# _catalog thay vì gốc container bronze, để không bao lên storage_root của schema bronze và landing_meta.
# Mọi schema đều có storage_root riêng, nên thư mục này chỉ là mặc định dự phòng.
resource "databricks_catalog" "instacart" {
  name         = "instacart"
  storage_root = "${local.lake_url["bronze"]}/_catalog"
  comment      = "Instacart Market Basket + Open Food Facts + retail_db"

  # Đường dẫn không còn tham chiếu external location, nên khai thứ tự tường minh.
  depends_on = [databricks_external_location.layer]
}

resource "databricks_schema" "s" {
  for_each = local.schemas

  catalog_name = databricks_catalog.instacart.name
  name         = each.key
  storage_root = "${local.lake_url[each.value]}/${each.key}"

  depends_on = [databricks_external_location.layer]
}

# Auto Loader đọc /Volumes/instacart/landing_meta/raw/<nguồn>/ (de/00 §4). Volume trỏ cả container
# landing vì ADF ghi landing/instacart, landing/retail_db, landing/off_api ngay dưới gốc.
resource "databricks_volume" "raw" {
  name             = "raw"
  catalog_name     = databricks_catalog.instacart.name
  schema_name      = databricks_schema.s["landing_meta"].name
  volume_type      = "EXTERNAL"
  storage_location = "${local.lake_url["landing"]}/"
  comment          = "Container landing (ADF ghi, chỉ thêm file)"

  depends_on = [databricks_external_location.layer]
}

# Checkpoint của streaming. Container checkpoints không có storage_root schema nào.
resource "databricks_volume" "checkpoints" {
  name             = "checkpoints"
  catalog_name     = databricks_catalog.instacart.name
  schema_name      = databricks_schema.s["ops"].name
  volume_type      = "EXTERNAL"
  storage_location = "${local.lake_url["checkpoints"]}/"
  comment          = "Checkpoint của streaming"

  depends_on = [databricks_external_location.layer]
}

# Hai volume dưới ds-data dùng thư mục exports, inbox, không chồng storage_root ds-data/ds_data.
# File txt cho Hadoop local (de/08 §5).
resource "databricks_volume" "ds_exports" {
  name             = "exports"
  catalog_name     = databricks_catalog.instacart.name
  schema_name      = databricks_schema.s["ds_data"].name
  volume_type      = "EXTERNAL"
  storage_location = "${local.lake_url["ds-data"]}/exports"
  comment          = "File xuất cho Hadoop MapReduce local"

  depends_on = [databricks_external_location.layer]
}

# Kết quả benchmark local upload lên (ds/03 §7).
resource "databricks_volume" "ml_inbox" {
  name             = "inbox"
  catalog_name     = databricks_catalog.instacart.name
  schema_name      = databricks_schema.s["ml"].name
  volume_type      = "EXTERNAL"
  storage_location = "${local.lake_url["ds-data"]}/inbox"
  comment          = "Kết quả benchmark chạy local"

  depends_on = [databricks_external_location.layer]
}
