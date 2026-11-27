# Stack 30-databricks: bên trong workspace (Unity Catalog, quyền, policy, warehouse).
# Xem docs/devops/05. Cần 10-core đã có workspace (bước 3.5) trước khi plan.
#
# [x] 1. Xác nhận metastore
# [x] 2. Service principal cho CI (application_id = ci_client_id), user theo vai trò
# [x] 3. Storage credential + external location theo container
# [x] 4. Catalog instacart, 8 schema, volume raw + checkpoints
# [x] 5. Grants Unity Catalog (một khối databricks_grants cho mỗi securable)
# [x] 6. Cluster policy (single-node, pipeline, ds-benchmark) + tắt unrestricted cluster creation
#        + ghi đè policy có sẵn Personal Compute cho giống single-node (AD-OPS-23)
# [x] 7. Secret scope dựa trên Key Vault (P1 apply từ máy local)
# [x] 8. Serverless SQL warehouse wh-instacart-bi
# [x] 9. Output cho DAB và Power BI

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
# có role Azure trên workspace nên được Databricks tự thêm, và thành admin khi mở workspace bằng
# nút Launch Workspace trên Azure portal.
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

# Thành viên nhóm. allow_cluster_create = false chỉ có tác dụng với người không phải admin. Kiểm
# 2026-11-26 (AD-OPS-23): nhóm admins chỉ có P1 và hai SP, P2–P4 chưa là admin nên chỉ tạo được
# cluster qua policy có CAN_USE. Nhưng thành viên có Contributor trên RG core (10-core), nên mở
# workspace từ Azure portal là thành admin và bỏ qua được entitlement lẫn policy (đánh đổi đã chấp
# nhận ở docs/devops/03). Kỷ luật chi phí vẫn dựa vào policy + budget.
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

# Mỗi container một external location. Thành viên không có grant trên external location:
# truy cập file đi qua volume (bước 5).
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

# ---------------------------------------------------------------------------
# Bước 5 · Quyền Unity Catalog theo vai (docs/devops/05 bước 5)
# ---------------------------------------------------------------------------
# databricks_grants ghi đè mọi grant không khai báo, nên mỗi securable đúng một khối. Một người có
# thể có nhiều vai (p1: de + ds, p4: ds + bi): nếu viết mỗi vai một dynamic grant thì cùng principal
# xuất hiện hai lần, API trả về một mục đã gộp và plan không bao giờ về No changes. Vì vậy quyền
# được gộp theo người trước (local.grants_by_member), mỗi securable chỉ lặp một lần theo người.
# Thành viên dù là workspace admin (xem bước 2) cũng không phải owner hay metastore admin, nên vẫn bị chặn đọc
# dữ liệu nếu không có grant ở đây.
locals {
  all_privileges = ["ALL_PRIVILEGES"]
  read_schema    = ["USE_SCHEMA", "SELECT"]
  rw_volume      = ["READ_VOLUME", "WRITE_VOLUME"]

  # Bảng quyền ở docs/devops/05 bước 5: securable → vai → privilege.
  role_privileges = {
    catalog = {
      de = ["USE_CATALOG", "CREATE_SCHEMA"] # CREATE_SCHEMA cho schema dev <user>_* (de/02 §2.2)
      ds = ["USE_CATALOG"]
      bi = ["USE_CATALOG"]
    }
    landing_meta      = { de = local.all_privileges }
    bronze            = { de = local.all_privileges }
    silver            = { de = local.all_privileges, ds = local.read_schema }
    gold              = { de = local.all_privileges, ds = local.read_schema, bi = local.read_schema }
    ml                = { de = local.read_schema, ds = local.all_privileges }
    ds_data           = { de = local.all_privileges, ds = local.read_schema }
    ops               = { de = local.all_privileges, ds = local.read_schema, bi = local.read_schema }
    instacart_sandbox = { de = local.all_privileges, ds = local.all_privileges }
    raw               = { de = local.rw_volume }
    checkpoints       = { de = local.rw_volume }
    exports           = { de = local.rw_volume, ds = ["READ_VOLUME"] }
    inbox             = { ds = local.rw_volume }
  }

  # securable → key người → hợp privilege của mọi vai người đó có. ALL_PRIVILEGES đã bao các
  # privilege khác nên rút gọn còn một mục. Người không có quyền gì trên securable thì bỏ ra.
  grants_by_member = {
    for sec, by_role in local.role_privileges : sec => {
      for m, privs in {
        for m in local.members : m => distinct(flatten([
          for role, keys in var.roles : contains(keys, m) ? lookup(by_role, role, []) : []
        ]))
      } : m => (contains(privs, "ALL_PRIVILEGES") ? local.all_privileges : sort(privs)) if length(privs) > 0
    }
  }

  # SP CI (deploy bundle, chạy job prod): ALL_PRIVILEGES trên mọi schema dữ liệu, trừ sandbox.
  # SP CI là owner của mọi securable ở đây (nó tạo ra), grant tường minh để không phụ thuộc owner.
  ci_schemas = setsubtract(keys(local.schemas), ["instacart_sandbox"])

  volumes = {
    raw         = databricks_volume.raw.id
    checkpoints = databricks_volume.checkpoints.id
    exports     = databricks_volume.ds_exports.id
    inbox       = databricks_volume.ml_inbox.id
  }
}

resource "databricks_grants" "catalog" {
  catalog = databricks_catalog.instacart.name

  dynamic "grant" {
    for_each = local.grants_by_member["catalog"]
    content {
      principal = databricks_user.members[grant.key].user_name
      # MANAGE cho uc_managers: xóa được catalog khi SP CI cũ (owner) mất đăng nhập. MANAGE áp xuống
      # mọi schema, bảng, volume bên trong (docs/devops/chuyen_sang_sub_d.md §4).
      privileges = contains(var.uc_managers, grant.key) ? concat(grant.value, ["MANAGE"]) : grant.value
    }
  }

  grant {
    principal  = databricks_service_principal.ci.application_id
    privileges = ["CREATE_SCHEMA", "USE_CATALOG"]
  }
}

resource "databricks_grants" "schema" {
  for_each = local.schemas

  schema = databricks_schema.s[each.key].id

  dynamic "grant" {
    for_each = local.grants_by_member[each.key]
    content {
      principal  = databricks_user.members[grant.key].user_name
      privileges = grant.value
    }
  }

  dynamic "grant" {
    for_each = contains(local.ci_schemas, each.key) ? [1] : []
    content {
      principal  = databricks_service_principal.ci.application_id
      privileges = local.all_privileges
    }
  }
}

# Volume nằm trong schema mà vai đã có ALL_PRIVILEGES thì quyền đã kế thừa; khai lại để bảng quyền
# đọc được ngay trong code. Riêng exports là nơi DS chỉ có SELECT trên ds_data nên cần READ_VOLUME.
resource "databricks_grants" "volume" {
  for_each = local.volumes

  volume = each.value

  dynamic "grant" {
    for_each = local.grants_by_member[each.key]
    content {
      principal  = databricks_user.members[grant.key].user_name
      privileges = grant.value
    }
  }

  grant {
    principal  = databricks_service_principal.ci.application_id
    privileges = local.rw_volume
  }
}

# Storage credential và external location do SP CI sở hữu, thành viên không có grant nào (bước 3).
# Chỉ cấp MANAGE cho uc_managers, để xóa được chúng trong metastore dùng chung khi chuyển subscription.
# MANAGE không kèm READ FILES hay WRITE FILES, nên không mở đường đọc file ngoài volume.
resource "databricks_grants" "storage_credential" {
  storage_credential = databricks_storage_credential.lake.name

  dynamic "grant" {
    for_each = toset(var.uc_managers)
    content {
      principal  = databricks_user.members[grant.key].user_name
      privileges = ["MANAGE"]
    }
  }
}

resource "databricks_grants" "external_location" {
  for_each = databricks_external_location.layer

  external_location = each.value.name

  dynamic "grant" {
    for_each = toset(var.uc_managers)
    content {
      principal  = databricks_user.members[grant.key].user_name
      privileges = ["MANAGE"]
    }
  }
}

# ---------------------------------------------------------------------------
# Bước 6 · Cluster policy và chặn tạo cluster tự do (docs/devops/05 bước 6, de/02 §2.1)
# ---------------------------------------------------------------------------
# Quota khoảng 4 vCPU nên mọi cluster phải là single-node Standard_D4ds_v4 (spike 3.4). Bundle tra
# policy theo tên, nên đổi tên policy là phải sửa bundles/ cùng lúc.
# Job cluster không dùng apply_policy_default_values (lỗi "Automated clusters do not support
# autotermination"), nên phải tự khai đủ mọi trường fixed dưới đây (bundles/AGENTS.md).
# autotermination_minutes có minValue để cluster tương tác không tắt tự động (giá trị 0) bị từ chối. Job cluster
# không khai trường này vẫn qua policy, cluster mang autotermination 0 và tắt khi job xong (thử ở AD-OPS-21b).
# Admin workspace bỏ qua được policy và entitlement. P2–P4 chưa thuộc admins (kiểm 2026-11-26, xem bước 2).
# Tag người dùng của cluster là cost_owner, không phải owner: workspace mang Azure tag owner = p1 (naming_tags),
# Databricks chép tag workspace xuống mọi cluster làm tag mặc định, nên custom_tags.owner bị đổi tên và policy
# báo thiếu tag (lỗi de-deploy prod của job seed_retail_db, 2026-11-18).
locals {
  node_type_id  = "Standard_D4ds_v4" # trùng biến node_type_id trong bundles/databricks.yml
  spark_version = "15.4.x-scala2.12" # DBR 15.4 LTS, khớp pyspark 3.5 ở pyproject

  # Phần chung của policy single-node và ds-benchmark; hai policy chỉ khác spark.master.
  single_node_policy = {
    "spark_version"                               = { type = "fixed", value = local.spark_version }
    "node_type_id"                                = { type = "fixed", value = local.node_type_id }
    "num_workers"                                 = { type = "fixed", value = 0 }
    "spark_conf.spark.databricks.cluster.profile" = { type = "fixed", value = "singleNode" }
    "custom_tags.ResourceClass"                   = { type = "fixed", value = "SingleNode" }
    "autotermination_minutes"                     = { type = "range", minValue = 10, maxValue = 20, defaultValue = 15 }
    "custom_tags.cost_owner"                      = { type = "unlimited", isOptional = false }
    "data_security_mode"                          = { type = "fixed", value = "SINGLE_USER" } # workspace không tự chọn access mode
    "instance_pool_id"                            = { type = "forbidden" }                    # VM idle trong pool vẫn chiếm quota
  }
}

locals {
  # instacart-single-node đầy đủ, gồm cả spark.master cho chế độ local.
  single_node_local_policy = merge(local.single_node_policy, {
    "spark_conf.spark.master" = { type = "fixed", value = "local[*]" }
  })
}

resource "databricks_cluster_policy" "single_node" {
  name       = "instacart-single-node"
  definition = jsonencode(local.single_node_local_policy)
}

# Policy có sẵn "Personal Compute" (họ personal-vm) được cấp cho mọi người dùng qua cài đặt Personal
# Compute ở cấp account, nên P2 (không phải admin) vẫn chọn được dù ACL của workspace chỉ có admins
# (AD-OPS-23, 2026-11-26). Bản gốc cho máy tới 16 vCPU và tự tắt "unlimited", mặc định 3 ngày.
# Ghi đè bằng Terraform thay vì đặt Delegate ở account console: không phụ thuộc tài khoản account
# admin và giữ được khi đổi tài khoản. Chỉ có tác dụng với người chưa là admin.
# Databricks không cho sửa kiểu cluster, chế độ cluster và access mode của policy này: ghi đè
# data_security_mode bị từ chối khi apply ("Overriding data_security_mode is forbidden for this default
# policy", tf-apply 2026-11-26). Vì vậy chỉ ghi đè máy, runtime, tự tắt và tag cost_owner. Phần còn lại
# họ personal-vm đã tự khóa: num_workers = 0, profile singleNode, cấm instance pool. Access mode giữ
# mặc định Auto của họ (ra Dedicated, tương đương SINGLE_USER).
# Provider nhận policy có sẵn theo cặp policy_family_id + name: create sửa policy cũ (không cần
# import), destroy chỉ gỡ phần ghi đè, không xóa policy.
resource "databricks_cluster_policy" "personal_compute" {
  name             = "Personal Compute"
  policy_family_id = "personal-vm"
  policy_family_definition_overrides = jsonencode({
    for k, v in local.single_node_policy : k => v
    if contains(["spark_version", "node_type_id", "autotermination_minutes", "custom_tags.cost_owner"], k)
  })
}

# Thực nghiệm E5 của DS (ds/04) đo speedup theo số core nên cho chọn spark.master; máy và runtime
# vẫn cố định để số đo so được với nhau.
resource "databricks_cluster_policy" "ds_benchmark" {
  name = "instacart-ds-benchmark"
  definition = jsonencode(merge(local.single_node_policy, {
    "spark_conf.spark.master" = {
      type         = "allowlist"
      values       = ["local[1]", "local[2]", "local[4]", "local[*]"]
      defaultValue = "local[*]"
    }
  }))
}

# Cluster do Lakeflow (LDP) tạo chỉ nhận policy loại dlt. Runtime do channel của pipeline quyết định
# nên không cố định spark_version; autotermination không áp cho cluster pipeline, thay bằng
# pipelines.clusterShutdown.delay trong configuration của pipeline.
# num_workers = 0 mà thiếu profile singleNode thì cluster không có executor; spike 3.4 chạy được nhờ
# khai profile này, nên pipeline trong bundle phải khai spark_conf giống mẫu ở de/02 §2.2.
resource "databricks_cluster_policy" "pipeline_single_node" {
  name = "instacart-pipeline-single-node"
  definition = jsonencode({
    "cluster_type"                                = { type = "fixed", value = "dlt" }
    "node_type_id"                                = { type = "fixed", value = local.node_type_id }
    "num_workers"                                 = { type = "fixed", value = 0 }
    "spark_conf.spark.databricks.cluster.profile" = { type = "fixed", value = "singleNode" }
    "custom_tags.cost_owner"                      = { type = "unlimited", isOptional = false }
    "instance_pool_id"                            = { type = "forbidden" }
  })
}

locals {
  # Policy → người được CAN_USE. ds-benchmark chỉ cho vai ds; SP CI dùng được cả ba (job prod).
  policies = {
    single_node  = { id = databricks_cluster_policy.single_node.id, users = local.members }
    pipeline     = { id = databricks_cluster_policy.pipeline_single_node.id, users = local.members }
    ds_benchmark = { id = databricks_cluster_policy.ds_benchmark.id, users = toset(lookup(var.roles, "ds", [])) }
  }
}

# databricks_permissions cũng ghi đè toàn bộ: mỗi policy đúng một khối.
resource "databricks_permissions" "policy_use" {
  for_each = local.policies

  cluster_policy_id = each.value.id

  dynamic "access_control" {
    for_each = each.value.users
    content {
      user_name        = databricks_user.members[access_control.value].user_name
      permission_level = "CAN_USE"
    }
  }

  access_control {
    service_principal_name = databricks_service_principal.ci.application_id
    permission_level       = "CAN_USE"
  }
}

# Không quản entitlement của nhóm users bằng Terraform: workspace này từ chối sửa entitlement của
# nhóm users và admins ("not permitted for this workspace", tf-apply AD-OPS-21). Nhóm users ở đây
# vốn không có entitlement nào (kể cả allow_cluster_create), nên quyền tạo cluster tự do chỉ đến từ
# entitlement của từng principal: thành viên, SP CI và SP Function đều đặt allow_cluster_create = false
# ở bước 2. Người không phải admin vì vậy chỉ tạo được cluster qua policy có CAN_USE.

# ---------------------------------------------------------------------------
# Bước 7 · Secret scope dựa trên Key Vault (docs/devops/05 bước 7)
# ---------------------------------------------------------------------------
# Notebook đọc bí mật bằng dbutils.secrets.get("kv", "<tên secret>"); giá trị nằm ở Key Vault của 10-core.
# Databricks đọc vault bằng ứng dụng AzureDatabricks (role Key Vault Secrets User cấp ở 10-core,
# azurerm_role_assignment.kv_databricks).
#
# API tạo scope dựa trên Key Vault chỉ nhận token Entra của người dùng, không nhận service principal
# hay managed identity. Vì vậy P1 tạo scope từ máy, trước khi merge PR thêm resource này:
#   terraform apply -target=databricks_secret_scope.kv -var-file=../../envs/30-databricks.tfvars
# ignore_changes = all để CI chỉ đọc lại scope, không cố tạo hay sửa nó.
resource "databricks_secret_scope" "kv" {
  name = "kv"

  keyvault_metadata {
    resource_id = local.core.key_vault_id
    dns_name    = local.core.key_vault_uri
  }

  lifecycle {
    ignore_changes = all
  }
}

# Vai de và ds đọc được scope, cộng SP CI (job prod). P1 là người tạo scope (ACL MANAGE), nên giữ
# MANAGE: PUT ACL READ cho cùng principal sẽ hạ quyền đó xuống.
locals {
  secret_readers = toset(concat(lookup(var.roles, "de", []), lookup(var.roles, "ds", [])))
}

resource "databricks_secret_acl" "kv_member" {
  for_each = local.secret_readers

  scope      = databricks_secret_scope.kv.name
  principal  = databricks_user.members[each.key].user_name
  permission = each.key == "p1" ? "MANAGE" : "READ"
}

resource "databricks_secret_acl" "kv_ci" {
  scope      = databricks_secret_scope.kv.name
  principal  = databricks_service_principal.ci.application_id
  permission = "READ"
}

# ---------------------------------------------------------------------------
# Bước 8 · Serverless SQL warehouse cho Power BI (docs/devops/05 bước 8)
# ---------------------------------------------------------------------------
# Serverless không chạy VM trong Sub A nên không tính vào quota 4 vCPU. Spike 3.4: wh-spike 2X-Small
# lên RUNNING sau 6 giây. Một cluster, tự tắt sau 2 phút không có truy vấn. UI chỉ cho chọn từ 5 phút,
# API/Terraform cho serverless đặt thấp hơn. Giảm 10 → 5 → 2 phút vì serverless SQL chiếm khoảng 2/3
# chi phí tuần đầu (docs/devops/chi_phi_quota.md §5); khởi động lại chỉ mất vài giây.
resource "databricks_sql_endpoint" "bi" {
  name                      = "wh-instacart-bi"
  cluster_size              = "2X-Small"
  min_num_clusters          = 1
  max_num_clusters          = 1
  auto_stop_mins            = 2
  enable_serverless_compute = true
  warehouse_type            = "PRO"

  tags {
    custom_tags {
      key   = "cost_owner" # không dùng "owner": trùng Azure tag của workspace (xem bước 6)
      value = "p3"         # P3 lo Power BI
    }
  }
}

# Mọi thành viên có vai được CAN_USE (bi đọc gold, de và ds chạy truy vấn kiểm tra); P1 CAN_MANAGE.
# SP CI là người tạo warehouse nên provider tự giữ quyền CAN_MANAGE của nó.
resource "databricks_permissions" "warehouse_use" {
  sql_endpoint_id = databricks_sql_endpoint.bi.id

  dynamic "access_control" {
    for_each = local.members
    content {
      user_name        = databricks_user.members[access_control.value].user_name
      permission_level = access_control.value == "p1" ? "CAN_MANAGE" : "CAN_USE"
    }
  }
}
