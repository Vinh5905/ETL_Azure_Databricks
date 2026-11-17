locals {
  tags = {
    project    = "instacart-etl"
    env        = "prod"
    owner      = "p1"
    managed_by = "terraform"
    stack      = "bootstrap"
  }

  # Subject của token OIDC mà GitHub phát cho từng loại job.
  # Repo bật use_immutable_subject nên subject có kèm ID số: repo:<owner>@<id>/<repo>@<id>:…
  # Job có `environment: X` nhận subject `…:environment:X` (không phải `ref:…`), nên cần đủ các dòng.
  gh_subjects = {
    main   = "${var.github_sub_prefix}:ref:refs/heads/main"
    pr     = "${var.github_sub_prefix}:pull_request"
    prod   = "${var.github_sub_prefix}:environment:prod"
    dev    = "${var.github_sub_prefix}:environment:dev"
    stream = "${var.github_sub_prefix}:environment:stream" # stream-toggle chạy theo lịch, không cần reviewer
  }
}

# ---------------------------------------------------------------------------
# Storage chứa Terraform state
# ---------------------------------------------------------------------------

resource "azurerm_resource_group" "tfstate" {
  name     = "rg-tfstate"
  location = var.location
  tags     = local.tags
}

resource "random_string" "sfx" {
  length  = 5
  special = false
  upper   = false
}

resource "azurerm_storage_account" "tfstate" {
  name                            = "sttfstate${random_string.sfx.result}"
  resource_group_name             = azurerm_resource_group.tfstate.name
  location                        = azurerm_resource_group.tfstate.location
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false
  shared_access_key_enabled       = false # chỉ cho truy cập bằng Entra ID

  blob_properties {
    versioning_enabled = true # khôi phục state cũ

    delete_retention_policy {
      days = 14
    }

    container_delete_retention_policy {
      days = 14
    }
  }

  tags = local.tags
}

resource "azurerm_storage_container" "tfstate" {
  name                  = "tfstate"
  storage_account_id    = azurerm_storage_account.tfstate.id
  container_access_type = "private"
}

resource "azurerm_management_lock" "tfstate" {
  name       = "lock-tfstate"
  scope      = azurerm_resource_group.tfstate.id
  lock_level = "CanNotDelete"
  notes      = "Chống xóa nhầm nơi chứa Terraform state."
}

# ---------------------------------------------------------------------------
# Danh tính cho GitHub Actions: UAMI + federated credential (OIDC, không secret)
# ---------------------------------------------------------------------------

resource "azurerm_user_assigned_identity" "ci" {
  name                = "id-github-ci"
  resource_group_name = azurerm_resource_group.tfstate.name
  location            = azurerm_resource_group.tfstate.location
  tags                = local.tags
}

resource "azurerm_federated_identity_credential" "gh" {
  for_each = local.gh_subjects

  name                      = "github-${each.key}"
  user_assigned_identity_id = azurerm_user_assigned_identity.ci.id
  issuer                    = "https://token.actions.githubusercontent.com"
  audience                  = ["api://AzureADTokenExchange"]
  subject                   = each.value
}
