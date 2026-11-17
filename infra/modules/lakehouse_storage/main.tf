# ADLS Gen2 cho lakehouse: wrapper quanh AVM storage account (docs/devops/03, bước 3.2).
# AVM 0.11.0 chỉ dùng provider azapi và cần Terraform >= 1.10.

terraform {
  required_version = "~> 1.10"

  required_providers {
    azapi = {
      source  = "Azure/azapi"
      version = "~> 2.12"
    }
  }
}

module "this" {
  source  = "Azure/avm-res-storage-storageaccount/azurerm"
  version = "0.11.0"

  name      = var.name
  parent_id = var.resource_group_id
  location  = var.location

  # AVM 0.11.0 mặc định Standard_ZRS và bỏ qua account_replication_type khi có account_sku_name.
  account_kind     = "StorageV2"
  account_sku_name = "Standard_LRS"
  access_tier      = "Hot"
  is_hns_enabled   = true

  # Chỉ xác thực bằng Entra ID / managed identity, không key, không SAS.
  shared_access_key_enabled       = false
  default_to_oauth_authentication = true
  allow_nested_items_to_be_public = false
  min_tls_version                 = "TLS1_2"

  # Không dùng private endpoint hay VNet injection (quy tắc chi phí), nên mở mạng public.
  # Databricks classic compute, ADF và máy thành viên đi vào bằng IP public; quyền chặn bằng RBAC.
  public_network_access_enabled = true
  network_rules = {
    default_action = "Allow"
    bypass         = ["AzureServices"]
  }

  containers = { for c in var.containers : c => { name = c } }

  storage_management_policy_rule = {
    landing_to_cool = {
      name    = "landing-to-cool"
      enabled = true
      filters = {
        blob_types   = ["blockBlob"]
        prefix_match = ["landing/"]
      }
      actions = {
        base_blob = {
          tier_to_cool_after_days_since_modification_greater_than = var.landing_cool_after_days
        }
      }
    }
  }

  enable_telemetry = false
  tags             = var.tags
}
