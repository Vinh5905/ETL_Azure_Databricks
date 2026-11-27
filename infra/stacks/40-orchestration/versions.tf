terraform {
  required_version = "~> 1.11"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    azapi = {
      source  = "Azure/azapi"
      version = "~> 2.12"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.60"
    }
  }

  backend "azurerm" {
    key = "orchestration.tfstate"
  }
}

provider "azurerm" {
  features {}
  subscription_id     = var.subscription_id_a
  storage_use_azuread = true
}

# azapi publish dataset ADF nguyên JSON (main.tf). Đăng nhập cùng nguồn với azurerm.
provider "azapi" {
  subscription_id = var.subscription_id_a
}

# Chỉ đọc: tra job ID của job prod do DAB tạo (main.tf, AD-OPS-26). Cấu hình giống 30-databricks.
# Local: `az login` của P1. CI: phiên đăng nhập của azure/login (UAMI).
provider "databricks" {
  host                        = local.core.databricks_workspace_url
  azure_workspace_resource_id = local.core.databricks_workspace_id
  auth_type                   = "azure-cli"
}
