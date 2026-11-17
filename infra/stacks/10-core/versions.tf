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
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # Phần còn lại lấy từ infra/backend.hcl:
  #   terraform init -backend-config=../../backend.hcl
  backend "azurerm" {
    key = "core.tfstate"
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy = false
    }
  }
  subscription_id     = var.subscription_id_a
  storage_use_azuread = true
}

# azapi dùng cho module AVM storage, Key Vault, Databricks workspace và DB retail_db (free offer). Đăng nhập theo cùng nguồn với azurerm (az login hoặc ARM_* trong CI).
provider "azapi" {
  subscription_id = var.subscription_id_a
}
