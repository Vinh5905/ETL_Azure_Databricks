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

  backend "azurerm" {
    key = "stream.tfstate"
  }
}

# Sub B: nơi tạo Event Hubs.
provider "azurerm" {
  features {}
  subscription_id = var.subscription_id_b
}

# azapi là provider bắt buộc của module AVM Event Hubs. Đăng nhập cùng nguồn với azurerm.
provider "azapi" {
  subscription_id = var.subscription_id_b
}

# Sub A: chỉ để ghi SAS của Event Hubs vào Key Vault core.
# Không purge khi destroy (giống 10-core): secret chỉ bị xóa mềm, lần apply sau
# provider khôi phục lại (recover_soft_deleted_secrets mặc định true) rồi ghi giá trị mới.
provider "azurerm" {
  alias = "core"
  features {
    key_vault {
      purge_soft_delete_on_destroy = false
      recover_soft_deleted_secrets = true
    }
  }
  subscription_id = var.subscription_id_a
}
