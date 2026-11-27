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
    key = "serving.tfstate"
  }
}

provider "azurerm" {
  features {}
  subscription_id     = var.subscription_id_a
  storage_use_azuread = true
}

# azapi là provider bắt buộc của module AVM storage account. Đăng nhập cùng nguồn với azurerm.
provider "azapi" {
  subscription_id = var.subscription_id_a
}
