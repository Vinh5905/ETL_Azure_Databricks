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
