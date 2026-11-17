terraform {
  required_version = "~> 1.10"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
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
