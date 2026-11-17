terraform {
  required_version = "~> 1.11"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }

  # Mỗi thành viên một state:
  #   terraform init -backend-config=../../backend.hcl -backend-config="key=sandbox-<member>.tfstate"
  backend "azurerm" {}
}

provider "azurerm" {
  features {}
  subscription_id = var.subscription_id_sandbox
}
