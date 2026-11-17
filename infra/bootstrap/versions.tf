terraform {
  required_version = "~> 1.9"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # Lần apply đầu chạy với local state, sau đó đã chuyển lên blob (docs/devops/02 §3 bước 6).
  # Máy mới chạy: terraform init -backend-config=../backend.hcl -backend-config="key=bootstrap.tfstate"
  backend "azurerm" {}
}

provider "azurerm" {
  features {}
  subscription_id     = var.subscription_id_a
  storage_use_azuread = true
}
