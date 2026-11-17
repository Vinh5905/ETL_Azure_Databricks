terraform {
  required_version = "~> 1.11"

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

  backend "azurerm" {
    key = "stream.tfstate"
  }
}

# Sub B: nơi tạo Event Hubs.
provider "azurerm" {
  features {}
  subscription_id = var.subscription_id_b
}

# Sub A: chỉ để ghi SAS của Event Hubs vào Key Vault core.
# Alias chưa có resource dùng tới cho đến bước ghi secret (AD-OPS-24), nên tạm tắt
# rule unused của tflint. Xóa dòng ignore khi đã có resource `provider = azurerm.core`.
# tflint-ignore: terraform_unused_declarations
provider "azurerm" {
  alias = "core"
  features {}
  subscription_id = var.subscription_id_a
}
