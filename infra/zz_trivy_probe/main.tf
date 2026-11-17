# TẠM THỜI (AD-OPS-17): lỗi trivy cố ý để kiểm job trivy chặn merge. Commit kế tiếp xóa thư mục này.
terraform {
  required_version = "~> 1.11"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
}

resource "azurerm_storage_account" "probe" {
  name                     = "stprobe"
  resource_group_name      = "rg-probe"
  location                 = "eastasia"
  account_tier             = "Standard"
  account_replication_type = "LRS"
  min_tls_version          = "TLS1_0"
}
