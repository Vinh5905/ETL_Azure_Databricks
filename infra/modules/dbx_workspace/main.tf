# Databricks workspace Premium + Access Connector (managed identity cho Unity Catalog).
# Dùng cho workspace sandbox của DS (stack 90-sandbox). Workspace chính ở 10-core dùng module AVM.

terraform {
  required_version = "~> 1.10"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
}

resource "azurerm_databricks_workspace" "this" {
  name                        = var.name
  resource_group_name         = var.resource_group_name
  location                    = var.location
  sku                         = var.sku
  managed_resource_group_name = "${var.resource_group_name}-dbw-managed"

  custom_parameters {
    no_public_ip = false # không dùng VNet injection / NAT (docs/devops/03, bước 3.5)
  }

  tags = var.tags
}

resource "azurerm_databricks_access_connector" "this" {
  name                = "dbac-${var.name}"
  resource_group_name = var.resource_group_name
  location            = var.location

  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
}
