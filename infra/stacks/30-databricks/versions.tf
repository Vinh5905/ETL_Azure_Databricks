terraform {
  required_version = "~> 1.10"

  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.60"
    }
  }

  backend "azurerm" {
    key = "databricks.tfstate"
  }
}

# Local: `az login` của P1. CI: phiên đăng nhập của azure/login (UAMI).
provider "databricks" {
  host                        = local.core.databricks_workspace_url
  azure_workspace_resource_id = local.core.databricks_workspace_id
  auth_type                   = "azure-cli"
}
