# Tên theo quy ước CAF và tag chuẩn cho mọi stack, để Cost Management lọc theo owner/stack.

terraform {
  required_version = "~> 1.11"
}

module "naming" {
  source  = "Azure/naming/azurerm"
  version = "0.4.2"
  suffix  = [var.project, var.env]
}

locals {
  tags = merge(
    {
      project    = var.project
      env        = var.env
      owner      = var.owner
      stack      = var.stack
      managed_by = "terraform"
    },
    var.extra_tags,
  )
}
