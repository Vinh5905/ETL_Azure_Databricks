variable "subscription_id_a" {
  description = "Subscription ID của Sub A. Local: terraform.auto.tfvars; CI: TF_VAR_subscription_id_a."
  type        = string
}

variable "location" {
  description = "Region chính đã chốt ở G0."
  type        = string

  validation {
    condition     = length(var.location) > 0
    error_message = "Chưa chốt region ở G0 (infra/envs/10-core.tfvars)."
  }
}

variable "tfstate_account" {
  description = "Storage account chứa state, để đọc output của bootstrap (UAMI CI). Đặt trong infra/envs/10-core.tfvars; CI: TF_VAR_tfstate_account."
  type        = string
}

variable "member_object_ids" {
  description = "Map tên vai → Entra Object ID của 4 thành viên, ví dụ { p1 = \"…\" }. Bước 3.6 cần key p1. Local: terraform.auto.tfvars; CI: GitHub Secret TF_VAR_MEMBER_OBJECT_IDS."
  type        = map(string)

  validation {
    condition     = contains(keys(var.member_object_ids), "p1")
    error_message = "member_object_ids phải có key p1 (Entra admin của SQL và Key Vault Secrets Officer)."
  }
}

variable "member_ips" {
  description = "Map tên vai → IP public của thành viên, mở firewall Azure SQL để chạy seed và đăng nhập tay. Local: terraform.auto.tfvars; CI: GitHub Secret TF_VAR_MEMBER_IPS (JSON)."
  type        = map(string)

  validation {
    condition     = alltrue([for ip in values(var.member_ips) : can(cidrhost("${ip}/32", 0))])
    error_message = "Mỗi giá trị trong member_ips phải là một địa chỉ IPv4."
  }
}

variable "member_emails" {
  description = "Map tên vai → email nhận cảnh báo budget (bước 3.9). Local: terraform.auto.tfvars; CI: GitHub Secret TF_VAR_MEMBER_EMAILS (JSON)."
  type        = map(string)
  sensitive   = true

  validation {
    condition     = alltrue([for e in values(var.member_emails) : can(regex("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$", e))])
    error_message = "Mỗi giá trị trong member_emails phải là một địa chỉ email."
  }
}

variable "databricks_app_object_id" {
  description = "Object ID của service principal ứng dụng AzureDatabricks trong tenant (az ad sp show --id 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --query id -o tsv). Dùng để cấp Key Vault Secrets User cho secret scope kv (30-databricks bước 7). Local: terraform.auto.tfvars; CI: GitHub Secret TF_VAR_DATABRICKS_APP_OBJECT_ID."
  type        = string

  validation {
    condition     = can(regex("^[0-9a-fA-F-]{36}$", var.databricks_app_object_id))
    error_message = "databricks_app_object_id phải là một GUID."
  }
}
