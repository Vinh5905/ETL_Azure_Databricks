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
  description = "Map tên vai → Entra Object ID của 4 thành viên, ví dụ { p1 = \"…\" }. Bước 3.6 cần key p1. Local: terraform.auto.tfvars; CI: GitHub Variable TF_VAR_MEMBER_OBJECT_IDS."
  type        = map(string)

  validation {
    condition     = contains(keys(var.member_object_ids), "p1")
    error_message = "member_object_ids phải có key p1 (Entra admin của SQL và Key Vault Secrets Officer)."
  }
}

variable "member_ips" {
  description = "Map tên vai → IP public của thành viên, mở firewall Azure SQL để chạy seed và đăng nhập tay. Local: terraform.auto.tfvars; CI: GitHub Variable TF_VAR_MEMBER_IPS (JSON)."
  type        = map(string)

  validation {
    condition     = alltrue([for ip in values(var.member_ips) : can(cidrhost("${ip}/32", 0))])
    error_message = "Mỗi giá trị trong member_ips phải là một địa chỉ IPv4."
  }
}
