variable "tfstate_account" {
  description = "Storage account chứa state. CI: TF_VAR_tfstate_account."
  type        = string
}

variable "member_emails" {
  description = "Map tên vai → email Entra của thành viên, cũng là tài khoản đăng nhập workspace (bước 2). Phải trùng userName trong Databricks (UPN Entra), nếu không Terraform tạo một user thứ hai. Dùng chung secret với 10-core. Local: terraform.auto.tfvars; CI: GitHub Secret TF_VAR_MEMBER_EMAILS (JSON)."
  type        = map(string)
  sensitive   = true

  validation {
    condition     = alltrue([for e in values(var.member_emails) : can(regex("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$", e))])
    error_message = "Mỗi giá trị trong member_emails phải là một địa chỉ email."
  }
}

variable "roles" {
  description = "Map vai trò (de, ds, bi) → danh sách key của member_emails, ví dụ { de = [\"p1\", \"p2\"] }. Chỉ chứa key nên để trong envs/30-databricks.tfvars. Quyền Unity Catalog cấp theo map này (bước 5)."
  type        = map(list(string))

  validation {
    condition     = length(setsubtract(keys(var.roles), ["de", "ds", "bi"])) == 0
    error_message = "roles chỉ nhận các vai de, ds, bi."
  }

  validation {
    condition     = alltrue([for k in flatten(values(var.roles)) : contains(nonsensitive(keys(var.member_emails)), k)])
    error_message = "Mỗi người trong roles phải là một key có trong member_emails."
  }
}
