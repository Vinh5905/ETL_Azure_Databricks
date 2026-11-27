variable "tfstate_account" {
  description = "Storage account chứa state. CI: TF_VAR_tfstate_account."
  type        = string
}

variable "member_upns" {
  description = "Map tên vai → tài khoản Entra (UPN, dạng <mssv>@ms.uit.edu.vn) mà thành viên dùng để đăng nhập workspace (bước 2). Khác email ở member_emails của 10-core (hộp thư gm, chỉ nhận cảnh báo budget): Databricks trên Azure đăng nhập qua Entra, nên user_name phải là UPN, nếu không Terraform tạo một user thứ hai. Local: terraform.auto.tfvars; CI: GitHub Secret TF_VAR_MEMBER_UPNS (JSON)."
  type        = map(string)
  sensitive   = true

  validation {
    condition     = alltrue([for e in values(var.member_upns) : can(regex("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$", e))])
    error_message = "Mỗi giá trị trong member_upns phải có dạng tên@miền."
  }
}

variable "roles" {
  description = "Map vai trò (de, ds, bi) → danh sách key của member_upns, ví dụ { de = [\"p1\", \"p2\"] }. Chỉ chứa key nên để trong envs/30-databricks.tfvars. Quyền Unity Catalog cấp theo map này (bước 5)."
  type        = map(list(string))

  validation {
    condition     = length(setsubtract(keys(var.roles), ["de", "ds", "bi"])) == 0
    error_message = "roles chỉ nhận các vai de, ds, bi."
  }

  validation {
    condition     = alltrue([for k in flatten(values(var.roles)) : contains(nonsensitive(keys(var.member_upns)), k)])
    error_message = "Mỗi người trong roles phải là một key có trong member_upns."
  }
}

variable "uc_managers" {
  description = "Key của member_upns được cấp MANAGE trên catalog instacart, storage credential và các external location. Dùng khi chuyển subscription (docs/devops/chuyen_sang_sub_d.md §4): SP CI cũ mất đăng nhập thì người này vẫn xóa được các đối tượng trong metastore dùng chung."
  type        = list(string)

  validation {
    condition     = alltrue([for k in var.uc_managers : contains(flatten(values(var.roles)), k)])
    error_message = "Mỗi người trong uc_managers phải có ít nhất một vai trong roles."
  }
}
