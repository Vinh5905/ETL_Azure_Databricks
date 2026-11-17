variable "name" {
  description = "Tên storage account, lấy từ module.nt.names.storage_account.name_unique."
  type        = string
}

variable "resource_group_id" {
  description = "ID của resource group chứa storage account."
  type        = string
}

variable "location" {
  description = "Region của storage account."
  type        = string
}

variable "containers" {
  description = "Container theo tầng dữ liệu (docs/devops/03, bước 3.2). Azure bắt tên dài 3–63 ký tự."
  type        = list(string)
  default     = ["landing", "bronze", "silver", "gold", "checkpoints", "mlflow", "ds-data"]

  validation {
    condition     = alltrue([for c in var.containers : can(regex("^[a-z0-9](-?[a-z0-9])+$", c)) && length(c) >= 3 && length(c) <= 63])
    error_message = "Tên container chỉ gồm chữ thường, số, gạch nối đơn, dài 3–63 ký tự."
  }

  validation {
    condition     = contains(var.containers, "landing")
    error_message = "Phải có container landing, vì lifecycle rule landing-to-cool trỏ vào nó."
  }
}

variable "landing_cool_after_days" {
  description = "Số ngày từ lần sửa cuối thì blob trong landing/ chuyển sang tier cool (rubric 3.4)."
  type        = number
  default     = 30
}

variable "tags" {
  description = "Tag chuẩn từ module naming_tags."
  type        = map(string)
}
