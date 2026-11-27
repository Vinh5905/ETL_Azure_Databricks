variable "subscription_id_a" {
  description = "Subscription ID của Sub A. CI: TF_VAR_subscription_id_a."
  type        = string
}

variable "tfstate_account" {
  description = "Storage account chứa state. CI: TF_VAR_tfstate_account."
  type        = string
}

variable "sql_admin_login" {
  description = "Login SQL admin của server retail (10-core bước 3.6), dùng cho ls_sql_retail. Mật khẩu lấy từ Key Vault."
  type        = string
}

variable "triggers_on" {
  description = "Bật trigger lịch tr_master_daily của ADF. Giữ false đến khi pl_master chạy tay đạt (docs/devops/06 mục 3, bước 8)."
  type        = bool
  default     = false
}
