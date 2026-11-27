variable "subscription_id_a" {
  description = "Subscription ID của Sub A. CI: TF_VAR_subscription_id_a."
  type        = string
}

variable "tfstate_account" {
  description = "Storage account chứa state. CI: TF_VAR_tfstate_account."
  type        = string
}

variable "endpoint_name" {
  description = "Tên Model Serving endpoint mà Function gọi (phương án A, docs/ds/05 §3.1)."
  type        = string
  default     = "instacart-basket-rules"
}

variable "serving_mode" {
  description = "endpoint: gọi Model Serving (phương án A). fallback: Function tự đọc luật trong ds-data/serving/ (phương án B)."
  type        = string
  default     = "endpoint"

  validation {
    condition     = contains(["endpoint", "fallback"], var.serving_mode)
    error_message = "serving_mode phải là endpoint hoặc fallback."
  }
}

variable "instance_memory_in_mb" {
  description = "Bộ nhớ mỗi instance Flex. Tính tiền theo GB-giây lúc chạy; phương án B nạp Parquet bằng pandas nên để 2048."
  type        = number
  default     = 2048

  validation {
    condition     = contains([512, 2048, 4096], var.instance_memory_in_mb)
    error_message = "Flex Consumption chỉ nhận 512, 2048 hoặc 4096 MB."
  }
}

variable "maximum_instance_count" {
  description = "Số instance tối đa khi scale out. Mặc định của provider là 100; để 40 cho chặn trần chi phí."
  type        = number
  default     = 40

  validation {
    condition     = var.maximum_instance_count >= 1 && var.maximum_instance_count <= 1000
    error_message = "maximum_instance_count của Flex nằm trong [1, 1000]."
  }
}
