variable "subscription_id_a" {
  description = "Subscription ID của Sub A (core). Đặt trong bootstrap.auto.tfvars, không commit."
  type        = string
}

variable "location" {
  description = "Region chính đã chốt ở G0 (docs/devops/spike_results.md)."
  type        = string

  validation {
    condition     = length(var.location) > 0
    error_message = "Chưa chốt region ở G0."
  }
}

variable "github_sub_prefix" {
  description = <<-EOT
    Phần đầu subject trong token OIDC của GitHub, ví dụ "repo:<owner>@<owner_id>/<repo>@<repo_id>".
    Lấy bằng: gh api repos/<owner>/<repo>/actions/oidc/customization/sub --jq .sub_claim_prefix
  EOT
  type        = string

  validation {
    condition     = can(regex("^repo:[^/]+/[^/:]+$", var.github_sub_prefix))
    error_message = "github_sub_prefix phải có dạng repo:<owner>[@id]/<repo>[@id], không có dấu : ở cuối."
  }
}

variable "member_object_ids" {
  description = "Map tên vai → Entra Object ID của 4 thành viên, ví dụ { p1 = \"…\", p2 = \"…\" }."
  type        = map(string)
}
