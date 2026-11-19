# infra/

Hạ tầng Azure + Databricks bằng Terraform. Kế hoạch chi tiết: [docs/devops](../docs/devops/00_tong_quan.md).

| Thư mục | State key | Subscription | Ai apply |
|---|---|---|---|
| `bootstrap/` | `bootstrap.tfstate` | A | P1, local, một lần |
| `stacks/10-core/` | `core.tfstate` | A | P1 local → CI `tf-apply` |
| `stacks/20-stream/` | `stream.tfstate` | B | CI `stream-toggle` / P3 |
| `stacks/30-databricks/` | `databricks.tfstate` | workspace ở A | P1 local → CI `tf-apply` |
| `stacks/40-orchestration/` | `orchestration.tfstate` | A | CI `tf-apply` |
| `stacks/90-sandbox/` | `sandbox-<member>.tfstate` | C / D | P4, local |

## Chạy một stack từ máy

```bash
az login && az account set --subscription <SUB_A_ID>
cd infra/stacks/10-core
terraform init -backend-config=../../backend.hcl
terraform plan -var-file=../../envs/10-core.tfvars
```

Giá trị riêng (subscription ID, `tfstate_account`…) đặt trong `terraform.auto.tfvars` ngay trong thư mục stack.
File `*.auto.tfvars` đã nằm trong `.gitignore`. CI truyền các giá trị này qua `TF_VAR_*` lấy từ GitHub Secrets.

## Trạng thái khung

Mỗi `main.tf` có checklist các bước theo tài liệu. Hiện mới viết xong:

- `bootstrap/`: đầy đủ (storage state, UAMI + federated credential, RBAC có điều kiện).
- `10-core`, `20-stream`: provider, backend, resource group.
- `30-databricks`: provider, kiểm tra metastore.
- `40-orchestration`: remote state, đọc resource group core.
- `90-sandbox`: workspace sandbox qua module `dbx_workspace`.
