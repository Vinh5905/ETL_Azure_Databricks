# Giá trị KHÔNG bí mật của stack 10-core. Subscription ID đi qua TF_VAR_* (CI) hoặc *.auto.tfvars (local).
location        = "eastasia"       # chốt G0 (docs/devops/spike_results.md §3)
tfstate_account = "sttfstatez87e6" # khớp infra/backend.hcl; dùng để đọc output của bootstrap
