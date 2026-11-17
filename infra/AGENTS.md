# infra/ – quy tắc riêng cho Terraform

Đọc cùng `infra/README.md` và `docs/devops/02`, `03`, `05`.

- **Mỗi stack một state.** `backend "azurerm" { key = "<stack>.tfstate" }`, phần còn lại lấy từ `../../backend.hcl`. Không gộp stack, không đổi `key` của stack đã apply.
- **Stack sau đọc stack trước qua `terraform_remote_state`.** Muốn có giá trị mới thì thêm `output` ở stack nguồn (ví dụ `10-core/outputs.tf`, bước 3.10). Không hardcode ID.
- **Biến:**
  - Giá trị riêng (subscription, object ID, email, IP) khai báo thành `variable`, không có `default`.
  - Giá trị không bí mật đặt trong `infra/envs/<stack>.tfvars`.
  - Biến mới mà CI cần thì thêm `TF_VAR_*` vào các workflow `tf-plan.yml`, `_tf-stack.yml`, `drift-check.yml`, và ghi lại trong `docs/devops/04` mục 4.
- **Tag** luôn lấy từ `module "nt"` (`modules/naming_tags`). Tên tài nguyên ưu tiên `module.nt.names.<loại>.name_unique`.
- **Role assignment:** CI chỉ được gán các role trong `local.ci_assignable_role_ids` (`bootstrap/rbac.tf`). Cần role khác thì phải sửa bootstrap trước, và P1 apply bằng tay.
- **`bootstrap/`** chỉ P1 chạy từ máy. Không sửa trừ khi được yêu cầu rõ.
- **Secret scope dựa trên Key Vault** (`30-databricks` bước 7) cần token của người dùng. Resource đó phải có `lifecycle { ignore_changes = all }` để CI không cố tạo lại.
- **Sau khi sửa:**
  - Chạy `terraform fmt` và `terraform -chdir=<dir> init -backend=false && terraform -chdir=<dir> validate` cho mọi thư mục bị đụng (module bị đụng thì validate cả các stack gọi nó).
  - Commit `.terraform.lock.hcl` nếu provider đổi.
- **Provider pin:** `azurerm ~> 4.0`, `databricks ~> 1.60`, `azapi ~> 2.12`, `random ~> 3.6`, Terraform `~> 1.10` (CI chạy `1.16.x`; riêng `bootstrap` vẫn `~> 1.9`).
