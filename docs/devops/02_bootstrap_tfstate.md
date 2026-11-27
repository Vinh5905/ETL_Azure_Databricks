# 02 · Bootstrap: nơi lưu Terraform state + danh tính cho CI (giai đoạn P1)

**Người làm:** P1 (Owner của Sub A). Chạy **một lần** từ máy local.
**Đầu vào:** kết quả G0 (region, Object ID của 4 thành viên, subscription ID).
**Đầu ra:** Storage Account chứa state; UAMI `id-github-ci` có federated credential cho repo; quyền vào state cho 4 người và CI.
**Xong khi:** cả 4 người chạy `terraform init` cho một stack thử mà không lỗi; workflow `whoami` trên GitHub đăng nhập Azure được (mục 6).

---

## 1. Vì sao cần bootstrap riêng

| Vấn đề | Nếu không có bootstrap | Bootstrap giải quyết |
|---|---|---|
| State nằm ở máy từng người | 4 người có 4 phiên bản "sự thật" khác nhau; apply đè lên nhau, tài nguyên bị tạo trùng hoặc bị xóa nhầm | Một state chung trên Azure Blob, mọi người cùng đọc |
| Hai người `apply` cùng lúc | State hỏng | Backend `azurerm` khóa bằng **blob lease**: người thứ hai phải đợi |
| State chứa giá trị nhạy cảm (connection string, key) | Lộ nếu commit nhầm | State không bao giờ vào git; Storage chỉ truy cập bằng Entra ID, tắt shared key |
| Lỡ tay xóa / ghi hỏng state | Mất khả năng quản lý tài nguyên | Bật versioning + soft delete để khôi phục |
| CI cần đăng nhập Azure | Phải lưu client secret trong GitHub (lâu dài, dễ lộ) | **OIDC**: GitHub phát token ngắn hạn, Azure tin token đó qua federated credential |
| "Con gà – quả trứng": Terraform cần state, nhưng state lại cần Terraform tạo | — | Bootstrap chạy với **local state**, tạo xong thì chuyển state của chính nó lên blob |

## 2. Cấu trúc thư mục

```
infra/
  bootstrap/
    versions.tf        # terraform + provider pin
    main.tf            # rg, storage, container, uami, federated credentials
    rbac.tf            # role cho 4 thành viên và CI
    variables.tf
    outputs.tf         # tên storage, client_id của UAMI → đặt vào GitHub Secrets
    bootstrap.auto.tfvars   # (gitignore) subscription id, object id thành viên, repo
  backend.hcl          # cấu hình backend dùng chung cho mọi stack (không chứa secret)
```

## 3. Các bước

### Bước 1 · Đăng nhập và chọn Sub A

```bash
az login            # tài khoản trường của P1
az account set --subscription <SUB_A_ID>
```

### Bước 2 · Viết `versions.tf`

Pin phiên bản để 4 người và CI dùng cùng provider. Kèm `storage_use_azuread` để provider thao tác blob bằng Entra ID.

```hcl
terraform {
  required_version = "~> 1.9"
  required_providers {
    azurerm = { source = "hashicorp/azurerm", version = "~> 4.0" }   # kiểm tra lại bản mới nhất khi code
    random  = { source = "hashicorp/random",  version = "~> 3.6" }
  }
}

provider "azurerm" {
  features {}
  subscription_id     = var.subscription_id_a
  storage_use_azuread = true
}
```

### Bước 3 · Storage Account chứa state (`main.tf`)

Dùng resource thô thay vì module AVM: bootstrap cần càng ít phụ thuộc càng tốt và phải dễ đọc.

```hcl
resource "azurerm_resource_group" "tfstate" {
  name     = "rg-tfstate"
  location = var.location
  tags     = local.tags
}

resource "random_string" "sfx" {
  length  = 5
  special = false
  upper   = false
}

resource "azurerm_storage_account" "tfstate" {
  name                            = "sttfstate${random_string.sfx.result}"
  resource_group_name             = azurerm_resource_group.tfstate.name
  location                        = var.location
  account_tier                    = "Standard"
  account_replication_type        = "LRS"          # rẻ nhất; state nhỏ
  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false
  shared_access_key_enabled       = false          # chỉ cho truy cập bằng Entra ID
  blob_properties {
    versioning_enabled = true                      # khôi phục state cũ
    delete_retention_policy           { days = 14 }
    container_delete_retention_policy { days = 14 }
  }
  tags = local.tags
}

resource "azurerm_storage_container" "tfstate" {
  name                  = "tfstate"
  storage_account_id    = azurerm_storage_account.tfstate.id
  container_access_type = "private"
}

resource "azurerm_management_lock" "tfstate" {        # chống xóa nhầm cả RG
  name       = "lock-tfstate"
  scope      = azurerm_resource_group.tfstate.id
  lock_level = "CanNotDelete"
}
```

### Bước 4 · Danh tính cho GitHub Actions: UAMI + federated credential

**Vì sao dùng UAMI thay cho App Registration / Service Principal:**
- Tenant trường thường chặn người dùng tạo App Registration.
- UAMI là tài nguyên Azure bình thường: Owner của subscription tự tạo được.
- UAMI vẫn nhận federated credential, nên GitHub đăng nhập được không cần secret.

```hcl
resource "azurerm_user_assigned_identity" "ci" {
  name                = "id-github-ci"
  resource_group_name = azurerm_resource_group.tfstate.name
  location            = var.location
  tags                = local.tags
}

locals {
  gh_subjects = {
    main   = "${var.github_sub_prefix}:ref:refs/heads/main"   # push lên main
    pr     = "${var.github_sub_prefix}:pull_request"          # plan trên PR
    prod   = "${var.github_sub_prefix}:environment:prod"      # apply / deploy cần reviewer duyệt
    dev    = "${var.github_sub_prefix}:environment:dev"
    stream = "${var.github_sub_prefix}:environment:stream"    # stream-toggle chạy theo lịch
  }
}

resource "azurerm_federated_identity_credential" "gh" {
  for_each                  = local.gh_subjects
  name                      = "github-${each.key}"
  user_assigned_identity_id = azurerm_user_assigned_identity.ci.id # azurerm 4.x: thay parent_id, bỏ resource_group_name
  issuer                    = "https://token.actions.githubusercontent.com"
  audience                  = ["api://AzureADTokenExchange"]
  subject                   = each.value
}
```

> Chú ý: khi job GitHub khai báo `environment: prod`, *subject* của token là `…:environment:prod` (không phải `ref:refs/heads/main`). Vì vậy phải tạo đủ các subject như trên.

> Chú ý: repo này bật `use_immutable_subject` (mặc định với repo mới của GitHub), nên subject có kèm ID số: `repo:<owner>@<owner_id>/<repo>@<repo_id>:…`. Nếu dùng dạng cũ `repo:<owner>/<repo>:…`, Entra báo `AADSTS700213: No matching federated identity record`. Lấy đúng prefix bằng:
>
> ```bash
> gh api repos/<owner>/<repo>/actions/oidc/customization/sub --jq .sub_claim_prefix
> ```

### Bước 5 · Phân quyền (`rbac.tf`)

| Ai | Scope | Role | Vì sao |
|---|---|---|---|
| 4 thành viên (Object ID) | Storage `sttfstate…` | Storage Blob Data Contributor | Đọc/ghi/khóa state khi chạy `terraform plan/apply` từ máy |
| UAMI CI | Storage `sttfstate…` | Storage Blob Data Contributor | Như trên, cho CI |
| UAMI CI | Sub A | Contributor | Tạo tài nguyên |
| UAMI CI | Sub A | Role Based Access Control Administrator (**có điều kiện**: chỉ được gán một số role như Storage Blob Data Contributor, Key Vault Secrets User, Reader) | Stack core tạo role assignment, mà Contributor không làm được việc này. Có điều kiện để CI không tự nâng quyền thành Owner |
| UAMI CI | Sub B | Contributor | **Chủ Sub B (P3) gán**, vì P1 không có quyền trên Sub B. Làm được vì cùng tenant |
| UAMI CI | Sub C | Contributor (tùy chọn) | Chỉ khi muốn CI dựng sandbox |

```hcl
resource "azurerm_role_assignment" "state_members" {
  for_each             = var.member_object_ids          # map: tên → object id
  scope                = azurerm_storage_account.tfstate.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = each.value
}

resource "azurerm_role_assignment" "ci_sub_a_contrib" {
  scope                = "/subscriptions/${var.subscription_id_a}"
  role_definition_name = "Contributor"
  principal_id         = azurerm_user_assigned_identity.ci.principal_id
}
# ... ci_state, ci_rbac_admin (kèm condition / condition_version = "2.0")
```

P3 tự chạy lệnh sau trên máy mình để gán quyền Sub B (hoặc thêm vào bootstrap bằng provider alias nếu P1 được cấp quyền):

```bash
az role assignment create --assignee-object-id <UAMI_PRINCIPAL_ID> --assignee-principal-type ServicePrincipal \
  --role Contributor --scope /subscriptions/<SUB_B_ID>
```

### Bước 6 · Apply với local state, rồi chuyển state lên blob

```bash
cd infra/bootstrap
terraform init
terraform apply                      # local state
terraform output                     # ghi lại storage_account_name, ci_client_id
```

Thêm khối backend vào `versions.tf` của bootstrap, rồi chuyển state:

```hcl
terraform { backend "azurerm" {} }
```

```bash
terraform init -backend-config=../backend.hcl -backend-config="key=bootstrap.tfstate" -migrate-state
rm terraform.tfstate*                # sau khi xác nhận state đã lên blob
```

`infra/backend.hcl` dùng chung cho mọi stack. File này không có secret nên được commit:

```hcl
resource_group_name  = "rg-tfstate"
storage_account_name = "sttfstate<sfx>"
container_name       = "tfstate"
use_azuread_auth     = true
```

Mỗi stack chỉ cần khai báo `key` riêng:

| Stack | State key | Subscription |
|---|---|---|
| bootstrap | `bootstrap.tfstate` | A |
| 10-core | `core.tfstate` | A |
| 20-stream | `stream.tfstate` | B |
| 30-databricks | `databricks.tfstate` | (workspace ở A) |
| 40-orchestration | `orchestration.tfstate` | A |
| 90-sandbox | `sandbox-<member>.tfstate` | C / D |

### Bước 7 · Cấu hình GitHub

```bash
gh secret set AZURE_CLIENT_ID      --body "<ci_client_id>"
gh secret set AZURE_TENANT_ID      --body "<tenant_id>"
gh secret set AZURE_SUBSCRIPTION_A --body "<sub_a_id>"
gh secret set AZURE_SUBSCRIPTION_B --body "<sub_b_id>"
gh secret set TFSTATE_ACCOUNT      --body "sttfstate<sfx>"
```

Các giá trị trên không phải bí mật theo nghĩa đăng nhập: không có federated credential khớp thì không ai đăng nhập được bằng chúng. Nhưng chúng phải là **Secrets**, không phải Variables: GitHub in khối `env:` của mỗi bước ra log trước khi bước che (`mask_private_values.sh`) có hiệu lực, và Variable hiện nguyên văn ở đó. Repo public nên ai cũng đọc được log (phát hiện khi làm AD-OPS-27, 2026-11-19).

Tạo environment `dev` và `prod` (Settings → Environments). `prod` yêu cầu 1 reviewer (P1 hoặc người được P1 chỉ định). Chi tiết ở [04_github_actions_cicd.md](04_github_actions_cicd.md).

## 4. Tag dùng chung (theo dõi chi phí theo người)

Mọi stack dùng module `naming_tags` (tài liệu 03), sinh ra các tag:

```hcl
tags = {
  project    = "instacart-etl"
  env        = "prod"            # prod | dev | sandbox
  owner      = "p1"              # người chịu trách nhiệm tài nguyên
  managed_by = "terraform"
  stack      = "10-core"
}
```

Cost Management lọc theo tag `owner` / `stack`, làm bảng chi phí cho rubric 5.4.

## 5. Khôi phục khi state gặp sự cố

| Tình huống | Cách xử lý |
|---|---|
| Khóa bị kẹt (CI bị hủy giữa chừng) | `terraform force-unlock <LOCK_ID>`. Chỉ chạy khi chắc chắn không còn ai đang apply |
| State bị ghi hỏng | Portal → container `tfstate` → blob → Versions → khôi phục bản trước |
| Tài nguyên tồn tại nhưng không có trong state | Dùng khối `import { to = … id = … }` (Terraform ≥ 1.5), rồi `plan` |

## 6. Kiểm tra hoàn tất

1. Một thành viên khác P1 chạy `terraform init -backend-config=../backend.hcl -backend-config="key=smoke.tfstate"` trên một thư mục trống có `backend "azurerm" {}`. Lệnh phải thành công, rồi xóa blob `smoke.tfstate`.
2. Tạo workflow tạm `.github/workflows/whoami.yml` chạy `azure/login@v2` với `client-id/tenant-id/subscription-id` rồi chạy `az account show`. Workflow phải in đúng Sub A.
3. Trong Portal, mục Storage → Access keys báo shared key đã bị tắt.
