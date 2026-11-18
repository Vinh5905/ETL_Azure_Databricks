# 04 · GitHub Actions và CI/CD (giai đoạn P3)

**Đầu vào:** bootstrap xong (đã có UAMI, các GitHub Variables, environment `dev`/`prod`); stack `10-core` đã apply ít nhất một lần từ máy P1.
**Đầu ra:** mọi thay đổi về hạ tầng, pipeline DE và Databricks job đi qua PR → kiểm tra → review → merge → tự động apply/deploy.
**Xong khi:** một PR thử thay đổi tag trong `10-core` chạy đủ chuỗi: check, plan có comment trên PR, merge, apply được duyệt và chạy xong. Thêm một PR thay đổi job deploy được lên workspace.

---

## 1. Luồng tổng thể

![Luồng CI/CD](diagrams/03_luong_cicd.png)

<sub>File gốc: [diagrams/devops_diagrams.drawio](diagrams/devops_diagrams.drawio), trang 3.</sub>

1. Thành viên tạo nhánh `feat/<vai>-<việc>` và mở PR vào `main`.
2. **`pr-checks`** chạy trên mọi PR: kiểm tra tĩnh, không đăng nhập Azure, nên an toàn cho mọi nhánh.
3. **`tf-plan`** chỉ chạy khi PR đụng `infra/**`. Workflow đăng nhập Azure bằng OIDC (subject `pull_request`) với quyền **đọc** state, chạy `terraform plan` rồi comment kết quả lên PR.
4. **`de-deploy` (dev)** chỉ chạy khi PR đụng `src/**` hoặc `bundles/**`. Workflow deploy bundle vào thư mục riêng của PR trong workspace (target `dev`) để chạy thử.
5. Người review đọc diff và kết quả plan, rồi approve. Branch protection yêu cầu mọi check xanh cùng 1 approve; với `infra/**` thì phải là P1 (CODEOWNERS).
6. Merge vào `main` sẽ kích hoạt **`tf-apply`** và **`de-deploy` (prod)**. Cả hai dùng environment `prod`, nên phải chờ reviewer bấm *Approve* trong tab Actions trước khi chạy.
7. Hai workflow chạy theo lịch hoặc chạy tay: **`stream-toggle`** (bật/tắt Event Hubs) và **`drift-check`** (plan hằng tuần để phát hiện ai đó sửa tay trên Portal).

## 2. Quy ước repository

```
.github/
  workflows/  pr-checks.yml tf-plan.yml tf-apply.yml de-deploy.yml stream-toggle.yml drift-check.yml
  CODEOWNERS
  pull_request_template.md
infra/        # tài liệu 02, 03, 05
bundles/      # databricks.yml + resources/*.yml (Databricks Asset Bundles)
src/          # common/, ingest/, pipelines/{batch,stream}/, tasks/, streaming/, ml/
adf/          # JSON pipeline/dataset (tài liệu 06)
powerbi/      # dự án Power BI dạng PBIP (lưu văn bản, diff được)
tests/        # pytest: unit test hàm transform với pyspark local
.pre-commit-config.yaml
.gitignore    # *.tfstate*, *.auto.tfvars, .terraform/, .databricks/, *.pbix (cache)
```

**CODEOWNERS**

```
/infra/                       @<P1>
/.github/                     @<P1>
/infra/stacks/20-stream/      @<P3> @<P1>
/adf/                        @<P2>
/src/ingest/                 @<P2>
/src/pipelines/batch/        @<P2>
/src/tasks/                  @<P2>
/src/pipelines/stream/       @<P3>
/src/streaming/               @<P3>
/powerbi/                     @<P3>
/src/ml/                      @<P4>
```

**Branch protection cho `main`:**
- Bắt buộc PR, bắt buộc 1 approve, bắt buộc review của code owner.
- Các status check bắt buộc: `pr-checks / terraform`, `pr-checks / trivy`, `pr-checks / python`, `pr-checks / java`, `pr-checks / bundle`. Job `cost` không bắt buộc.
- Không cho force-push, không cho xóa nhánh.
- Bật "Require branches to be up to date" để plan luôn chạy trên code mới nhất.

**pre-commit** chạy trên máy trước khi commit, để CI ít khi báo đỏ:

```yaml
repos:
  - repo: https://github.com/antonbabenko/pre-commit-terraform
    rev: v1.x.x
    hooks: [ { id: terraform_fmt }, { id: terraform_validate }, { id: terraform_tflint } ]
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.x.x
    hooks: [ { id: ruff }, { id: ruff-format } ]
  - repo: https://github.com/gitleaks/gitleaks
    rev: v8.x.x
    hooks: [ { id: gitleaks } ]           # chặn lỡ commit connection string / key
```

**Quản lý công việc (rubric 5.4):** dùng GitHub Projects (board Todo / Doing / Review / Done). Mỗi issue gắn label `infra`, `de-batch`, `de-stream`, `bi`, `ds` và gắn milestone theo các cổng `G0`…`G5`. PR liên kết issue bằng `Closes #…`. Khi viết báo cáo, ảnh chụp board, biểu đồ Insights → Contributors và lịch sử PR là bằng chứng phân chia công việc.

## 3. Chi tiết từng workflow

### 3.1 `pr-checks.yml`: kiểm tra tĩnh (mọi PR)

**Vì sao:** bắt lỗi rẻ nhất, sớm nhất, không cần đăng nhập Azure.

| Job | Bước | Công cụ |
|---|---|---|
| `terraform` | fmt -check · `init -backend=false` · validate cho từng stack · tflint (ruleset azurerm) | `hashicorp/setup-terraform@v3`, `terraform-linters/setup-tflint` |
| `trivy` | `trivy config` trên `infra/`, **chặn merge** khi có lỗi HIGH/CRITICAL. Ngoại lệ phải có lý do: `#trivy:ignore:<id>` ngay tại resource, hoặc `.trivyignore.yaml` ở gốc repo cho thư mục không sửa được (`bootstrap`) | `aquasecurity/trivy-action` |
| `python` | `uv sync` · `ruff check` · `pytest -q` (pyspark local, dữ liệu nhỏ trong `tests/fixtures/`; gồm test thuật toán DS ở `tests/ml/`) | `astral-sh/setup-uv` |
| `java` (luôn chạy để làm check bắt buộc; chỉ build khi PR đụng `src/mapreduce/**`) | `mvn -B verify`: build jar MapReduce và chạy test trên `MiniMRYarnCluster` ([ds/03 §4.3](../ds/03_hadoop_mapreduce_local.md)) | `dorny/paths-filter`, `actions/setup-java` (cache Maven) |
| `bundle` | `databricks bundle validate -t dev` (chỉ kiểm tra cú pháp, không cần đăng nhập nếu không đọc biến từ workspace) | `databricks/setup-cli` |
| `cost` (không bắt buộc; chỉ PR đụng `infra/**`) | `infracost breakdown` trên nhánh gốc, `infracost diff` trên PR → comment chi phí ước tính tăng/giảm (cập nhật một comment). Chỉ đọc HCL, không đăng nhập Azure. Thiếu secret (vd. PR từ fork) thì bỏ qua | `infracost/actions/setup` (cần API key miễn phí lưu trong Secrets) |

```yaml
name: pr-checks
on: pull_request
permissions: { contents: read, pull-requests: write }
jobs:
  terraform:
    runs-on: ubuntu-latest
    strategy: { matrix: { stack: [bootstrap, stacks/10-core, stacks/20-stream, stacks/30-databricks, stacks/40-orchestration, stacks/50-serving, stacks/90-sandbox] } }
    defaults:
      run:
        working-directory: infra/${{ matrix.stack }}
    steps:
      - uses: actions/checkout@v4
      - uses: hashicorp/setup-terraform@v3
        with: { terraform_version: "1.16.x" }
      - run: terraform fmt -check -recursive
      - run: terraform init -backend=false -input=false
      - run: terraform validate
      - uses: terraform-linters/setup-tflint@v4
      - run: tflint --init && tflint
  python:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v5
      - uses: actions/setup-java@v4          # pyspark cần JVM
        with: { distribution: temurin, java-version: "17" }
      - run: uv sync --frozen && uv run ruff check . && uv run pytest -q
  java:                                     # job luôn chạy (để làm required check), bước nặng chỉ chạy khi đụng MapReduce
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dorny/paths-filter@v3
        id: changed
        with:
          filters: |
            mr:
              - 'src/mapreduce/**'
      - if: steps.changed.outputs.mr == 'true'
        uses: actions/setup-java@v4
        with: { distribution: temurin, java-version: "11", cache: maven }
      - if: steps.changed.outputs.mr == 'true'
        run: mvn -B -q verify
        working-directory: src/mapreduce
```

`pyspark.ml` (FP-Growth trong test DS) cần **numpy**. Phải thêm `numpy` vào nhóm `dev` của `pyproject.toml` cùng PR đầu tiên có `tests/ml/`; nếu không, job `python` báo `ModuleNotFoundError: numpy`.

### 3.2 `tf-plan.yml`: plan và comment lên PR

**Vì sao:** người review thấy **chính xác** những gì sẽ được tạo, sửa hoặc xóa trên Azure trước khi bấm approve.

```yaml
name: tf-plan
on:
  pull_request:
    paths: ["infra/**"]
permissions: { id-token: write, contents: read, pull-requests: write }   # id-token cho OIDC
concurrency:
  group: tf-${{ github.head_ref }}
  cancel-in-progress: true
env:
  ARM_USE_OIDC: "true"
  ARM_CLIENT_ID: ${{ vars.AZURE_CLIENT_ID }}
  ARM_TENANT_ID: ${{ vars.AZURE_TENANT_ID }}
  ARM_SUBSCRIPTION_ID: ${{ vars.AZURE_SUBSCRIPTION_A }}
jobs:
  detect:            # dorny/paths-filter → output "stacks" = danh sách stack bị đổi (JSON)
    runs-on: ubuntu-latest
  plan:
    needs: detect
    runs-on: ubuntu-latest
    strategy:
      matrix:
        stack: ${{ fromJson(needs.detect.outputs.stacks) }}
    steps:
      - uses: actions/checkout@v4
      - run: scripts/ci/mask_private_values.sh   # che ID, IP trong log (repo public)
      - uses: azure/login@v2
        with:
          client-id: ${{ vars.AZURE_CLIENT_ID }}
          tenant-id: ${{ vars.AZURE_TENANT_ID }}
          subscription-id: ${{ vars.AZURE_SUBSCRIPTION_A }}
      - uses: hashicorp/setup-terraform@v3
      - run: terraform init -input=false -backend-config=../../backend.hcl
        working-directory: infra/stacks/${{ matrix.stack }}
      - run: terraform plan -input=false -no-color -lock-timeout=5m -var-file=../../envs/${{ matrix.stack }}.tfvars -out=tfplan
      # Tóm tắt bằng scripts/ci/plan_summary.sh rồi comment lên PR bằng actions/github-script
```

**Repo public, nên comment chỉ có tóm tắt, không có plan đầy đủ.** Plan đầy đủ chứa subscription ID (trong mọi resource ID), object ID của thành viên (`principal_id`) và IP. Ai không đăng nhập cũng đọc được comment, log và issue của repo public.

| Chỗ | Nội dung |
|---|---|
| Comment trên PR | Dòng `Plan: X to add, Y to change, Z to destroy` (hoặc `No changes.`); danh sách `hành động  địa chỉ tài nguyên`, gồm cả tài nguyên bị lệch ngoài Terraform (`drift`); cảnh báo ⚠️ nếu có tài nguyên bị xóa hoặc tạo lại. Plan lỗi thì chỉ có các dòng `Error: …`. GUID và IPv4 còn sót bị thay bằng `<guid>`, `<ip>`. |
| Log của job | Plan đầy đủ. Bước `mask_private_values.sh` chạy đầu tiên nên subscription, tenant, client ID, object ID và IP hiện thành `***`. |

Người review cần xem thuộc tính cụ thể (ví dụ role nào, scope nào) thì mở log của job: link nằm cuối comment. Không đính kèm plan làm artifact, vì artifact của repo public ai cũng tải được.

Biến không bí mật (Object ID của thành viên, IP...) được đọc từ GitHub Variables rồi chuyển thành `TF_VAR_*`, vì CI không có file `*.auto.tfvars`.

### 3.3 `tf-apply.yml`: apply sau khi merge (environment `prod`)

**Vì sao cần environment `prod`:**
- Thêm bước **duyệt tay** trước khi thay đổi hạ tầng thật.
- Subject OIDC `environment:prod` khớp với federated credential riêng.
- Tab *Deployments* của GitHub ghi lại ai duyệt, lúc nào. Đây là bằng chứng quản lý dự án.

```yaml
name: tf-apply
on:
  push: { branches: [main], paths: ["infra/**"] }
  workflow_dispatch:
permissions: { id-token: write, contents: read }
concurrency: { group: tf-apply, cancel-in-progress: false }      # không bao giờ hủy apply giữa chừng
jobs:
  core:          { uses: ./.github/workflows/_tf-stack.yml, with: { stack: 10-core },          secrets: inherit }
  databricks:    { needs: core,       uses: ./.github/workflows/_tf-stack.yml, with: { stack: 30-databricks },    secrets: inherit }
  orchestration: { needs: databricks, uses: ./.github/workflows/_tf-stack.yml, with: { stack: 40-orchestration }, secrets: inherit }
```

`_tf-stack.yml` là **reusable workflow** gồm các bước login, init, plan, apply. Nó khai báo `environment: prod` và chỉ chạy apply khi plan có thay đổi (`-detailed-exitcode` = 2). `20-stream` **không** nằm trong chuỗi này, vì stack đó có workflow riêng.

### 3.4 `de-deploy.yml`: đưa code DE lên Databricks (Databricks Asset Bundles)

**Vì sao dùng DAB thay vì Terraform cho job:**
- Code notebook/Python và định nghĩa job đi cùng nhau trong một bundle, và deploy cùng nhau.
- Target `dev` tự thêm tiền tố `[dev <user>]` và tạm dừng lịch chạy, nên 4 người không chạy đè job của nhau.
- `databricks bundle run` cho phép chạy thử ngay từ CI.

| Sự kiện | Target | Hành động |
|---|---|---|
| PR đụng `src/**`, `bundles/**` | `dev` | `bundle deploy -t dev`, chạy job smoke-test trên tập dữ liệu mẫu |
| Push lên `main` | `prod` (environment `prod`) | `bundle deploy -t prod`. Lịch chạy chính thức do ADF trigger, nên bundle không tự đặt schedule |
| Đóng PR | `dev` | `bundle destroy -t dev --auto-approve` để dọn job của PR |

```yaml
      - uses: azure/login@v2                 # cùng UAMI
        with: { client-id: ..., tenant-id: ..., subscription-id: ... }
      - uses: databricks/setup-cli@main
      - run: databricks bundle deploy -t prod
        working-directory: bundles
        env:
          DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
          DATABRICKS_AUTH_TYPE: azure-cli     # dùng phiên đăng nhập của azure/login
```

UAMI phải được **thêm vào workspace** dưới dạng service principal (dùng application ID của UAMI) và được cấp quyền trong Unity Catalog. Việc này làm ở stack `30-databricks` ([05](05_databricks_platform.md)).

### 3.5 `stream-toggle.yml`: bật/tắt Event Hubs (Sub B)

**Vì sao:** Event Hubs Standard tính tiền theo giờ dù không có message. Chỉ bật trong buổi làm việc hoặc demo.

```yaml
on:
  workflow_dispatch:
    inputs: { action: { type: choice, options: [up, down], default: up } }
  schedule:
    - cron: "0 17 * * *"        # 00:00 giờ VN: luôn tắt
jobs:
  toggle:
    runs-on: ubuntu-latest
    environment: stream             # không có required reviewer, để lịch đêm tự chạy được (xem dưới)
    env:
      ARM_SUBSCRIPTION_ID: ${{ vars.AZURE_SUBSCRIPTION_B }}
    steps:
      - ...login, init (key = stream.tfstate)
      - run: terraform ${{ (github.event.inputs.action || 'down') == 'up' && 'apply' || 'destroy' }} -auto-approve
```

Khi chạy theo lịch thì không có reviewer bấm duyệt. Vì vậy dùng environment riêng `stream`, không đặt required reviewer, chỉ cho phép nhánh `main`. Thêm federated credential `environment:stream` vào bootstrap.

### 3.6 `drift-check.yml`: phát hiện sửa tay

Chạy hằng tuần vào thứ Hai với `terraform plan -detailed-exitcode` cho các stack `10/30/40`. Nếu có thay đổi thì tự mở issue gắn label `drift`. Mục đích là giữ nguyên tắc "không sửa tay trên Portal".

Issue cũng công khai, nên thân issue chỉ có tóm tắt giống comment của `tf-plan` (§3.2) và link tới log của job. `tf-apply` (`_tf-stack.yml`) cũng chạy `mask_private_values.sh` đầu job để log apply không lộ ID.

### 3.7 `pbi-deploy.yml` (tùy chọn)

Chỉ làm nếu spike 3.6 cho thấy tenant cho phép service principal gọi Power BI REST API. Khi đó workflow:
1. Import semantic model và report từ thư mục `powerbi/` vào workspace `instacart-bi`.
2. Cập nhật tham số (host và HTTP path của warehouse).
3. Kích hoạt refresh.

Nếu tenant không cho phép thì publish tay từ Power BI Desktop theo checklist ở tài liệu 06. Thư mục `powerbi/` (định dạng PBIP) vẫn được review qua PR.

### 3.8 `func-deploy.yml` (tùy chọn, G5)

Chỉ làm khi đã dựng stack `50-serving` ([03 §5.1](03_terraform_azure.md)).
- **Kích hoạt:** push lên `main` có đụng `src/serving_func/**`.
- **Các bước:** đăng nhập Azure bằng OIDC (cùng UAMI CI), rồi `Azure/functions-action@v1` deploy thư mục `src/serving_func/` lên `func-instacart-recommend`.
- **Quyền:** UAMI CI cần role *Website Contributor* trên Function App. Role này được gán trong `50-serving`.
- **Sau khi deploy:** gọi thử `POST /api/recommend` với giỏ `[24852]`, kỳ vọng mã 200. Function key lấy từ Key Vault lúc chạy, không lưu trong GitHub.

## 4. Secrets và Variables

| Tên | Loại | Dùng ở |
|---|---|---|
| `AZURE_CLIENT_ID`, `AZURE_TENANT_ID` | Variable | Mọi workflow đăng nhập Azure |
| `AZURE_SUBSCRIPTION_A`, `AZURE_SUBSCRIPTION_B` | Variable | Terraform, `stream-toggle` |
| `TFSTATE_ACCOUNT` | Variable | `backend.hcl` / remote state |
| `DATABRICKS_HOST` | Variable | `de-deploy` |
| `TF_VAR_MEMBER_OBJECT_IDS` (JSON, map vai → object ID) | Variable, map vào `TF_VAR_member_object_ids` | Stack `10-core` (bước 3.6, 3.8), `30-databricks` |
| `TF_VAR_MEMBER_IPS` (JSON, map vai → IP public) | Variable, map vào `TF_VAR_member_ips` | Stack `10-core` (firewall Azure SQL, bước 3.6) |
| `TF_VAR_MEMBER_EMAILS` (JSON, map vai → email) | **Secret**, map vào `TF_VAR_member_emails` | Stack `10-core` (action group nhận cảnh báo budget, bước 3.9) và `30-databricks` (tài khoản đăng nhập workspace của thành viên, bước 2). Secret vì repo public và Variable hiện rõ trong log |
| `INFRACOST_API_KEY` | **Secret** | `pr-checks / cost`. Token của service account trên Infracost Cloud (dashboard → Org settings → Service accounts → Create, đặt hạn dùng có giới hạn). Token chỉ hiện một lần; đặt bằng `gh secret set INFRACOST_API_KEY` rồi dán vào |

Không có client secret, PAT hay connection string nào nằm trong GitHub. Mọi bí mật lúc chạy đều nằm trong Key Vault.

## 5. Kiểm tra hoàn tất

1. PR thử thêm tag `ci_test = "1"` vào `10-core`. `pr-checks` xanh, `tf-plan` comment đúng "1 to change", P1 approve, merge. `tf-apply` chờ duyệt, được duyệt thì apply xong, và lần plan sau báo No changes.
2. PR thử sửa một job trong `bundles/`. Job `[dev …]` xuất hiện trong workspace; đóng PR thì job đó biến mất.
3. Chạy `stream-toggle` với `up`, rồi với `down`. Namespace Event Hubs được tạo rồi bị xóa, secret trong Key Vault thay đổi theo.
4. Thử push thẳng lên `main` thì bị từ chối.
