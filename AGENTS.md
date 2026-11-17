# AGENTS.md

Hướng dẫn chung cho **mọi coding agent** làm việc trong repo này: Claude Code, Codex, Cursor, GitHub Copilot, Gemini… Agent nào không tự đọc file này thì cấu hình cho nó đọc `AGENTS.md`.

Cách tổ chức các file hướng dẫn:
- Thư mục `infra/`, `src/`, `bundles/` có `AGENTS.md` riêng. Khi làm việc trong thư mục nào thì đọc thêm file của thư mục đó.
- Phần dành riêng cho Claude Code (skill, hook, quyền) nằm ở `CLAUDE.md` và `.claude/`.

## Dự án

Đồ án nhóm 4 người: ETL dữ liệu **Instacart Market Basket** trên **Azure + Databricks**.
- **Nguồn phụ:** Open Food Facts và một CSDL Azure SQL `retail_db` tự sinh.
- **Đầu ra:** dashboard Power BI và phân tích luật kết hợp (DApriori, DATID, FP-Growth; so sánh Hadoop MapReduce với Spark).
- **Bài báo nền tảng của phần DS:** Fernandez-Basso, Ruiz, Martin-Bautista, *New Spark solutions for distributed frequent itemset and association rule mining algorithms*, Cluster Computing 27 (2024), [doi:10.1007/s10586-023-04014-w](https://doi.org/10.1007/s10586-023-04014-w).
  - Phần DS bám bài này. Chỗ nào làm đúng, chỗ nào điều chỉnh hay bổ sung được chốt ở `docs/ds/00_tong_quan_ds.md` §2. Đọc mục đó trước khi viết code trong `src/ml/` hay `src/mapreduce/`.
- **Rubric chấm điểm:**
  - Trọng tâm là mục 4 (phân tích + trực quan).
  - Cần thêm bằng chứng cho mục 3 (hiệu năng, ETL tự động, độ trễ khác region) và mục 5.4 (làm việc nhóm, chi phí, quota).

**Tài liệu là nguồn sự thật. Đọc tài liệu tương ứng trước khi viết code:**

| Việc | Đọc |
|---|---|
| Mục đích, kiến trúc, quy trình chung của cả dự án (bắt đầu ở đây) | `docs/00_tong_quan_du_an.md` |
| Phân việc: mã task `AD-*`, phụ thuộc, đầu ra, issue tương ứng | `docs/tasks/README.md` (đồng bộ board: `scripts/sync_task_board.py`) |
| Hạ tầng, CI/CD, Unity Catalog, cổng G0–G5 | `docs/devops/00_tong_quan.md` → file 01–06 |
| Pipeline dữ liệu (ingest, bronze, silver, gold, streaming, chạy lại, dữ liệu cho DS) | `docs/de/00_tong_quan_de.md` → file 01–08 |
| Data Science (DApriori/DATID/FP-Growth, thực nghiệm, Hadoop MapReduce local, MLflow, API) | `docs/ds/00_tong_quan_ds.md` → file 01–05 |
| Ý nghĩa bảng, cột, khóa, số dòng đối soát | `docs/data/data_dictionary.md` |
| Kiến trúc tổng thể | `docs/architecture/*.png` (hình 1 tổng thể, 2 bên trong Databricks, 3 chỉ icon) |

**Vai trò:**
- **P1** Platform/DevOps (Sub A)
- **P2** DE batch
- **P3** DE streaming (Sub B)
- **P4** BI/DS (Sub C/D)

Quyền duyệt PR theo `.github/CODEOWNERS`.

## Kiến trúc tổng quát

**Luồng dữ liệu**
- ADF copy nguồn ngoài (Zenodo CSV, CDC từ Azure SQL, Open Food Facts) vào container `landing`.
- Container `landing` được mount thành volume `/Volumes/instacart/landing_meta/raw/`.
- **Lakeflow Declarative Pipelines** xử lý bronze → silver → gold:
  - Hai pipeline: `instacart_batch`, `instacart_stream`.
  - API: `from pyspark import pipelines as dp`.
- Serverless SQL warehouse phục vụ Power BI.
- Streaming: simulator → Event Hubs → Kafka source. Event Hubs đặt ở Sub B, **cố ý khác region** với Sub A.

**Batch và stream gặp nhau ở silver qua view** (`UNION ALL`). Lý do: mỗi bảng chỉ thuộc một pipeline.

**Chạy lại an toàn theo từng tầng**

| Tầng | Quy tắc |
|---|---|
| Landing | Bất biến. Tên thư mục tính từ nội dung (`release=`, `window=<lsn>`), không từ thời điểm chạy |
| Bronze | Chỉ append |
| Silver | Upsert theo khóa (`AUTO CDC`, `event_id`) |
| Gold | Materialized view, tính lại được |

**Phân chia Terraform và DAB**
- **Terraform lo "nền":** `infra/` tạo workspace, catalog, quyền, policy, warehouse, ADF pipeline.
- **DAB lo "việc":** `bundles/` (Databricks Asset Bundles) deploy job, pipeline và code `src/`.

**Stack Terraform**
- Tách theo vòng đời, mỗi stack một state trên Azure Blob (`infra/backend.hcl`).
- Stack đọc output của nhau qua `terraform_remote_state`.
- Thứ tự apply:
  1. `bootstrap`
  2. `10-core`
  3. `30-databricks`
  4. `20-stream`
  5. bundle deploy prod
  6. `40-orchestration`
  7. `90-sandbox`
- Khi destroy thì đi theo thứ tự ngược lại.

**CI (`.github/workflows/`)**
- Đăng nhập Azure bằng OIDC qua UAMI `id-github-ci`, không có client secret.

| Sự kiện | Workflow |
|---|---|
| Mọi PR | `pr-checks` |
| PR đụng `infra/**` | `tf-plan` (comment plan lên PR) |
| Merge vào `main` | `tf-apply` và `de-deploy` prod, chờ duyệt ở environment `prod` |
| Mỗi đêm | `stream-toggle` tắt Event Hubs |
| Hằng tuần | `drift-check` |
| Sự kiện issue, PR, review; mỗi 30 phút | `sync-task-board` (đồng bộ Status và Waiting on của board phân việc) |

**Trạng thái hiện tại: repo mới có khung**
- Các `main.tf` trong `infra/stacks/*` có checklist `[x]/[ ]` theo từng bước của tài liệu.
- `src/` đang theo cấu trúc tạm.
- Cấu trúc đích nằm ở `docs/de/00_tong_quan_de.md` §6: `src/ingest/`, `src/pipelines/{batch,stream}/`, `src/tasks/`, `src/common/`.

## Lệnh

```bash
uv sync                                   # Python 3.11 + pyspark 3.5 (khớp DBR 15.4 LTS)
uv run pytest -q                          # toàn bộ test (cần Java 17+)
uv run pytest tests/test_common_spark.py::test_add_ingest_metadata_adds_columns   # một test
uv run ruff check . && uv run ruff format --check .
python3 scripts/commit_lint.py --message "feat(infra): add core storage"   # kiểm commit message
```

```bash
# Terraform, không cần Azure:
terraform fmt -check -recursive infra
terraform -chdir=infra/stacks/10-core init -backend=false && terraform -chdir=infra/stacks/10-core validate
# Plan thật (cần az login + quyền đọc state):
terraform -chdir=infra/stacks/10-core init -backend-config=../../backend.hcl
terraform -chdir=infra/stacks/10-core plan -var-file=../../envs/10-core.tfvars
```

```bash
databricks bundle validate -t dev         # chạy trong bundles/, cần DATABRICKS_HOST + đăng nhập
```

## Quy tắc bắt buộc

**Lệnh không được chạy**
- `terraform apply/destroy/force-unlock/state rm`.
- `databricks bundle deploy|destroy -t prod`.
- `az group delete`, `az resource delete`.
- `az keyvault secret show`.
- `git push --force`, `git commit --no-verify`.
- Apply chỉ đi qua PR → workflow `tf-apply`. Ngoại lệ: `bootstrap` và secret scope Key Vault do P1 tự chạy.
- Với Claude Code, các lệnh trên đã bị chặn bằng hook. **Agent khác phải tự tuân thủ.**

**Bí mật và giá trị riêng**
- Không đọc, không ghi `*.auto.tfvars`, `*.tfstate`, `.env`.
- Không đưa subscription ID, object ID, email, connection string vào file commit.
- Giá trị không bí mật đặt trong `infra/envs/<stack>.tfvars`.
- Giá trị riêng đi qua `TF_VAR_*` hoặc GitHub Variables.

**Module Terraform**
- Ưu tiên Azure Verified Modules, pin `version` chính xác, đặt `enable_telemetry = false`.
- Kiểm tra tên tham số trong README của đúng version đó. Code mẫu trong tài liệu chỉ để minh họa.
- Nếu AVM thiếu tính năng thì dùng resource `azurerm_*` thô, không fork module.
- Mỗi bước trong checklist của stack là một PR nhỏ. Làm xong thì đánh dấu `[x]` trong `main.tf`.
- `databricks_grants` ghi đè mọi grant không được khai báo, nên mỗi securable chỉ có một khối.

**Chi phí và quota** (Azure for Students, khoảng 4 vCPU, 100 USD)
- Chỉ dùng cluster single-node theo policy `instacart-single-node`, có tự tắt.
- SQL warehouse phải là serverless.
- Không dùng instance pool, NAT gateway, VNet injection.

**Sơ đồ**
- Chỉ dùng draw.io: file `.drawio` và PNG xuất ra, nhúng ảnh kèm dòng "File gốc".
- Không dùng Mermaid hay ASCII art trong tài liệu.

**Quy ước viết**
- Tài liệu và comment trong code viết bằng tiếng Việt.

## Commit và Pull Request (bắt buộc)

Chi tiết và ví dụ ở `CONTRIBUTING.md`. Luật được kiểm bằng `scripts/commit_lint.py` (hook `commit-msg` và workflow `pr-lint`); sai là CI báo đỏ, không merge được.

**Ngôn ngữ:** tên nhánh, commit message, tiêu đề PR viết **tiếng Anh**. Mô tả PR viết **tiếng Việt** có dấu.

**Commit:** `<type>(<scope>): <subject>`
- `type`: `feat` `fix` `docs` `refactor` `test` `perf` `ci` `build` `chore` `style` `revert`.
- `scope`: `infra` `de` `stream` `bundle` `adf` `bi` `ds` `ci` `docs` `deps` `agents`.
- `subject`: động từ mệnh lệnh, chữ thường đầu, không dấu chấm cuối, dòng đầu ≤ 72 ký tự, nói rõ thay đổi gì ở đâu. Cấm `update`, `fix bug`, `wip`, `misc`…
- Thân (nếu có) bằng tiếng Anh, giải thích vì sao. Agent thêm `Co-Authored-By:` ở footer.
- Một commit một thay đổi logic. Trước khi commit, chạy `python3 scripts/commit_lint.py --message "<message>"`.
- Không `--no-verify`, không commit thẳng vào `main`, chỉ commit khi người dùng yêu cầu.

**Nhánh:** `<type>/<vai>-<việc>`, ví dụ `feat/p1-core-storage`.

**PR**
- Tiêu đề cùng định dạng commit (merge bằng squash nên tiêu đề thành commit trên `main`).
- Mô tả theo `.github/pull_request_template.md`, đủ 5 mục không được trống:
  `## Tóm tắt`, `## Đã làm`, `## Chưa làm / để PR sau`, `## Cách kiểm tra`, `## Ảnh hưởng`.
- **Đã làm**: chỉ ghi việc có trong diff và đã xong. **Chưa làm**: ghi trung thực mọi việc dở, bước bị bỏ qua, giả định chưa kiểm; không có thì ghi `Không có`.
- Thứ chưa chạy thử phải ghi "chưa kiểm" ở **Cách kiểm tra**.
- Tạo bằng `gh pr create --title "…" --body-file <file>` (CLI không tự áp template). Kiểm thân trước: `python3 scripts/commit_lint.py --title "…" --pr-body-file <file>`.
- Chỉ push và mở PR khi người dùng đồng ý.

## Databricks agent skills (bắt buộc khi làm phần Databricks)

Trước khi viết hoặc sửa bất kỳ thứ gì liên quan Databricks, agent phải xác nhận đã có bộ skill chính thức **databricks** (`databricks/databricks-agent-skills`). Các phần liên quan gồm: `bundles/`, `src/pipelines`, `infra/stacks/30-databricks`, Unity Catalog, job, SQL warehouse, streaming.

- **Đã có:** dùng skill phù hợp:
  - `databricks-core` (CLI, auth)
  - `databricks-pipelines` (LDP)
  - `databricks-dabs`
  - `databricks-jobs`
  - `databricks-unity-catalog`
  - `databricks-spark-structured-streaming`
  - `databricks-dbsql`
  - `databricks-serverless`
- **Chưa có:** dừng lại, báo người dùng và đưa đúng lệnh cài cho agent đang dùng. Chỉ chạy lệnh cài khi người dùng đồng ý.

| Agent | Lệnh cài |
|---|---|
| Claude Code | `/plugin marketplace add databricks/databricks-agent-skills` rồi `/plugin install databricks@databricks-agent-skills` (hoặc cài "databricks" từ Anthropic Directory) |
| Codex | `codex plugin marketplace add databricks/databricks-agent-skills` rồi `codex plugin add databricks`, sau đó chạy `/hooks` một lần để bật hook |
| GitHub Copilot | `copilot plugin marketplace add databricks/databricks-agent-skills` rồi `copilot plugin install databricks@databricks-agent-skills` |
| Mọi agent (qua CLI) | `databricks aitools install` (tự nhận diện agent; cần Databricks CLI) |

**Skill khác CLI.**
- Skill cho agent kiến thức và quy trình. Nó dùng được ngay, kể cả khi chưa cài CLI.
- Databricks CLI (`brew tap databricks/tap && brew install databricks`) giúp agent thao tác thật trên workspace.
- Cài CLI và đăng nhập ở khoảng G1, khi đã có workspace.

## Quy trình dùng chung

Các file dưới đây là checklist viết bằng văn bản. Claude Code gọi chúng bằng `/tên`. **Agent khác đọc file rồi làm theo y như vậy.**

| Việc | File |
|---|---|
| Kiểm tra local như CI trước khi commit/PR | `.claude/skills/check/SKILL.md` |
| Soạn và tạo commit đúng quy ước | `.claude/skills/commit/SKILL.md` |
| Soạn và mở Pull Request | `.claude/skills/pr/SKILL.md` |
| Làm một bước Terraform theo checklist stack | `.claude/skills/tf-step/SKILL.md` |
| Đối chiếu cổng G0–G5 | `.claude/skills/gate-check/SKILL.md` |
| Review thay đổi hạ tầng | `.claude/agents/infra-reviewer.md` |
| Review code dữ liệu | `.claude/agents/data-reviewer.md` |

## Công cụ

**Đã có trên máy P1:** `terraform`, `uv`, `az`, `gh`, Java.

**Cài khi cần** (danh sách P0 ở `docs/devops/01`):
- `brew install tflint trivy pre-commit && pre-commit install`
- Databricks CLI (xem mục trên).
