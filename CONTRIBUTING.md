# Quy ước commit, nhánh và Pull Request

Áp dụng **bắt buộc** cho mọi người và mọi AI agent (Claude Code, Codex, Copilot…). Được kiểm tự động ở 3 lớp:

| Lớp | Kiểm gì | Khi nào |
|---|---|---|
| Hook `commit-msg` (pre-commit) | Commit message | Ngay lúc `git commit` trên máy (sau khi chạy `pre-commit install`) |
| Workflow `pr-lint` | Tiêu đề PR, mọi commit trong PR, mô tả PR | Mỗi lần mở / sửa / push PR. Là status check bắt buộc |
| Thủ công | `python3 scripts/commit_lint.py --message "…"` | Khi muốn thử trước |

Luật nằm trong một chỗ duy nhất: `scripts/commit_lint.py`. Sửa luật thì sửa script, test (`tests/test_commit_lint.py`) và file này.

## Tóm tắt ngôn ngữ

| Phần | Ngôn ngữ |
|---|---|
| Tên nhánh | Tiếng Anh |
| Commit message (dòng đầu và thân) | **Tiếng Anh** |
| Tiêu đề PR | **Tiếng Anh**, cùng định dạng commit |
| Mô tả PR | **Tiếng Việt** có dấu, theo template |
| Review comment, tài liệu `docs/`, comment trong code | Tiếng Việt |

## 1. Commit message

Theo [Conventional Commits 1.0](https://www.conventionalcommits.org/):

```text
<type>(<scope>): <subject>

<body: vì sao thay đổi, tiếng Anh, tùy chọn>

<footer: Closes #12, BREAKING CHANGE: …, Co-Authored-By: …>
```

**type**

| type | Dùng khi |
|---|---|
| `feat` | Thêm tính năng, tài nguyên, bảng, pipeline mới |
| `fix` | Sửa lỗi |
| `docs` | Chỉ sửa tài liệu |
| `refactor` | Đổi cấu trúc code, không đổi hành vi |
| `test` | Thêm hoặc sửa test |
| `perf` | Cải thiện hiệu năng |
| `ci` | Workflow GitHub Actions |
| `build` | Đóng gói, dependency, `pyproject.toml` |
| `chore` | Việc lặt vặt khác: cấu hình agent, `.gitignore`… |
| `style` | Chỉ định dạng |
| `revert` | Hoàn tác commit trước |

**scope** (không bắt buộc, nhưng nên có): `infra`, `de`, `stream`, `bundle`, `adf`, `bi`, `ds`, `ci`, `docs`, `deps`, `agents`.

**subject**
- Động từ mệnh lệnh, chữ thường đầu câu: `add`, `fix`, `remove`, `rename`. Không viết `Added`, `Adds`, `Add`.
- Nói rõ **cái gì, ở đâu**. Không có dấu câu ở cuối. Dòng đầu tối đa 72 ký tự.
- Cấm subject chung chung: `update`, `fix bug`, `wip`, `misc`, `changes`…
- Thay đổi phá tương thích: thêm `!` sau type/scope (`feat(infra)!: …`) và ghi `BREAKING CHANGE:` ở footer.

**Thân** (tùy chọn): giải thích *vì sao*, tiếng Anh, cách dòng đầu một dòng trống, mỗi dòng ≤ 100 ký tự.

**Một commit = một thay đổi logic.** Không gộp Terraform với pipeline dữ liệu trong cùng commit. Commit `fixup!` / `squash!` được phép lúc làm, nhưng phải gộp trước khi merge.

**AI agent** thêm dòng `Co-Authored-By: <tên model> <email>` ở footer.

Ví dụ đúng:

```text
feat(infra): add landing container and lifecycle rule to core storage
fix(de): deduplicate order_items by event_id on rerun
docs(de): document SCD2 rules for dim_product
ci: lint PR titles and commit messages
chore(deps): bump ruff to 0.8.4
```

Ví dụ sai: `update code`, `Fix bug`, `feat: Thêm storage`, `feat(infra): add storage.`, `WIP`.

## 2. Nhánh

`<type>/<vai>-<việc>` bằng tiếng Anh, kebab-case. Ví dụ: `feat/p1-core-storage`, `fix/p2-bronze-dedup`, `docs/p4-powerbi-guide`.
- Không commit thẳng vào `main`; mọi thay đổi đi qua PR.
- Một PR nên nhỏ: một bước trong checklist stack, hoặc một bảng/pipeline.

### Giữ nhánh cập nhật với `main`

**Mỗi lần push hoặc mở PR, nhánh phải chứa `main` mới nhất của repo gốc.** `main` ở máy có thể đã cũ, nên luôn so với `origin/main` (làm trên fork thì là remote trỏ tới repo gốc, thường đặt tên `upstream`).

- **Tạo nhánh mới:** `git fetch origin && git switch -c <nhánh> origin/main`.
- **Trước khi push/mở PR:**
  1. `git fetch origin`, rồi `git rev-list --count HEAD..origin/main`. Kết quả khác `0` là nhánh đang tụt sau `main`.
  2. Cập nhật bằng `git rebase origin/main`, gỡ conflict nếu có.
  3. Chạy lại kiểm tra (`/check` hoặc `uv run pytest -q`).
  4. Nhánh chưa từng push: `git push -u origin HEAD`. Nhánh đã push: `git push --force-with-lease`.
- **Force push:**
  - Chỉ dùng `--force-with-lease`, và chỉ trên **nhánh PR của chính mình**. Nhánh của người khác chỉ force khi chủ nhánh đồng ý.
  - **Không bao giờ force lên `main`.** Không dùng `--force`, `-f` hay refspec có dấu `+`.
  - `main` còn được branch protection chặn force push ở phía GitHub; hook `guard_bash.sh` của Claude Code chặn thêm ở máy.
- **PR xếp chồng (PR B làm tiếp trên nhánh của PR A chưa merge) được phép:**
  - PR B để **draft**, ghi `Depends on #<A>` ở mục Liên quan.
  - Sau khi A squash merge, commit của A trên `main` khác với các commit A trong nhánh B, nên `git rebase origin/main` thường sẽ conflict. Dùng:
    `git rebase --onto origin/main <commit cuối của A> <nhánh B>` để chỉ chuyển các commit riêng của B.
  - Kiểm `git diff --stat origin/main...HEAD` chỉ còn file của B, rồi mới `git push --force-with-lease` và chuyển PR sang ready.
- Cập nhật lúc mở PR là để thấy conflict sớm. Lúc merge, branch protection (`strict`) còn bắt nhánh phải up-to-date lần nữa; khi đó bấm **Update branch** trên GitHub là đủ.

## 3. Pull Request

**Tiêu đề PR:** tiếng Anh, **cùng định dạng commit**. PR được merge bằng *squash*, nên tiêu đề PR trở thành commit trên `main`.

**Mô tả PR:** tiếng Việt, dùng đúng `.github/pull_request_template.md`. Năm mục sau là **bắt buộc** và không được để trống; nếu không có gì thì ghi `Không có`:

| Mục | Ghi gì |
|---|---|
| `## Tóm tắt` | 1–3 câu: PR làm gì, vì sao |
| `## Đã làm` | Gạch đầu dòng từng việc **đã thật sự làm xong** trong PR |
| `## Chưa làm / để PR sau` | Việc trong phạm vi nhưng **chưa làm**, việc làm dở, giả định chưa kiểm, TODO còn lại. Người sau (hoặc AI sau) đọc mục này để làm tiếp |
| `## Cách kiểm tra` | Lệnh đã chạy và kết quả; bước kiểm tay cho reviewer |
| `## Ảnh hưởng` | Tài nguyên mới, chi phí ước tính, quyền mới, thay đổi phá tương thích, việc người khác phải làm |

Quy tắc trung thực:
- Chỉ ghi vào "Đã làm" những gì có trong diff và đã kiểm. Thứ chưa chạy thử thì ghi rõ "chưa kiểm" ở "Cách kiểm tra".
- Không giấu việc dở: test bỏ qua, bước BỎ QUA vì thiếu công cụ, checklist chưa tick đều phải nằm ở "Chưa làm".

**Tạo PR bằng CLI** (`gh pr create --body` không tự dùng template):

```bash
gh pr create --title "feat(infra): add core storage account" --body-file /path/to/body.md
```

### PR từ fork: tự chạy phần CI bỏ qua (bắt buộc)

GitHub không đưa GitHub Variables và token OIDC cho workflow chạy từ PR của fork. Vì vậy các bước cần đăng nhập Azure hay Databricks **không chạy**, dù check vẫn có thể báo xanh:

| Workflow | Với PR từ fork |
|---|---|
| `pr-checks / bundle` | Chỉ in notice rồi báo xanh, **không** chạy `bundle validate` |
| `de-deploy` (`dev`, `dev-cleanup`) | Bị skip, không có job `[dev …]` nào được tạo |
| `tf-plan` | Không có comment plan |
| `pr-checks / cost` | Bỏ qua vì thiếu secret |

Các kiểm tra chỉ đọc code (`python`, `terraform` validate, `trivy`, `pr-lint`…) vẫn chạy bình thường.

**Nhận biết:** `git remote get-url origin` không trỏ tới `Vinh5905/ETL_Azure_Databricks`, hoặc `gh pr view <số> --json isCrossRepository` trả `true`. Nhánh đẩy thẳng lên repo gốc thì CI chạy đủ, không cần làm thêm phần dưới.

**Việc phải tự làm trước khi chuyển PR sang ready** (ghi lệnh và kết quả vào `## Cách kiểm tra`):
- **PR đụng `src/**` hoặc `bundles/**`:** từ máy mình, trong `bundles/`:
  1. `databricks bundle validate -t dev`
  2. `databricks bundle deploy -t dev`, rồi chạy job hoặc pipeline `[dev <user>] …` mà PR sửa.
  3. Xong việc thì `databricks bundle destroy -t dev` nếu không cần giữ resource dev nữa (thay cho `dev-cleanup`).
- **PR đụng `infra/**` hoặc `adf/**`:** nhờ P1 chạy plan chỉ đọc trên nhánh của PR và dán tóm tắt lên PR. Không merge khi chưa có plan.
- Ghi rõ trong `## Chưa làm / để PR sau` bước nào chưa tự chạy được.

**Merge:** squash merge, cần review của CODEOWNERS và các status check xanh (`pr-checks`, `pr-lint`).

## 4. Cài hook trên máy

```bash
brew install pre-commit
pre-commit install            # cài cả hook pre-commit và commit-msg (xem default_install_hook_types)
```

Không dùng `git commit --no-verify` để lách hook.

## 5. Cấu hình GitHub (P1 làm một lần)

Settings → General → Pull Requests:
- Chỉ bật **Allow squash merging**, default message = **Pull request title and description**.
- Bật **Automatically delete head branches**.

Branch protection của `main`: thêm `pr-lint / title`, `pr-lint / commits`, `pr-lint / body` vào danh sách status check bắt buộc.
