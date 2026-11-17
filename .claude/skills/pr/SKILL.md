---
name: pr
description: Soạn và mở Pull Request đúng quy ước dự án - tiêu đề tiếng Anh dạng Conventional Commits, mô tả tiếng Việt theo template với đủ mục Đã làm / Chưa làm. Kiểm bằng scripts/commit_lint.py trước khi tạo. Dùng khi người dùng bảo tạo PR hoặc gõ /pr.
argument-hint: "[base, mặc định main]"
---

# /pr – mở Pull Request đúng quy ước

Chỉ push và tạo PR khi người dùng đã đồng ý.

## Quy trình

1. **Cập nhật nhánh với `main` (CONTRIBUTING.md §2):**
   - Đang ở `main` thì dừng: không mở PR từ `main`.
   - `git status` phải sạch; còn thay đổi thì hỏi có cần `/commit` trước không.
   - `git fetch origin`, rồi `git rev-list --count HEAD..origin/main`. Khác `0` thì `git rebase origin/main`.
   - Nhánh làm tiếp trên PR khác đã squash merge (commit cũ của PR nền vẫn còn trong nhánh):
     `git rebase --onto origin/main <commit cuối của PR nền>`. Commit cuối của PR nền chưa rõ thì hỏi người dùng.
   - Conflict không tự gỡ chắc chắn được thì dừng, báo file và đoạn conflict cho người dùng.
   - Rebase xong thì chạy lại kiểm tra ở bước 2.
2. **Thu thập:**
   - `git log --oneline origin/main..HEAD` và `git diff --stat origin/main...HEAD`, đọc diff đầy đủ.
     Diff phải chỉ chứa phần việc của PR này; còn lẫn phần của PR khác thì quay lại bước 1.
   - Kết quả kiểm tra đã chạy trong phiên (ví dụ `/check`). Chưa chạy, hoặc vừa rebase, thì chạy `/check` cho phần bị đổi.
3. **Kiểm commit trong nhánh:** `python3 scripts/commit_lint.py --range origin/main..HEAD`.
   Có lỗi thì báo người dùng và đề nghị sửa (rebase/reword) trước.
4. **Tiêu đề (tiếng Anh):** `<type>(<scope>): <subject>`, tóm cả PR. Thường là commit chính của nhánh.
5. **Mô tả (tiếng Việt có dấu)**, viết vào file tạm trong scratchpad, theo đúng các mục của `.github/pull_request_template.md`:
   - `## Tóm tắt`: 1–3 câu, làm gì và vì sao.
   - `## Đã làm`: gạch đầu dòng, **chỉ** việc có trong diff và đã xong.
   - `## Chưa làm / để PR sau`: mọi việc dở, bước checklist chưa tick, kiểm tra bị BỎ QUA (thiếu tflint, chưa có workspace…), giả định chưa xác minh. Không có thì ghi `Không có`. Đây là mục người/AI sau dựa vào để làm tiếp, phải trung thực.
   - `## Cách kiểm tra`: lệnh đã chạy và kết quả thật; ghi "chưa kiểm" cho phần chưa chạy.
   - `## Ảnh hưởng`: tài nguyên và chi phí ước tính, quyền mới, breaking change, việc người khác phải làm.
   - `## Liên quan` và `## Checklist`: giữ như template, chỉ tick mục đã thật sự làm.
     PR làm tiếp trên PR khác chưa merge: ghi `Depends on #<số>` và tạo ở dạng draft (`--draft`).
   - Kết thúc mô tả bằng dòng attribution của Claude Code nếu hệ thống yêu cầu.
6. **Kiểm:** `python3 scripts/commit_lint.py --title "<tiêu đề>" --pr-body-file <file>`. Sửa tới khi sạch.
7. **Cho người dùng xem** tiêu đề và mô tả, chờ đồng ý.
8. **Tạo:**
   ```bash
   git push -u origin HEAD                # nhánh chưa từng push
   git push --force-with-lease            # nhánh đã push và vừa rebase; không bao giờ lên main
   gh pr create --base main --title "<tiêu đề>" --body-file <file>
   ```
9. Báo lại link PR. Không bật auto-merge, không tự merge.
