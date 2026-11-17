#!/usr/bin/env bash
# Tóm tắt một lần terraform plan để đăng lên PR hoặc issue công khai.
#   plan_summary.sh <plan.log> [tfplan]
# Có tfplan: ghi summary.txt (hành động<TAB>địa chỉ, gồm cả drift) và totals.txt (dòng "Plan: …").
# Không có tfplan (plan lỗi): totals.txt chứa các dòng "Error: …".
# Không đưa thuộc tính tài nguyên ra ngoài vì chúng chứa subscription ID, object ID, IP.
# GUID và IPv4 còn sót (ví dụ trong tên khóa for_each) được thay bằng <guid>/<ip>.
set -euo pipefail

log="$1"
plan="${2:-}"

redact() {
  sed -E 's/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}/<guid>/g; s/([0-9]{1,3}\.){3}[0-9]{1,3}/<ip>/g'
}

: >summary.txt
if [ -n "$plan" ] && [ -f "$plan" ]; then
  terraform show -json "$plan" | jq -r '
    ( .resource_drift[]? | "drift\t\(.address)" ),
    ( .resource_changes[]?
      | select(.change.actions != ["no-op"] and .change.actions != ["read"])
      | "\(.change.actions | join("/"))\t\(.address)" )' | redact >summary.txt
  grep -m1 -E '^(Plan:|No changes\.)' "$log" | redact >totals.txt || echo "Không đọc được dòng tổng của plan." >totals.txt
else
  { grep -E 'Error: ' "$log" || true; } | sed -E 's/^[^E]*Error: /Error: /' | sort -u | head -20 | redact >totals.txt
fi
