#!/usr/bin/env bash
# Tóm tắt một lần terraform plan để đăng lên PR hoặc issue công khai.
#   plan_summary.sh <plan.log> [tfplan]
# Có tfplan: ghi summary.txt (hành động<TAB>địa chỉ, gồm cả drift) và totals.txt (dòng "Plan: …",
# hoặc danh sách tên output đổi khi plan chỉ đổi output).
# Không có tfplan (plan lỗi): totals.txt chứa các dòng "Error: …".
# Không đưa thuộc tính tài nguyên ra ngoài vì chúng chứa subscription ID, object ID, IP.
# GUID, IPv4 và email còn sót (ví dụ trong tên khóa for_each hay dòng Error) được thay bằng
# <guid>/<ip>/<email>. ::add-mask:: chỉ che log của job, không che comment PR hay issue drift.
set -euo pipefail

log="$1"
plan="${2:-}"

redact() {
  sed -E 's/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}/<guid>/g; s/([0-9]{1,3}\.){3}[0-9]{1,3}/<ip>/g; s/[^[:space:]@"'\'']+@[^[:space:]@"'\'']+\.[A-Za-z]{2,}/<email>/g'
}

: >summary.txt
if [ -n "$plan" ] && [ -f "$plan" ]; then
  json="$(terraform show -json "$plan")"
  jq -r '
    ( .resource_drift[]? | "drift\t\(.address)" ),
    ( .resource_changes[]?
      | select(.change.actions != ["no-op"] and .change.actions != ["read"])
      | "\(.change.actions | join("/"))\t\(.address)" )' <<<"$json" | redact >summary.txt
  if ! grep -m1 -E '^(Plan:|No changes\.)' "$log" | redact >totals.txt; then
    # Plan chỉ đổi output thì Terraform không in dòng "Plan:". Chỉ ghi tên output, không ghi giá trị.
    outs="$(jq -r '[.output_changes // {} | to_entries[]
      | select(.value.actions != ["no-op"]) | "\(.value.actions | join("/")) \(.key)"] | join(", ")' <<<"$json")"
    if [ -n "$outs" ]; then
      echo "Không đổi tài nguyên, chỉ đổi output: $outs" >totals.txt
    else
      echo "Không đọc được dòng tổng của plan." >totals.txt
    fi
  fi
else
  { grep -E 'Error: ' "$log" || true; } | sed -E 's/^[^E]*Error: /Error: /' | sort -u | head -20 | redact >totals.txt
fi
