#!/usr/bin/env bash
# Che giá trị riêng trong log GitHub Actions. Repo public nên log của job ai cũng xem được.
# Gọi ở bước đầu job (sau checkout): mọi bước sau in *** thay cho subscription, tenant,
# client ID, object ID và IP của thành viên. Chỉ che log, không đổi nội dung file trên đĩa.
set -euo pipefail

mask() {
  if [ -n "${1:-}" ]; then echo "::add-mask::$1"; fi
}

for v in "${ARM_SUBSCRIPTION_ID:-}" "${TF_VAR_subscription_id_a:-}" "${TF_VAR_subscription_id_b:-}" \
  "${ARM_TENANT_ID:-}" "${ARM_CLIENT_ID:-}"; do
  mask "$v"
done

# Biến dạng JSON map (vai → giá trị).
for json in "${TF_VAR_member_object_ids:-}" "${TF_VAR_member_ips:-}"; do
  [ -n "$json" ] || continue
  while read -r v; do mask "$v"; done < <(jq -r '.[]' <<<"$json")
done
