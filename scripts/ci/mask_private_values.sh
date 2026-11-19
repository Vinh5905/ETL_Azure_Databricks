#!/usr/bin/env bash
# Che giá trị riêng trong log GitHub Actions. Repo public nên log của job ai cũng xem được.
# Gọi ở bước đầu job (sau checkout): mọi bước sau in *** thay cho subscription, tenant,
# client ID, object ID, IP và email của thành viên. Chỉ che log, không đổi nội dung file trên đĩa.
# Các giá trị này phải đặt ở GitHub Secret, không phải Variable: GitHub in khối env: của các bước
# chạy trước script này (checkout và chính bước này), nên Variable vẫn lộ ở đó (docs/devops/04 §4).
# GitHub tự che Secret nguyên chuỗi; script này che thêm từng giá trị bên trong Secret dạng JSON.
set -euo pipefail

mask() {
  if [ -n "${1:-}" ]; then echo "::add-mask::$1"; fi
}

for v in "${ARM_SUBSCRIPTION_ID:-}" "${TF_VAR_subscription_id_a:-}" "${TF_VAR_subscription_id_b:-}" \
  "${ARM_TENANT_ID:-}" "${ARM_CLIENT_ID:-}" "${TF_VAR_databricks_app_object_id:-}"; do
  mask "$v"
done

# Biến dạng JSON map (vai → giá trị). member_emails, member_upns là secret nên GitHub chỉ che nguyên chuỗi JSON,
# phải che từng email để log của 30-databricks (databricks_user) không lộ địa chỉ.
for json in "${TF_VAR_member_object_ids:-}" "${TF_VAR_member_ips:-}" "${TF_VAR_member_emails:-}" \
  "${TF_VAR_member_upns:-}"; do
  [ -n "$json" ] || continue
  # Che cả bản chữ thường: Databricks có thể trả userName đã hạ chữ.
  while read -r v; do
    mask "$v"
    mask "$(tr '[:upper:]' '[:lower:]' <<<"$v")"
  done < <(jq -r '.[]' <<<"$json")
done
