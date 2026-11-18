# Giá trị không bí mật của stack 30-databricks. tfstate_account đi qua TF_VAR_tfstate_account,
# email thành viên đi qua TF_VAR_member_emails (secret).

# Vai trò → key của member_emails (docs/devops/05 bước 2 và bảng quyền ở bước 5).
# bi chỉ đọc gold: để người chỉ có vai bi (không thuộc de) thì mới thử được "không đọc được bronze".
roles = {
  de = ["p1", "p2", "p3"]
  ds = ["p1", "p4"]
  bi = ["p4"]
}
