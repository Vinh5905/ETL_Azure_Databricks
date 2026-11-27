# Giá trị không bí mật của stack 50-serving. Subscription ID và tfstate_account đi qua TF_VAR_* (CI)
# hoặc *.auto.tfvars (local).

# Phương án A (mặc định): gọi Model Serving. Đổi sang "fallback" khi dùng phương án B (docs/ds/05 §3.3).
serving_mode  = "endpoint"
endpoint_name = "instacart-basket-rules"

# Mức rẻ nhất hợp lý: không always-ready, scale out tối đa 40 instance.
instance_memory_in_mb  = 2048
maximum_instance_count = 40
