output "name" {
  description = "Tên storage account."
  value       = module.this.name
}

output "resource_id" {
  description = "Resource ID của storage account, dùng cho role assignment (bước 3.4, 3.7, 3.8)."
  value       = module.this.resource_id
}

output "containers" {
  description = "Tên các container đã tạo."
  value       = sort(keys(module.this.containers))
}
