output "rg_name" {
  value = azurerm_resource_group.stream.name
}

output "namespace_suffix" {
  description = "Hậu tố dùng cho tên namespace evhns-instacart-<sfx>."
  value       = random_string.sfx.result
}

output "core_key_vault_id" {
  description = "Key Vault core sẽ nhận secret eh-*; rỗng nếu 10-core chưa có bước 3.3."
  value       = try(data.terraform_remote_state.core.outputs.key_vault_id, null)
}

output "eventhub_namespace" {
  description = "Tên namespace Event Hubs. Bootstrap server Kafka là <tên>.servicebus.windows.net:9093."
  value       = module.eh.resource_eventhubs[local.eh_hub].namespace_name
}
