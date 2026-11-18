output "core_rg_id" {
  value = data.azurerm_resource_group.core.id
}

output "linked_service_names" {
  value = sort(concat(
    [
      azurerm_data_factory_linked_service_key_vault.kv.name,
      azurerm_data_factory_linked_service_data_lake_storage_gen2.adls.name,
      azurerm_data_factory_linked_service_azure_sql_database.sql_retail.name,
      azurerm_data_factory_linked_custom_service.dbx.name,
    ],
    [for ls in azurerm_data_factory_linked_custom_service.http : ls.name],
  ))
}

output "dataset_names" {
  value = sort(keys(azapi_resource.dataset))
}
