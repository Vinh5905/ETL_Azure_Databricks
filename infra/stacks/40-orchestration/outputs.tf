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

output "pipeline_names" {
  value = sort(concat(keys(azapi_resource.pipeline_tier0), keys(azapi_resource.pipeline_tier1), keys(azapi_resource.pipeline_tier2)))

  precondition {
    condition     = length(local.pipeline_unplaced) == 0
    error_message = "Không xếp được tầng cho pipeline (tên => pipeline nó gọi): ${jsonencode({ for n in local.pipeline_unplaced : n => local.pipeline_refs[n] })}. Nguyên nhân có thể: gọi pl_ingest_off/pl_master, gọi pipeline không có trong adf/pipelines, tự gọi hoặc gọi vòng, lồng quá 3 tầng, hoặc tên không theo quy ước pl_[A-Za-z0-9_]."
  }
}
