output "metastore_id" {
  value = data.databricks_current_metastore.this.id
}

# Bước 9 · Output cho DAB và Power BI (docs/devops/05 bước 9). Bundle tra policy theo tên
# (lookup cluster_policy), nên các output này chủ yếu để tra tay và cho Power BI.
output "catalog" {
  value = databricks_catalog.instacart.name
}

output "cluster_policy_id" {
  value = databricks_cluster_policy.single_node.id
}

output "ci_sp_application_id" {
  description = "Client ID của UAMI id-github-ci (BUNDLE_VAR_ci_sp_id)."
  value       = databricks_service_principal.ci.application_id
  sensitive   = true
}

output "warehouse_id" {
  value = databricks_sql_endpoint.bi.id
}

output "warehouse_http_path" {
  description = "HTTP path cho Power BI (Server hostname = DATABRICKS_HOST)."
  value       = databricks_sql_endpoint.bi.odbc_params[0].path
}
