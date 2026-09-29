output "observability_dataset_id" {
  description = "BigQuery dataset ID for the GSK Enterprise Observability Lakehouse."
  value       = google_bigquery_dataset.gsk_observability_demo.dataset_id
}

output "sink_writer_service_account" {
  description = "Dedicated Cloud Logging sink writer identity granted dataset-scoped roles/bigquery.dataEditor."
  value       = google_logging_project_sink.gsk_bq_telemetry_sink.writer_identity
}

output "telemetry_table_id" {
  description = "Partitioned and hierarchically clustered EBRS telemetry table ID."
  value       = google_bigquery_table.enterprise_telemetry_partitioned.table_id
}

output "system_logs_table_id" {
  description = "Partitioned and clustered system logs table ID."
  value       = google_bigquery_table.system_logs.table_id
}

output "property_graph_routine_id" {
  description = "BigQuery routine ID that compiles the gsk_infrastructure_dependency_graph ISO GQL Property Graph."
  value       = google_bigquery_routine.deploy_property_graph_gql.routine_id
}
