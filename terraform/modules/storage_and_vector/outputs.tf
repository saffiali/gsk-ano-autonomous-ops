output "dataset_id" {
  description = "BigQuery dataset ID hosting the GSK ANO analytical tables."
  value       = google_bigquery_dataset.ano_dataset.dataset_id
}

output "dataset_self_link" {
  description = "Self link URI of the BigQuery dataset."
  value       = google_bigquery_dataset.ano_dataset.self_link
}

output "raw_logs_table_id" {
  description = "Table ID of the raw_logs table."
  value       = google_bigquery_table.raw_logs.table_id
}

output "log_embeddings_table_id" {
  description = "Table ID of the log_embeddings table."
  value       = google_bigquery_table.log_embeddings.table_id
}

output "gmp_metrics_table_id" {
  description = "Table ID of the gmp_metrics table."
  value       = google_bigquery_table.gmp_metrics.table_id
}

output "topology_edges_table_id" {
  description = "Table ID of the topology_edges table."
  value       = google_bigquery_table.topology_edges.table_id
}

output "change_calendar_table_id" {
  description = "Table ID of the change_calendar table."
  value       = google_bigquery_table.change_calendar.table_id
}

output "incidents_predictions_table_id" {
  description = "Table ID of the incidents_predictions table."
  value       = google_bigquery_table.incidents_predictions.table_id
}

output "vector_index_routine_id" {
  description = "Routine ID for the Vector Search Index creation stored procedure."
  value       = google_bigquery_routine.create_vector_index.routine_id
}

output "semantic_search_routine_id" {
  description = "Routine ID for the VECTOR_SEARCH semantic outlier stored procedure."
  value       = google_bigquery_routine.semantic_outlier_vector_search.routine_id
}
