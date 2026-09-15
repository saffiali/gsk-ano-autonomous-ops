output "bigquery_dataset_id" {
  description = "Canonical BigQuery dataset ID hosting the 6 operational tables and BQML models."
  value       = module.storage_and_vector.dataset_id
}

output "raw_logs_topic_id" {
  description = "Pub/Sub topic ID receiving filtered Cloud Logging operational events."
  value       = module.ingestion.raw_logs_topic_id
}

output "incidents_topic_id" {
  description = "Pub/Sub topic ID receiving correlated predictive incidents for Eventarc remediation."
  value       = module.ingestion.correlated_incidents_topic_id
}

output "embedding_worker_uri" {
  description = "Cloud Run v2 service URI for the streaming log chunking and Vertex AI embedding pipeline."
  value       = module.embedding_pipeline.embedding_worker_uri
}

output "remediation_webhook_uri" {
  description = "Cloud Run v2 service URI for the preemptive self-healing remediation webhook handler."
  value       = module.alerting_and_remediation.remediation_webhook_uri
}

output "bqml_model_names" {
  description = "Map of trained BigQuery ML model identifiers across Capabilities 1, 2, and 3."
  value       = module.bqml_analytics.bqml_model_names
}

output "alert_policy_ids" {
  description = "List of Cloud Monitoring alert policy IDs configured with change-window noise suppression."
  value       = module.alerting_and_remediation.alert_policy_ids
}
