output "embedding_worker_service_name" {
  description = "Name of the Cloud Run v2 embedding worker service."
  value       = google_cloud_run_v2_service.embedding_worker.name
}

output "embedding_worker_uri" {
  description = "HTTPS endpoint URI of the Cloud Run v2 embedding worker service."
  value       = google_cloud_run_v2_service.embedding_worker.uri
}

output "embedding_worker_sa_email" {
  description = "Service account email for the embedding worker."
  value       = google_service_account.embedding_worker_sa.email
}

output "eventarc_trigger_name" {
  description = "Name of the Eventarc trigger routing Pub/Sub raw_logs to the embedding worker."
  value       = google_eventarc_trigger.raw_logs_trigger.name
}
