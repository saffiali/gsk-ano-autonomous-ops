output "raw_logs_topic_id" {
  description = "Fully qualified resource ID of the raw_logs Pub/Sub topic."
  value       = google_pubsub_topic.raw_logs.id
}

output "raw_logs_topic_name" {
  description = "Short name of the raw_logs Pub/Sub topic."
  value       = google_pubsub_topic.raw_logs.name
}

output "raw_logs_dlq_topic_id" {
  description = "Fully qualified resource ID of the raw_logs dead-letter Pub/Sub topic."
  value       = google_pubsub_topic.raw_logs_dlq.id
}

output "raw_logs_subscription_id" {
  description = "Fully qualified resource ID of the raw_logs subscription."
  value       = google_pubsub_subscription.raw_logs_sub.id
}

output "raw_logs_subscription_name" {
  description = "Short name of the raw_logs subscription."
  value       = google_pubsub_subscription.raw_logs_sub.name
}

output "gmp_metrics_topic_id" {
  description = "Fully qualified resource ID of the gmp_metrics Pub/Sub topic."
  value       = google_pubsub_topic.gmp_metrics.id
}

output "gmp_metrics_topic_name" {
  description = "Short name of the gmp_metrics Pub/Sub topic."
  value       = google_pubsub_topic.gmp_metrics.name
}

output "gmp_metrics_dlq_topic_id" {
  description = "Fully qualified resource ID of the gmp_metrics dead-letter Pub/Sub topic."
  value       = google_pubsub_topic.gmp_metrics_dlq.id
}

output "gmp_metrics_subscription_id" {
  description = "Fully qualified resource ID of the gmp_metrics subscription."
  value       = google_pubsub_subscription.gmp_metrics_sub.id
}

output "correlated_incidents_topic_id" {
  description = "Fully qualified resource ID of the correlated_incidents Pub/Sub topic."
  value       = google_pubsub_topic.correlated_incidents.id
}

output "correlated_incidents_topic_name" {
  description = "Short name of the correlated_incidents Pub/Sub topic."
  value       = google_pubsub_topic.correlated_incidents.name
}

output "correlated_incidents_dlq_topic_id" {
  description = "Fully qualified resource ID of the correlated_incidents dead-letter Pub/Sub topic."
  value       = google_pubsub_topic.correlated_incidents_dlq.id
}

output "correlated_incidents_subscription_id" {
  description = "Fully qualified resource ID of the correlated_incidents subscription."
  value       = google_pubsub_subscription.correlated_incidents_sub.id
}

output "log_sink_writer_identity" {
  description = "Service account identity of the Cloud Logging operational sink."
  value       = google_logging_project_sink.ano_operational_sink.writer_identity
}
