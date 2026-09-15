output "remediation_webhook_service_name" {
  description = "Name of the Cloud Run v2 remediation webhook service."
  value       = google_cloud_run_v2_service.remediation_webhook.name
}

output "remediation_webhook_uri" {
  description = "HTTPS endpoint URI of the Cloud Run v2 remediation webhook service."
  value       = google_cloud_run_v2_service.remediation_webhook.uri
}

output "remediation_sa_email" {
  description = "Service account email for the remediation webhook."
  value       = google_service_account.remediation_sa.email
}

output "email_notification_channel_id" {
  description = "Resource ID of the Cloud Monitoring email notification channel."
  value       = google_monitoring_notification_channel.email_channel.id
}

output "webhook_notification_channel_id" {
  description = "Resource ID of the Cloud Monitoring webhook notification channel."
  value       = google_monitoring_notification_channel.webhook_channel.id
}

output "alert_policy_id" {
  description = "Resource ID of the unsuppressed root-cause Cloud Monitoring alert policy."
  value       = google_monitoring_alert_policy.unsuppressed_root_cause_alert.id
}

output "alert_policy_ids" {
  description = "List of Cloud Monitoring alert policy IDs."
  value = [
    google_monitoring_alert_policy.unsuppressed_root_cause_alert.id
  ]
}
