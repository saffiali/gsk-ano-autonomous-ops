variable "project_id" {
  description = "Google Cloud Project ID hosting the Cloud Run remediation webhook, Eventarc trigger, and Cloud Monitoring alerts."
  type        = string
}

variable "region" {
  description = "Google Cloud region for Cloud Run v2 and Eventarc."
  type        = string
}

variable "environment" {
  description = "Deployment environment identifier (prod, staging, dr)."
  type        = string
}

variable "dataset_id" {
  description = "BigQuery dataset ID containing change_calendar and incidents_predictions tables."
  type        = string
}

variable "correlated_incidents_topic_id" {
  description = "Fully qualified resource ID of the correlated_incidents Pub/Sub topic."
  type        = string
}

variable "remediation_webhook_image" {
  description = "Container image URI for the preemptive self-healing remediation webhook handler."
  type        = string
}

variable "notification_email" {
  description = "Email address for SOC alert escalation on unsuppressed root-cause incidents."
  type        = string
}

variable "pagerduty_service_key" {
  description = "Sensitive integration key for PagerDuty/ServiceNow incident escalation."
  type        = string
  sensitive   = true
}

variable "labels" {
  description = "Resource labels applied to Cloud Run, Eventarc, and Monitoring resources."
  type        = map(string)
  default     = {}
}
