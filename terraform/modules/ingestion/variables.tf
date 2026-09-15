variable "project_id" {
  description = "Google Cloud Project ID hosting the Cloud Logging sinks and Pub/Sub topics."
  type        = string
}

variable "project_number" {
  description = "Google Cloud Project Number for Pub/Sub service agent dead-letter forwarding IAM bindings."
  type        = string
}

variable "region" {
  description = "Primary deployment region for Pub/Sub message storage policy."
  type        = string
}

variable "environment" {
  description = "Deployment environment identifier (prod, staging, dr)."
  type        = string
}

variable "log_sink_filter" {
  description = "Cloud Logging inclusion filter expression for operational telemetry."
  type        = string
}

variable "labels" {
  description = "Resource labels applied to Pub/Sub topics and subscriptions."
  type        = map(string)
  default     = {}
}
