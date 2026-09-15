variable "project_id" {
  description = "Google Cloud Project ID hosting GSK Autonomous Operations (ANO) infrastructure."
  type        = string
}

variable "project_number" {
  description = "Google Cloud Project Number used for service agent IAM bindings (e.g. Pub/Sub DLQ forwarding)."
  type        = string
  default     = "123456789012"
}

variable "region" {
  description = "Primary Google Cloud region for BigQuery datasets, Cloud Run services, and Eventarc triggers."
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Deployment environment identifier (prod, staging, dr)."
  type        = string
  default     = "prod"
}

variable "dataset_id" {
  description = "BigQuery dataset ID hosting the 6 partitioned and clustered operational tables and BQML models."
  type        = string
  default     = "gsk_ano_ops"
}

variable "log_sink_filter" {
  description = "Cloud Logging inclusion filter routing compute, application, database, and network telemetry."
  type        = string
  default     = "severity >= WARNING OR jsonPayload.domain =~ \"(COMPUTE|APPLICATION|DATABASE|NETWORK)\""
}

variable "embedding_model_name" {
  description = "Vertex AI text embedding model name used for semantic log vectorization."
  type        = string
  default     = "text-embedding-005"
}

variable "embedding_worker_image" {
  description = "Artifact Registry container image URI for the streaming log chunker and Vertex AI embedding worker."
  type        = string
  default     = "us-central1-docker.pkg.dev/gsk-ano-prod/ano-containers/embedding-worker:v1.0.0"
}

variable "remediation_webhook_image" {
  description = "Artifact Registry container image URI for the Eventarc preemptive self-healing remediation webhook."
  type        = string
  default     = "us-central1-docker.pkg.dev/gsk-ano-prod/ano-containers/remediation-webhook:v1.0.0"
}

variable "notification_email" {
  description = "Email address for Cloud Monitoring SOC alerting on unsuppressed root-cause incidents."
  type        = string
  default     = "gsk-ano-soc-oncall@gsk.com"
}

variable "pagerduty_service_key" {
  description = "Sensitive integration key for PagerDuty/ServiceNow incident escalation."
  type        = string
  default     = "placeholder-pd-service-key"
  sensitive   = true
}

variable "labels" {
  description = "Enterprise cost-allocation and governance labels applied to all GSK ANO resources."
  type        = map(string)
  default = {
    project     = "gsk-autonomous-operations"
    managed_by  = "terraform"
    cost_center = "gsk-it-hosting-network"
    initiative  = "predictive-self-healing"
  }
}
