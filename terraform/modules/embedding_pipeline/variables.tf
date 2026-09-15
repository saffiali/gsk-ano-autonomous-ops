variable "project_id" {
  description = "Google Cloud Project ID hosting the Cloud Run embedding worker and Eventarc trigger."
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
  description = "BigQuery dataset ID containing the log_embeddings table."
  type        = string
}

variable "raw_logs_topic_id" {
  description = "Fully qualified resource ID of the raw_logs Pub/Sub topic for Eventarc transport."
  type        = string
}

variable "raw_logs_subscription_name" {
  description = "Short name of the raw_logs Pub/Sub subscription for IAM consumer binding."
  type        = string
}

variable "embedding_worker_image" {
  description = "Container image URI for the streaming log chunking and Vertex AI embedding worker."
  type        = string
}

variable "embedding_model_name" {
  description = "Vertex AI text embedding model name (text-embedding-005)."
  type        = string
  default     = "text-embedding-005"
}

variable "labels" {
  description = "Resource labels applied to Cloud Run v2 and Eventarc resources."
  type        = map(string)
  default     = {}
}
