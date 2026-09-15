variable "project_id" {
  description = "Google Cloud Project ID hosting the BigQuery dataset and tables."
  type        = string
}

variable "region" {
  description = "Google Cloud location for the BigQuery dataset."
  type        = string
}

variable "environment" {
  description = "Deployment environment identifier (prod, staging, dr)."
  type        = string
}

variable "dataset_id" {
  description = "BigQuery dataset ID for GSK Autonomous Operations (ANO)."
  type        = string
  default     = "gsk_ano_ops"
}

variable "labels" {
  description = "Resource labels applied to the BigQuery dataset and tables."
  type        = map(string)
  default     = {}
}
