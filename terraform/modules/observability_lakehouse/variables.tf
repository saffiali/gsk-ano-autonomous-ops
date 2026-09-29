variable "project_id" {
  description = "Google Cloud Project ID hosting the GSK Enterprise Observability Lakehouse."
  type        = string
}

variable "region" {
  description = "BigQuery dataset location/region for the observability lakehouse."
  type        = string
  default     = "europe-west2"
}

variable "environment" {
  description = "Deployment environment identifier (prod, staging, dev)."
  type        = string
  default     = "prod"
}

variable "observability_dataset_id" {
  description = "Target BigQuery dataset ID for the GSK Enterprise Observability Platform."
  type        = string
  default     = "gsk_observability_demo"
}

variable "labels" {
  description = "Enterprise cost-allocation and governance labels applied to observability resources."
  type        = map(string)
  default     = {}
}
