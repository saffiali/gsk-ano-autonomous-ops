variable "project_id" {
  description = "Google Cloud Project ID hosting BigQuery ML models and routines."
  type        = string
}

variable "region" {
  description = "Google Cloud region for BigQuery ML execution."
  type        = string
}

variable "environment" {
  description = "Deployment environment identifier (prod, staging, dr)."
  type        = string
}

variable "dataset_id" {
  description = "BigQuery dataset ID hosting operational tables and BQML models."
  type        = string
}

variable "gmp_metrics_table_id" {
  description = "Table ID for GMP metrics telemetry (gmp_metrics)."
  type        = string
  default     = "gmp_metrics"
}

variable "topology_edges_table_id" {
  description = "Table ID for cross-domain dependency graph edges (topology_edges)."
  type        = string
  default     = "topology_edges"
}

variable "change_calendar_table_id" {
  description = "Table ID for scheduled maintenance windows (change_calendar)."
  type        = string
  default     = "change_calendar"
}

variable "incidents_predictions_table_id" {
  description = "Table ID for correlated incident predictions (incidents_predictions)."
  type        = string
  default     = "incidents_predictions"
}
