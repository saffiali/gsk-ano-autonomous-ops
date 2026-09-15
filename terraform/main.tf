provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}

module "ingestion" {
  source = "./modules/ingestion"

  project_id      = var.project_id
  project_number  = var.project_number
  region          = var.region
  environment     = var.environment
  log_sink_filter = var.log_sink_filter
  labels          = var.labels
}

module "storage_and_vector" {
  source = "./modules/storage_and_vector"

  project_id  = var.project_id
  region      = var.region
  environment = var.environment
  dataset_id  = var.dataset_id
  labels      = var.labels
}

module "bqml_analytics" {
  source = "./modules/bqml_analytics"

  project_id                     = var.project_id
  region                         = var.region
  environment                    = var.environment
  dataset_id                     = module.storage_and_vector.dataset_id
  gmp_metrics_table_id           = module.storage_and_vector.gmp_metrics_table_id
  topology_edges_table_id        = module.storage_and_vector.topology_edges_table_id
  change_calendar_table_id       = module.storage_and_vector.change_calendar_table_id
  incidents_predictions_table_id = module.storage_and_vector.incidents_predictions_table_id
}

module "embedding_pipeline" {
  source = "./modules/embedding_pipeline"

  project_id                 = var.project_id
  region                     = var.region
  environment                = var.environment
  dataset_id                 = module.storage_and_vector.dataset_id
  raw_logs_topic_id          = module.ingestion.raw_logs_topic_id
  raw_logs_subscription_name = module.ingestion.raw_logs_subscription_name
  embedding_worker_image     = var.embedding_worker_image
  embedding_model_name       = var.embedding_model_name
  labels                     = var.labels
}

module "alerting_and_remediation" {
  source = "./modules/alerting_and_remediation"

  project_id                    = var.project_id
  region                        = var.region
  environment                   = var.environment
  dataset_id                    = module.storage_and_vector.dataset_id
  correlated_incidents_topic_id = module.ingestion.correlated_incidents_topic_id
  remediation_webhook_image     = var.remediation_webhook_image
  notification_email            = var.notification_email
  pagerduty_service_key         = var.pagerduty_service_key
  labels                        = var.labels
}
