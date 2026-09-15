resource "google_pubsub_topic" "raw_logs" {
  name    = "gsk-ano-raw-logs-${var.environment}"
  project = var.project_id
  labels  = var.labels

  message_storage_policy {
    allowed_persistence_regions = [var.region]
  }
}

resource "google_pubsub_topic" "raw_logs_dlq" {
  name    = "gsk-ano-raw-logs-dlq-${var.environment}"
  project = var.project_id
  labels  = var.labels

  message_storage_policy {
    allowed_persistence_regions = [var.region]
  }
}

resource "google_pubsub_topic" "gmp_metrics" {
  name    = "gsk-ano-gmp-metrics-${var.environment}"
  project = var.project_id
  labels  = var.labels

  message_storage_policy {
    allowed_persistence_regions = [var.region]
  }
}

resource "google_pubsub_topic" "gmp_metrics_dlq" {
  name    = "gsk-ano-gmp-metrics-dlq-${var.environment}"
  project = var.project_id
  labels  = var.labels

  message_storage_policy {
    allowed_persistence_regions = [var.region]
  }
}

resource "google_pubsub_topic" "correlated_incidents" {
  name    = "gsk-ano-correlated-incidents-${var.environment}"
  project = var.project_id
  labels  = var.labels

  message_storage_policy {
    allowed_persistence_regions = [var.region]
  }
}

resource "google_pubsub_topic" "correlated_incidents_dlq" {
  name    = "gsk-ano-correlated-incidents-dlq-${var.environment}"
  project = var.project_id
  labels  = var.labels

  message_storage_policy {
    allowed_persistence_regions = [var.region]
  }
}

resource "google_logging_project_sink" "ano_operational_sink" {
  name                   = "gsk-ano-operational-log-sink-${var.environment}"
  project                = var.project_id
  destination            = "pubsub.googleapis.com/${google_pubsub_topic.raw_logs.id}"
  filter                 = var.log_sink_filter
  unique_writer_identity = true

  exclusions {
    name        = "exclude-health-checks"
    description = "Exclude high-frequency Google Cloud load balancer and Kubernetes health check probes"
    filter      = "httpRequest.userAgent =~ \"GoogleHC\" OR httpRequest.userAgent =~ \"kube-probe\""
  }

  exclusions {
    name        = "exclude-cloudaudit-data-access"
    description = "Exclude noisy read-only Cloud Audit data access logs"
    filter      = "logName =~ \"cloudaudit.googleapis.com%2Fdata_access\" AND protoPayload.methodName =~ \"(get|list|watch)\""
  }
}

resource "google_pubsub_subscription" "raw_logs_sub" {
  name                       = "gsk-ano-raw-logs-sub-${var.environment}"
  project                    = var.project_id
  topic                      = google_pubsub_topic.raw_logs.id
  ack_deadline_seconds       = 60
  message_retention_duration = "604800s"
  labels                     = var.labels

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.raw_logs_dlq.id
    max_delivery_attempts = 5
  }
}

resource "google_pubsub_subscription" "raw_logs_dlq_sub" {
  name                       = "gsk-ano-raw-logs-dlq-sub-${var.environment}"
  project                    = var.project_id
  topic                      = google_pubsub_topic.raw_logs_dlq.id
  ack_deadline_seconds       = 60
  message_retention_duration = "604800s"
  labels                     = var.labels
}

resource "google_pubsub_subscription" "gmp_metrics_sub" {
  name                       = "gsk-ano-gmp-metrics-sub-${var.environment}"
  project                    = var.project_id
  topic                      = google_pubsub_topic.gmp_metrics.id
  ack_deadline_seconds       = 60
  message_retention_duration = "604800s"
  labels                     = var.labels

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.gmp_metrics_dlq.id
    max_delivery_attempts = 5
  }
}

resource "google_pubsub_subscription" "gmp_metrics_dlq_sub" {
  name                       = "gsk-ano-gmp-metrics-dlq-sub-${var.environment}"
  project                    = var.project_id
  topic                      = google_pubsub_topic.gmp_metrics_dlq.id
  ack_deadline_seconds       = 60
  message_retention_duration = "604800s"
  labels                     = var.labels
}

resource "google_pubsub_subscription" "correlated_incidents_sub" {
  name                       = "gsk-ano-correlated-incidents-sub-${var.environment}"
  project                    = var.project_id
  topic                      = google_pubsub_topic.correlated_incidents.id
  ack_deadline_seconds       = 60
  message_retention_duration = "604800s"
  labels                     = var.labels

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.correlated_incidents_dlq.id
    max_delivery_attempts = 5
  }
}

resource "google_pubsub_subscription" "correlated_incidents_dlq_sub" {
  name                       = "gsk-ano-correlated-incidents-dlq-sub-${var.environment}"
  project                    = var.project_id
  topic                      = google_pubsub_topic.correlated_incidents_dlq.id
  ack_deadline_seconds       = 60
  message_retention_duration = "604800s"
  labels                     = var.labels
}

resource "google_pubsub_topic_iam_member" "sink_publisher" {
  project = var.project_id
  topic   = google_pubsub_topic.raw_logs.name
  role    = "roles/pubsub.publisher"
  member  = google_logging_project_sink.ano_operational_sink.writer_identity
}

resource "google_pubsub_topic_iam_member" "raw_logs_dlq_publisher" {
  project = var.project_id
  topic   = google_pubsub_topic.raw_logs_dlq.name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:service-${var.project_number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

resource "google_pubsub_topic_iam_member" "gmp_metrics_dlq_publisher" {
  project = var.project_id
  topic   = google_pubsub_topic.gmp_metrics_dlq.name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:service-${var.project_number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

resource "google_pubsub_topic_iam_member" "correlated_incidents_dlq_publisher" {
  project = var.project_id
  topic   = google_pubsub_topic.correlated_incidents_dlq.name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:service-${var.project_number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

resource "google_pubsub_subscription_iam_member" "raw_logs_dlq_subscriber" {
  project      = var.project_id
  subscription = google_pubsub_subscription.raw_logs_sub.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:service-${var.project_number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

resource "google_pubsub_subscription_iam_member" "gmp_metrics_dlq_subscriber" {
  project      = var.project_id
  subscription = google_pubsub_subscription.gmp_metrics_sub.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:service-${var.project_number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

resource "google_pubsub_subscription_iam_member" "correlated_incidents_dlq_subscriber" {
  project      = var.project_id
  subscription = google_pubsub_subscription.correlated_incidents_sub.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:service-${var.project_number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}
