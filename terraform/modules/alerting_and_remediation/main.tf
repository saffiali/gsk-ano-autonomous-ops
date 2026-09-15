resource "google_service_account" "remediation_sa" {
  account_id   = "gsk-ano-remediation-sa"
  display_name = "GSK ANO Preemptive Remediation Webhook Service Account (${var.environment})"
  description  = "Dedicated least-privilege service account for Eventarc self-healing webhook and Cloud Workflows dispatch."
  project      = var.project_id
}

resource "google_project_iam_member" "remediation_compute_admin" {
  project = var.project_id
  role    = "roles/compute.instanceAdmin.v1"
  member  = "serviceAccount:${google_service_account.remediation_sa.email}"
}

resource "google_project_iam_member" "remediation_workflows_invoker" {
  project = var.project_id
  role    = "roles/workflows.invoker"
  member  = "serviceAccount:${google_service_account.remediation_sa.email}"
}

resource "google_bigquery_dataset_iam_member" "remediation_bq_editor" {
  project    = var.project_id
  dataset_id = var.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.remediation_sa.email}"
}

resource "google_project_iam_member" "remediation_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.remediation_sa.email}"
}

resource "google_cloud_run_v2_service" "remediation_webhook" {
  name     = "gsk-ano-remediation-webhook-${var.environment}"
  location = var.region
  project  = var.project_id
  ingress  = "INGRESS_TRAFFIC_INTERNAL_ONLY"
  labels   = var.labels

  template {
    service_account = google_service_account.remediation_sa.email

    scaling {
      min_instance_count = 1
      max_instance_count = 10
    }

    containers {
      image = var.remediation_webhook_image

      resources {
        limits = {
          cpu    = "1"
          memory = "2Gi"
        }
      }

      env {
        name  = "PROJECT_ID"
        value = var.project_id
      }
      env {
        name  = "REGION"
        value = var.region
      }
      env {
        name  = "DATASET_ID"
        value = var.dataset_id
      }
      env {
        name  = "CHANGE_CALENDAR_TABLE"
        value = "change_calendar"
      }
      env {
        name  = "INCIDENTS_TABLE"
        value = "incidents_predictions"
      }
      env {
        name  = "COOLDOWN_SECONDS"
        value = "900"
      }
      env {
        name  = "PAGERDUTY_SERVICE_KEY"
        value = var.pagerduty_service_key
      }
    }
  }
}

resource "google_cloud_run_v2_service_iam_member" "remediation_eventarc_invoker" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.remediation_webhook.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.remediation_sa.email}"
}

resource "google_eventarc_trigger" "incidents_remediation_trigger" {
  name            = "gsk-ano-incidents-remediation-trigger-${var.environment}"
  location        = var.region
  project         = var.project_id
  service_account = google_service_account.remediation_sa.email
  labels          = var.labels

  matching_criteria {
    attribute = "type"
    value     = "google.cloud.pubsub.topic.v1.messagePublished"
  }

  transport {
    pubsub {
      topic = var.correlated_incidents_topic_id
    }
  }

  destination {
    cloud_run_service {
      service = google_cloud_run_v2_service.remediation_webhook.name
      region  = var.region
    }
  }
}

resource "google_monitoring_notification_channel" "email_channel" {
  display_name = "GSK ANO Operations On-Call Email (${var.environment})"
  type         = "email"
  project      = var.project_id
  user_labels  = var.labels

  labels = {
    email_address = var.notification_email
  }
}

resource "google_monitoring_notification_channel" "webhook_channel" {
  display_name = "GSK ANO Preemptive Remediation Webhook Channel (${var.environment})"
  type         = "webhook_tokenauth"
  project      = var.project_id
  user_labels  = var.labels

  labels = {
    url = google_cloud_run_v2_service.remediation_webhook.uri
  }
}

resource "google_monitoring_alert_policy" "unsuppressed_root_cause_alert" {
  display_name = "GSK ANO - Unsuppressed Root-Cause Predictive Incident (${var.environment})"
  project      = var.project_id
  combiner     = "OR"
  enabled      = true
  user_labels  = var.labels

  conditions {
    display_name = "Unsuppressed Root-Cause Prediction Anomaly"

    condition_matched_log {
      filter = "resource.type=\"cloud_run_revision\" AND jsonPayload.suppressed_by_change_window=false AND jsonPayload.remediation_status!=\"SUPPRESSED\" AND jsonPayload.anomaly_probability>=0.85"
    }
  }

  alert_strategy {
    notification_rate_limit {
      period = "300s"
    }
    auto_close = "1800s"
  }

  notification_channels = [
    google_monitoring_notification_channel.email_channel.id,
    google_monitoring_notification_channel.webhook_channel.id
  ]

  documentation {
    content   = "Actionable root-cause anomaly predicted by GSK Autonomous Operations AI pipeline outside of scheduled change_calendar maintenance windows."
    mime_type = "text/markdown"
  }
}
