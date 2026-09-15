resource "google_service_account" "embedding_worker_sa" {
  account_id   = "gsk-ano-embed-worker-sa"
  display_name = "GSK ANO Streaming Log Embedding Worker Service Account (${var.environment})"
  description  = "Dedicated least-privilege service account for Cloud Run log chunker and Vertex AI text-embedding-005 worker."
  project      = var.project_id
}

resource "google_project_iam_member" "embedding_vertex_user" {
  project = var.project_id
  role    = "roles/aiplatform.user"
  member  = "serviceAccount:${google_service_account.embedding_worker_sa.email}"
}

resource "google_bigquery_dataset_iam_member" "embedding_bq_editor" {
  project    = var.project_id
  dataset_id = var.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.embedding_worker_sa.email}"
}

resource "google_pubsub_subscription_iam_member" "embedding_sub_consumer" {
  project      = var.project_id
  subscription = var.raw_logs_subscription_name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${google_service_account.embedding_worker_sa.email}"
}

resource "google_project_iam_member" "embedding_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.embedding_worker_sa.email}"
}

resource "google_cloud_run_v2_service" "embedding_worker" {
  name     = "gsk-ano-embedding-worker-${var.environment}"
  location = var.region
  project  = var.project_id
  ingress  = "INGRESS_TRAFFIC_INTERNAL_ONLY"
  labels   = var.labels

  template {
    service_account = google_service_account.embedding_worker_sa.email

    scaling {
      min_instance_count = 1
      max_instance_count = 20
    }

    containers {
      image = var.embedding_worker_image

      resources {
        limits = {
          cpu    = "2"
          memory = "4Gi"
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
        name  = "TABLE_ID"
        value = "log_embeddings"
      }
      env {
        name  = "EMBEDDING_MODEL"
        value = var.embedding_model_name
      }
      env {
        name  = "EMBEDDING_DIMENSIONS"
        value = "768"
      }
      env {
        name  = "WINDOW_SIZE"
        value = "5"
      }
      env {
        name  = "WINDOW_STRIDE"
        value = "2"
      }
    }
  }
}

resource "google_cloud_run_v2_service_iam_member" "eventarc_invoker" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.embedding_worker.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.embedding_worker_sa.email}"
}

resource "google_eventarc_trigger" "raw_logs_trigger" {
  name            = "gsk-ano-raw-logs-trigger-${var.environment}"
  location        = var.region
  project         = var.project_id
  service_account = google_service_account.embedding_worker_sa.email
  labels          = var.labels

  matching_criteria {
    attribute = "type"
    value     = "google.cloud.pubsub.topic.v1.messagePublished"
  }

  transport {
    pubsub {
      topic = var.raw_logs_topic_id
    }
  }

  destination {
    cloud_run_service {
      service = google_cloud_run_v2_service.embedding_worker.name
      region  = var.region
    }
  }
}
