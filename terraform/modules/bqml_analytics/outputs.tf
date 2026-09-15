output "cap1_model_name" {
  description = "Fully qualified BQML model name for Capability 1 (Server Unresponsiveness LOGISTIC_REG)."
  value       = local.cap1_model_fqn
}

output "cap2_model_name" {
  description = "Fully qualified BQML model name for Capability 2 (Cross-Domain RCA BOOSTED_TREE_CLASSIFIER)."
  value       = local.cap2_model_fqn
}

output "cap3_model_name" {
  description = "Fully qualified BQML model name for Capability 3 (Rolling Baseline ARIMA_PLUS_XREG)."
  value       = local.cap3_model_fqn
}

output "cap1_routine_id" {
  description = "Routine ID for training Capability 1 model."
  value       = google_bigquery_routine.train_cap1_unresponsiveness.routine_id
}

output "cap2_routine_id" {
  description = "Routine ID for training Capability 2 model."
  value       = google_bigquery_routine.train_cap2_cross_domain_rca.routine_id
}

output "cap3_routine_id" {
  description = "Routine ID for training Capability 3 model."
  value       = google_bigquery_routine.train_cap3_rolling_baseline.routine_id
}

output "anomaly_suppression_routine_id" {
  description = "Routine ID for real-time anomaly detection and change_calendar suppression."
  value       = google_bigquery_routine.evaluate_and_suppress_anomalies.routine_id
}

output "bqml_model_names" {
  description = "Map of all 3 trained BQML model identifiers."
  value = {
    capability_1_unresponsiveness = local.cap1_model_fqn
    capability_2_cross_domain_rca = local.cap2_model_fqn
    capability_3_rolling_baseline = local.cap3_model_fqn
  }
}
