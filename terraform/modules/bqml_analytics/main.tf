locals {
  cap1_model_fqn = "${var.project_id}.${var.dataset_id}.model_cap1_server_unresponsiveness"
  cap2_model_fqn = "${var.project_id}.${var.dataset_id}.model_cap2_cross_domain_rca"
  cap3_model_fqn = "${var.project_id}.${var.dataset_id}.model_cap3_rolling_baseline_arima"
  source_tables = {
    metrics   = var.gmp_metrics_table_id
    topology  = var.topology_edges_table_id
    calendar  = var.change_calendar_table_id
    incidents = var.incidents_predictions_table_id
  }
}

resource "google_bigquery_routine" "train_cap1_unresponsiveness" {
  dataset_id      = var.dataset_id
  routine_id      = "sp_train_cap1_unresponsiveness"
  routine_type    = "PROCEDURE"
  language        = "SQL"
  project         = var.project_id
  description     = "Capability 1 (Server Unresponsiveness Prediction, 15 to 30 min lead): Trains LOGISTIC_REG model on OS starvation indicators in ${local.source_tables.metrics}."
  definition_body = <<-EOF
BEGIN
  CREATE OR REPLACE MODEL `${var.project_id}.${var.dataset_id}.model_cap1_server_unresponsiveness`
  OPTIONS(
    model_type = 'LOGISTIC_REG',
    input_label_cols = ['unresponsive_in_15_30m'],
    auto_class_weights = TRUE,
    l2_reg = 0.1,
    max_iterations = 50,
    learn_rate_strategy = 'LINE_SEARCH',
    data_split_method = 'AUTO_SPLIT'
  ) AS
  SELECT
    cpu_utilization_pct,
    throughput_rps,
    SAFE_DIVIDE(cpu_utilization_pct, NULLIF(throughput_rps, 0.0)) AS cpu_to_throughput_ratio,
    thread_starvation_count,
    io_wait_pct,
    disk_latency_ms,
    socket_exhaustion_count,
    unresponsive_in_15_30m
  FROM
    `${var.project_id}.${var.dataset_id}.gmp_metrics`
  WHERE
    timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
    AND domain = 'COMPUTE'
    AND unresponsive_in_15_30m IS NOT NULL;
END;
EOF
}

resource "google_bigquery_routine" "train_cap2_cross_domain_rca" {
  dataset_id      = var.dataset_id
  routine_id      = "sp_train_cap2_cross_domain_rca"
  routine_type    = "PROCEDURE"
  language        = "SQL"
  project         = var.project_id
  description     = "Capability 2 (Cross-Domain Correlation and RCA, 30 to 60 min lead): Trains BOOSTED_TREE_CLASSIFIER joining ${local.source_tables.metrics} across ${local.source_tables.topology}."
  definition_body = <<-EOF
BEGIN
  CREATE OR REPLACE MODEL `${var.project_id}.${var.dataset_id}.model_cap2_cross_domain_rca`
  OPTIONS(
    model_type = 'BOOSTED_TREE_CLASSIFIER',
    input_label_cols = ['root_cause_domain'],
    auto_class_weights = TRUE,
    max_iterations = 100,
    learn_rate = 0.1,
    subsample = 0.8,
    max_tree_depth = 6,
    data_split_method = 'AUTO_SPLIT'
  ) AS
  SELECT
    app.apache_tomcat_latency_ms,
    app.cpu_utilization_pct AS app_cpu_pct,
    db.db_lock_wait_ms,
    db.db_active_sessions,
    db.disk_latency_ms AS db_disk_latency_ms,
    net.ospf_flap_count,
    net.packet_loss_pct,
    edge_app_db.criticality_weight AS app_to_db_weight,
    edge_db_net.criticality_weight AS db_to_net_weight,
    app.root_cause_domain
  FROM
    `${var.project_id}.${var.dataset_id}.gmp_metrics` AS app
  INNER JOIN
    `${var.project_id}.${var.dataset_id}.topology_edges` AS edge_app_db
    ON app.service_name = edge_app_db.source_entity_id
    AND edge_app_db.target_domain = 'DATABASE'
  INNER JOIN
    `${var.project_id}.${var.dataset_id}.gmp_metrics` AS db
    ON edge_app_db.target_entity_id = db.host_id
    AND TIMESTAMP_TRUNC(app.timestamp, MINUTE) = TIMESTAMP_TRUNC(db.timestamp, MINUTE)
  INNER JOIN
    `${var.project_id}.${var.dataset_id}.topology_edges` AS edge_db_net
    ON db.host_id = edge_db_net.source_entity_id
    AND edge_db_net.target_domain = 'NETWORK'
  INNER JOIN
    `${var.project_id}.${var.dataset_id}.gmp_metrics` AS net
    ON edge_db_net.target_entity_id = net.host_id
    AND TIMESTAMP_TRUNC(app.timestamp, MINUTE) = TIMESTAMP_TRUNC(net.timestamp, MINUTE)
  WHERE
    app.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
    AND app.domain = 'APPLICATION'
    AND app.root_cause_domain IS NOT NULL;
END;
EOF
}

resource "google_bigquery_routine" "train_cap3_rolling_baseline" {
  dataset_id      = var.dataset_id
  routine_id      = "sp_train_cap3_rolling_baseline"
  routine_type    = "PROCEDURE"
  language        = "SQL"
  project         = var.project_id
  description     = "Capability 3 (Dynamic Baselines): Trains 90-day rolling ARIMA_PLUS_XREG time-series model with exogenous regressors from ${local.source_tables.calendar}."
  definition_body = <<-EOF
BEGIN
  CREATE OR REPLACE MODEL `${var.project_id}.${var.dataset_id}.model_cap3_rolling_baseline_arima`
  OPTIONS(
    model_type = 'ARIMA_PLUS_XREG',
    time_series_timestamp_col = 'timestamp_hour',
    time_series_data_col = 'avg_latency_ms',
    time_series_id_col = ['service_name', 'host_id'],
    auto_arima = TRUE,
    data_frequency = 'HOURLY',
    holiday_region = 'GLOBAL',
    clean_spikes_and_dips = TRUE
  ) AS
  SELECT
    TIMESTAMP_TRUNC(m.timestamp, HOUR) AS timestamp_hour,
    m.service_name,
    m.host_id,
    AVG(COALESCE(m.apache_tomcat_latency_ms, m.disk_latency_ms, 0.0)) AS avg_latency_ms,
    MAX(IF(c.change_id IS NOT NULL AND c.status IN ('SCHEDULED', 'IN_PROGRESS'), 1, 0)) AS is_scheduled_change_window,
    MAX(COALESCE(c.change_risk_level, 0)) AS change_risk_level
  FROM
    `${var.project_id}.${var.dataset_id}.gmp_metrics` AS m
  LEFT JOIN
    `${var.project_id}.${var.dataset_id}.change_calendar` AS c
    ON (m.host_id = c.target_entity_id OR m.service_name = c.target_entity_id)
    AND m.timestamp BETWEEN c.start_time AND c.end_time
  WHERE
    m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
  GROUP BY
    timestamp_hour, m.service_name, m.host_id;
END;
EOF
}

resource "google_bigquery_routine" "evaluate_and_suppress_anomalies" {
  dataset_id      = var.dataset_id
  routine_id      = "sp_evaluate_and_suppress_anomalies"
  routine_type    = "PROCEDURE"
  language        = "SQL"
  project         = var.project_id
  description     = "Capability 3 Anomaly Detection & Change-Window Noise Suppression: Executes ML.DETECT_ANOMALIES joined with ${local.source_tables.calendar} into ${local.source_tables.incidents}."
  definition_body = <<-EOF
BEGIN
  INSERT INTO `${var.project_id}.${var.dataset_id}.incidents_predictions` (
    incident_id,
    prediction_timestamp,
    capability_type,
    affected_entity_id,
    root_cause_entity_id,
    root_cause_domain,
    lead_time_minutes,
    anomaly_probability,
    semantic_nearest_neighbor_log_id,
    cosine_distance,
    suppressed_by_change_window,
    active_change_id,
    recommended_action,
    remediation_status,
    remediation_execution_id
  )
  SELECT
    GENERATE_UUID() AS incident_id,
    CURRENT_TIMESTAMP() AS prediction_timestamp,
    'CAP3_DYNAMIC_BASELINE_ANOMALY' AS capability_type,
    a.host_id AS affected_entity_id,
    a.host_id AS root_cause_entity_id,
    'APPLICATION' AS root_cause_domain,
    30 AS lead_time_minutes,
    a.anomaly_probability,
    CAST(NULL AS STRING) AS semantic_nearest_neighbor_log_id,
    CAST(NULL AS FLOAT64) AS cosine_distance,
    IF(c.change_id IS NOT NULL AND c.suppress_alerts = TRUE, TRUE, FALSE) AS suppressed_by_change_window,
    c.change_id AS active_change_id,
    CASE
      WHEN c.change_id IS NOT NULL AND c.suppress_alerts = TRUE THEN 'NO_ACTION_SUPPRESSED'
      WHEN a.is_anomaly = TRUE AND a.anomaly_probability >= 0.95 THEN 'DRAIN_AND_RESTART_WORKERS'
      ELSE 'NO_ACTION_SUPPRESSED'
    END AS recommended_action,
    CASE
      WHEN c.change_id IS NOT NULL AND c.suppress_alerts = TRUE THEN 'SUPPRESSED'
      WHEN a.is_anomaly = TRUE AND a.anomaly_probability >= 0.95 THEN 'PENDING'
      ELSE 'SUPPRESSED'
    END AS remediation_status,
    CAST(NULL AS STRING) AS remediation_execution_id
  FROM
    ML.DETECT_ANOMALIES(
      MODEL `${var.project_id}.${var.dataset_id}.model_cap3_rolling_baseline_arima`,
      STRUCT(0.95 AS anomaly_prob_threshold),
      (
        SELECT
          TIMESTAMP_TRUNC(m.timestamp, HOUR) AS timestamp_hour,
          m.service_name,
          m.host_id,
          AVG(COALESCE(m.apache_tomcat_latency_ms, m.disk_latency_ms, 0.0)) AS avg_latency_ms,
          MAX(IF(c.change_id IS NOT NULL AND c.status IN ('SCHEDULED', 'IN_PROGRESS'), 1, 0)) AS is_scheduled_change_window,
          MAX(COALESCE(c.change_risk_level, 0)) AS change_risk_level
        FROM
          `${var.project_id}.${var.dataset_id}.gmp_metrics` AS m
        LEFT JOIN
          `${var.project_id}.${var.dataset_id}.change_calendar` AS c
          ON (m.host_id = c.target_entity_id OR m.service_name = c.target_entity_id)
          AND m.timestamp BETWEEN c.start_time AND c.end_time
        WHERE
          m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        GROUP BY
          timestamp_hour, m.service_name, m.host_id
      )
    ) AS a
  LEFT JOIN
    `${var.project_id}.${var.dataset_id}.change_calendar` AS c
    ON (a.host_id = c.target_entity_id OR a.service_name = c.target_entity_id)
    AND a.timestamp_hour BETWEEN TIMESTAMP_TRUNC(c.start_time, HOUR) AND TIMESTAMP_TRUNC(c.end_time, HOUR)
    AND c.status IN ('SCHEDULED', 'IN_PROGRESS')
  WHERE
    a.is_anomaly = TRUE;
END;
EOF
}
