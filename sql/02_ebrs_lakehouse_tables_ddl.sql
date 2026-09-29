-- =============================================================================
-- GSK Enterprise Observability Platform — 02: EBRS Lakehouse Tables DDL
-- Hierarchy: [enterprise_domain] -> [site_location] -> [system_id] -> [cluster_id] -> [application_tier] -> [host_id]
-- Target Dataset: `gke-demos-363017.gsk_observability_demo`
-- =============================================================================

-- 1. Core EBRS Multi-Site & Multi-Tier Telemetry Table
CREATE TABLE IF NOT EXISTS `gke-demos-363017.gsk_observability_demo.enterprise_telemetry_partitioned`
(
  timestamp TIMESTAMP NOT NULL,
  enterprise_domain STRING,   -- 'Pharma_Manufacturing', 'R_and_D', 'Commercial'
  site_location STRING,       -- 'Site_A_London', 'Site_B_Stevenage', 'Site_C_Ware'
  system_id STRING,           -- 'EBRS', 'LIMS', 'MES_BATCH'
  cluster_id STRING,          -- 'aks-prod-stv-01', 'onprem-esxi-cluster-04'
  application_tier STRING,    -- 'Web_Frontend', 'Database_Backend', 'Batch_Processing'
  host_id STRING NOT NULL,
  hostname STRING NOT NULL,
  cpu_usage_pct FLOAT64,
  memory_usage_pct FLOAT64,
  io_wait_ms FLOAT64,
  network_bytes_sec INT64,
  status STRING,              -- 'HEALTHY', 'ANOMALY', 'DOWN'
  active_connections INT64
)
PARTITION BY DATE(timestamp)
CLUSTER BY site_location, system_id, application_tier, host_id
OPTIONS (
  partition_expiration_days = 90,
  description = "Partitioned and hierarchically clustered raw telemetry table for GSK enterprise monitoring"
);

-- 2. System & Application Logs Table (for 5-Minute Sliding-Window Correlation & AI Entity Extraction)
CREATE TABLE IF NOT EXISTS `gke-demos-363017.gsk_observability_demo.system_logs`
(
  timestamp TIMESTAMP NOT NULL,
  log_id STRING,
  host_id STRING NOT NULL,
  hostname STRING NOT NULL,
  site_location STRING,
  system_id STRING,
  application_tier STRING,
  severity STRING NOT NULL,   -- 'INFO', 'WARNING', 'ERROR', 'CRITICAL', 'FATAL'
  service_name STRING,
  message STRING NOT NULL
)
PARTITION BY DATE(timestamp)
CLUSTER BY host_id, severity
OPTIONS (
  partition_expiration_days = 90,
  description = "Partitioned and clustered unstructured/structured system logs for temporal metric-to-log correlation"
);

-- 3. ServiceNow Planned Maintenance Windows Table (External Regressor for ARIMA_PLUS_XREG)
CREATE TABLE IF NOT EXISTS `gke-demos-363017.gsk_observability_demo.servicenow_maintenance_windows`
(
  change_id STRING NOT NULL,
  host_id STRING,
  hostname STRING NOT NULL,
  site_location STRING,
  system_id STRING,
  start_time TIMESTAMP NOT NULL,
  end_time TIMESTAMP NOT NULL,
  is_maintenance_window INT64 NOT NULL, -- 1 during planned change window, 0 otherwise
  change_type STRING,
  description STRING
)
PARTITION BY DATE(start_time)
CLUSTER BY hostname
OPTIONS (
  description = "ServiceNow planned maintenance windows used as external regressor in ARIMA_PLUS_XREG"
);

-- 4. Gemini 2.5 Flash Root-Cause Analysis Output Table (ML.GENERATE_TEXT)
CREATE TABLE IF NOT EXISTS `gke-demos-363017.gsk_observability_demo.incident_root_cause_analysis`
(
  event_timestamp TIMESTAMP,
  hostname STRING,
  anomaly_score FLOAT64,
  prompt STRING,
  gemini_root_cause_analysis STRING
)
OPTIONS (
  description = "Materialized 3-sentence SRE incident root-cause analyses synthesized by Gemini 2.5 Flash"
);

-- 5. Gemini 2.5 Flash Structured Log Entities Output Table (AI.GENERATE_TABLE)
CREATE TABLE IF NOT EXISTS `gke-demos-363017.gsk_observability_demo.structured_log_entities`
(
  timestamp TIMESTAMP,
  hostname STRING,
  root_cause_category STRING,
  failed_component STRING,
  error_code STRING,
  recommended_action STRING,
  confidence_score FLOAT64
)
OPTIONS (
  description = "Schema-enforced structured incident entities extracted from unstructured error logs via AI.GENERATE_TABLE"
);
