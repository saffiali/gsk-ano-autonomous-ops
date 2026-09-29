-- =============================================================================
-- GSK Enterprise Observability Platform — 03: BQML ARIMA_PLUS, ARIMA_PLUS_XREG & TimesFM 2.5 Baselines
-- Features: HORIZON = 10000, HOLIDAY_REGION = 'GB', TIME_SERIES_ID_COL = 'hostname',
--           GAP_FILL() with COALESCE(cpu_usage, 0.0), ServiceNow is_maintenance regressor,
--           SPIKE_ANOMALY / SILENT_HOST_DROP_TO_ZERO / DIP_ANOMALY classification,
--           and TimesFM 2.5 AI.DETECT_ANOMALIES & AI.FORECAST.
-- =============================================================================

-- 0. Helper View: Dense 1-Minute Telemetry View with GAP_FILL() Zero-Filling
CREATE OR REPLACE VIEW `gke-demos-363017.gsk_observability_demo.dense_minute_telemetry_view` AS
WITH time_bounds AS (
  SELECT
    TIMESTAMP_SUB(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MINUTE), INTERVAL 14 DAY) AS start_ts,
    TIMESTAMP_SUB(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MINUTE), INTERVAL 1 MINUTE) AS end_ts
),
raw_minute_telemetry AS (
  SELECT
    TIMESTAMP_TRUNC(timestamp, MINUTE) AS ts_minute,
    hostname,
    AVG(cpu_usage_pct) AS cpu_usage
  FROM `gke-demos-363017.gsk_observability_demo.enterprise_telemetry_partitioned`
  CROSS JOIN time_bounds b
  WHERE timestamp >= b.start_ts
    AND timestamp <= b.end_ts
  GROUP BY ts_minute, hostname
),
active_hosts AS (
  SELECT DISTINCT hostname FROM raw_minute_telemetry
),
anchored_telemetry AS (
  SELECT ts_minute, hostname, cpu_usage FROM raw_minute_telemetry
  UNION ALL
  SELECT b.start_ts AS ts_minute, h.hostname, 0.0 AS cpu_usage FROM active_hosts h CROSS JOIN time_bounds b
  UNION ALL
  SELECT b.end_ts AS ts_minute, h.hostname, 0.0 AS cpu_usage FROM active_hosts h CROSS JOIN time_bounds b
),
deduped_telemetry AS (
  SELECT ts_minute, hostname, MAX(cpu_usage) AS cpu_usage
  FROM anchored_telemetry
  GROUP BY ts_minute, hostname
)
SELECT
  ts_minute,
  hostname,
  COALESCE(cpu_usage, 0.0) AS cpu_usage
FROM GAP_FILL(
  TABLE deduped_telemetry,
  ts_column => 'ts_minute',
  bucket_width => INTERVAL 1 MINUTE,
  partitioning_columns => ['hostname'],
  value_columns => [('cpu_usage', 'null')]
);

-- 1. Standard Multi-Host ARIMA_PLUS Model (HORIZON = 10000, HOLIDAY_REGION = 'GB')
CREATE OR REPLACE MODEL `gke-demos-363017.gsk_observability_demo.host_cpu_arima_model`
OPTIONS (
  MODEL_TYPE = 'ARIMA_PLUS',
  TIME_SERIES_TIMESTAMP_COL = 'ts_minute',
  TIME_SERIES_DATA_COL = 'cpu_usage',
  TIME_SERIES_ID_COL = 'hostname',
  DATA_FREQUENCY = 'PER_MINUTE',
  HORIZON = 10000,                         -- 10,000 1-minute steps (~7 days) prevents NULL bounds
  HOLIDAY_REGION = 'GB',                   -- UK Statutory Holiday calendar for GSK sites
  AUTO_ARIMA = TRUE,
  CLEAN_SPIKES_AND_DIPS = TRUE
) AS
WITH time_bounds AS (
  SELECT
    TIMESTAMP_SUB(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MINUTE), INTERVAL 14 DAY) AS start_ts,
    TIMESTAMP_SUB(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MINUTE), INTERVAL 1 MINUTE) AS end_ts
),
raw_minute_telemetry AS (
  SELECT
    TIMESTAMP_TRUNC(timestamp, MINUTE) AS ts_minute,
    hostname,
    AVG(cpu_usage_pct) AS cpu_usage
  FROM `gke-demos-363017.gsk_observability_demo.enterprise_telemetry_partitioned`
  CROSS JOIN time_bounds b
  WHERE timestamp >= b.start_ts
    AND timestamp <= b.end_ts
  GROUP BY ts_minute, hostname
),
active_hosts AS (
  SELECT DISTINCT hostname FROM raw_minute_telemetry
),
anchored_telemetry AS (
  SELECT ts_minute, hostname, cpu_usage FROM raw_minute_telemetry
  UNION ALL
  SELECT b.start_ts AS ts_minute, h.hostname, 0.0 AS cpu_usage FROM active_hosts h CROSS JOIN time_bounds b
  UNION ALL
  SELECT b.end_ts AS ts_minute, h.hostname, 0.0 AS cpu_usage FROM active_hosts h CROSS JOIN time_bounds b
),
deduped_telemetry AS (
  SELECT ts_minute, hostname, MAX(cpu_usage) AS cpu_usage
  FROM anchored_telemetry
  GROUP BY ts_minute, hostname
)
SELECT
  ts_minute,
  hostname,
  COALESCE(cpu_usage, 0.0) AS cpu_usage
FROM GAP_FILL(
  TABLE deduped_telemetry,
  ts_column => 'ts_minute',
  bucket_width => INTERVAL 1 MINUTE,
  partitioning_columns => ['hostname'],
  value_columns => [('cpu_usage', 'null')]
);

-- 2. Multivariate Modeling with Planned Maintenance Windows (ARIMA_PLUS_XREG)
CREATE OR REPLACE MODEL `gke-demos-363017.gsk_observability_demo.host_cpu_arimax_model`
OPTIONS (
  MODEL_TYPE = 'ARIMA_PLUS_XREG',
  TIME_SERIES_TIMESTAMP_COL = 'ts_minute',
  TIME_SERIES_DATA_COL = 'cpu_usage',
  TIME_SERIES_ID_COL = 'hostname',
  DATA_FREQUENCY = 'PER_MINUTE',
  HORIZON = 10000,
  HOLIDAY_REGION = 'GB',
  AUTO_ARIMA = TRUE,
  CLEAN_SPIKES_AND_DIPS = TRUE
) AS
SELECT
  t.ts_minute,
  t.hostname,
  t.cpu_usage,
  -- External regressor: 1 during ServiceNow planned change/maintenance window, 0 otherwise
  COALESCE(m.is_maintenance_window, 0) AS is_maintenance
FROM `gke-demos-363017.gsk_observability_demo.dense_minute_telemetry_view` t
LEFT JOIN `gke-demos-363017.gsk_observability_demo.servicenow_maintenance_windows` m
  ON t.hostname = m.hostname
  AND t.ts_minute BETWEEN m.start_time AND m.end_time;

-- 3. Composite Multi-Signal Training View (UNION ALL + Composite IDs)
CREATE OR REPLACE VIEW `gke-demos-363017.gsk_observability_demo.unified_telemetry_signal_view` AS
-- Signal 1: 1-minute CPU utilization
SELECT 
  TIMESTAMP_TRUNC(timestamp, MINUTE) AS ts_minute,
  hostname,
  'cpu_pct' AS metric_name,
  AVG(cpu_usage_pct) AS metric_value
FROM `gke-demos-363017.gsk_observability_demo.enterprise_telemetry_partitioned`
GROUP BY ts_minute, hostname

UNION ALL

-- Signal 2: 1-minute Log Count Volume
SELECT 
  TIMESTAMP_TRUNC(timestamp, MINUTE) AS ts_minute,
  hostname,
  'log_volume' AS metric_name,
  CAST(COUNT(*) AS FLOAT64) AS metric_value
FROM `gke-demos-363017.gsk_observability_demo.system_logs`
GROUP BY ts_minute, hostname;

-- 4. Multi-Signal ARIMA Model Training (TIME_SERIES_ID_COL = ['hostname', 'metric_name'])
CREATE OR REPLACE MODEL `gke-demos-363017.gsk_observability_demo.composite_host_signals_arima`
OPTIONS (
  MODEL_TYPE = 'ARIMA_PLUS',
  TIME_SERIES_TIMESTAMP_COL = 'ts_minute',
  TIME_SERIES_DATA_COL = 'metric_value',
  TIME_SERIES_ID_COL = ['hostname', 'metric_name'], -- Grouped by host and signal type
  DATA_FREQUENCY = 'PER_MINUTE',
  HORIZON = 10000,
  HOLIDAY_REGION = 'GB',
  AUTO_ARIMA = TRUE,
  CLEAN_SPIKES_AND_DIPS = TRUE
) AS
SELECT ts_minute, hostname, metric_name, metric_value
FROM `gke-demos-363017.gsk_observability_demo.unified_telemetry_signal_view`
WHERE ts_minute >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY);

-- 5. Method A: BQML ML.DETECT_ANOMALIES with Dynamic Bounds & Anomaly Classification
WITH active_hosts AS (
  SELECT DISTINCT hostname
  FROM `gke-demos-363017.gsk_observability_demo.enterprise_telemetry_partitioned`
  WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
),
minute_grid AS (
  SELECT ts_minute, h.hostname
  FROM UNNEST(
    GENERATE_TIMESTAMP_ARRAY(
      TIMESTAMP_SUB(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MINUTE), INTERVAL 15 MINUTE),
      TIMESTAMP_SUB(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MINUTE), INTERVAL 1 MINUTE),
      INTERVAL 1 MINUTE
    )
  ) AS ts_minute
  CROSS JOIN active_hosts h
),
recent_telemetry AS (
  SELECT
    TIMESTAMP_TRUNC(timestamp, MINUTE) AS ts_minute,
    hostname,
    AVG(cpu_usage_pct) AS actual_cpu
  FROM `gke-demos-363017.gsk_observability_demo.enterprise_telemetry_partitioned`
  WHERE timestamp >= TIMESTAMP_SUB(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MINUTE), INTERVAL 15 MINUTE)
    AND timestamp < TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MINUTE)
  GROUP BY ts_minute, hostname
),
dense_eval_series AS (
  SELECT
    g.ts_minute,
    g.hostname,
    CAST(COALESCE(r.actual_cpu, 0.0) AS FLOAT64) AS cpu_usage
  FROM minute_grid g
  LEFT JOIN recent_telemetry r
    ON g.ts_minute = r.ts_minute AND g.hostname = r.hostname
)
SELECT
  hostname,
  ts_minute,
  ROUND(cpu_usage, 2) AS actual_cpu,
  ROUND(GREATEST(0.0, lower_bound), 2) AS expected_lower_bound,
  ROUND(upper_bound, 2) AS expected_upper_bound,
  is_anomaly,
  ROUND(anomaly_probability, 4) AS anomaly_probability,
  CASE
    WHEN is_anomaly AND cpu_usage > upper_bound THEN 'SPIKE_ANOMALY'
    WHEN is_anomaly AND cpu_usage < lower_bound AND cpu_usage = 0.0 THEN 'SILENT_HOST_DROP_TO_ZERO'
    WHEN is_anomaly AND cpu_usage < lower_bound THEN 'DIP_ANOMALY'
    ELSE 'NORMAL'
  END AS anomaly_classification
FROM ML.DETECT_ANOMALIES(
  MODEL `gke-demos-363017.gsk_observability_demo.host_cpu_arima_model`,
  STRUCT(0.85 AS anomaly_prob_threshold),
  TABLE dense_eval_series
)
ORDER BY is_anomaly DESC, anomaly_probability DESC, ts_minute DESC;

-- 6. Helper Views for TimesFM 2.5 Evaluation (historical_cpu_view & recent_cpu_eval_view)
CREATE OR REPLACE VIEW `gke-demos-363017.gsk_observability_demo.historical_cpu_view` AS
SELECT
  TIMESTAMP_TRUNC(timestamp, MINUTE) AS ts_minute,
  hostname,
  AVG(cpu_usage_pct) AS cpu_usage
FROM `gke-demos-363017.gsk_observability_demo.enterprise_telemetry_partitioned`
WHERE timestamp < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
GROUP BY ts_minute, hostname;

CREATE OR REPLACE VIEW `gke-demos-363017.gsk_observability_demo.recent_cpu_eval_view` AS
SELECT
  TIMESTAMP_TRUNC(timestamp, MINUTE) AS ts_minute,
  hostname,
  AVG(cpu_usage_pct) AS cpu_usage
FROM `gke-demos-363017.gsk_observability_demo.enterprise_telemetry_partitioned`
WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
GROUP BY ts_minute, hostname;

-- 7. Method B: Zero-Shot Anomaly Detection via TimesFM 2.5 (AI.DETECT_ANOMALIES)
SELECT
  hostname,
  ts_minute,
  ROUND(actual_value, 2) AS actual_cpu,
  ROUND(lower_bound, 2) AS lower_bound,
  ROUND(upper_bound, 2) AS upper_bound,
  is_anomaly,
  ROUND(anomaly_probability, 4) AS anomaly_probability
FROM AI.DETECT_ANOMALIES(
  TABLE `gke-demos-363017.gsk_observability_demo.historical_cpu_view`,
  TABLE `gke-demos-363017.gsk_observability_demo.recent_cpu_eval_view`,
  data_col => 'cpu_usage',
  timestamp_col => 'ts_minute',
  id_cols => ['hostname'],
  anomaly_prob_threshold => 0.85
)
WHERE is_anomaly = TRUE
ORDER BY anomaly_probability DESC;

-- 8. Method C: Zero-Shot Forecasting via TimesFM 2.5 (AI.FORECAST)
SELECT
  hostname,
  forecast_timestamp,
  ROUND(forecast_value, 2) AS projected_cpu,
  ROUND(prediction_interval_lower_bound, 2) AS lower_bound,
  ROUND(prediction_interval_upper_bound, 2) AS upper_bound
FROM AI.FORECAST(
  TABLE `gke-demos-363017.gsk_observability_demo.recent_cpu_eval_view`,
  data_col => 'cpu_usage',
  timestamp_col => 'ts_minute',
  id_cols => ['hostname'],
  horizon => 60,                 -- Predict next 60 minutes
  confidence_level => 0.95
)
ORDER BY hostname, forecast_timestamp ASC;
