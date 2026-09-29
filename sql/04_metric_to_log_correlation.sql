-- =============================================================================
-- GSK Enterprise Observability Platform — 04: Temporal Metric-to-Log Correlation
-- Joins metric anomalies in `enterprise_telemetry_partitioned` with unstructured
-- error/critical/fatal logs in `system_logs` emitted in the preceding 5 minutes.
-- =============================================================================

WITH anomaly_events AS (
  SELECT
    host_id,
    hostname,
    site_location,
    application_tier,
    cpu_usage_pct,
    memory_usage_pct,
    timestamp
  FROM `gke-demos-363017.gsk_observability_demo.enterprise_telemetry_partitioned`
  WHERE status = 'ANOMALY'
    AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
),
correlated_events AS (
  SELECT 
    a.host_id,
    a.hostname,
    a.site_location,
    a.application_tier,
    ROUND(a.cpu_usage_pct, 1) AS cpu_pct,
    ROUND(a.memory_usage_pct, 1) AS mem_pct,
    STRING_AGG(l.message, ' | ' ORDER BY l.timestamp DESC LIMIT 3) AS correlated_error_traces,
    MAX(a.timestamp) AS last_anomaly_timestamp
  FROM anomaly_events a
  LEFT JOIN `gke-demos-363017.gsk_observability_demo.system_logs` l
    ON a.host_id = l.host_id
    AND l.severity IN ('ERROR', 'CRITICAL', 'FATAL')
    AND l.timestamp BETWEEN TIMESTAMP_SUB(a.timestamp, INTERVAL 5 MINUTE) AND a.timestamp
  GROUP BY a.host_id, a.hostname, a.site_location, a.application_tier, a.cpu_usage_pct, a.memory_usage_pct
)
SELECT * FROM correlated_events ORDER BY last_anomaly_timestamp DESC;
