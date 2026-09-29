-- =============================================================================
-- GSK Enterprise Observability Platform — 06: Gemini 2.5 Flash Incident Synthesis
-- Remote Model (`ENDPOINT = 'gemini-2.5-flash'`), `ML.GENERATE_TEXT` Root-Cause
-- Analysis (`incident_root_cause_analysis`), and `AI.GENERATE_TABLE` Schema-Enforced
-- Entity Extraction (`structured_log_entities`).
-- =============================================================================

-- 1. Create Gemini 2.5 Flash Remote Model via BigQuery Cloud Resource Connection
CREATE OR REPLACE MODEL `gke-demos-363017.gsk_observability_demo.gemini_2_5_flash`
REMOTE WITH CONNECTION `projects/gke-demos-363017/locations/europe-west2/connections/gsk_vertex_remote_connection`
OPTIONS (ENDPOINT = 'gemini-2.5-flash');

-- 2. Pattern 1: Natural Language Root-Cause Synthesis (`ML.GENERATE_TEXT`)
CREATE OR REPLACE TABLE `gke-demos-363017.gsk_observability_demo.incident_root_cause_analysis` AS
SELECT 
  event_timestamp,
  hostname,
  anomaly_score,
  prompt,
  ml_generate_text_llm_result AS gemini_root_cause_analysis
FROM ML.GENERATE_TEXT(
  MODEL `gke-demos-363017.gsk_observability_demo.gemini_2_5_flash`,
  (
    SELECT 
      t.timestamp AS event_timestamp,
      t.hostname,
      t.cpu_usage_pct AS anomaly_score,
      CONCAT(
        'You are an expert SRE triage assistant for GSK manufacturing and enterprise IT.\n',
        'Analyze the following telemetry incident:\n',
        '- Hostname: ', t.hostname, '\n',
        '- Application Tier: ', t.application_tier, '\n',
        '- CPU Spike: ', CAST(ROUND(t.cpu_usage_pct, 1) AS STRING), '%\n',
        '- Memory Saturation: ', CAST(ROUND(t.memory_usage_pct, 1) AS STRING), '%\n',
        '- Preceding Error Logs: \n', IFNULL(STRING_AGG(l.message, '\n' ORDER BY l.timestamp DESC LIMIT 5), 'None'), '\n\n',
        'Provide a concise 3-sentence technical summary explaining:\n',
        '1. Immediate root cause.\n',
        '2. Downstream service blast radius.\n',
        '3. Immediate corrective action for the on-call engineer.'
      ) AS prompt
    FROM `gke-demos-363017.gsk_observability_demo.enterprise_telemetry_partitioned` t
    LEFT JOIN `gke-demos-363017.gsk_observability_demo.system_logs` l
      ON t.hostname = l.hostname 
      AND l.timestamp BETWEEN TIMESTAMP_SUB(t.timestamp, INTERVAL 5 MINUTE) AND t.timestamp
    WHERE t.status = 'ANOMALY'
      AND t.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
    GROUP BY t.timestamp, t.hostname, t.application_tier, t.cpu_usage_pct, t.memory_usage_pct
  ),
  STRUCT(
    0.2 AS temperature,
    1024 AS max_output_tokens,
    TRUE AS flatten_json_output
  )
);

-- 3. Pattern 2: Structured Entity Extraction from Unstructured Logs (`AI.GENERATE_TABLE`)
-- Blueprint Reference Pattern:
-- SELECT t.timestamp, t.hostname, extracted.root_cause_category, extracted.failed_component,
--        extracted.error_code, extracted.recommended_action, extracted.confidence_score
-- FROM `gsk-corp-obvspoc-brown-dev.gsk_observability_demo.system_logs` t,
-- AI.GENERATE_TABLE(
--   TABLE `gsk-corp-obvspoc-brown-dev.gsk_observability_demo.system_logs`,
--   MODEL `gsk-corp-obvspoc-brown-dev.gsk_observability_demo.gemini_2_5_flash`,
--   prompt => CONCAT('Extract structured incident attributes from this error log: ', t.message),
--   output_schema => 'root_cause_category STRING, failed_component STRING, error_code STRING, recommended_action STRING, confidence_score FLOAT64'
-- ) AS extracted
-- WHERE t.severity IN ('ERROR', 'CRITICAL', 'FATAL')
--   AND t.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);

-- BigQuery Standard SQL Validated Invocation (`AI.GENERATE_TABLE`):
CREATE OR REPLACE TABLE `gke-demos-363017.gsk_observability_demo.structured_log_entities` AS
SELECT
  timestamp,
  hostname,
  root_cause_category,
  failed_component,
  error_code,
  recommended_action,
  confidence_score
FROM AI.GENERATE_TABLE(
  MODEL `gke-demos-363017.gsk_observability_demo.gemini_2_5_flash`,
  (
    SELECT
      timestamp,
      hostname,
      CONCAT('Extract structured incident attributes from this error log: ', message) AS prompt
    FROM `gke-demos-363017.gsk_observability_demo.system_logs`
    WHERE severity IN ('ERROR', 'CRITICAL', 'FATAL')
      AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
  ),
  STRUCT(
    'root_cause_category STRING, failed_component STRING, error_code STRING, recommended_action STRING, confidence_score FLOAT64' AS output_schema,
    0.1 AS temperature
  )
);
