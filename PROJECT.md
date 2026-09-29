# Project: GSK Autonomous Operations (ANO) & Enterprise Observability Platform ("Neuro")

## Architecture
The GSK Autonomous Operations (ANO) & Enterprise Observability Platform transitions global IT, hosting, pharmaceutical manufacturing (EBRS, LIMS, MES_BATCH), and network operations (12,000 VMs, 1,500 Applications, 4,000 Apache/Tomcat instances, 600 Relational DBs, 800 Network Switches; ~71.2B daily events / 4.8 TB/day intake across `Site_A_London`, `Site_B_Stevenage`, and `Site_C_Ware`) from reactive troubleshooting to predictive, self-healing, AI-driven operations on Google Cloud Platform (GCP), deployed live in **`gke-demos-363017`**.

### Core Platform Layers (Rounds 1–3)
1. **Zero-ETL & Dataset-Scoped Ingestion (`terraform/modules/ingestion`, `terraform/modules/observability_lakehouse`, `config/otel-collector-config.yaml`, `src/promql_micro_batcher.py`)**:
   - Cloud Logging Project Sink (`gsk_bq_telemetry_sink`) with `use_partitioned_tables = true` and `unique_writer_identity = true`, granted least-privilege `roles/bigquery.dataEditor` strictly at the BigQuery dataset level (`google_bigquery_dataset_iam_member` on `gsk_observability_demo`).
   - OpenTelemetry Collector DaemonSet (`config/otel-collector-config.yaml`) scraping `127.0.0.1:9090` and `localhost:8080`, enforcing `[memory_limiter, resourcedetection, batch]` (`memory_limiter` first), and dual-exporting via `otlp` (`telemetry.googleapis.com:443`) and `googlecloud`.
   - Serverless PromQL `query_range` (`step=60s`) Micro-Batcher (`src/promql_micro_batcher.py`) with `extract_metric_float()` deterministic `0.0` float preservation and BigQuery streaming / Storage Write API support.
2. **EBRS Multi-Site & Multi-Tier Telemetry Lakehouse (`gke-demos-363017.gsk_observability_demo` & `gke-demos-363017.gsk_ano_ops`)**:
   - `enterprise_telemetry_partitioned`: Partitioned by `DATE(timestamp)` (90-day expiration) and hierarchically clustered by `site_location, system_id, application_tier, host_id`.
   - `system_logs`: Partitioned by `DATE(timestamp)` and clustered by `host_id, severity`.
   - `servicenow_maintenance_windows`: Planned maintenance windows (`CHG0049281`, `CHG0051024`) with `is_maintenance_window` binary external regressor.
   - `incident_root_cause_analysis` & `structured_log_entities`: Materialized Gemini 2.5 Flash RCA summaries and extracted log entities.
3. **Full-Stack Multi-Domain ISO GQL Property Graph (`gsk_infrastructure_dependency_graph`)**:
   - 10-step DDL materializing 4 Node Tables (`nodes_switches`, `nodes_hypervisors`, `nodes_hosts`, `nodes_applications`) and 5 Edge Tables (`edges_connected_to`, `edges_hosts_vm`, `edges_runs_app`, `edges_app_communicates`, `edges_network_flows`) into `CREATE OR REPLACE PROPERTY GRAPH gsk_infrastructure_dependency_graph`.
   - Compliant `GRAPH_TABLE` traversal queries across `(Switch: sw-core-stv-01)-[:CONNECTED_TO]->(Hypervisor: esxi-cluster-04)-[:HOSTS]->(Host)-[:RUNS]->(Application)` projecting scalar properties (`src.hostname AS src_host`, `dst.hostname AS dst_host`) and JSON visualization structures.
4. **In-Database BigQuery ML Baselines (`ARIMA_PLUS`, `ARIMA_PLUS_XREG`, TimesFM 2.5) & Gemini 2.5 Flash Synthesis**:
   - `host_cpu_arima_model` (`ARIMA_PLUS`), `host_cpu_arimax_model` (`ARIMA_PLUS_XREG` with ServiceNow regressor), and `composite_host_signals_arima` configured with `HORIZON = 10000`, `HOLIDAY_REGION = 'GB'`, and `GAP_FILL()` 1-minute bucket zero-filling (`COALESCE(cpu_usage, 0.0)`).
   - Anomaly detection (`ML.DETECT_ANOMALIES` & TimesFM 2.5 `AI.DETECT_ANOMALIES` / `AI.FORECAST`) classifying `SPIKE_ANOMALY`, `SILENT_HOST_DROP_TO_ZERO`, `DIP_ANOMALY`, and `NORMAL`.
   - 5-minute sliding-window metric-to-log temporal correlation and Vertex AI `gemini-2.5-flash` remote model synthesis (`ML.GENERATE_TEXT` & `AI.GENERATE_TABLE`).
5. **Interactive MVP Workbench & GSK "Neuro" Conversational AI Assistant (`src/observability_mvp_server.py` & `src/demo_dashboard.py`)**:
   - Dynamic Confidence Band & Anomaly Explorer, Full-Stack ISO GQL Topology Visualizer across Cascading Failure Scenarios A, B, and C, and the GSK "Neuro" natural-language SRE assistant translating operator questions into verified BigQuery SQL / ISO GQL queries.

---

## Feature Inventory
Every feature identified during Survey across Rounds 1, 2, and 3 is mapped below to its assigned milestone/work package. No feature is unassigned.

| # | Feature | Description | Milestone | Source | Status |
|---|---------|-------------|-----------|--------|--------|
| F1 | Architecture Blueprint (`docs/ARCHITECTURE_AND_PIPELINE.md`) | 5-pillar ANO architecture, estate math, IVF/Tree-AH vs AlloyDB `pgvector`, 3 capabilities, KPI formulas. | WP3 (R1) | R1 | DONE |
| F2 | Deployment & Training Runbook (`docs/DEPLOYMENT_GUIDE.md`) | Step-by-step production runbook for Terraform, backfill, Vector Index, and BQML. | WP3 (R1) | R1 | DONE |
| F3 | Terraform Root Module (`terraform/*.tf`) | Root IaC configuration wiring all submodules with clean variable/output contracts. | WP1 (R1) / WP7 (R3) | R1/R3 | DONE |
| F4 | Ingestion Submodule (`terraform/modules/ingestion`) | Log sink, 3 Pub/Sub topics, 3 DLQ topics, subscriptions, least-privilege IAM. | WP1 (R1) | R1 | DONE |
| F5 | Storage & Vector Submodule (`terraform/modules/storage_and_vector`) | Dataset `gsk_ano_ops`, 6 partitioned/clustered tables, Vector Search Index DDL. | WP1 (R1) | R1 | DONE |
| F6 | BQML Analytics Submodule (`terraform/modules/bqml_analytics`) | BQML routines for Capabilities 1, 2, and 3. | WP1 (R1) | R1 | DONE |
| F7 | Embedding Pipeline Submodule (`terraform/modules/embedding_pipeline`) | Cloud Run v2 worker service, SA, IAM, and Eventarc trigger. | WP1 (R1) | R1 | DONE |
| F8 | Alerting & Remediation Submodule (`terraform/modules/alerting_and_remediation`) | Cloud Run v2 remediation webhook, Eventarc trigger, and Monitoring alert policies. | WP1 (R1) | R1 | DONE |
| F9 | Reference Embedding Worker (`src/embedding_worker.py`) | Multi-line stack trace coalescing, regex token masking, sliding-window chunking, 768-dim `text-embedding-005`. | WP2 (R1) | R1 | DONE |
| F10 | Reference Remediation Webhook (`src/remediation_webhook.py`) | `change_calendar` suppression check, cooldown rate limiting, Cloud Workflows dispatch. | WP2 (R1) | R1 | DONE |
| F11 | Automated Verification Suite (`tests/validate_all.py`) | Self-contained test runner for HCL, SQL/GQL, Python unit tests, live GCP state, and REST APIs. | WP2/WP6/WP10 | R1/R2/R3 | DONE |
| F12 | Shared Analytical Mirror (`src/mirror_store.py`) | Dual-mode persistence & query engine for `gsk_ano_ops` and local SQLite mirror. | WP4 (R2) | R2 | DONE |
| F13 | Automated Deployment Script (`scripts/deploy_to_gcp.py`) | Idempotent deployer for `gke-demos-363017.gsk_ano_ops` and local mirror. | WP4 (R2) | R2 | DONE |
| F14 | Live Customer Demo Telemetry Seeder (`src/seed_live_demo.py`) | Seeder for `gsk_ano_ops` 4-Act demo scenarios. | WP4 (R2) | R2 | DONE |
| F15 | Interactive 4-Act CLI Demo Runner & Runbook (`src/demo_runner.py`, `docs/CUSTOMER_DEMO_RUNBOOK.md`) | CLI presenter and runbook for Acts 1–4. | WP5 (R2) | R2 | DONE |
| F16 | Interactive Executive Web UI Dashboard (`src/demo_dashboard.py`) | Zero-dependency Python HTTP server serving Executive AI-Ops Demo Dashboard and REST APIs. | WP5 (R2) / WP9 (R3) | R2/R3 | DONE |
| F17 | Round 2 Verification & GitHub Push | 24/24 verification checks and GitHub push. | WP6 (R2) | R2 | DONE |
| F18 | Remediated Observability Architecture Blueprint (`docs/GSK_OBSERVABILITY_PLATFORM_ARCHITECTURE.md`) | Complete 12-section `/custom-architect`-approved GSK Enterprise Observability Platform blueprint with Findings 1–6 remediations. | WP7 (R3) | R3 Survey | DONE |
| F19 | OpenTelemetry Collector Dual-Pipeline Config (`config/otel-collector-config.yaml`) | DaemonSet config with `127.0.0.1:9090`, `[memory_limiter, resourcedetection, batch]`, `otlp` (`telemetry.googleapis.com:443`) + `googlecloud` exporters. | WP7 (R3) | R3 Survey | DONE |
| F20 | Ready-to-Execute SQL/DDL/GQL Script Catalog (`sql/*.sql`) | Complete SQL/GQL catalog: dataset & IAM (`01`), EBRS lakehouse tables (`02`), `ARIMA_PLUS`/`ARIMA_PLUS_XREG`/TimesFM (`03`), 5m metric-to-log correlation (`04`), 10-step ISO GQL Property Graph & `GRAPH_TABLE` queries (`05`), Gemini 2.5 Flash `ML.GENERATE_TEXT` & `AI.GENERATE_TABLE` (`06`). | WP7 (R3) | R3 Survey | DONE |
| F21 | Observability Lakehouse Terraform Submodule (`terraform/modules/observability_lakehouse/`) | Modular Terraform defining `gsk_observability_demo`, `gsk_bq_telemetry_sink` (`use_partitioned_tables = true`), dataset-scoped `google_bigquery_dataset_iam_member` (`roles/bigquery.dataEditor`), 14 lakehouse/graph tables, and BQML/Graph routines. | WP7 (R3) | R3 Survey | DONE |
| F22 | Serverless PromQL Micro-Batcher (`src/promql_micro_batcher.py`) | PromQL `query_range` (`step=60s`) micro-batcher with `extract_metric_float()` deterministic `0.0` float preservation and BigQuery streaming/Storage Write API support. | WP8 (R3) | R3 Survey | DONE |
| F23 | EBRS Multi-Stage Cascading Failure Seeder (`src/seed_ebrs_observability.py`) | Multi-site telemetry, log, maintenance, and 10-table ISO GQL topology seeder across `Site_A_London`, `Site_B_Stevenage`, `Site_C_Ware` (`EBRS`, `LIMS`, `MES_BATCH`) for Cascading Scenarios A (JVM OOM on `srv-b-batch-02`), B (PostgreSQL 500/500 saturation cascading to `srv-a-web-01`/`srv-a-web-04`), and C (`/mnt/gsk_batch` IO wait >940ms & silent host drop to `0.0`). | WP8 (R3) | R3 Survey | DONE |
| F24 | Live GCP Observability MVP Deployer (`scripts/deploy_observability_mvp.py`) | Idempotent live deployment script provisioning `gke-demos-363017.gsk_observability_demo`, all 14 tables, `gsk_infrastructure_dependency_graph` Property Graph, BQML/TimesFM/Gemini views & models, and local SQLite/JSON mirror fallback. | WP8 (R3) | R3 Survey | DONE |
| F25 | Interactive MVP Workbench & GSK "Neuro" Conversational AI (`src/observability_mvp_server.py` & `src/demo_dashboard.py`) | Web UI & CLI workbench featuring Dynamic Confidence Band & Anomaly Explorer, Full-Stack ISO GQL Topology Visualizer (`Switch -> Hypervisor -> Host -> Application`), and GSK "Neuro" Conversational AI Assistant (`/api/neuro/chat`). | WP9 (R3) | R3 Survey | DONE |
| F26 | E2E Verification Suite (`tests/validate_all.py` Suite 5) & GitHub Push | Comprehensive automated verification of R1–R5 (HCL, SQL/GQL, `0.0` float edge cases, live `gke-demos-363017.gsk_observability_demo` tables & `GRAPH_TABLE` traversal, MVP & "Neuro" REST APIs), `TEST_INFRA.md`/`TEST_READY.md`, and GitHub push to `origin/main`. | WP10 (R3) | R3 Survey | DONE |

---

## Milestones
| # | Name | Scope | Dependencies | Status |
|---|------|-------|--------------|--------|
| WP1–WP6 | Rounds 1 & 2 Baseline Implementation & Verification | Core ANO Terraform, Python workers, `gsk_ano_ops` live deployment, 4-Act Demo Runner & Dashboard, 24/24 test suite. | None | DONE |
| Survey R3 | Round 3 Phase 0 Survey (3 Parallel Explorers/Spec Miners) | `remediated_blueprint.md` spec mining, codebase compatibility analysis, live `gke-demos-363017` & Git readiness check. | WP1–WP6 | DONE (`81df0bc1`, `91aefe71`, `b5aabd44`) |
| WP7 | Blueprint Docs, OTel Config, SQL/GQL Catalog & Terraform (`observability_lakehouse`) | `docs/GSK_OBSERVABILITY_PLATFORM_ARCHITECTURE.md`, `config/otel-collector-config.yaml`, `sql/*.sql`, `terraform/modules/observability_lakehouse/*`, root `terraform/*.tf`. | Survey R3 | DONE (`ef574415`) |
| WP8 | PromQL Micro-Batcher, EBRS Cascading Seeder & Live GCP Deployment (`gke-demos-363017`) | `src/promql_micro_batcher.py`, `src/seed_ebrs_observability.py`, `scripts/deploy_observability_mvp.py`, and live provisioning/verification of `gke-demos-363017.gsk_observability_demo` + `gsk_infrastructure_dependency_graph`. | Survey R3 | DONE (`77d82233`) |
| WP9 | Interactive MVP Workbench & GSK "Neuro" Conversational AI Assistant | `src/observability_mvp_server.py` and `src/demo_dashboard.py` integration with Dynamic Confidence Bands, ISO GQL Topology Visualizer, and "Neuro" NL-to-SQL/GQL chat assistant. | Survey R3 | DONE (`cef44d99`) |
| WP10 | E2E Verification Suite (`tests/validate_all.py` Suite 5), `TEST_READY.md` & Docs | Expand `tests/validate_all.py` for Round 3 HCL, SQL/GQL, `0.0` float edge cases, live `gke-demos-363017.gsk_observability_demo` tables/graph/queries, and MVP/Neuro endpoints; update `README.md`, `TEST_INFRA.md`, `TEST_READY.md`. | WP7, WP8, WP9 | DONE (`83d59673`) |
| Gate R3 | Round 3 Unified Review, Challenge & Forensic Audit Gate + GitHub Push | 2 Reviewers (`APPROVE`), 2 Challengers (`APPROVE`), and 1 Forensic Auditor (`CLEAN`) verifying 100% pass rate and live GCP deployment, followed by `git push origin main`. | WP10 | DONE |

---

## Interface Contracts

### 1. BigQuery Dataset & Table Contracts (`gke-demos-363017.gsk_observability_demo`, Location: `EU`)
- **Core Lakehouse Tables**:
  - `enterprise_telemetry_partitioned`: `timestamp TIMESTAMP NOT NULL`, `enterprise_domain STRING`, `site_location STRING`, `system_id STRING`, `cluster_id STRING`, `application_tier STRING`, `host_id STRING NOT NULL`, `hostname STRING NOT NULL`, `cpu_usage_pct FLOAT64`, `memory_usage_pct FLOAT64`, `io_wait_ms FLOAT64`, `network_bytes_sec INT64`, `status STRING`, `active_connections INT64`. Partitioned by `DATE(timestamp)` (90d expiration), clustered by `site_location, system_id, application_tier, host_id`.
  - `system_logs`: `timestamp TIMESTAMP NOT NULL`, `log_id STRING`, `host_id STRING NOT NULL`, `hostname STRING NOT NULL`, `site_location STRING`, `system_id STRING`, `application_tier STRING`, `severity STRING NOT NULL`, `service_name STRING`, `message STRING NOT NULL`. Partitioned by `DATE(timestamp)`, clustered by `host_id, severity`.
  - `servicenow_maintenance_windows`: `change_id STRING NOT NULL`, `host_id STRING`, `hostname STRING NOT NULL`, `site_location STRING`, `system_id STRING`, `start_time TIMESTAMP NOT NULL`, `end_time TIMESTAMP NOT NULL`, `is_maintenance_window INT64 NOT NULL`, `change_type STRING`, `description STRING`.
  - `incident_root_cause_analysis`: `event_timestamp TIMESTAMP`, `hostname STRING`, `anomaly_score FLOAT64`, `prompt STRING`, `gemini_root_cause_analysis STRING`.
  - `structured_log_entities`: `timestamp TIMESTAMP`, `hostname STRING`, `root_cause_category STRING`, `failed_component STRING`, `error_code STRING`, `recommended_action STRING`, `confidence_score FLOAT64`.
- **10-Step ISO GQL Property Graph (`gsk_infrastructure_dependency_graph`)**:
  - Node Tables:
    1. `nodes_switches` (`KEY (switch_id)`, `LABEL Switch`, properties: `switch_id, hostname, site_location, management_ip, model`)
    2. `nodes_hypervisors` (`KEY (hypervisor_id)`, `LABEL Hypervisor`, properties: `hypervisor_id, hostname, site_location, cluster_name, esxi_version`)
    3. `nodes_hosts` (`KEY (host_id)`, `LABEL Host`, properties: `host_id, hostname, site_location, application_type, operating_system`)
    4. `nodes_applications` (`KEY (app_id)`, `LABEL Application`, properties: `app_id, name, tier, system_id, criticality`)
  - Edge Tables:
    5. `edges_connected_to` (`KEY (edge_id)`, `SOURCE KEY (switch_id) REFERENCES switches`, `DESTINATION KEY (hypervisor_id) REFERENCES hypervisors`, `LABEL CONNECTED_TO`, properties: `port_name, speed_gbps`)
    6. `edges_hosts_vm` (`KEY (edge_id)`, `SOURCE KEY (hypervisor_id) REFERENCES hypervisors`, `DESTINATION KEY (host_id) REFERENCES hosts`, `LABEL HOSTS`, properties: `allocated_vcpus, allocated_ram_gb`)
    7. `edges_runs_app` (`KEY (edge_id)`, `SOURCE KEY (host_id) REFERENCES hosts`, `DESTINATION KEY (app_id) REFERENCES applications`, `LABEL RUNS`, properties: `process_id, listen_port`)
    8. `edges_app_communicates` (`KEY (edge_id)`, `SOURCE KEY (source_app_id) REFERENCES applications`, `DESTINATION KEY (target_app_id) REFERENCES applications`, `LABEL COMMUNICATES_WITH`, properties: `protocol, avg_latency_ms`)
    9. `edges_network_flows` (`KEY (edge_id)`, `SOURCE KEY (source_host_id) REFERENCES hosts`, `DESTINATION KEY (destination_host_id) REFERENCES hosts`, `LABEL CommunicatesWith`, properties: `avg_traffic`)

### 2. Python Module & REST API Contracts
- `src/promql_micro_batcher.py`:
  - `extract_metric_float(raw_val: Any) -> float`: Deterministically returns `0.0` for `0`, `0.0`, `"0"`, `"0.0"`, `None`, `NaN`, `Inf`, or non-numeric strings, and exact `float(raw_val)` for valid floats.
  - `fetch_and_ingest_metrics(project_id=..., dataset_id=..., table_id=..., session=..., bq_inserter=...) -> list[dict]`: Queries PromQL `query_range` (`step="60s"`) and streams formatted rows into `enterprise_telemetry_partitioned`.
- `src/seed_ebrs_observability.py` & `scripts/deploy_observability_mvp.py`:
  - `seed_ebrs_observability(project_id="gke-demos-363017", dataset_id="gsk_observability_demo", mirror_path=...) -> dict`: Seeds multi-site telemetry, logs, maintenance windows, 9 topology tables, and RCA records for Cascading Scenarios A, B, and C.
  - `deploy_observability_mvp(project_id="gke-demos-363017", dataset_id="gsk_observability_demo", location="EU", ...) -> dict`: Provisions live BigQuery dataset, 14 tables, `gsk_infrastructure_dependency_graph`, views, and BQML models, and verifies live `GRAPH_TABLE` & correlation queries.
- `src/observability_mvp_server.py` & `src/demo_dashboard.py`:
  - `NEURO_SYSTEM_PROMPT`: Exact production GSK "Neuro" system prompt from `remediated_blueprint.md` Section 10.
  - `create_observability_mvp_server(host="127.0.0.1", port=0, project_id="gke-demos-363017", dataset_id="gsk_observability_demo", mirror_path=None)` (and integrated routes in `create_dashboard_server`):
    - `GET /api/observability/status`: Dataset, table row counts, property graph status, and KPI summary.
    - `GET /api/observability/anomalies`: Dynamic confidence band series (`actual_cpu`, `expected_lower_bound`, `expected_upper_bound`, `anomaly_classification` including `SPIKE_ANOMALY`, `SILENT_HOST_DROP_TO_ZERO`, and suppressed maintenance window `CHG0049281`).
    - `GET /api/observability/topology`: Full-Stack ISO GQL Property Graph (`Switch: sw-core-stv-01 -> Hypervisor: esxi-cluster-04 -> Host -> Application`) nodes, edges, and blast-radius paths.
    - `GET /api/observability/scenarios` & `POST /api/observability/scenarios/<A|B|C>`: Multi-stage cascading waterfall details, correlated logs, and Gemini 2.5 3-sentence SRE RCA summaries for Scenarios A, B, and C.
    - `POST /api/neuro/chat`: Accepts `{"question": "..."}` and returns `{"question": ..., "system_prompt": NEURO_SYSTEM_PROMPT, "query_type": "SQL"|"ISO_GQL", "generated_query": ..., "results": [...], "executive_summary": {...}}`.

---

## Code Layout & Write Ownership
| Work Package | Exclusive File Write Ownership |
|---|---|
| **WP7** (`worker_r3_arch_tf_1`) | `docs/GSK_OBSERVABILITY_PLATFORM_ARCHITECTURE.md`, `config/otel-collector-config.yaml`, `sql/*.sql`, `terraform/modules/observability_lakehouse/*`, `terraform/main.tf`, `terraform/variables.tf`, `terraform/outputs.tf`, `terraform/terraform.tfvars.example` |
| **WP8** (`worker_r3_gcp_deploy_2`) | `src/promql_micro_batcher.py`, `src/seed_ebrs_observability.py`, `scripts/deploy_observability_mvp.py` (plus live BigQuery dataset `gke-demos-363017.gsk_observability_demo` and `.cache/gsk_observability_mvp_mirror.db`) |
| **WP9** (`worker_r3_workbench_neuro_3`) | `src/observability_mvp_server.py`, `src/demo_dashboard.py` |
| **WP10** (`worker_r3_e2e_tests_4`) | `tests/validate_all.py`, `TEST_INFRA.md`, `TEST_READY.md`, `README.md` |
