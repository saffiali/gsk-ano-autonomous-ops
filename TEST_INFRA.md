# Test Infrastructure & 4-Tier Coverage Architecture (`TEST_INFRA.md`)

## 1. Overview & Verification Philosophy

The **GSK Autonomous Operations (ANO) & Enterprise Observability Platform ("Neuro")** repository enforces a zero-external-dependency, deterministic, and live-cloud-verified test infrastructure executed via `python3 tests/validate_all.py`.

Following the **Test Coverage Audit Playbook** (`test-coverage-audit`), the test suite spans **5 test suites (`Suites 1–5`)** containing **30 test methods** and **260+ granular assertions** mapped across all **26 project features (`F1–F26`)** and structured into a **4-Tier Test Methodology**:

1. **Tier 1 — Feature Coverage (Positive Functional & Contract Verification)**:
   - Validates every architectural document, Terraform HCL module, OpenTelemetry Collector configuration, SQL/DDL/ISO GQL script catalog (`sql/01..06`), Python streaming/batching pipeline, BQML routine, and interactive Web UI / "Neuro" REST API endpoint against `PROJECT.md` and `ORIGINAL_REQUEST.md` contracts.
2. **Tier 2 — Boundary, Negative & Corner Cases (Adversarial Input & Failure Modes)**:
   - Exercises float `0.0` vs. `None`/`NaN`/`Inf`/`-0.0`/boolean coercion in `extract_metric_float()`, negative/invalid HTTP inputs (`400 Bad Request` on invalid scenario IDs, empty NL chat questions), secret-leak regex scanning, IAM least-privilege negative assertions (forbidding project-wide `roles/owner`, `roles/editor`, `roles/bigquery.admin`), and unclosed HCL block / invalid reference detection.
3. **Tier 3 — Cross-Feature Combinations (Multi-Component Integration)**:
   - Verifies cross-module Terraform variable/output wiring across root `terraform/main.tf` and all 6 submodules (`ingestion`, `storage_and_vector`, `bqml_analytics`, `embedding_pipeline`, `alerting_and_remediation`, `observability_lakehouse`), schema parity between Terraform `google_bigquery_table` JSON schemas, `sql/*.sql` DDL scripts, and SQLite/BigQuery table seeders, and integration of `/api/observability/*` + `/api/neuro/chat` into both `src/observability_mvp_server.py` and `src/demo_dashboard.py`.
4. **Tier 4 — Real-World Application & Live GCP Scenarios (`gke-demos-363017`)**:
   - Executes live end-to-end validation against Google Cloud project **`gke-demos-363017`** (dataset `gsk_observability_demo` and `gsk_ano_ops` in `EU`), verifying all 14 live BigQuery tables have `COUNT(*) > 0`, executing live ISO GQL `GRAPH_TABLE` multi-domain traversals (`Switch -> Hypervisor -> Host -> Application`) on `gsk_infrastructure_dependency_graph`, running 5-minute sliding-window metric-to-log correlation across Cascading Failure Scenarios A, B, and C, and executing live TimesFM 2.5 (`AI.DETECT_ANOMALIES`, `AI.FORECAST`) queries.

---

## 2. Test Suite Architecture (`tests/validate_all.py`)

| Suite ID | TestCase Class | Test Methods | Scope & Primary Target |
| :--- | :--- | :--- | :--- |
| **Suite 1** | `TerraformValidationSuite` | `test_1_1` .. `test_1_6` (6 tests) | Custom HCL v2 tokenizer & AST parser validating syntax, balanced braces/quotes/heredocs, all 6 required submodules (23 `.tf` files), 38+ GCP resources, cross-module variable/output references, and dataset-scoped IAM least-privilege rules. |
| **Suite 2** | `SQLSchemaValidationSuite` | `test_2_1` .. `test_2_3` (3 tests) | Exact schema alignment for `gsk_ano_ops` tables (`raw_logs`, `log_embeddings`, `gmp_metrics`, `topology_edges`, `change_calendar`, `incidents_predictions`), `TREE_AH` Vector Index DDL, and BQML Capabilities 1, 2, and 3 stored procedures. |
| **Suite 3** | `PythonPipelineUnitTestSuite` | `test_3_1` .. `test_3_9` (9 tests) | Unit and adversarial tests for `src/embedding_worker.py` (coalescing, regex scrubbing, sliding-window chunking, Vertex AI `text-embedding-005`) and `src/remediation_webhook.py` (`change_calendar` suppression, cooldown rate limiting, Cloud Workflows dispatch, thread safety). |
| **Suite 4** | `Round2LiveDemoAndDashboardSuite` | `test_4_1` .. `test_4_6` (6 tests) | End-to-end verification of `scripts/deploy_to_gcp.py`, `src/seed_live_demo.py`, `src/demo_runner.py` (Acts 1–4), `src/demo_dashboard.py` HTTP REST APIs, `docs/CUSTOMER_DEMO_RUNBOOK.md`, `README.md`, and git-secrets safety scan. |
| **Suite 5** | `Round3ObservabilityPlatformSuite` | `test_5_1` .. `test_5_6` (6 tests) | End-to-end verification of `docs/GSK_OBSERVABILITY_PLATFORM_ARCHITECTURE.md` (Findings 1–6), `config/otel-collector-config.yaml`, `sql/01..06`, `terraform/modules/observability_lakehouse/`, `src/promql_micro_batcher.py` (`0.0` float edge cases), `src/seed_ebrs_observability.py` (Scenarios A/B/C), live `gke-demos-363017.gsk_observability_demo` BigQuery tables + ISO GQL `GRAPH_TABLE` + TimesFM, and `src/observability_mvp_server.py` + GSK "Neuro" (`/api/neuro/chat`). |

---

## 3. 4-Tier Coverage Matrix Across All 26 Features (`F1–F26`)

| Feature ID | Feature Name | Tier 1: Feature Coverage | Tier 2: Boundary & Corner Cases | Tier 3: Cross-Feature Integration | Tier 4: Real-World & Live GCP Scenarios | Status |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **F1** | Architecture Blueprint (`docs/ARCHITECTURE_AND_PIPELINE.md`) | `test_1_3`, `test_2_2` | `test_4_6` (secret scan) | Cross-checked with Terraform schemas (`test_2_1`) | Estate sizing (12k VMs, 71.2B events/day) | PASS |
| **F2** | Deployment & Training Runbook (`docs/DEPLOYMENT_GUIDE.md`) | `test_2_3`, `test_4_5` | `test_4_6` | Matches BQML routine signatures (`test_2_3`) | Production backfill & training runbook | PASS |
| **F3** | Terraform Root Module (`terraform/*.tf`) | `test_1_1`, `test_1_2`, `test_1_3` | Unbalanced brace/string parser checks (`test_1_2`) | `test_1_5` (wires all 6 submodules & outputs) | `gke-demos-363017` `europe-west2` / `EU` config | PASS |
| **F4** | Ingestion Submodule (`terraform/modules/ingestion`) | `test_1_4` (sink, 3 topics, 3 DLQs) | Exclusion filter & retry backoff bounds | `test_1_5`, `test_1_6` (least-privilege IAM) | High-volume Cloud Logging & GMP ingestion | PASS |
| **F5** | Storage & Vector Submodule (`terraform/modules/storage_and_vector`) | `test_1_4`, `test_2_1`, `test_2_2` | Partition expiration & `TREE_AH` `COSINE` checks | Schema parity with `src/mirror_store.py` (`test_4_1`) | Live dataset `gke-demos-363017.gsk_ano_ops` | PASS |
| **F6** | BQML Analytics Submodule (`terraform/modules/bqml_analytics`) | `test_1_4`, `test_2_3` | Threshold & lead-time window checks | Queries `gmp_metrics`, `topology_edges`, `change_calendar` | Capabilities 1 (15–30m), 2 (30–60m), 3 (90d) | PASS |
| **F7** | Embedding Pipeline Submodule (`terraform/modules/embedding_pipeline`) | `test_1_4`, `test_1_5` | Cloud Run scaling & concurrency limits | Wired to Pub/Sub & BigQuery tables (`test_1_5`) | Streaming `text-embedding-005` worker | PASS |
| **F8** | Alerting & Remediation Submodule (`terraform/modules/alerting_and_remediation`) | `test_1_4`, `test_1_5` | Alert rate-limiting & auto-close windows | Eventarc -> Cloud Run -> Cloud Workflows | Self-healing OSPF reroute & JVM drain | PASS |
| **F9** | Reference Embedding Worker (`src/embedding_worker.py`) | `test_3_1`, `test_3_2`, `test_3_3`, `test_3_4` | `test_3_9` (empty logs, huge stack traces, non-UTF8) | Pub/Sub CloudEvent -> Vertex AI -> BQ writer (`test_3_7`) | Java/Oracle stack trace normalization | PASS |
| **F10** | Reference Remediation Webhook (`src/remediation_webhook.py`) | `test_3_5`, `test_3_6`, `test_3_8` | `test_3_9` (concurrent webhook lock & hourly cap) | `change_calendar` check + Workflows dispatch (`test_3_7`) | `CHG0049281` suppression vs. active remediation | PASS |
| **F11** | Automated Verification Suite (`tests/validate_all.py`) | Suites 1–5 (30 test methods) | Negative assertions in every suite | Cross-layer HCL + SQL + Python + HTTP validation | Live `gke-demos-363017` verification | PASS |
| **F12** | Shared Analytical Mirror (`src/mirror_store.py`) | `test_4_1`, `test_4_2` | Offline fallback when credentials unavailable | Bridges `deploy_to_gcp.py`, `demo_runner.py`, `demo_dashboard.py` | Dual-mode BigQuery + SQLite mirror | PASS |
| **F13** | Automated Deployment Script (`scripts/deploy_to_gcp.py`) | `test_4_1` (`--verify`) | Idempotent re-execution & dry-run fallback | Provisions schemas matching `storage_and_vector` | Targets `gke-demos-363017.gsk_ano_ops` | PASS |
| **F14** | Live Customer Demo Telemetry Seeder (`src/seed_live_demo.py`) | `test_4_2` | Idempotent seeding & live incident injection | Populates tables queried by `demo_runner.py` | 90-day seasonal metrics + `CHG0049281` | PASS |
| **F15** | Interactive 4-Act CLI Demo Runner & Runbook (`src/demo_runner.py`, `docs/CUSTOMER_DEMO_RUNBOOK.md`) | `test_4_3`, `test_4_5` | Single-act vs. `--act all` CLI execution | Executes SQL against mirror & live BigQuery | Full 4-Act presenter flow & talking points | PASS |
| **F16** | Interactive Executive Web UI Dashboard (`src/demo_dashboard.py`) | `test_4_4`, `test_5_6` | Invalid act/scenario HTTP 400 error handling | Integrates Round 2 Acts 1–4 + Round 3 Observability/Neuro | Live Executive Web UI + REST APIs | PASS |
| **F17** | Round 2 Verification & GitHub Push | `test_4_1` .. `test_4_6` | `test_4_6` (zero secrets in working tree) | End-to-end Round 1 + Round 2 regression safety | Verified in git history & `validate_all.py` | PASS |
| **F18** | Remediated Observability Architecture Blueprint (`docs/GSK_OBSERVABILITY_PLATFORM_ARCHITECTURE.md`) | `test_5_1` (12 sections + Executive Summary) | Negative check for broken `SeeVertex`, `[w]`, `[ew]`, `'gemini-model-endpoint'` | Contains exact `NEURO_SYSTEM_PROMPT` used by server | Remediates all 6 `/custom-architect` findings | PASS |
| **F19** | OpenTelemetry Collector Dual-Pipeline Config (`config/otel-collector-config.yaml`) | `test_5_1` | Forbids `0.0.0.0:9090` & `googlemanagedprometheus`; enforces `memory_limiter` first | Aligns resource attributes (`Pharma_Manufacturing`) with BQ | Dual-export (`otlp` + `googlecloud`) | PASS |
| **F20** | Ready-to-Execute SQL/DDL/GQL Script Catalog (`sql/01..06`) | `test_5_2` | Forbids `TO_JSON(src)` on graph elements & placeholder endpoints | Exact table & routine parity with `observability_lakehouse` | Executed live against `gke-demos-363017` | PASS |
| **F21** | Observability Lakehouse Terraform Submodule (`terraform/modules/observability_lakehouse/`) | `test_1_3`, `test_5_2` (14 tables, 4 routines, sink, IAM) | `test_1_6` (dataset-scoped `google_bigquery_dataset_iam_member` only) | Wired into root `terraform/main.tf`, `variables.tf`, `outputs.tf` | Provisions `gsk_observability_demo` lakehouse | PASS |
| **F22** | Serverless PromQL Micro-Batcher (`src/promql_micro_batcher.py`) | `test_5_3` | `0`, `0.0`, `-0.0`, `"0"`, `"0.0"`, `None`, `NaN`, `Inf`, `-Inf`, `bool`, `dict` -> `0.0` | Merges CPU + Memory matrix streams into BQ schema | Preserves `0.0` for silent host drops (Scenario C) | PASS |
| **F23** | EBRS Multi-Stage Cascading Failure Seeder (`src/seed_ebrs_observability.py`) | `test_5_4` (all 14 tables) | Verifies exact thresholds (`98.8%` heap, `500/500` pool, `>940ms` IO wait, `0.0` drop) | Populates both SQLite mirror and live BigQuery | Multi-site EBRS/LIMS/MES_BATCH Scenarios A, B, C | PASS |
| **F24** | Live GCP Observability MVP Deployer (`scripts/deploy_observability_mvp.py`) | `test_5_5` | Verifies active GCP credentials & non-zero row counts across all 14 tables | Compiles `sql/01..06` and seeds via `seed_ebrs_observability.py` | Live `gke-demos-363017.gsk_observability_demo` + `GRAPH_TABLE` + TimesFM | PASS |
| **F25** | Interactive MVP Workbench & GSK "Neuro" Conversational AI (`src/observability_mvp_server.py`) | `test_5_6` | Empty question `""` fallback & HTTP 400 on `/api/observability/scenarios/INVALID_SCENARIO` | Mounted in both `observability_mvp_server.py` and `demo_dashboard.py` | Interactive confidence bands, ISO GQL graph, Neuro NL-to-SQL/GQL chat | PASS |
| **F26** | E2E Verification Suite (`tests/validate_all.py` Suite 5), `TEST_INFRA.md` & `TEST_READY.md` | `test_5_1` .. `test_5_6` | Full regression across Suites 1–5 (30/30 tests) | Verifies all 26 features end-to-end | 100% pass rate with live GCP verification | PASS |

---

## 4. How to Run the Verification Suite

```bash
# Run the complete 30-test verification suite (Suites 1-5)
python3 tests/validate_all.py

# Run PromQL Micro-Batcher standalone self-test (0.0 float edge cases)
python3 src/promql_micro_batcher.py --self-test

# Run live GCP verification against gke-demos-363017.gsk_observability_demo
python3 scripts/deploy_observability_mvp.py --verify-only

# Run Neuro & Observability MVP Server standalone smoke check
python3 src/observability_mvp_server.py --self-test
```
