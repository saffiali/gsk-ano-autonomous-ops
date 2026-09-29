-- =============================================================================
-- GSK Enterprise Observability Platform — 01: Dataset & Least-Privilege IAM
-- Target Live Deployment: `gke-demos-363017.gsk_observability_demo` (Location: EU)
-- Blueprint Reference: `gsk-corp-obvspoc-brown-dev.gsk_observability_demo`
-- =============================================================================

-- 1. Create the write-enabled BigQuery Observability Dataset
-- Note: Log Analytics linked datasets (`projects.locations.buckets.links.create`)
-- are read-only views and cannot be used as Cloud Logging Sink write destinations.
CREATE SCHEMA IF NOT EXISTS `gke-demos-363017.gsk_observability_demo`
OPTIONS (
  location = 'EU',
  description = 'GSK Enterprise Observability Lakehouse: Unified telemetry, metric rollups, BQML baselines, and ISO GQL property graphs'
);

-- 2. Dataset-Scoped Least-Privilege IAM Assignment (SQL DCL)
-- Grants roles/bigquery.dataEditor strictly on the target schema/dataset
-- to the Cloud Logging sink unique writerIdentity service account (never project-wide).
GRANT `roles/bigquery.dataEditor`
ON SCHEMA `gke-demos-363017.gsk_observability_demo`
TO "serviceAccount:p424242424242-999999@gcp-sa-logging.iam.gserviceaccount.com";

-- Reference Blueprint DCL (`gsk-corp-obvspoc-brown-dev.gsk_observability_demo`):
-- GRANT `roles/bigquery.dataEditor`
-- ON SCHEMA `gsk-corp-obvspoc-brown-dev.gsk_observability_demo`
-- TO "serviceAccount:p424242424242-999999@gcp-sa-logging.iam.gserviceaccount.com";
