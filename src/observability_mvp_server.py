#!/usr/bin/env python3
"""GSK Enterprise Observability Platform ("Neuro") — Interactive MVP Workbench & Server.

Self-contained, zero-external-dependency Python HTTP server and CLI runner
connected to `gke-demos-363017.gsk_observability_demo` with sub-50ms local
SQLite/JSON mirror fallback.

Delivers:
  1. Exact Production GSK "Neuro" System Prompt (`NEURO_SYSTEM_PROMPT`) from
     Section 10 of the remediated architecture blueprint.
  2. Dynamic Confidence Band & Anomaly Explorer (`/api/observability/anomalies`):
     Time-series telemetry with `actual_cpu`, `expected_lower_bound`,
     `expected_upper_bound`, `anomaly_probability`, and `anomaly_classification`
     (`SPIKE_ANOMALY`, `SILENT_HOST_DROP_TO_ZERO`, `DIP_ANOMALY`,
     `SUPPRESSED_MAINTENANCE_WINDOW` during `CHG0049281`, and `NORMAL`),
     including `GAP_FILL()` zero-filled heartbeat drops (`0.0`).
  3. Full-Stack ISO GQL Topology Visualizer (`/api/observability/topology`):
     4-layer multi-domain property graph
     `(Switch: sw-core-stv-01)-[:CONNECTED_TO]->(Hypervisor: esxi-cluster-04)`
     `-[:HOSTS]->(Host)-[:RUNS]->(Application)<-[:COMMUNICATES_WITH]-(Application)`
     with nodes, edges, scalar `GRAPH_TABLE` path matches, and cascading blast
     radius across Scenarios A, B, and C.
  4. Multi-Stage Cascading Failure Waterfalls (`/api/observability/scenarios` &
     `/api/observability/scenarios/<A|B|C>`):
     Scenario A (JVM OOM on `srv-b-batch-02`), Scenario B (PostgreSQL 500/500
     saturation cascading to `srv-a-web-01` / `srv-a-web-04`), and Scenario C
     (`/mnt/gsk_batch` IO wait >940ms & silent drop to `0.0`), complete with
     5-minute sliding-window correlated logs, Gemini 2.5 Flash 3-sentence SRE
     RCA summaries (`incident_root_cause_analysis`), and extracted log entities
     (`structured_log_entities`).
  5. GSK "Neuro" Conversational AI Assistant (`POST /api/neuro/chat`):
     Translates operator natural-language questions into verified BigQuery SQL
     or ISO GQL `GRAPH_TABLE` queries, executes them against the mirror (and
     optionally live BigQuery), and returns structured executive incident
     summaries (`hostname`, `physical_site`, `impacted_application`,
     `measured_utilization`, `remediation_runbook`, `three_sentence_rca`).
"""

import argparse
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import threading
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
  sys.path.insert(0, str(PROJECT_ROOT))

DEFAULT_PROJECT_ID = "gke-demos-363017"
DEFAULT_DATASET_ID = "gsk_observability_demo"
DEFAULT_MIRROR_PATH = PROJECT_ROOT / ".cache" / "gsk_observability_mvp_mirror.db"

# Exact Production System Prompt for GSK "Neuro" (Blueprint Section 10)
NEURO_SYSTEM_PROMPT = """Role:
You are the AI Infrastructure Observability Assistant for GSK's Enterprise Operations. Your objective is to triage infrastructure incidents across pharmaceutical manufacturing (EBRS), research compute clusters, and cloud-native systems.

Operational Directives:
1. Active telemetry is stored in `gsk-corp-obvspoc-brown-dev.gsk_observability_demo.enterprise_telemetry_partitioned`.
2. Telemetry Schema Attributes:
   - timestamp (TIMESTAMP): UTC event time
   - site_location (STRING): 'Site_A_London', 'Site_B_Stevenage', 'Site_C_Ware'
   - system_id (STRING): System identifier (e.g., 'EBRS', 'LIMS')
   - application_tier (STRING): 'Web_Frontend', 'Database_Backend', 'Batch_Processing'
   - hostname (STRING): Host identifier (e.g., 'srv-b-batch-02')
   - cpu_usage_pct (FLOAT64), memory_usage_pct (FLOAT64), io_wait_ms (FLOAT64)
   - status (STRING): 'HEALTHY', 'ANOMALY', 'DOWN'
3. Investigation Protocol:
   - When asked about active service degradation, query rows where `status IN ('ANOMALY', 'DOWN')` within the last 60 minutes.
   - For database saturation, execute GQL queries against `gsk-corp-obvspoc-brown-dev.gsk_observability_demo.gsk_infrastructure_dependency_graph` to identify upstream web nodes experiencing HTTP 504 timeouts.
   - Formulate executive incident summaries detailing: Hostname, Physical Site, Impacted Application, Measured Resource Utilization, and Actionable Remediation Runbooks."""


OBSERVABILITY_TABLES = [
    "enterprise_telemetry_partitioned",
    "system_logs",
    "servicenow_maintenance_windows",
    "incident_root_cause_analysis",
    "structured_log_entities",
    "nodes_switches",
    "nodes_hypervisors",
    "nodes_hosts",
    "nodes_applications",
    "edges_connected_to",
    "edges_hosts_vm",
    "edges_runs_app",
    "edges_app_communicates",
    "edges_network_flows",
]


class ObservabilityMVPState:
  """Thread-safe analytical & graph state engine for the GSK Observability MVP."""

  def __init__(
      self,
      project_id: str = DEFAULT_PROJECT_ID,
      dataset_id: str = DEFAULT_DATASET_ID,
      mirror_path: Optional[str] = None,
      live_bq: bool = False,
  ) -> None:
    self.project_id = project_id
    self.dataset_id = dataset_id
    self.live_bq = live_bq
    self._lock = threading.Lock()

    if mirror_path:
      self.mirror_db_path = Path(mirror_path)
    else:
      self.mirror_db_path = DEFAULT_MIRROR_PATH

    self._memory_conn: Optional[sqlite3.Connection] = None
    self._ensure_mirror_tables_seeded()

  def _get_connection(self) -> sqlite3.Connection:
    if self._memory_conn is not None:
      return self._memory_conn
    try:
      self.mirror_db_path.parent.mkdir(parents=True, exist_ok=True)
      conn = sqlite3.connect(str(self.mirror_db_path), timeout=10.0)
      conn.row_factory = sqlite3.Row
      return conn
    except Exception:  # pylint: disable=broad-except
      self._memory_conn = sqlite3.connect(":memory:", check_same_thread=False)
      self._memory_conn.row_factory = sqlite3.Row
      return self._memory_conn

  def _close_connection(self, conn: sqlite3.Connection) -> None:
    if conn is not self._memory_conn:
      conn.close()

  def _ensure_mirror_tables_seeded(self) -> None:
    """Idempotently creates and seeds all 14 EBRS lakehouse & ISO GQL tables in SQLite."""
    conn = self._get_connection()
    try:
      cur = conn.cursor()
      cur.executescript(
          """
          CREATE TABLE IF NOT EXISTS enterprise_telemetry_partitioned (
              timestamp TEXT NOT NULL,
              enterprise_domain TEXT,
              site_location TEXT,
              system_id TEXT,
              cluster_id TEXT,
              application_tier TEXT,
              host_id TEXT NOT NULL,
              hostname TEXT NOT NULL,
              cpu_usage_pct REAL,
              memory_usage_pct REAL,
              io_wait_ms REAL,
              network_bytes_sec INTEGER,
              status TEXT,
              active_connections INTEGER
          );

          CREATE TABLE IF NOT EXISTS system_logs (
              timestamp TEXT NOT NULL,
              log_id TEXT,
              host_id TEXT NOT NULL,
              hostname TEXT NOT NULL,
              site_location TEXT,
              system_id TEXT,
              application_tier TEXT,
              severity TEXT NOT NULL,
              service_name TEXT,
              message TEXT NOT NULL
          );

          CREATE TABLE IF NOT EXISTS servicenow_maintenance_windows (
              change_id TEXT NOT NULL,
              host_id TEXT,
              hostname TEXT NOT NULL,
              site_location TEXT,
              system_id TEXT,
              start_time TEXT NOT NULL,
              end_time TEXT NOT NULL,
              is_maintenance_window INTEGER NOT NULL,
              change_type TEXT,
              description TEXT
          );

          CREATE TABLE IF NOT EXISTS incident_root_cause_analysis (
              event_timestamp TEXT,
              hostname TEXT,
              anomaly_score REAL,
              prompt TEXT,
              gemini_root_cause_analysis TEXT
          );

          CREATE TABLE IF NOT EXISTS structured_log_entities (
              timestamp TEXT,
              hostname TEXT,
              root_cause_category TEXT,
              failed_component TEXT,
              error_code TEXT,
              recommended_action TEXT,
              confidence_score REAL
          );

          CREATE TABLE IF NOT EXISTS nodes_switches (
              switch_id TEXT PRIMARY KEY,
              hostname TEXT,
              site_location TEXT,
              management_ip TEXT,
              model TEXT
          );

          CREATE TABLE IF NOT EXISTS nodes_hypervisors (
              hypervisor_id TEXT PRIMARY KEY,
              hostname TEXT,
              site_location TEXT,
              cluster_name TEXT,
              esxi_version TEXT
          );

          CREATE TABLE IF NOT EXISTS nodes_hosts (
              host_id TEXT PRIMARY KEY,
              hostname TEXT,
              site_location TEXT,
              application_type TEXT,
              operating_system TEXT
          );

          CREATE TABLE IF NOT EXISTS nodes_applications (
              app_id TEXT PRIMARY KEY,
              name TEXT,
              tier TEXT,
              system_id TEXT,
              criticality TEXT
          );

          CREATE TABLE IF NOT EXISTS edges_connected_to (
              edge_id TEXT PRIMARY KEY,
              switch_id TEXT,
              hypervisor_id TEXT,
              port_name TEXT,
              speed_gbps INTEGER
          );

          CREATE TABLE IF NOT EXISTS edges_hosts_vm (
              edge_id TEXT PRIMARY KEY,
              hypervisor_id TEXT,
              host_id TEXT,
              allocated_vcpus INTEGER,
              allocated_ram_gb INTEGER
          );

          CREATE TABLE IF NOT EXISTS edges_runs_app (
              edge_id TEXT PRIMARY KEY,
              host_id TEXT,
              app_id TEXT,
              process_id INTEGER,
              listen_port INTEGER
          );

          CREATE TABLE IF NOT EXISTS edges_app_communicates (
              edge_id TEXT PRIMARY KEY,
              source_app_id TEXT,
              target_app_id TEXT,
              protocol TEXT,
              avg_latency_ms REAL
          );

          CREATE TABLE IF NOT EXISTS edges_network_flows (
              edge_id TEXT PRIMARY KEY,
              source_host_id TEXT,
              destination_host_id TEXT,
              avg_traffic REAL
          );
          """
      )

      # Seed nodes_switches if empty
      cur.execute("SELECT COUNT(*) FROM nodes_switches")
      if cur.fetchone()[0] == 0:
        cur.executemany(
            "INSERT OR IGNORE INTO nodes_switches VALUES (?, ?, ?, ?, ?)",
            [
                ("sw-core-stv-01", "sw-core-stv-01", "Site_B_Stevenage", "10.44.0.1", "Cisco-Nexus-9336C-FX2"),
                ("sw-core-lon-01", "sw-core-lon-01", "Site_A_London", "10.40.0.1", "Cisco-Nexus-9336C-FX2"),
                ("sw-core-ware-01", "sw-core-ware-01", "Site_C_Ware", "10.48.0.1", "Arista-7280SR3-48YC8"),
            ],
        )

      cur.execute("SELECT COUNT(*) FROM nodes_hypervisors")
      if cur.fetchone()[0] == 0:
        cur.executemany(
            "INSERT OR IGNORE INTO nodes_hypervisors VALUES (?, ?, ?, ?, ?)",
            [
                ("esxi-cluster-04", "esxi-cluster-04", "Site_B_Stevenage", "onprem-esxi-cluster-04", "VMware-ESXi-8.0U2"),
                ("gke-prod-lon-01", "gke-prod-lon-01", "Site_A_London", "gke-prod-lon-01", "GKE-1.30-COS"),
                ("aks-prod-ware-02", "aks-prod-ware-02", "Site_C_Ware", "aks-prod-ware-02", "AKS-1.29-Ubuntu"),
            ],
        )

      cur.executemany(
          "INSERT OR IGNORE INTO nodes_hosts VALUES (?, ?, ?, ?, ?)",
          [
              ("srv-b-batch-02", "srv-b-batch-02", "Site_B_Stevenage", "Batch_Processing", "RHEL-9.3"),
              ("srv-b-db-01", "srv-b-db-01", "Site_B_Stevenage", "Database_Backend", "RHEL-9.3"),
              ("srv-a-web-01", "srv-a-web-01", "Site_A_London", "Web_Frontend", "Ubuntu-22.04"),
              ("srv-a-web-04", "srv-a-web-04", "Site_A_London", "Web_Frontend", "Ubuntu-22.04"),
              ("srv-c-batch-03", "srv-c-batch-03", "Site_C_Ware", "Batch_Processing", "RHEL-9.3"),
              ("srv-c-ware-01", "srv-c-ware-01", "Site_C_Ware", "Batch_Processing", "RHEL-9.3"),
              ("ora-db-stv-01", "ora-db-stv-01", "Site_B_Stevenage", "Database_Backend", "Oracle-Linux-8.9"),
          ],
      )

      cur.execute("SELECT COUNT(*) FROM nodes_applications")
      if cur.fetchone()[0] == 0:
        cur.executemany(
            "INSERT OR IGNORE INTO nodes_applications VALUES (?, ?, ?, ?, ?)",
            [
                ("app-ebrs-batch", "EBRS-Batch-Engine", "Batch", "EBRS", "Tier-1-GXP"),
                ("app-ebrs-db", "EBRS-Database-Core", "Database", "EBRS", "Tier-1-GXP"),
                ("app-ebrs-web", "EBRS-Web-Portal", "Frontend", "EBRS", "Tier-1-GXP"),
                ("app-lims-api", "LIMS-Core-API", "Backend", "LIMS", "Tier-1-GXP"),
                ("app-mes-sched", "MES-Batch-Scheduler", "Batch", "MES_BATCH", "Tier-1-GXP"),
            ],
        )

      cur.execute("SELECT COUNT(*) FROM edges_connected_to")
      if cur.fetchone()[0] == 0:
        cur.executemany(
            "INSERT OR IGNORE INTO edges_connected_to VALUES (?, ?, ?, ?, ?)",
            [
                ("edge-conn-stv-01", "sw-core-stv-01", "esxi-cluster-04", "Eth1/1", 100),
                ("edge-conn-lon-01", "sw-core-lon-01", "gke-prod-lon-01", "Eth1/2", 100),
                ("edge-conn-ware-01", "sw-core-ware-01", "aks-prod-ware-02", "Eth1/3", 40),
            ],
        )

      cur.execute("SELECT COUNT(*) FROM edges_hosts_vm")
      if cur.fetchone()[0] == 0:
        cur.executemany(
            "INSERT OR IGNORE INTO edges_hosts_vm VALUES (?, ?, ?, ?, ?)",
            [
                ("edge-host-vm-01", "esxi-cluster-04", "srv-b-batch-02", 16, 64),
                ("edge-host-vm-02", "esxi-cluster-04", "srv-b-db-01", 32, 128),
                ("edge-host-vm-03", "gke-prod-lon-01", "srv-a-web-01", 8, 32),
                ("edge-host-vm-04", "gke-prod-lon-01", "srv-a-web-04", 8, 32),
                ("edge-host-vm-05", "aks-prod-ware-02", "srv-c-ware-01", 16, 64),
                ("edge-host-vm-06", "esxi-cluster-04", "ora-db-stv-01", 32, 128),
            ],
        )

      cur.execute("SELECT COUNT(*) FROM edges_runs_app")
      if cur.fetchone()[0] == 0:
        cur.executemany(
            "INSERT OR IGNORE INTO edges_runs_app VALUES (?, ?, ?, ?, ?)",
            [
                ("edge-runs-01", "srv-b-batch-02", "app-ebrs-batch", 18422, 8080),
                ("edge-runs-02", "srv-b-db-01", "app-ebrs-db", 9410, 5432),
                ("edge-runs-03", "srv-a-web-01", "app-ebrs-web", 4120, 443),
                ("edge-runs-04", "srv-a-web-04", "app-ebrs-web", 4188, 443),
                ("edge-runs-05", "srv-c-ware-01", "app-mes-sched", 22104, 8443),
                ("edge-runs-06", "ora-db-stv-01", "app-lims-api", 7310, 1521),
            ],
        )

      cur.execute("SELECT COUNT(*) FROM edges_app_communicates")
      if cur.fetchone()[0] == 0:
        cur.executemany(
            "INSERT OR IGNORE INTO edges_app_communicates VALUES (?, ?, ?, ?, ?)",
            [
                ("edge-app-comm-01", "app-ebrs-web", "app-ebrs-batch", "gRPC/TLS", 42.5),
                ("edge-app-comm-02", "app-ebrs-web", "app-ebrs-db", "PostgreSQL/TCP", 1850.0),
                ("edge-app-comm-03", "app-mes-sched", "app-ebrs-batch", "HTTPS", 65.0),
                ("edge-app-comm-04", "app-lims-api", "app-ebrs-db", "JDBC", 28.4),
            ],
        )

      cur.execute("SELECT COUNT(*) FROM edges_network_flows")
      if cur.fetchone()[0] == 0:
        cur.executemany(
            "INSERT OR IGNORE INTO edges_network_flows VALUES (?, ?, ?, ?)",
            [
                ("edge-flow-01", "srv-a-web-01", "srv-b-db-01", 84500000.0),
                ("edge-flow-02", "srv-a-web-04", "srv-b-db-01", 79200000.0),
                ("edge-flow-03", "srv-a-web-01", "srv-b-batch-02", 52400000.0),
                ("edge-flow-04", "srv-c-ware-01", "srv-b-batch-02", 41800000.0),
            ],
        )

      cur.execute("SELECT COUNT(*) FROM servicenow_maintenance_windows")
      if cur.fetchone()[0] == 0:
        cur.executemany(
            "INSERT INTO servicenow_maintenance_windows VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "CHG0049281",
                    "ora-db-stv-01",
                    "ora-db-stv-01",
                    "Site_B_Stevenage",
                    "LIMS",
                    "2026-09-29T14:00:00Z",
                    "2026-09-29T18:00:00Z",
                    1,
                    "PLANNED_OS_DB_PATCH",
                    "Scheduled quarterly PSU & kernel maintenance on Stevenage DB tier (CHG0049281)",
                ),
                (
                    "CHG0051024",
                    "srv-a-web-02",
                    "srv-a-web-02",
                    "Site_A_London",
                    "EBRS",
                    "2026-09-30T01:00:00Z",
                    "2026-09-30T03:00:00Z",
                    1,
                    "ROLLING_INGRESS_UPGRADE",
                    "Planned TLS certificate rotation on London ingress web nodes",
                ),
            ],
        )

      cur.execute("SELECT COUNT(*) FROM enterprise_telemetry_partitioned")
      if cur.fetchone()[0] == 0:
        telemetry_rows = [
            # Scenario A: srv-b-batch-02 JVM Memory Leak & OOM Collapse (55% -> 75% -> 92% -> 98.8%)
            ("2026-09-29T15:40:00Z", "Pharma_Manufacturing", "Site_B_Stevenage", "EBRS", "onprem-esxi-cluster-04", "Batch_Processing", "srv-b-batch-02", "srv-b-batch-02", 52.4, 55.0, 14.2, 42000000, "HEALTHY", 48),
            ("2026-09-29T15:45:00Z", "Pharma_Manufacturing", "Site_B_Stevenage", "EBRS", "onprem-esxi-cluster-04", "Batch_Processing", "srv-b-batch-02", "srv-b-batch-02", 68.1, 75.0, 22.8, 48500000, "HEALTHY", 64),
            ("2026-09-29T15:50:00Z", "Pharma_Manufacturing", "Site_B_Stevenage", "EBRS", "onprem-esxi-cluster-04", "Batch_Processing", "srv-b-batch-02", "srv-b-batch-02", 84.6, 92.0, 45.0, 51200000, "ANOMALY", 82),
            ("2026-09-29T15:55:00Z", "Pharma_Manufacturing", "Site_B_Stevenage", "EBRS", "onprem-esxi-cluster-04", "Batch_Processing", "srv-b-batch-02", "srv-b-batch-02", 94.5, 98.8, 68.4, 54900000, "ANOMALY", 96),
            # Scenario B: srv-b-db-01 PostgreSQL Saturation (350/500 -> 500/500) & Web Tier Blast Radius (srv-a-web-01, srv-a-web-04)
            ("2026-09-29T15:48:00Z", "Pharma_Manufacturing", "Site_B_Stevenage", "EBRS", "onprem-esxi-cluster-04", "Database_Backend", "srv-b-db-01", "srv-b-db-01", 95.2, 86.4, 110.5, 82000000, "ANOMALY", 350),
            ("2026-09-29T15:53:00Z", "Pharma_Manufacturing", "Site_B_Stevenage", "EBRS", "onprem-esxi-cluster-04", "Database_Backend", "srv-b-db-01", "srv-b-db-01", 96.4, 91.2, 185.0, 94000000, "ANOMALY", 500),
            ("2026-09-29T15:54:00Z", "Pharma_Manufacturing", "Site_A_London", "EBRS", "gke-prod-lon-01", "Web_Frontend", "srv-a-web-01", "srv-a-web-01", 89.2, 81.5, 18.0, 76000000, "ANOMALY", 480),
            ("2026-09-29T15:54:00Z", "Pharma_Manufacturing", "Site_A_London", "EBRS", "gke-prod-lon-01", "Web_Frontend", "srv-a-web-04", "srv-a-web-04", 8.5, 79.8, 16.5, 12000000, "ANOMALY", 495),
            # Scenario C: srv-c-ware-01 Storage IO Saturation (>520ms -> >940ms) & Silent Host Drop to 0.0
            ("2026-09-29T15:46:00Z", "Pharma_Manufacturing", "Site_C_Ware", "MES_BATCH", "aks-prod-ware-02", "Batch_Processing", "srv-c-ware-01", "srv-c-ware-01", 64.0, 71.0, 545.0, 38000000, "ANOMALY", 42),
            ("2026-09-29T15:51:00Z", "Pharma_Manufacturing", "Site_C_Ware", "MES_BATCH", "aks-prod-ware-02", "Batch_Processing", "srv-c-ware-01", "srv-c-ware-01", 78.5, 74.2, 965.0, 14000000, "ANOMALY", 19),
            ("2026-09-29T15:56:00Z", "Pharma_Manufacturing", "Site_C_Ware", "MES_BATCH", "aks-prod-ware-02", "Batch_Processing", "srv-c-ware-01", "srv-c-ware-01", 0.0, 0.0, 0.0, 0, "DOWN", 0),
            # ServiceNow Suppressed Maintenance Host: ora-db-stv-01 during CHG0049281
            ("2026-09-29T15:52:00Z", "Pharma_Manufacturing", "Site_B_Stevenage", "LIMS", "onprem-esxi-cluster-04", "Database_Backend", "ora-db-stv-01", "ora-db-stv-01", 88.4, 82.0, 64.0, 41000000, "HEALTHY", 168),
        ]
        cur.executemany(
            "INSERT INTO enterprise_telemetry_partitioned VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            telemetry_rows,
        )

      cur.execute("SELECT COUNT(*) FROM system_logs")
      if cur.fetchone()[0] == 0:
        log_rows = [
            # Scenario A logs
            ("2026-09-29T15:51:00Z", "log-ebrs-a1", "srv-b-batch-02", "srv-b-batch-02", "Site_B_Stevenage", "EBRS", "Batch_Processing", "ERROR", "EBRS-Batch-Engine", "ERROR: JVM G1GC pause duration 1,240ms exceeded SLA threshold (heap utilization 92.0%) on srv-b-batch-02"),
            ("2026-09-29T15:54:00Z", "log-ebrs-a2", "srv-b-batch-02", "srv-b-batch-02", "Site_B_Stevenage", "EBRS", "Batch_Processing", "FATAL", "EBRS-Batch-Engine", "FATAL: java.lang.OutOfMemoryError: Java heap space at com.gsk.ebrs.batch.SparkGenomicsAggregator.processRecord(SparkGenomicsAggregator.java:418)"),
            ("2026-09-29T15:55:00Z", "log-ebrs-a3", "srv-b-batch-02", "srv-b-batch-02", "Site_B_Stevenage", "EBRS", "Batch_Processing", "CRITICAL", "kernel", "CRITICAL: Out of memory: Killed process 18422 (java) total-vm:67108864kB, anon-rss:65421312kB, exit code 137"),
            # Scenario B logs
            ("2026-09-29T15:52:00Z", "log-ebrs-b1", "srv-b-db-01", "srv-b-db-01", "Site_B_Stevenage", "EBRS", "Database_Backend", "CRITICAL", "EBRS-Database-Core", "CRITICAL: PostgreSQL connection pool exhausted 500/500 — remaining client requests queued"),
            ("2026-09-29T15:53:00Z", "log-ebrs-b2", "srv-b-db-01", "srv-b-db-01", "Site_B_Stevenage", "EBRS", "Database_Backend", "ERROR", "EBRS-Database-Core", "ERROR: deadlock detected on relation clinical_trial_records (Process 9410 waits for ShareLock on transaction 884120)"),
            ("2026-09-29T15:54:00Z", "log-ebrs-b3", "srv-a-web-01", "srv-a-web-01", "Site_A_London", "EBRS", "Web_Frontend", "ERROR", "EBRS-Web-Portal", "ERROR: HTTP 504 Gateway Timeout while contacting backend database srv-b-db-01:5432 (upstream pool saturated)"),
            ("2026-09-29T15:54:00Z", "log-ebrs-b4", "srv-a-web-04", "srv-a-web-04", "Site_A_London", "EBRS", "Web_Frontend", "ERROR", "EBRS-Web-Portal", "ERROR: HTTP 504 Gateway Timeout while contacting backend database srv-b-db-01:5432 (worker threads exhausted)"),
            # Scenario C logs
            ("2026-09-29T15:46:00Z", "log-ebrs-c1", "srv-c-ware-01", "srv-c-ware-01", "Site_C_Ware", "MES_BATCH", "Batch_Processing", "ERROR", "MES-Batch-Scheduler", "ERROR: Shared storage mount /mnt/gsk_batch IOPS saturation detected (io_wait_ms=545.0ms > 520ms threshold)"),
            ("2026-09-29T15:51:00Z", "log-ebrs-c2", "srv-c-ware-01", "srv-c-ware-01", "Site_C_Ware", "MES_BATCH", "Batch_Processing", "CRITICAL", "kubelet", "CRITICAL: NFS/SAN mount /mnt/gsk_batch stalled (io_wait_ms=965.0ms > 940ms); node heartbeat daemon blocked — cluster orchestrator evicting host srv-c-ware-01"),
        ]
        cur.executemany(
            "INSERT INTO system_logs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            log_rows,
        )

      cur.execute("SELECT COUNT(*) FROM incident_root_cause_analysis")
      if cur.fetchone()[0] == 0:
        rca_rows = [
            (
                "2026-09-29T15:55:00Z",
                "srv-b-batch-02",
                94.5,
                "Analyze telemetry incident on srv-b-batch-02 (Batch_Processing, CPU 94.5%, Memory 98.8%, java.lang.OutOfMemoryError).",
                (
                    "1. Immediate root cause: Unbounded Spark genomics batch aggregation on srv-b-batch-02 drove JVM heap utilization from 55.0% to 98.8% with 1,240ms GC pauses, culminating in java.lang.OutOfMemoryError and Linux OOM killer termination (exit code 137). "
                    "2. Downstream service blast radius: EBRS-Batch-Engine batch record signing pipelines at Site_B_Stevenage are halted and upstream EBRS-Web-Portal batch status polling requests are degraded. "
                    "3. Immediate corrective action: Drain active batch shards from srv-b-batch-02, restart EBRS-Batch-Engine with -XX:+UseG1GC and capped partition chunk sizes, and replay uncommitted EBRS batch checkpoints."
                ),
            ),
            (
                "2026-09-29T15:53:00Z",
                "srv-b-db-01",
                96.4,
                "Analyze telemetry incident on srv-b-db-01 (Database_Backend, CPU 96.4%, PostgreSQL connection pool exhausted 500/500, HTTP 504 on srv-a-web-01/04).",
                (
                    "1. Immediate root cause: Unindexed concurrent write locks on relation clinical_trial_records saturated CPU (96.4%) on srv-b-db-01 and exhausted the PostgreSQL connection pool (500/500). "
                    "2. Downstream service blast radius: ISO GQL topology traversal confirms upstream web ingress nodes srv-a-web-01 and srv-a-web-04 in Site_A_London exhausted worker thread pools waiting on EBRS-Database-Core, emitting HTTP 504 Gateway Timeouts across EBRS-Web-Portal. "
                    "3. Immediate corrective action: Terminate blocking PID 9410 holding the ShareLock on clinical_trial_records, route read traffic to read-replicas via PgBouncer, and recycle stalled HTTP worker pools on srv-a-web-01 and srv-a-web-04."
                ),
            ),
            (
                "2026-09-29T15:56:00Z",
                "srv-c-ware-01",
                965.0,
                "Analyze telemetry incident on srv-c-ware-01 (Batch_Processing, /mnt/gsk_batch io_wait 965ms, silent host drop to 0.0).",
                (
                    "1. Immediate root cause: Shared storage mount /mnt/gsk_batch experienced severe SAN IOPS saturation with I/O wait surging from 545.0ms to 965.0ms, blocking kernel D-state threads and starving the node heartbeat daemon on srv-c-ware-01. "
                    "2. Downstream service blast radius: Cluster orchestrator evicted srv-c-ware-01 causing telemetry to drop to 0.0 (caught via GAP_FILL() as SILENT_HOST_DROP_TO_ZERO), stalling MES-Batch-Scheduler jobs at Site_C_Ware. "
                    "3. Immediate corrective action: Cordon srv-c-ware-01, remount /mnt/gsk_batch with hard IOPS QoS throttling on the storage array, and reschedule evicted MES_BATCH pods onto healthy Ware worker nodes."
                ),
            ),
        ]
        cur.executemany(
            "INSERT INTO incident_root_cause_analysis VALUES (?, ?, ?, ?, ?)",
            rca_rows,
        )

      cur.execute("SELECT COUNT(*) FROM structured_log_entities")
      if cur.fetchone()[0] == 0:
        entity_rows = [
            (
                "2026-09-29T15:54:00Z",
                "srv-b-batch-02",
                "JVM_HEAP_EXHAUSTION_OOM",
                "EBRS-Batch-Engine (SparkGenomicsAggregator)",
                "java.lang.OutOfMemoryError / EXIT_137",
                "Restart JVM service with G1GC memory limits and drain stuck EBRS batch queue",
                0.994,
            ),
            (
                "2026-09-29T15:53:00Z",
                "srv-b-db-01",
                "DATABASE_CONNECTION_POOL_EXHAUSTION",
                "PostgreSQL clinical_trial_records (EBRS-Database-Core)",
                "POOL_500_OF_500 / PG_DEADLOCK",
                "Terminate deadlocked backend transaction 884120 and scale PgBouncer pool",
                0.989,
            ),
            (
                "2026-09-29T15:54:00Z",
                "srv-a-web-01",
                "UPSTREAM_GATEWAY_TIMEOUT_CASCADE",
                "EBRS-Web-Portal Ingress (Site_A_London)",
                "HTTP_504_GATEWAY_TIMEOUT",
                "Trip circuit breaker to srv-b-db-01 and drain wedged Tomcat/Nginx worker threads",
                0.978,
            ),
            (
                "2026-09-29T15:51:00Z",
                "srv-c-ware-01",
                "STORAGE_IOPS_STALL_AND_NODE_EVICTION",
                "/mnt/gsk_batch (MES-Batch-Scheduler)",
                "IO_WAIT_965MS / SILENT_HOST_DROP_TO_ZERO",
                "Cordon node srv-c-ware-01, throttle SAN volume IOPS, and reschedule MES_BATCH jobs",
                0.996,
            ),
        ]
        cur.executemany(
            "INSERT INTO structured_log_entities VALUES (?, ?, ?, ?, ?, ?, ?)",
            entity_rows,
        )

      conn.commit()
    finally:
      self._close_connection(conn)

  def _run_live_bq_query(self, sql_query: str) -> Optional[list[dict[str, Any]]]:
    """Optionally executes a SQL/GQL query against live BigQuery if live_bq=True."""
    if not self.live_bq:
      return None
    bq_bin = shutil.which("bq")
    if not bq_bin:
      return None
    try:
      proc = subprocess.run(
          [
              bq_bin,
              f"--project_id={self.project_id}",
              "query",
              "--use_legacy_sql=false",
              "--format=json",
              sql_query,
          ],
          capture_output=True,
          text=True,
          timeout=12,
          check=False,
      )
      if proc.returncode == 0 and proc.stdout.strip():
        parsed = json.loads(proc.stdout)
        if isinstance(parsed, list):
          return parsed
    except Exception:  # pylint: disable=broad-except
      pass
    return None

  def get_observability_status(self) -> dict[str, Any]:
    """Returns dataset, table row counts, property graph status, and KPI summary."""
    table_counts: dict[str, int] = {}
    conn = self._get_connection()
    try:
      cur = conn.cursor()
      for tbl in OBSERVABILITY_TABLES:
        try:
          cur.execute(f"SELECT COUNT(*) FROM {tbl}")
          table_counts[tbl] = int(cur.fetchone()[0])
        except Exception:  # pylint: disable=broad-except
          table_counts[tbl] = 0
    finally:
      self._close_connection(conn)

    return {
        "status": "HEALTHY",
        "project_id": self.project_id,
        "dataset_id": self.dataset_id,
        "location": "EU",
        "region": "europe-west2",
        "execution_mode": "LIVE_BIGQUERY_AND_SQLITE_MIRROR" if self.live_bq else "SQLITE_MIRROR_SUB_50MS",
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "property_graph": "gsk_infrastructure_dependency_graph",
        "property_graph_fqn": f"{self.project_id}.{self.dataset_id}.gsk_infrastructure_dependency_graph",
        "property_graph_status": "DEPLOYED",
        "models": [
            "host_cpu_arima_model",
            "host_cpu_arimax_model",
            "composite_host_signals_arima",
            "gemini_2_5_flash",
            "timesfm_2_5",
        ],
        "sites": ["Site_A_London", "Site_B_Stevenage", "Site_C_Ware"],
        "systems": ["EBRS", "LIMS", "MES_BATCH"],
        "table_counts": table_counts,
        "tables": list(OBSERVABILITY_TABLES),
        "kpis": {
            "monitored_hosts_arima": 6000,
            "monitored_vms_estate": 12000,
            "daily_events_billions": 71.2,
            "arima_horizon_minutes": 10000,
            "holiday_region": "GB",
            "partition_expiration_days": 90,
            "monthly_on_demand_eval_cost_usd": 1.30,
            "gemini_2_5_flash_monthly_cost_usd": 0.10,
        },
    }

  def get_anomalies_payload(
      self,
      hostname_filter: Optional[str] = None,
      classification_filter: Optional[str] = None,
  ) -> dict[str, Any]:
    """Serves Dynamic Confidence Band & Anomaly Explorer time-series points."""
    series: list[dict[str, Any]] = [
        # Scenario A: srv-b-batch-02 progression (NORMAL -> SPIKE_ANOMALY)
        {
            "hostname": "srv-b-batch-02",
            "host_id": "srv-b-batch-02",
            "site_location": "Site_B_Stevenage",
            "system_id": "EBRS",
            "application_tier": "Batch_Processing",
            "ts_minute": "2026-09-29T15:40:00Z",
            "actual_cpu": 52.4,
            "expected_lower_bound": 38.0,
            "expected_upper_bound": 68.0,
            "is_anomaly": False,
            "anomaly_probability": 0.142,
            "is_maintenance_window": 0,
            "active_change_id": None,
            "anomaly_classification": "NORMAL",
            "scenario_id": "A",
        },
        {
            "hostname": "srv-b-batch-02",
            "host_id": "srv-b-batch-02",
            "site_location": "Site_B_Stevenage",
            "system_id": "EBRS",
            "application_tier": "Batch_Processing",
            "ts_minute": "2026-09-29T15:50:00Z",
            "actual_cpu": 84.6,
            "expected_lower_bound": 38.5,
            "expected_upper_bound": 68.2,
            "is_anomaly": True,
            "anomaly_probability": 0.968,
            "is_maintenance_window": 0,
            "active_change_id": None,
            "anomaly_classification": "SPIKE_ANOMALY",
            "scenario_id": "A",
        },
        {
            "hostname": "srv-b-batch-02",
            "host_id": "srv-b-batch-02",
            "site_location": "Site_B_Stevenage",
            "system_id": "EBRS",
            "application_tier": "Batch_Processing",
            "ts_minute": "2026-09-29T15:55:00Z",
            "actual_cpu": 94.5,
            "expected_lower_bound": 39.0,
            "expected_upper_bound": 68.5,
            "is_anomaly": True,
            "anomaly_probability": 0.994,
            "is_maintenance_window": 0,
            "active_change_id": None,
            "anomaly_classification": "SPIKE_ANOMALY",
            "scenario_id": "A",
        },
        # Scenario B: srv-b-db-01 SPIKE_ANOMALY & srv-a-web-04 DIP_ANOMALY
        {
            "hostname": "srv-b-db-01",
            "host_id": "srv-b-db-01",
            "site_location": "Site_B_Stevenage",
            "system_id": "EBRS",
            "application_tier": "Database_Backend",
            "ts_minute": "2026-09-29T15:53:00Z",
            "actual_cpu": 96.4,
            "expected_lower_bound": 42.0,
            "expected_upper_bound": 72.0,
            "is_anomaly": True,
            "anomaly_probability": 0.996,
            "is_maintenance_window": 0,
            "active_change_id": None,
            "anomaly_classification": "SPIKE_ANOMALY",
            "scenario_id": "B",
        },
        {
            "hostname": "srv-a-web-01",
            "host_id": "srv-a-web-01",
            "site_location": "Site_A_London",
            "system_id": "EBRS",
            "application_tier": "Web_Frontend",
            "ts_minute": "2026-09-29T15:54:00Z",
            "actual_cpu": 89.2,
            "expected_lower_bound": 30.0,
            "expected_upper_bound": 65.0,
            "is_anomaly": True,
            "anomaly_probability": 0.978,
            "is_maintenance_window": 0,
            "active_change_id": None,
            "anomaly_classification": "SPIKE_ANOMALY",
            "scenario_id": "B",
        },
        {
            "hostname": "srv-a-web-04",
            "host_id": "srv-a-web-04",
            "site_location": "Site_A_London",
            "system_id": "EBRS",
            "application_tier": "Web_Frontend",
            "ts_minute": "2026-09-29T15:54:00Z",
            "actual_cpu": 8.5,
            "expected_lower_bound": 28.0,
            "expected_upper_bound": 62.0,
            "is_anomaly": True,
            "anomaly_probability": 0.912,
            "is_maintenance_window": 0,
            "active_change_id": None,
            "anomaly_classification": "DIP_ANOMALY",
            "scenario_id": "B",
        },
        # Scenario C: srv-c-batch-03 / srv-c-ware-01 Storage Stall & GAP_FILL() Silent Host Drop to 0.0
        {
            "hostname": "srv-c-batch-03",
            "host_id": "srv-c-batch-03",
            "site_location": "Site_C_Ware",
            "system_id": "MES_BATCH",
            "application_tier": "Batch_Processing",
            "ts_minute": "2026-09-29T15:56:00Z",
            "actual_cpu": 0.0,
            "expected_lower_bound": 32.5,
            "expected_upper_bound": 64.0,
            "is_anomaly": True,
            "anomaly_probability": 0.998,
            "is_maintenance_window": 0,
            "active_change_id": None,
            "anomaly_classification": "SILENT_HOST_DROP_TO_ZERO",
            "gap_filled": True,
            "scenario_id": "C",
        },
        {
            "hostname": "srv-c-ware-01",
            "host_id": "srv-c-ware-01",
            "site_location": "Site_C_Ware",
            "system_id": "MES_BATCH",
            "application_tier": "Batch_Processing",
            "ts_minute": "2026-09-29T15:51:00Z",
            "actual_cpu": 78.5,
            "expected_lower_bound": 32.0,
            "expected_upper_bound": 64.0,
            "is_anomaly": True,
            "anomaly_probability": 0.954,
            "is_maintenance_window": 0,
            "active_change_id": None,
            "anomaly_classification": "SPIKE_ANOMALY",
            "scenario_id": "C",
        },
        {
            "hostname": "srv-c-ware-01",
            "host_id": "srv-c-ware-01",
            "site_location": "Site_C_Ware",
            "system_id": "MES_BATCH",
            "application_tier": "Batch_Processing",
            "ts_minute": "2026-09-29T15:56:00Z",
            "actual_cpu": 0.0,
            "expected_lower_bound": 32.5,
            "expected_upper_bound": 64.0,
            "is_anomaly": True,
            "anomaly_probability": 0.998,
            "is_maintenance_window": 0,
            "active_change_id": None,
            "anomaly_classification": "SILENT_HOST_DROP_TO_ZERO",
            "gap_filled": True,
            "scenario_id": "C",
        },
        # ServiceNow Planned Maintenance Suppression: CHG0049281 on ora-db-stv-01
        {
            "hostname": "ora-db-stv-01",
            "host_id": "ora-db-stv-01",
            "site_location": "Site_B_Stevenage",
            "system_id": "LIMS",
            "application_tier": "Database_Backend",
            "ts_minute": "2026-09-29T15:52:00Z",
            "actual_cpu": 88.4,
            "expected_lower_bound": 35.0,
            "expected_upper_bound": 89.5,
            "is_anomaly": False,
            "anomaly_probability": 0.142,
            "is_maintenance_window": 1,
            "active_change_id": "CHG0049281",
            "anomaly_classification": "SUPPRESSED_MAINTENANCE_WINDOW",
            "scenario_id": "MAINTENANCE",
        },
    ]

    if hostname_filter:
      series = [r for r in series if r["hostname"] == hostname_filter]
    if classification_filter:
      series = [
          r for r in series if r["anomaly_classification"] == classification_filter
      ]

    classification_counts: dict[str, int] = {}
    for item in series:
      cls_name = item["anomaly_classification"]
      classification_counts[cls_name] = classification_counts.get(cls_name, 0) + 1

    sql_query = f"""SELECT
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
  MODEL `{self.project_id}.{self.dataset_id}.host_cpu_arima_model`,
  STRUCT(0.85 AS anomaly_prob_threshold),
  TABLE dense_eval_series
)
ORDER BY is_anomaly DESC, anomaly_probability DESC, ts_minute DESC;"""

    timesfm_sql_query = f"""SELECT
  hostname,
  ts_minute,
  ROUND(actual_value, 2) AS actual_cpu,
  ROUND(lower_bound, 2) AS lower_bound,
  ROUND(upper_bound, 2) AS upper_bound,
  is_anomaly,
  ROUND(anomaly_probability, 4) AS anomaly_probability
FROM AI.DETECT_ANOMALIES(
  TABLE `{self.project_id}.{self.dataset_id}.historical_cpu_view`,
  TABLE `{self.project_id}.{self.dataset_id}.recent_cpu_eval_view`,
  data_col => 'cpu_usage',
  timestamp_col => 'ts_minute',
  id_cols => ['hostname'],
  anomaly_prob_threshold => 0.85
)
WHERE is_anomaly = TRUE
ORDER BY anomaly_probability DESC;"""

    return {
        "status": "SUCCESS",
        "project_id": self.project_id,
        "dataset_id": self.dataset_id,
        "model_metadata": {
            "primary_model": f"{self.project_id}.{self.dataset_id}.host_cpu_arima_model",
            "xreg_model": f"{self.project_id}.{self.dataset_id}.host_cpu_arimax_model",
            "foundation_model": "TimesFM 2.5 (AI.DETECT_ANOMALIES & AI.FORECAST)",
            "horizon": 10000,
            "holiday_region": "GB",
            "time_series_id_col": "hostname",
            "gap_fill_enabled": True,
            "anomaly_prob_threshold": 0.85,
            "active_maintenance_change_id": "CHG0049281",
        },
        "classifications": [
            "SPIKE_ANOMALY",
            "SILENT_HOST_DROP_TO_ZERO",
            "DIP_ANOMALY",
            "SUPPRESSED_MAINTENANCE_WINDOW",
            "NORMAL",
        ],
        "classification_counts": classification_counts,
        "total_points": len(series),
        "anomalies": series,
        "series": series,
        "time_series": series,
        "sql_query": sql_query,
        "timesfm_sql_query": timesfm_sql_query,
    }

  def get_topology_payload(self) -> dict[str, Any]:
    """Serves the 4-layer ISO GQL Property Graph (`Switch -> Hypervisor -> Host -> Application`)."""
    conn = self._get_connection()
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    try:
      cur = conn.cursor()

      cur.execute("SELECT * FROM nodes_switches")
      for r in cur.fetchall():
        d = dict(r)
        nodes.append({
            "id": d["switch_id"],
            "node_id": d["switch_id"],
            "label": "Switch",
            "node_type": "Switch",
            "layer": 1,
            "hostname": d["hostname"],
            "name": d["hostname"],
            "site_location": d["site_location"],
            "management_ip": d["management_ip"],
            "model": d["model"],
            "status": "IMPACTED_FABRIC" if d["switch_id"] == "sw-core-stv-01" else "HEALTHY",
        })

      cur.execute("SELECT * FROM nodes_hypervisors")
      for r in cur.fetchall():
        d = dict(r)
        nodes.append({
            "id": d["hypervisor_id"],
            "node_id": d["hypervisor_id"],
            "label": "Hypervisor",
            "node_type": "Hypervisor",
            "layer": 2,
            "hostname": d["hostname"],
            "name": d["hostname"],
            "site_location": d["site_location"],
            "cluster_name": d["cluster_name"],
            "esxi_version": d["esxi_version"],
            "status": "HOSTING_ANOMALOUS_VMS" if d["hypervisor_id"] == "esxi-cluster-04" else "HEALTHY",
        })

      host_status_map = {
          "srv-b-batch-02": "SCENARIO_A_JVM_OOM_ANOMALY",
          "srv-b-db-01": "SCENARIO_B_DB_POOL_EXHAUSTED_500_500",
          "srv-a-web-01": "SCENARIO_B_UPSTREAM_HTTP_504",
          "srv-a-web-04": "SCENARIO_B_UPSTREAM_HTTP_504",
          "srv-c-ware-01": "SCENARIO_C_SILENT_DROP_TO_ZERO",
          "ora-db-stv-01": "SUPPRESSED_MAINTENANCE_CHG0049281",
      }
      cur.execute("SELECT * FROM nodes_hosts")
      for r in cur.fetchall():
        d = dict(r)
        nodes.append({
            "id": d["host_id"],
            "node_id": d["host_id"],
            "label": "Host",
            "node_type": "Host",
            "layer": 3,
            "hostname": d["hostname"],
            "name": d["hostname"],
            "site_location": d["site_location"],
            "application_type": d["application_type"],
            "operating_system": d["operating_system"],
            "status": host_status_map.get(d["host_id"], "HEALTHY"),
        })

      cur.execute("SELECT * FROM nodes_applications")
      for r in cur.fetchall():
        d = dict(r)
        nodes.append({
            "id": d["app_id"],
            "node_id": d["app_id"],
            "label": "Application",
            "node_type": "Application",
            "layer": 4,
            "hostname": d["name"],
            "name": d["name"],
            "tier": d["tier"],
            "system_id": d["system_id"],
            "criticality": d["criticality"],
            "status": "DEGRADED" if d["system_id"] in ("EBRS", "MES_BATCH") else "SUPPRESSED",
        })

      cur.execute("SELECT * FROM edges_connected_to")
      for r in cur.fetchall():
        d = dict(r)
        edges.append({
            "edge_id": d["edge_id"],
            "source": d["switch_id"],
            "target": d["hypervisor_id"],
            "label": "CONNECTED_TO",
            "relationship_type": "CONNECTED_TO",
            "port_name": d["port_name"],
            "speed_gbps": d["speed_gbps"],
        })

      cur.execute("SELECT * FROM edges_hosts_vm")
      for r in cur.fetchall():
        d = dict(r)
        edges.append({
            "edge_id": d["edge_id"],
            "source": d["hypervisor_id"],
            "target": d["host_id"],
            "label": "HOSTS",
            "relationship_type": "HOSTS",
            "allocated_vcpus": d["allocated_vcpus"],
            "allocated_ram_gb": d["allocated_ram_gb"],
        })

      cur.execute("SELECT * FROM edges_runs_app")
      for r in cur.fetchall():
        d = dict(r)
        edges.append({
            "edge_id": d["edge_id"],
            "source": d["host_id"],
            "target": d["app_id"],
            "label": "RUNS",
            "relationship_type": "RUNS",
            "process_id": d["process_id"],
            "listen_port": d["listen_port"],
        })

      cur.execute("SELECT * FROM edges_app_communicates")
      for r in cur.fetchall():
        d = dict(r)
        edges.append({
            "edge_id": d["edge_id"],
            "source": d["source_app_id"],
            "target": d["target_app_id"],
            "label": "COMMUNICATES_WITH",
            "relationship_type": "COMMUNICATES_WITH",
            "protocol": d["protocol"],
            "avg_latency_ms": d["avg_latency_ms"],
        })

      cur.execute("SELECT * FROM edges_network_flows")
      for r in cur.fetchall():
        d = dict(r)
        edges.append({
            "edge_id": d["edge_id"],
            "source": d["source_host_id"],
            "target": d["destination_host_id"],
            "label": "CommunicatesWith",
            "relationship_type": "CommunicatesWith",
            "avg_traffic": d["avg_traffic"],
        })
    finally:
      self._close_connection(conn)

    graph_table_paths = [
        {
            "failing_switch": "sw-core-stv-01",
            "impacted_hypervisor": "esxi-cluster-04",
            "impacted_vm": "srv-b-batch-02",
            "impacted_backend": "EBRS-Batch-Engine",
            "impacted_frontend": "EBRS-Web-Portal",
            "src_host": "srv-a-web-01",
            "src_site": "Site_A_London",
            "dst_host": "srv-b-batch-02",
            "dst_site": "Site_B_Stevenage",
            "traffic_bytes_sec": 52400000.0,
            "scenario": "A",
        },
        {
            "failing_switch": "sw-core-stv-01",
            "impacted_hypervisor": "esxi-cluster-04",
            "impacted_vm": "srv-b-db-01",
            "impacted_backend": "EBRS-Database-Core",
            "impacted_frontend": "EBRS-Web-Portal",
            "src_host": "srv-a-web-01",
            "src_site": "Site_A_London",
            "dst_host": "srv-b-db-01",
            "dst_site": "Site_B_Stevenage",
            "traffic_bytes_sec": 84500000.0,
            "scenario": "B",
        },
        {
            "failing_switch": "sw-core-stv-01",
            "impacted_hypervisor": "esxi-cluster-04",
            "impacted_vm": "srv-b-db-01",
            "impacted_backend": "EBRS-Database-Core",
            "impacted_frontend": "EBRS-Web-Portal",
            "src_host": "srv-a-web-04",
            "src_site": "Site_A_London",
            "dst_host": "srv-b-db-01",
            "dst_site": "Site_B_Stevenage",
            "traffic_bytes_sec": 79200000.0,
            "scenario": "B",
        },
        {
            "failing_switch": "sw-core-ware-01",
            "impacted_hypervisor": "aks-prod-ware-02",
            "impacted_vm": "srv-c-ware-01",
            "impacted_backend": "EBRS-Batch-Engine",
            "impacted_frontend": "MES-Batch-Scheduler",
            "src_host": "srv-c-ware-01",
            "src_site": "Site_C_Ware",
            "dst_host": "srv-b-batch-02",
            "dst_site": "Site_B_Stevenage",
            "traffic_bytes_sec": 41800000.0,
            "scenario": "C",
        },
    ]

    blast_radius_by_scenario = {
        "A": {
            "scenario_id": "A",
            "title": "JVM Memory Leak & OOM Collapse (Batch Pipeline)",
            "full_stack_chain": "sw-core-stv-01 -> esxi-cluster-04 -> srv-b-batch-02 -> EBRS-Batch-Engine <- EBRS-Web-Portal",
            "root_cause_host": "srv-b-batch-02",
            "impacted_applications": ["EBRS-Batch-Engine", "EBRS-Web-Portal", "MES-Batch-Scheduler"],
        },
        "B": {
            "scenario_id": "B",
            "title": "Database Saturation & Web Tier Blast Radius",
            "full_stack_chain": "sw-core-stv-01 -> esxi-cluster-04 -> srv-b-db-01 -> EBRS-Database-Core <- EBRS-Web-Portal (srv-a-web-01, srv-a-web-04)",
            "root_cause_host": "srv-b-db-01",
            "upstream_impacted_hosts": ["srv-a-web-01", "srv-a-web-04"],
            "impacted_applications": ["EBRS-Database-Core", "EBRS-Web-Portal"],
        },
        "C": {
            "scenario_id": "C",
            "title": "Storage IO Saturation & Silent Host Drop to 0.0",
            "full_stack_chain": "sw-core-ware-01 -> aks-prod-ware-02 -> srv-c-ware-01 (/mnt/gsk_batch) -> MES-Batch-Scheduler -> EBRS-Batch-Engine",
            "root_cause_host": "srv-c-ware-01",
            "impacted_applications": ["MES-Batch-Scheduler", "EBRS-Batch-Engine"],
        },
    }

    iso_gql_query = f"""GRAPH `{self.project_id}.{self.dataset_id}.gsk_infrastructure_dependency_graph`
MATCH (sw:Switch {{switch_id: 'sw-core-stv-01'}})-[:CONNECTED_TO]->(hyp:Hypervisor)-[:HOSTS]->(vm:Host)-[:RUNS]->(backend:Application)<-[:COMMUNICATES_WITH]-(frontend:Application)
RETURN
  sw.hostname AS failing_switch,
  hyp.hostname AS impacted_hypervisor,
  vm.hostname AS impacted_vm,
  backend.name AS impacted_backend,
  frontend.name AS impacted_frontend;"""

    graph_table_sql_query = f"""SELECT
  failing_switch,
  impacted_hypervisor,
  impacted_vm,
  impacted_backend,
  impacted_frontend
FROM GRAPH_TABLE(
  `{self.project_id}.{self.dataset_id}.gsk_infrastructure_dependency_graph`,
  MATCH (sw:Switch {{switch_id: 'sw-core-stv-01'}})-[:CONNECTED_TO]->(hyp:Hypervisor)-[:HOSTS]->(vm:Host)-[:RUNS]->(backend:Application)<-[:COMMUNICATES_WITH]-(frontend:Application)
  RETURN
    sw.hostname AS failing_switch,
    hyp.hostname AS impacted_hypervisor,
    vm.hostname AS impacted_vm,
    backend.name AS impacted_backend,
    frontend.name AS impacted_frontend
);"""

    return {
        "status": "SUCCESS",
        "project_id": self.project_id,
        "dataset_id": self.dataset_id,
        "graph_name": "gsk_infrastructure_dependency_graph",
        "graph_fqn": f"{self.project_id}.{self.dataset_id}.gsk_infrastructure_dependency_graph",
        "node_labels": ["Switch", "Hypervisor", "Host", "Application"],
        "edge_labels": [
            "CONNECTED_TO",
            "HOSTS",
            "RUNS",
            "COMMUNICATES_WITH",
            "CommunicatesWith",
        ],
        "nodes": nodes,
        "edges": edges,
        "graph_table_paths": graph_table_paths,
        "traversal_paths": graph_table_paths,
        "paths": graph_table_paths,
        "blast_radius_by_scenario": blast_radius_by_scenario,
        "scenarios": blast_radius_by_scenario,
        "iso_gql_query": iso_gql_query,
        "graph_table_sql_query": graph_table_sql_query,
        "graph_visualization_json": {
            "total_dependency_paths": len(graph_table_paths),
            "nodes": sorted({p["src_host"] for p in graph_table_paths} | {p["dst_host"] for p in graph_table_paths}),
            "edges": [
                {
                    "source": p["src_host"],
                    "source_site": p["src_site"],
                    "target": p["dst_host"],
                    "target_site": p["dst_site"],
                    "traffic_bytes_sec": p["traffic_bytes_sec"],
                }
                for p in graph_table_paths
            ],
        },
    }

  def _build_scenarios_catalog(self) -> dict[str, dict[str, Any]]:
    """Constructs the 3 multi-stage cascading failure scenarios (A, B, C)."""
    return {
        "A": {
            "scenario_id": "A",
            "title": "Scenario A: JVM Memory Leak & OOM Collapse (Batch Pipeline)",
            "primary_host": "srv-b-batch-02",
            "site_location": "Site_B_Stevenage",
            "system_id": "EBRS",
            "application_tier": "Batch_Processing",
            "impacted_application": "EBRS-Batch-Engine",
            "anomaly_classification": "SPIKE_ANOMALY",
            "anomaly_probability": 0.994,
            "stages": [
                {
                    "stage": 1,
                    "name": "Stage 1 (Creep)",
                    "timestamp": "2026-09-29T15:40:00Z -> 2026-09-29T15:50:00Z",
                    "description": "Spark genomics worker leaks heap objects; memory increases from 55.0% to 75.0% to 92.0% with GC pause times exceeding 1,200ms (1,240ms).",
                    "metrics": {
                        "memory_progression_pct": [55.0, 75.0, 92.0],
                        "cpu_usage_pct": 84.6,
                        "gc_pause_ms": 1240,
                    },
                },
                {
                    "stage": 2,
                    "name": "Stage 2 (Breach & Early Warning)",
                    "timestamp": "2026-09-29T15:55:00Z",
                    "description": "Heap utilization reaches 98.8% and CPU spikes to 94.5%, breaching ARIMA_PLUS dynamic bounds (P = 0.994). SREs receive predictive alerts prior to process termination.",
                    "metrics": {
                        "memory_usage_pct": 98.8,
                        "cpu_usage_pct": 94.5,
                        "expected_upper_bound_cpu": 68.5,
                        "anomaly_probability": 0.994,
                    },
                },
                {
                    "stage": 3,
                    "name": "Stage 3 (Process Termination)",
                    "timestamp": "2026-09-29T15:55:30Z",
                    "description": "JVM throws FATAL: java.lang.OutOfMemoryError: Java heap space. Linux OOM Killer terminates the process (exit code 137).",
                    "metrics": {
                        "exception": "java.lang.OutOfMemoryError: Java heap space",
                        "oom_killer_exit_code": 137,
                        "killed_pid": 18422,
                    },
                },
            ],
            "correlated_logs": [
                {
                    "timestamp": "2026-09-29T15:51:00Z",
                    "hostname": "srv-b-batch-02",
                    "severity": "ERROR",
                    "message": "ERROR: JVM G1GC pause duration 1,240ms exceeded SLA threshold (heap utilization 92.0%) on srv-b-batch-02",
                },
                {
                    "timestamp": "2026-09-29T15:54:00Z",
                    "hostname": "srv-b-batch-02",
                    "severity": "FATAL",
                    "message": "FATAL: java.lang.OutOfMemoryError: Java heap space at com.gsk.ebrs.batch.SparkGenomicsAggregator.processRecord(SparkGenomicsAggregator.java:418)",
                },
                {
                    "timestamp": "2026-09-29T15:55:00Z",
                    "hostname": "srv-b-batch-02",
                    "severity": "CRITICAL",
                    "message": "CRITICAL: Out of memory: Killed process 18422 (java) total-vm:67108864kB, anon-rss:65421312kB, exit code 137",
                },
            ],
            "incident_root_cause_analysis": {
                "event_timestamp": "2026-09-29T15:55:00Z",
                "hostname": "srv-b-batch-02",
                "anomaly_score": 94.5,
                "model": "gemini-2.5-flash",
                "gemini_root_cause_analysis": (
                    "1. Immediate root cause: Unbounded Spark genomics batch aggregation on srv-b-batch-02 drove JVM heap utilization from 55.0% to 98.8% with 1,240ms GC pauses, culminating in java.lang.OutOfMemoryError and Linux OOM killer termination (exit code 137). "
                    "2. Downstream service blast radius: EBRS-Batch-Engine batch record signing pipelines at Site_B_Stevenage are halted and upstream EBRS-Web-Portal batch status polling requests are degraded. "
                    "3. Immediate corrective action: Drain active batch shards from srv-b-batch-02, restart EBRS-Batch-Engine with -XX:+UseG1GC and capped partition chunk sizes, and replay uncommitted EBRS batch checkpoints."
                ),
            },
            "structured_log_entities": [
                {
                    "timestamp": "2026-09-29T15:54:00Z",
                    "hostname": "srv-b-batch-02",
                    "root_cause_category": "JVM_HEAP_EXHAUSTION_OOM",
                    "failed_component": "EBRS-Batch-Engine (SparkGenomicsAggregator)",
                    "error_code": "java.lang.OutOfMemoryError / EXIT_137",
                    "recommended_action": "Restart JVM service with G1GC memory limits and drain stuck EBRS batch queue",
                    "confidence_score": 0.994,
                }
            ],
            "gql_topology_chain": "sw-core-stv-01 -> esxi-cluster-04 -> srv-b-batch-02 -> EBRS-Batch-Engine <- EBRS-Web-Portal",
        },
        "B": {
            "scenario_id": "B",
            "title": "Scenario B: Database Saturation & Web Tier Blast Radius",
            "primary_host": "srv-b-db-01",
            "upstream_impacted_hosts": ["srv-a-web-01", "srv-a-web-04"],
            "site_location": "Site_B_Stevenage -> Site_A_London",
            "system_id": "EBRS",
            "application_tier": "Database_Backend -> Web_Frontend",
            "impacted_application": "EBRS-Database-Core -> EBRS-Web-Portal",
            "anomaly_classification": "SPIKE_ANOMALY",
            "anomaly_probability": 0.996,
            "stages": [
                {
                    "stage": 1,
                    "name": "Stage 1 (Load Surge)",
                    "timestamp": "2026-09-29T15:48:00Z",
                    "description": "CPU utilization exceeds 95% (96.4%); active connection pool reaches 350/500 with lock queues accumulating.",
                    "metrics": {
                        "cpu_usage_pct": 96.4,
                        "active_connections": 350,
                        "max_connections": 500,
                    },
                },
                {
                    "stage": 2,
                    "name": "Stage 2 (Deadlock & Exhaustion)",
                    "timestamp": "2026-09-29T15:53:00Z",
                    "description": "Connection pool exhausts (CRITICAL: PostgreSQL connection pool exhausted 500/500). Write deadlocks occur on clinical_trial_records.",
                    "metrics": {
                        "active_connections": 500,
                        "max_connections": 500,
                        "deadlock_relation": "clinical_trial_records",
                    },
                },
                {
                    "stage": 3,
                    "name": "Stage 3 (Blast Radius Cascade)",
                    "timestamp": "2026-09-29T15:54:00Z",
                    "description": "Connected Web Frontend nodes (srv-a-web-01, srv-a-web-04) exhaust thread pools, emitting ERROR: HTTP 504 Gateway Timeout while contacting backend database.",
                    "metrics": {
                        "impacted_web_hosts": ["srv-a-web-01", "srv-a-web-04"],
                        "http_status": 504,
                        "gql_chain": "sw-core-stv-01 -> esxi-cluster-04 -> srv-b-db-01 -> EBRS-Database-Core <- EBRS-Web-Portal",
                    },
                },
            ],
            "correlated_logs": [
                {
                    "timestamp": "2026-09-29T15:52:00Z",
                    "hostname": "srv-b-db-01",
                    "severity": "CRITICAL",
                    "message": "CRITICAL: PostgreSQL connection pool exhausted 500/500 — remaining client requests queued",
                },
                {
                    "timestamp": "2026-09-29T15:53:00Z",
                    "hostname": "srv-b-db-01",
                    "severity": "ERROR",
                    "message": "ERROR: deadlock detected on relation clinical_trial_records (Process 9410 waits for ShareLock on transaction 884120)",
                },
                {
                    "timestamp": "2026-09-29T15:54:00Z",
                    "hostname": "srv-a-web-01",
                    "severity": "ERROR",
                    "message": "ERROR: HTTP 504 Gateway Timeout while contacting backend database srv-b-db-01:5432 (upstream pool saturated)",
                },
                {
                    "timestamp": "2026-09-29T15:54:00Z",
                    "hostname": "srv-a-web-04",
                    "severity": "ERROR",
                    "message": "ERROR: HTTP 504 Gateway Timeout while contacting backend database srv-b-db-01:5432 (worker threads exhausted)",
                },
            ],
            "incident_root_cause_analysis": {
                "event_timestamp": "2026-09-29T15:53:00Z",
                "hostname": "srv-b-db-01",
                "anomaly_score": 96.4,
                "model": "gemini-2.5-flash",
                "gemini_root_cause_analysis": (
                    "1. Immediate root cause: Unindexed concurrent write locks on relation clinical_trial_records saturated CPU (96.4%) on srv-b-db-01 and exhausted the PostgreSQL connection pool (500/500). "
                    "2. Downstream service blast radius: ISO GQL topology traversal confirms upstream web ingress nodes srv-a-web-01 and srv-a-web-04 in Site_A_London exhausted worker thread pools waiting on EBRS-Database-Core, emitting HTTP 504 Gateway Timeouts across EBRS-Web-Portal. "
                    "3. Immediate corrective action: Terminate blocking PID 9410 holding the ShareLock on clinical_trial_records, route read traffic to read-replicas via PgBouncer, and recycle stalled HTTP worker pools on srv-a-web-01 and srv-a-web-04."
                ),
            },
            "structured_log_entities": [
                {
                    "timestamp": "2026-09-29T15:53:00Z",
                    "hostname": "srv-b-db-01",
                    "root_cause_category": "DATABASE_CONNECTION_POOL_EXHAUSTION",
                    "failed_component": "PostgreSQL clinical_trial_records (EBRS-Database-Core)",
                    "error_code": "POOL_500_OF_500 / PG_DEADLOCK",
                    "recommended_action": "Terminate deadlocked backend transaction 884120 and scale PgBouncer pool",
                    "confidence_score": 0.989,
                },
                {
                    "timestamp": "2026-09-29T15:54:00Z",
                    "hostname": "srv-a-web-01",
                    "root_cause_category": "UPSTREAM_GATEWAY_TIMEOUT_CASCADE",
                    "failed_component": "EBRS-Web-Portal Ingress (Site_A_London)",
                    "error_code": "HTTP_504_GATEWAY_TIMEOUT",
                    "recommended_action": "Trip circuit breaker to srv-b-db-01 and drain wedged Tomcat/Nginx worker threads",
                    "confidence_score": 0.978,
                },
            ],
            "gql_topology_chain": "sw-core-stv-01 -> esxi-cluster-04 -> srv-b-db-01 -> EBRS-Database-Core <- EBRS-Web-Portal (srv-a-web-01, srv-a-web-04)",
        },
        "C": {
            "scenario_id": "C",
            "title": "Scenario C: Storage IO Saturation & Silent Host Drop to 0.0",
            "primary_host": "srv-c-ware-01",
            "site_location": "Site_C_Ware",
            "system_id": "MES_BATCH",
            "application_tier": "Batch_Processing",
            "impacted_application": "MES-Batch-Scheduler",
            "storage_mount": "/mnt/gsk_batch",
            "anomaly_classification": "SILENT_HOST_DROP_TO_ZERO",
            "anomaly_probability": 0.998,
            "stages": [
                {
                    "stage": 1,
                    "name": "Stage 1 (IO Saturation)",
                    "timestamp": "2026-09-29T15:46:00Z",
                    "description": "Shared storage mount /mnt/gsk_batch reaches IOPS saturation; IO wait exceeds 520ms (545.0ms).",
                    "metrics": {
                        "storage_mount": "/mnt/gsk_batch",
                        "io_wait_ms": 545.0,
                        "cpu_usage_pct": 64.0,
                    },
                },
                {
                    "stage": 2,
                    "name": "Stage 2 (Storage Stall, Eviction & Silent Drop to 0.0)",
                    "timestamp": "2026-09-29T15:51:00Z -> 2026-09-29T15:56:00Z",
                    "description": "IO wait exceeds 940ms (965.0ms), causing the node heartbeat daemon to block. The cluster orchestrator evicts the host from active scheduling, and telemetry drops to 0.0 (caught by GAP_FILL() as SILENT_HOST_DROP_TO_ZERO).",
                    "metrics": {
                        "storage_mount": "/mnt/gsk_batch",
                        "peak_io_wait_ms": 965.0,
                        "post_eviction_cpu_pct": 0.0,
                        "anomaly_classification": "SILENT_HOST_DROP_TO_ZERO",
                    },
                },
            ],
            "correlated_logs": [
                {
                    "timestamp": "2026-09-29T15:46:00Z",
                    "hostname": "srv-c-ware-01",
                    "severity": "ERROR",
                    "message": "ERROR: Shared storage mount /mnt/gsk_batch IOPS saturation detected (io_wait_ms=545.0ms > 520ms threshold)",
                },
                {
                    "timestamp": "2026-09-29T15:51:00Z",
                    "hostname": "srv-c-ware-01",
                    "severity": "CRITICAL",
                    "message": "CRITICAL: NFS/SAN mount /mnt/gsk_batch stalled (io_wait_ms=965.0ms > 940ms); node heartbeat daemon blocked — cluster orchestrator evicting host srv-c-ware-01",
                },
            ],
            "incident_root_cause_analysis": {
                "event_timestamp": "2026-09-29T15:56:00Z",
                "hostname": "srv-c-ware-01",
                "anomaly_score": 965.0,
                "model": "gemini-2.5-flash",
                "gemini_root_cause_analysis": (
                    "1. Immediate root cause: Shared storage mount /mnt/gsk_batch experienced severe SAN IOPS saturation with I/O wait surging from 545.0ms to 965.0ms, blocking kernel D-state threads and starving the node heartbeat daemon on srv-c-ware-01. "
                    "2. Downstream service blast radius: Cluster orchestrator evicted srv-c-ware-01 causing telemetry to drop to 0.0 (caught via GAP_FILL() as SILENT_HOST_DROP_TO_ZERO), stalling MES-Batch-Scheduler jobs at Site_C_Ware. "
                    "3. Immediate corrective action: Cordon srv-c-ware-01, remount /mnt/gsk_batch with hard IOPS QoS throttling on the storage array, and reschedule evicted MES_BATCH pods onto healthy Ware worker nodes."
                ),
            },
            "structured_log_entities": [
                {
                    "timestamp": "2026-09-29T15:51:00Z",
                    "hostname": "srv-c-ware-01",
                    "root_cause_category": "STORAGE_IOPS_STALL_AND_NODE_EVICTION",
                    "failed_component": "/mnt/gsk_batch (MES-Batch-Scheduler)",
                    "error_code": "IO_WAIT_965MS / SILENT_HOST_DROP_TO_ZERO",
                    "recommended_action": "Cordon node srv-c-ware-01, throttle SAN volume IOPS, and reschedule MES_BATCH jobs",
                    "confidence_score": 0.996,
                }
            ],
            "gql_topology_chain": "sw-core-ware-01 -> aks-prod-ware-02 -> srv-c-ware-01 (/mnt/gsk_batch) -> MES-Batch-Scheduler -> EBRS-Batch-Engine",
        },
    }

  def get_scenarios_payload(self, scenario_id: Optional[str] = None) -> dict[str, Any]:
    """Serves cascading failure waterfall scenarios (all or specific A, B, C)."""
    catalog = self._build_scenarios_catalog()
    if scenario_id:
      norm_id = scenario_id.strip().upper().replace("SCENARIO_", "").replace("SCENARIO", "")
      if norm_id not in catalog:
        raise ValueError(f"Unknown scenario_id '{scenario_id}'. Expected A, B, or C.")
      item = catalog[norm_id]
      return {
          "status": "SUCCESS",
          "project_id": self.project_id,
          "dataset_id": self.dataset_id,
          "scenario_id": norm_id,
          "scenario": item,
          **item,
      }

    return {
        "status": "SUCCESS",
        "project_id": self.project_id,
        "dataset_id": self.dataset_id,
        "total_scenarios": 3,
        "scenarios": catalog,
        "scenario_list": [catalog["A"], catalog["B"], catalog["C"]],
    }

  def handle_neuro_chat(self, question: str) -> dict[str, Any]:
    """Translates operator natural-language questions into verified SQL/ISO GQL and returns executive RCA."""
    clean_q = (question or "").strip()
    if not clean_q:
      clean_q = "What active service degradation and anomalies are occurring across EBRS sites right now?"

    q_lower = clean_q.lower()
    catalog = self._build_scenarios_catalog()

    # Determine intent & generate verified BigQuery SQL or ISO GQL query
    if any(
        kw in q_lower
        for kw in [
            "database",
            "postgres",
            "500/500",
            "504",
            "blast radius",
            "topology",
            "gql",
            "switch",
            "hypervisor",
            "esxi",
            "sw-core-stv-01",
            "srv-a-web",
            "srv-b-db",
            "scenario b",
        ]
    ):
      intent = "TOPOLOGY_BLAST_RADIUS_GQL"
      query_type = "ISO_GQL"
      generated_query = f"""SELECT
  failing_switch,
  impacted_hypervisor,
  impacted_vm,
  impacted_backend,
  impacted_frontend
FROM GRAPH_TABLE(
  `{self.project_id}.{self.dataset_id}.gsk_infrastructure_dependency_graph`,
  MATCH (sw:Switch {{switch_id: 'sw-core-stv-01'}})-[:CONNECTED_TO]->(hyp:Hypervisor)-[:HOSTS]->(vm:Host)-[:RUNS]->(backend:Application)<-[:COMMUNICATES_WITH]-(frontend:Application)
  RETURN
    sw.hostname AS failing_switch,
    hyp.hostname AS impacted_hypervisor,
    vm.hostname AS impacted_vm,
    backend.name AS impacted_backend,
    frontend.name AS impacted_frontend
);"""
      rows = self.get_topology_payload()["graph_table_paths"][:3]
      scen_b = catalog["B"]
      executive_summary = {
          "hostname": "srv-b-db-01 (cascading to srv-a-web-01, srv-a-web-04)",
          "physical_site": "Site_B_Stevenage -> Site_A_London",
          "impacted_application": "EBRS-Database-Core & EBRS-Web-Portal (EBRS)",
          "measured_utilization": "CPU: 96.4% | DB Pool: 500/500 (Exhausted) | Web Ingress: HTTP 504 Gateway Timeout",
          "remediation_runbook": (
              "RUNBOOK-EBRS-DB-02: 1) Terminate deadlocked PostgreSQL PID 9410 on clinical_trial_records; "
              "2) Enable PgBouncer transaction pooling & read-replica routing; "
              "3) Recycle wedged HTTP worker pools on srv-a-web-01 and srv-a-web-04."
          ),
          "three_sentence_rca": scen_b["incident_root_cause_analysis"]["gemini_root_cause_analysis"],
      }

    elif any(
        kw in q_lower
        for kw in [
            "storage",
            "io_wait",
            "io wait",
            "iops",
            "gsk_batch",
            "silent",
            "0.0",
            "drop to zero",
            "gap_fill",
            "ware",
            "srv-c-ware",
            "scenario c",
        ]
    ):
      intent = "STORAGE_SILENT_DROP_SQL"
      query_type = "SQL"
      generated_query = f"""SELECT
  hostname,
  ts_minute,
  ROUND(cpu_usage, 2) AS actual_cpu,
  ROUND(GREATEST(0.0, lower_bound), 2) AS expected_lower_bound,
  ROUND(upper_bound, 2) AS expected_upper_bound,
  ROUND(anomaly_probability, 4) AS anomaly_probability,
  CASE
    WHEN is_anomaly AND cpu_usage > upper_bound THEN 'SPIKE_ANOMALY'
    WHEN is_anomaly AND cpu_usage < lower_bound AND cpu_usage = 0.0 THEN 'SILENT_HOST_DROP_TO_ZERO'
    WHEN is_anomaly AND cpu_usage < lower_bound THEN 'DIP_ANOMALY'
    ELSE 'NORMAL'
  END AS anomaly_classification
FROM ML.DETECT_ANOMALIES(
  MODEL `{self.project_id}.{self.dataset_id}.host_cpu_arima_model`,
  STRUCT(0.85 AS anomaly_prob_threshold),
  TABLE `{self.project_id}.{self.dataset_id}.recent_cpu_eval_view`
)
WHERE hostname = 'srv-c-ware-01'
ORDER BY ts_minute DESC;"""
      rows = [
          r
          for r in self.get_anomalies_payload()["anomalies"]
          if r["hostname"] == "srv-c-ware-01"
      ]
      scen_c = catalog["C"]
      executive_summary = {
          "hostname": "srv-c-ware-01",
          "physical_site": "Site_C_Ware",
          "impacted_application": "MES-Batch-Scheduler (MES_BATCH / /mnt/gsk_batch)",
          "measured_utilization": "IO Wait: 965.0ms (>940ms stall) | Post-Eviction CPU: 0.0% (SILENT_HOST_DROP_TO_ZERO, P=0.998)",
          "remediation_runbook": (
              "RUNBOOK-MES-SAN-03: 1) Cordon evicted host srv-c-ware-01; "
              "2) Apply SAN QoS IOPS throttling on volume /mnt/gsk_batch; "
              "3) Reschedule MES-Batch-Scheduler workloads onto healthy aks-prod-ware-02 nodes."
          ),
          "three_sentence_rca": scen_c["incident_root_cause_analysis"]["gemini_root_cause_analysis"],
      }

    elif any(
        kw in q_lower
        for kw in [
            "maintenance",
            "servicenow",
            "chg0049281",
            "suppress",
            "arimax",
            "xreg",
        ]
    ):
      intent = "MAINTENANCE_SUPPRESSION_SQL"
      query_type = "SQL"
      generated_query = f"""SELECT
  t.timestamp,
  t.hostname,
  t.site_location,
  t.system_id,
  t.cpu_usage_pct,
  m.change_id,
  m.is_maintenance_window,
  m.description
FROM `{self.project_id}.{self.dataset_id}.enterprise_telemetry_partitioned` t
INNER JOIN `{self.project_id}.{self.dataset_id}.servicenow_maintenance_windows` m
  ON t.hostname = m.hostname
  AND t.timestamp BETWEEN m.start_time AND m.end_time
WHERE m.change_id = 'CHG0049281';"""
      rows = [
          r
          for r in self.get_anomalies_payload()["anomalies"]
          if r.get("active_change_id") == "CHG0049281"
      ]
      executive_summary = {
          "hostname": "ora-db-stv-01",
          "physical_site": "Site_B_Stevenage",
          "impacted_application": "LIMS-Core-API (LIMS Database Tier)",
          "measured_utilization": "CPU: 88.4% (Within ARIMA_PLUS_XREG Maintenance Bound 89.5% | Change: CHG0049281)",
          "remediation_runbook": (
              "RUNBOOK-SNOW-SUPPRESS-04: No pager escalation required — host ora-db-stv-01 is inside active "
              "ServiceNow maintenance window CHG0049281 (is_maintenance_window = 1). Resume standard monitoring at 18:00 UTC."
          ),
          "three_sentence_rca": (
              "1. Immediate root cause: Elevated CPU utilization (88.4%) on ora-db-stv-01 is caused by scheduled quarterly PSU patching under active ServiceNow change window CHG0049281. "
              "2. Downstream service blast radius: Zero unplanned customer impact; ARIMA_PLUS_XREG external regressor (is_maintenance_window=1) widened upper confidence bounds to 89.5% and suppressed false-positive paging. "
              "3. Immediate corrective action: Maintain automated alert suppression until CHG0049281 completes and verify post-patch baseline convergence."
          ),
      }

    else:
      # Default / Scenario A / Active service degradation across EBRS
      intent = "ACTIVE_SERVICE_DEGRADATION_SQL"
      query_type = "SQL"
      generated_query = f"""SELECT
  t.timestamp,
  t.hostname,
  t.site_location,
  t.system_id,
  t.application_tier,
  ROUND(t.cpu_usage_pct, 1) AS cpu_usage_pct,
  ROUND(t.memory_usage_pct, 1) AS memory_usage_pct,
  ROUND(t.io_wait_ms, 1) AS io_wait_ms,
  t.status,
  STRING_AGG(l.message, ' | ' ORDER BY l.timestamp DESC LIMIT 3) AS correlated_error_logs
FROM `{self.project_id}.{self.dataset_id}.enterprise_telemetry_partitioned` t
LEFT JOIN `{self.project_id}.{self.dataset_id}.system_logs` l
  ON t.hostname = l.hostname
  AND l.severity IN ('ERROR', 'CRITICAL', 'FATAL')
  AND l.timestamp BETWEEN TIMESTAMP_SUB(t.timestamp, INTERVAL 5 MINUTE) AND t.timestamp
WHERE t.status IN ('ANOMALY', 'DOWN')
  AND t.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 MINUTE)
GROUP BY
  t.timestamp, t.hostname, t.site_location, t.system_id,
  t.application_tier, t.cpu_usage_pct, t.memory_usage_pct, t.io_wait_ms, t.status
ORDER BY t.timestamp DESC;"""

      conn = self._get_connection()
      try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT timestamp, hostname, site_location, system_id, application_tier,
                   cpu_usage_pct, memory_usage_pct, io_wait_ms, status, active_connections
            FROM enterprise_telemetry_partitioned
            WHERE status IN ('ANOMALY', 'DOWN')
            ORDER BY timestamp DESC
            """
        )
        rows = [dict(r) for r in cur.fetchall()]
      finally:
        self._close_connection(conn)

      scen_a = catalog["A"]
      executive_summary = {
          "hostname": "srv-b-batch-02 (plus srv-b-db-01, srv-a-web-01, srv-a-web-04, srv-c-ware-01)",
          "physical_site": "Site_B_Stevenage (EBRS Batch & DB), Site_A_London (Web), Site_C_Ware (MES_BATCH)",
          "impacted_application": "EBRS-Batch-Engine (EBRS Batch_Processing)",
          "measured_utilization": "CPU: 94.5% | Memory: 98.8% (Heap OOM) | GC Pause: 1,240ms | Anomaly Prob: 0.994",
          "remediation_runbook": (
              "RUNBOOK-EBRS-JVM-01: 1) Drain batch shards from srv-b-batch-02; "
              "2) Restart EBRS-Batch-Engine with -XX:+UseG1GC and 32GB heap cap; "
              "3) Replay uncommitted EBRS batch checkpoints and verify downstream PostgreSQL pool recovery."
          ),
          "three_sentence_rca": scen_a["incident_root_cause_analysis"]["gemini_root_cause_analysis"],
      }

    live_rows = self._run_live_bq_query(generated_query)
    effective_rows = live_rows if live_rows else rows

    answer_markdown = (
        f"### GSK 'Neuro' Executive Incident Summary\n"
        f"- **Hostname**: `{executive_summary['hostname']}`\n"
        f"- **Physical Site**: `{executive_summary['physical_site']}`\n"
        f"- **Impacted Application**: `{executive_summary['impacted_application']}`\n"
        f"- **Measured Resource Utilization**: `{executive_summary['measured_utilization']}`\n"
        f"- **Actionable Remediation Runbook**: {executive_summary['remediation_runbook']}\n\n"
        f"**Gemini 2.5 Flash 3-Sentence RCA**:\n{executive_summary['three_sentence_rca']}"
    )

    return {
        "status": "SUCCESS",
        "question": clean_q,
        "system_prompt": NEURO_SYSTEM_PROMPT,
        "intent": intent,
        "query_type": query_type,
        "generated_query": generated_query,
        "rows": effective_rows,
        "results": effective_rows,
        "row_count": len(effective_rows),
        "executive_summary": executive_summary,
        "answer": answer_markdown,
    }


OBSERVABILITY_WORKBENCH_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta name="description" content="GSK Enterprise Observability & Neuro AI — Interactive Step-by-Step Executive Demo Walkthrough">
  <title>GSK Enterprise Observability &amp; "Neuro" AI — Guided Executive Demo</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg-page: #f8fafc;
      --bg-surface: #ffffff;
      --bg-subtle: #f1f5f9;
      --text-primary: #0f172a;
      --text-secondary: #475569;
      --text-muted: #64748b;
      --border-light: #e2e8f0;
      --border-strong: #cbd5e1;
      --brand-orange: #f36633;
      --brand-blue: #0284c7;
      --brand-blue-bg: #e0f2fe;
      --danger: #dc2626;
      --danger-bg: #fef2f2;
      --danger-border: #fecaca;
      --warning: #d97706;
      --warning-bg: #fffbeb;
      --warning-border: #fde68a;
      --success: #059669;
      --success-bg: #ecfdf5;
      --success-border: #a7f3d0;
      --info: #0369a1;
      --info-bg: #f0f9ff;
      --info-border: #bae6fd;
      --shadow-sm: 0 1px 3px rgba(15, 23, 42, 0.06);
      --shadow-md: 0 4px 14px rgba(15, 23, 42, 0.07);
    }

    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
      background-color: var(--bg-page);
      color: var(--text-primary);
      line-height: 1.5;
      min-height: 100vh;
    }

    /* Top Brand & Guided Stepper Bar */
    .top-header {
      background: var(--bg-surface);
      border-bottom: 1px solid var(--border-light);
      position: sticky;
      top: 0;
      z-index: 50;
      box-shadow: var(--shadow-sm);
    }
    .header-inner {
      max-width: 1280px;
      margin: 0 auto;
      padding: 14px 24px;
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
    }
    .brand-group {
      display: flex;
      align-items: center;
      gap: 14px;
    }
    .brand-badge {
      background: var(--brand-orange);
      color: #fff;
      font-weight: 800;
      font-size: 14px;
      padding: 6px 12px;
      border-radius: 8px;
      letter-spacing: 0.04em;
    }
    .brand-title h1 {
      font-size: 18px;
      font-weight: 700;
      color: var(--text-primary);
      letter-spacing: -0.01em;
    }
    .brand-title p {
      font-size: 12px;
      color: var(--text-muted);
    }
    .step-controls {
      display: flex;
      align-items: center;
      gap: 10px;
    }
    .btn {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 8px 16px;
      border-radius: 8px;
      font-size: 13px;
      font-weight: 600;
      cursor: pointer;
      border: 1px solid var(--border-strong);
      background: var(--bg-surface);
      color: var(--text-primary);
      transition: all 0.15s ease;
      text-decoration: none;
    }
    .btn:hover { background: var(--bg-subtle); }
    .btn-primary {
      background: var(--brand-blue);
      color: #fff;
      border-color: var(--brand-blue);
    }
    .btn-primary:hover { background: #0369a1; }

    /* 4-Step Visual Progress Navigation */
    .stepper-bar {
      max-width: 1280px;
      margin: 0 auto;
      padding: 12px 24px 0;
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 12px;
    }
    .step-tab {
      background: var(--bg-subtle);
      border: 1px solid var(--border-light);
      border-bottom: 3px solid transparent;
      border-radius: 10px 10px 0 0;
      padding: 12px 16px;
      cursor: pointer;
      text-align: left;
      transition: all 0.15s ease;
    }
    .step-tab:hover { background: #e2e8f0; }
    .step-tab.active {
      background: var(--bg-surface);
      border-color: var(--border-light);
      border-bottom-color: var(--brand-orange);
      box-shadow: 0 -2px 8px rgba(15, 23, 42, 0.04);
    }
    .step-num {
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      color: var(--brand-orange);
      display: block;
      margin-bottom: 2px;
    }
    .step-name {
      font-size: 14px;
      font-weight: 700;
      color: var(--text-primary);
      display: block;
    }
    .step-desc {
      font-size: 12px;
      color: var(--text-muted);
      display: block;
      margin-top: 2px;
    }

    /* Main Container & Story Banner */
    .container {
      max-width: 1280px;
      margin: 0 auto;
      padding: 24px;
    }
    .step-panel { display: none; }
    .step-panel.active { display: block; }

    .presenter-banner {
      background: linear-gradient(135deg, #fff7ed 0%, #f0f9ff 100%);
      border: 1px solid #fed7aa;
      border-radius: 12px;
      padding: 18px 22px;
      margin-bottom: 24px;
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
    }
    .presenter-banner-text { flex: 1; min-width: 280px; }
    .presenter-label {
      display: inline-block;
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      color: #c2410c;
      background: #ffedd5;
      padding: 3px 8px;
      border-radius: 4px;
      margin-bottom: 6px;
    }
    .presenter-banner h2 {
      font-size: 18px;
      font-weight: 700;
      color: var(--text-primary);
      margin-bottom: 4px;
    }
    .presenter-banner p {
      font-size: 14px;
      color: var(--text-secondary);
    }

    /* Executive KPI Grid */
    .grid-3 {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
      gap: 16px;
      margin-bottom: 24px;
    }
    .grid-2 {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(440px, 1fr));
      gap: 20px;
      margin-bottom: 24px;
    }
    .card {
      background: var(--bg-surface);
      border: 1px solid var(--border-light);
      border-radius: 12px;
      padding: 20px;
      box-shadow: var(--shadow-sm);
    }
    .card-clickable {
      cursor: pointer;
      transition: transform 0.15s ease, box-shadow 0.15s ease, border-color 0.15s ease;
    }
    .card-clickable:hover {
      transform: translateY(-2px);
      box-shadow: var(--shadow-md);
    }
    .card-clickable.selected {
      border: 2px solid var(--brand-blue);
      background: #f8fdff;
    }

    .badge {
      display: inline-flex;
      align-items: center;
      padding: 3px 10px;
      border-radius: 999px;
      font-size: 11px;
      font-weight: 700;
      letter-spacing: 0.02em;
    }
    .badge-danger { background: var(--danger-bg); color: var(--danger); border: 1px solid var(--danger-border); }
    .badge-warning { background: var(--warning-bg); color: var(--warning); border: 1px solid var(--warning-border); }
    .badge-success { background: var(--success-bg); color: var(--success); border: 1px solid var(--success-border); }
    .badge-info { background: var(--info-bg); color: var(--info); border: 1px solid var(--info-border); }

    .big-stat {
      font-size: 28px;
      font-weight: 800;
      color: var(--text-primary);
      margin: 8px 0 4px;
      letter-spacing: -0.02em;
    }
    .card-subtitle {
      font-size: 13px;
      color: var(--text-secondary);
    }

    /* Clean Tables */
    .table-wrap { overflow-x: auto; }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }
    th {
      text-align: left;
      padding: 10px 12px;
      background: var(--bg-subtle);
      color: var(--text-secondary);
      font-weight: 600;
      font-size: 12px;
      border-bottom: 1px solid var(--border-light);
    }
    td {
      padding: 12px;
      border-bottom: 1px solid var(--border-light);
      color: var(--text-primary);
      vertical-align: top;
    }
    tr:hover td { background: #f8fafc; }

    /* Step 2: 4-Column Topology Map */
    .topo-grid {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 16px;
      margin: 20px 0;
    }
    .topo-col {
      background: var(--bg-subtle);
      border: 1px solid var(--border-light);
      border-radius: 12px;
      padding: 14px;
    }
    .topo-col-header {
      font-size: 12px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--text-secondary);
      margin-bottom: 12px;
      padding-bottom: 8px;
      border-bottom: 1px solid var(--border-strong);
    }
    .topo-node {
      background: var(--bg-surface);
      border: 1px solid var(--border-light);
      border-radius: 8px;
      padding: 12px;
      margin-bottom: 10px;
      box-shadow: var(--shadow-sm);
      transition: all 0.2s ease;
    }
    .topo-node.active-path {
      border: 2px solid var(--danger);
      background: var(--danger-bg);
      box-shadow: 0 4px 12px rgba(220, 38, 38, 0.14);
    }
    .topo-node-title {
      font-size: 13px;
      font-weight: 700;
      color: var(--text-primary);
    }
    .topo-node-meta {
      font-size: 11px;
      color: var(--text-muted);
      margin-top: 2px;
    }

    /* Step 3: 3-Stage Waterfall Cards */
    .stage-list {
      display: flex;
      flex-direction: column;
      gap: 14px;
    }
    .stage-item {
      background: var(--bg-surface);
      border: 1px solid var(--border-light);
      border-left: 4px solid var(--brand-blue);
      border-radius: 10px;
      padding: 16px;
    }
    .stage-item.stage-2 { border-left-color: var(--warning); }
    .stage-item.stage-3 { border-left-color: var(--danger); }

    .rca-point {
      background: var(--bg-subtle);
      border-radius: 10px;
      padding: 14px 16px;
      margin-bottom: 12px;
    }
    .rca-point-label {
      font-size: 12px;
      font-weight: 700;
      text-transform: uppercase;
      color: var(--brand-blue);
      margin-bottom: 4px;
    }

    /* Step 4: Ask Neuro Assistant */
    .pill-row {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-bottom: 16px;
    }
    .prompt-pill {
      background: var(--info-bg);
      color: var(--info);
      border: 1px solid var(--info-border);
      border-radius: 999px;
      padding: 7px 14px;
      font-size: 12px;
      font-weight: 600;
      cursor: pointer;
      transition: all 0.15s ease;
    }
    .prompt-pill:hover {
      background: #bae6fd;
    }
    .search-bar {
      display: flex;
      gap: 10px;
      margin-bottom: 20px;
    }
    .search-input {
      flex: 1;
      padding: 12px 16px;
      border: 1px solid var(--border-strong);
      border-radius: 10px;
      font-size: 14px;
      font-family: inherit;
    }
    details.tech-drawer {
      margin-top: 18px;
      background: var(--bg-subtle);
      border: 1px solid var(--border-light);
      border-radius: 8px;
      padding: 10px 14px;
    }
    details.tech-drawer summary {
      cursor: pointer;
      font-size: 12px;
      font-weight: 600;
      color: var(--text-secondary);
    }
    pre.code-block {
      margin-top: 10px;
      background: #0f172a;
      color: #e2e8f0;
      padding: 14px;
      border-radius: 8px;
      font-family: 'JetBrains Mono', monospace;
      font-size: 12px;
      overflow-x: auto;
    }
    @media (max-width: 900px) {
      .stepper-bar, .topo-grid { grid-template-columns: 1fr; }
      .grid-2 { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>

  <!-- TOP HEADER & GUIDED NAVIGATION -->
  <header class="top-header">
    <div class="header-inner">
      <div class="brand-group">
        <span class="brand-badge">GSK &times; Google Cloud</span>
        <div class="brand-title">
          <h1>Enterprise Observability &amp; "Neuro" AI Assistant</h1>
          <p>Interactive Step-by-Step Executive Demo &bull; Live on GCP Project <strong>gke-demos-363017</strong> (London / EU)</p>
        </div>
      </div>
      <div class="step-controls">
        <button class="btn" id="btn-prev-step" onclick="changeStep(-1)">&larr; Previous Step</button>
        <button class="btn btn-primary" id="btn-next-step" onclick="changeStep(1)">Next Step: Full-Stack Map &rarr;</button>
        <a class="btn" href="http://saffi-jetski-dev.c.googlers.com:8080/">ANO 4-Act Command Center</a>
      </div>
    </div>

    <!-- 4-STEP VISUAL WIZARD TABS -->
    <nav class="stepper-bar" aria-label="Demo Walkthrough Steps">
      <button class="step-tab active" id="tab-step-1" onclick="goToStep(1)">
        <span class="step-num">Step 1 of 4</span>
        <span class="step-name">Smart Baselines &amp; Noise Reduction</span>
        <span class="step-desc">Catch real crashes &amp; silence maintenance alarms</span>
      </button>
      <button class="step-tab" id="tab-step-2" onclick="goToStep(2)">
        <span class="step-num">Step 2 of 4</span>
        <span class="step-name">Full-Stack Blast Radius Map</span>
        <span class="step-desc">Physical Switch &rarr; VMware &rarr; Server &rarr; App</span>
      </button>
      <button class="step-tab" id="tab-step-3" onclick="goToStep(3)">
        <span class="step-num">Step 3 of 4</span>
        <span class="step-name">3 Real Manufacturing Scenarios</span>
        <span class="step-desc">Step-by-step timeline &amp; AI Root Cause</span>
      </button>
      <button class="step-tab" id="tab-step-4" onclick="goToStep(4)">
        <span class="step-num">Step 4 of 4</span>
        <span class="step-name">Ask "Neuro" AI Assistant</span>
        <span class="step-desc">Plain-English questions &rarr; Instant playbooks</span>
      </button>
    </nav>
  </header>

  <main class="container">

    <!-- =====================================================================
         STEP 1: SMART BASELINES & NOISE SUPPRESSION
         ===================================================================== -->
    <section class="step-panel active" id="panel-step-1">
      <div class="presenter-banner">
        <div class="presenter-banner-text">
          <span class="presenter-label">Step 1 Presenter Story &bull; Why Static Thresholds Fail</span>
          <h2>Replacing 80% Static Alerts with Self-Learning AI Baselines Across 6,000+ GSK Servers</h2>
          <p>
            Traditional monitoring pages engineers whenever CPU crosses 80%&mdash;even during planned ServiceNow maintenance&mdash;while missing "silent freezes" when a server stops reporting (0%). Click the 3 cards below to see how BigQuery ML distinguishes real emergencies from planned maintenance.
          </p>
        </div>
        <button class="btn btn-primary" onclick="goToStep(2)">Continue to Step 2: Blast Radius Map &rarr;</button>
      </div>

      <!-- 3 Clickable Outcome Highlight Cards -->
      <div class="grid-3">
        <div class="card card-clickable selected" id="filter-card-ALL" onclick="filterAnomalies('ALL')">
          <span class="badge badge-danger">1. Early Crash Detection (Stevenage)</span>
          <div class="big-stat">94.5% CPU</div>
          <p class="card-subtitle">
            <strong>EBRS Batch Server (srv-b-batch-02)</strong> breached its normal AI ceiling (<strong>68.5%</strong>) 15 minutes before a Java Out-of-Memory crash.
          </p>
        </div>

        <div class="card card-clickable" id="filter-card-SUPPRESSED" onclick="filterAnomalies('SUPPRESSED_MAINTENANCE_WINDOW')">
          <span class="badge badge-warning">2. Planned Patching Silenced (0 Pager Noise)</span>
          <div class="big-stat">88.4% CPU &bull; Suppressed</div>
          <p class="card-subtitle">
            <strong>Oracle LIMS DB (ora-db-stv-01)</strong> spiked during ServiceNow Change <strong>CHG0049281</strong>. AI recognized the maintenance window and silenced the false alarm.
          </p>
        </div>

        <div class="card card-clickable" id="filter-card-ZERO" onclick="filterAnomalies('SILENT_HOST_DROP_TO_ZERO')">
          <span class="badge badge-info">3. Silent Server Freeze Caught (Ware)</span>
          <div class="big-stat">0.0% &bull; Heartbeat Lost</div>
          <p class="card-subtitle">
            <strong>Ware Batch Server (srv-c-ware-01)</strong> froze on shared storage and dropped to <strong>0.0%</strong> (<code>SILENT_HOST_DROP_TO_ZERO</code>). Caught immediately via AI zero-fill.
          </p>
        </div>
      </div>

      <!-- Visual Confidence Band Chart + Live Server Health Table -->
      <div class="grid-2">
        <div class="card">
          <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:12px;">
            <div>
              <h3 style="font-size:15px; font-weight:700;">Visual AI Confidence Corridor vs. Actual Server Load</h3>
              <p style="font-size:12px; color:var(--text-muted);">Green shaded band = Expected normal operating range learned by BigQuery ML</p>
            </div>
            <span class="badge badge-success">7-Day Horizon &bull; UK Holidays ('GB')</span>
          </div>
          <svg viewBox="0 0 600 240" style="width:100%; height:auto; background:#f8fafc; border:1px solid #e2e8f0; border-radius:8px;">
            <!-- Grid lines -->
            <line x1="50" y1="30" x2="570" y2="30" stroke="#e2e8f0" stroke-dasharray="4 4"/>
            <line x1="50" y1="90" x2="570" y2="90" stroke="#e2e8f0" stroke-dasharray="4 4"/>
            <line x1="50" y1="150" x2="570" y2="150" stroke="#e2e8f0" stroke-dasharray="4 4"/>
            <line x1="50" y1="205" x2="570" y2="205" stroke="#cbd5e1"/>
            <text x="12" y="34" font-size="10" fill="#64748b">100%</text>
            <text x="18" y="94" font-size="10" fill="#64748b">70%</text>
            <text x="18" y="154" font-size="10" fill="#64748b">40%</text>
            <text x="24" y="208" font-size="10" fill="#64748b">0%</text>

            <!-- Shaded Green Normal Confidence Corridor (32% to 68%) -->
            <polygon points="50,95 180,92 310,95 430,52 560,94 560,158 430,130 310,155 180,156 50,155" fill="#bbf7d0" fill-opacity="0.55"/>
            <polyline points="50,95 180,92 310,95 430,52 560,94" fill="none" stroke="#10b981" stroke-width="1.5" stroke-dasharray="5 3"/>

            <!-- Actual CPU Load Line -->
            <polyline points="50,128 140,122 230,42 330,125 430,56 530,205" fill="none" stroke="#0f172a" stroke-width="2.5"/>

            <!-- Pin 1: Spike Breach (94.5%) -->
            <circle cx="230" cy="42" r="7" fill="#dc2626"/>
            <rect x="145" y="8" width="175" height="24" rx="5" fill="#fef2f2" stroke="#fecaca"/>
            <text x="153" y="24" font-size="10" font-weight="700" fill="#dc2626">srv-b-batch-02: 94.5% (Spike!)</text>

            <!-- Pin 2: Planned Maintenance Suppressed (88.4%) -->
            <circle cx="430" cy="56" r="7" fill="#d97706"/>
            <rect x="335" y="22" width="190" height="24" rx="5" fill="#fffbeb" stroke="#fde68a"/>
            <text x="343" y="38" font-size="10" font-weight="700" fill="#b45309">CHG0049281: 88.4% (Suppressed)</text>

            <!-- Pin 3: Silent Drop to 0.0% -->
            <circle cx="530" cy="205" r="7" fill="#0284c7"/>
            <rect x="385" y="172" width="180" height="24" rx="5" fill="#f0f9ff" stroke="#bae6fd"/>
            <text x="393" y="188" font-size="10" font-weight="700" fill="#0369a1">srv-c-ware-01: 0.0% (Frozen!)</text>
          </svg>
        </div>

        <div class="card">
          <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:12px;">
            <div>
              <h3 style="font-size:15px; font-weight:700;">Live Evaluated Server Signals</h3>
              <p style="font-size:12px; color:var(--text-muted);">Showing plain-English AI classification across Stevenage, London, and Ware</p>
            </div>
            <button class="btn" style="padding:4px 10px; font-size:11px;" onclick="filterAnomalies('ALL')">Show All</button>
          </div>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Server &amp; Site</th>
                  <th>Actual Load</th>
                  <th>Normal Ceiling</th>
                  <th>AI Verdict &amp; Action</th>
                </tr>
              </thead>
              <tbody id="anomalies-tbody">
                <tr><td colspan="4">Loading live telemetry...</td></tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </section>

    <!-- =====================================================================
         STEP 2: FULL-STACK BLAST RADIUS MAP (ISO GQL PROPERTY GRAPH)
         ===================================================================== -->
    <section class="step-panel" id="panel-step-2">
      <div class="presenter-banner">
        <div class="presenter-banner-text">
          <span class="presenter-label">Step 2 Presenter Story &bull; Eliminating Operational Silos</span>
          <h2>One-Click Blast Radius: From Physical SolarWinds Switch to Manufacturing App</h2>
          <p>
            Previously, network teams looked at SolarWinds, virtualization teams looked at VMware, and app teams looked at EBRS logs. BigQuery's Property Graph (<code>gsk_infrastructure_dependency_graph</code>) connects all 4 layers so you can trace any outage in seconds.
          </p>
        </div>
        <button class="btn btn-primary" onclick="goToStep(3)">Continue to Step 3: Incident Replay &rarr;</button>
      </div>

      <!-- Interactive Path Highlighter Buttons -->
      <div style="display:flex; flex-wrap:wrap; gap:10px; margin-bottom:16px; align-items:center;">
        <span style="font-size:13px; font-weight:700; color:var(--text-secondary);">Click to Highlight Dependency Chain:</span>
        <button class="btn" onclick="highlightTopology('A')">Highlight Scenario A (Stevenage Batch Memory Crash)</button>
        <button class="btn" onclick="highlightTopology('B')">Highlight Scenario B (Stevenage DB Lock &rarr; London Web 504s)</button>
        <button class="btn" onclick="highlightTopology('C')">Highlight Scenario C (Ware Storage Stall &rarr; Scheduler Drop)</button>
      </div>

      <div id="topo-impact-banner" class="card" style="margin-bottom:18px; background:#fef2f2; border-color:#fecaca;">
        <strong style="color:#dc2626; font-size:13px;">Active Highlighted Blast Radius Path:</strong>
        <span id="topo-impact-text" style="font-size:13px; color:#0f172a; margin-left:8px;">
          Stevenage Core Switch (<code>sw-core-stv-01</code>) &rarr; VMware ESXi Cluster (<code>esxi-cluster-04</code>) &rarr; Database Server (<code>srv-b-db-01</code>) &rarr; <code>EBRS-Database-Core</code> &larr; <strong>Impacts Operators on <code>EBRS-Web-Portal</code> (London)</strong>
        </span>
      </div>

      <!-- 4-Column Visual Architecture Map -->
      <div class="topo-grid" id="topo-columns">
        <div class="topo-col">
          <div class="topo-col-header">1. Physical Network (SolarWinds)</div>
          <div id="topo-col-switch"></div>
        </div>
        <div class="topo-col">
          <div class="topo-col-header">2. Hypervisors &amp; Clusters (VMware/AKS)</div>
          <div id="topo-col-hypervisor"></div>
        </div>
        <div class="topo-col">
          <div class="topo-col-header">3. Virtual Servers (ServiceNow CMDB)</div>
          <div id="topo-col-host"></div>
        </div>
        <div class="topo-col">
          <div class="topo-col-header">4. Manufacturing Apps (GxP / EBRS)</div>
          <div id="topo-col-app"></div>
        </div>
      </div>
    </section>

    <!-- =====================================================================
         STEP 3: STEP-BY-STEP INCIDENT REPLAY (SCENARIOS A, B, C)
         ===================================================================== -->
    <section class="step-panel" id="panel-step-3">
      <div class="presenter-banner">
        <div class="presenter-banner-text">
          <span class="presenter-label">Step 3 Presenter Story &bull; Automated Root Cause Analysis</span>
          <h2>Walk Through 3 Real-World Manufacturing Incidents Caught Automatically</h2>
          <p>
            Select any of the 3 manufacturing scenarios below to see how the platform correlates metric spikes with the preceding 5 minutes of system error logs and uses <strong>Gemini 2.5 Flash</strong> to write a 3-point executive action plan.
          </p>
        </div>
        <button class="btn btn-primary" onclick="goToStep(4)">Continue to Step 4: Ask "Neuro" AI &rarr;</button>
      </div>

      <!-- Scenario Selector Cards -->
      <div class="grid-3">
        <div class="card card-clickable selected" id="scen-card-A" onclick="loadScenario('A')">
          <span class="badge badge-danger">Scenario A &bull; Stevenage (EBRS)</span>
          <h3 style="font-size:16px; font-weight:700; margin:8px 0 4px;">Batch Memory Leak &amp; Crash</h3>
          <p class="card-subtitle">Spark genomics batch worker leaks memory from 55% &rarr; 98.8% before crashing.</p>
        </div>
        <div class="card card-clickable" id="scen-card-B" onclick="loadScenario('B')">
          <span class="badge badge-warning">Scenario B &bull; Stevenage &rarr; London</span>
          <h3 style="font-size:16px; font-weight:700; margin:8px 0 4px;">Database Lock &rarr; Web Portal 504s</h3>
          <p class="card-subtitle">PostgreSQL 500/500 pool exhaustion in Stevenage causes Web Portal timeouts in London.</p>
        </div>
        <div class="card card-clickable" id="scen-card-C" onclick="loadScenario('C')">
          <span class="badge badge-info">Scenario C &bull; Ware (MES Batch)</span>
          <h3 style="font-size:16px; font-weight:700; margin:8px 0 4px;">Storage Freeze &amp; Silent Drop to 0%</h3>
          <p class="card-subtitle">Shared storage <code>/mnt/gsk_batch</code> stalls at 965ms I/O wait, evicting the server.</p>
        </div>
      </div>

      <!-- Visual Waterfall + Executive 3-Sentence Summary -->
      <div class="grid-2">
        <div class="card">
          <h3 id="scen-title" style="font-size:16px; font-weight:700; margin-bottom:4px;">Scenario Progression Timeline</h3>
          <p id="scen-meta" style="font-size:12px; color:var(--text-muted); margin-bottom:16px;"></p>
          <div class="stage-list" id="scen-stages"></div>

          <h4 style="font-size:13px; font-weight:700; text-transform:uppercase; color:var(--text-secondary); margin:20px 0 10px;">
            Correlated System Logs (Captured Within 5-Minute Window)
          </h4>
          <div id="scen-logs" style="display:flex; flex-direction:column; gap:8px;"></div>
        </div>

        <div class="card">
          <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:14px;">
            <div>
              <h3 style="font-size:16px; font-weight:700;">Gemini 2.5 Flash &mdash; Executive Incident Briefing</h3>
              <p style="font-size:12px; color:var(--text-muted);">Synthesized automatically from metrics, logs, and topology graph</p>
            </div>
            <span class="badge badge-success">Confidence: 99.4%</span>
          </div>

          <div id="scen-rca-cards"></div>

          <div class="card" style="background:var(--bg-subtle); margin-top:16px;">
            <div style="font-size:11px; font-weight:700; text-transform:uppercase; color:var(--text-secondary); margin-bottom:6px;">
              Full-Stack Topology Chain Involved
            </div>
            <div id="scen-chain" style="font-family:'JetBrains Mono', monospace; font-size:12px; color:var(--text-primary);"></div>
          </div>
        </div>
      </div>
    </section>

    <!-- =====================================================================
         STEP 4: ASK "NEURO" CONVERSATIONAL AI ASSISTANT
         ===================================================================== -->
    <section class="step-panel" id="panel-step-4">
      <div class="presenter-banner">
        <div class="presenter-banner-text">
          <span class="presenter-label">Step 4 Presenter Story &bull; Conversational Operations ("Neuro")</span>
          <h2>Ask Plain-English Questions &mdash; Get Verified Answers &amp; Recovery Playbooks</h2>
          <p>
            Click any suggested question below (or type your own). <strong>Neuro</strong> automatically translates your question into a verified BigQuery SQL or ISO GQL Graph query and returns an executive incident summary.
          </p>
        </div>
        <button class="btn" onclick="goToStep(1)">&larr; Restart Guided Tour</button>
      </div>

      <div class="card" style="margin-bottom:20px;">
        <div style="font-size:12px; font-weight:700; color:var(--text-secondary); margin-bottom:8px;">
          One-Click Demo Questions (Click to Ask Neuro):
        </div>
        <div class="pill-row">
          <button class="prompt-pill" onclick="askPreset('What active service degradation is occurring across EBRS manufacturing sites right now?')">
            1. What active incidents are happening across GSK sites right now?
          </button>
          <button class="prompt-pill" onclick="askPreset('Show blast radius for PostgreSQL connection pool exhaustion 500/500 and upstream HTTP 504 timeouts')">
            2. Why is the London EBRS Web Portal failing with HTTP 504 timeouts?
          </button>
          <button class="prompt-pill" onclick="askPreset('Investigate silent host drop to 0.0 and /mnt/gsk_batch storage IO wait in Site_C_Ware')">
            3. Did the Ware batch server silently freeze and drop to 0%?
          </button>
          <button class="prompt-pill" onclick="askPreset('Is CHG0049281 suppressed by ServiceNow maintenance window?')">
            4. Why was no pager alert sent for the Stevenage Oracle DB spike (CHG0049281)?
          </button>
        </div>

        <div class="search-bar">
          <input id="neuro-input" class="search-input" type="text"
            value="Show blast radius for PostgreSQL connection pool exhaustion 500/500 and upstream HTTP 504 timeouts"
            placeholder="Ask Neuro a question about servers, applications, or maintenance windows..." />
          <button class="btn btn-primary" onclick="askNeuro()">Ask Neuro AI</button>
        </div>

        <!-- Executive Briefing Output -->
        <div id="neuro-executive-card"></div>

        <!-- Collapsible Technical Drawer (Hidden by Default so UI stays clean!) -->
        <details class="tech-drawer">
          <summary>View Technical Details (Auto-Generated BigQuery SQL / ISO GQL Query &amp; Raw Records)</summary>
          <pre class="code-block" id="neuro-sql-box">Loading query...</pre>
        </details>
      </div>
    </section>

  </main>

  <script>
    let currentStep = 1;
    let cachedAnomalies = [];
    let cachedTopology = null;

    const stepTitles = [
      "",
      "Smart Baselines & Noise Reduction",
      "Full-Stack Blast Radius Map",
      "3 Real Manufacturing Scenarios",
      "Ask 'Neuro' AI Assistant"
    ];

    function goToStep(stepNum) {
      currentStep = stepNum;
      for (let i = 1; i <= 4; i++) {
        document.getElementById('tab-step-' + i).classList.toggle('active', i === stepNum);
        document.getElementById('panel-step-' + i).classList.toggle('active', i === stepNum);
      }
      document.getElementById('btn-prev-step').disabled = (stepNum === 1);
      const nextBtn = document.getElementById('btn-next-step');
      if (stepNum < 4) {
        nextBtn.textContent = 'Next Step: ' + stepTitles[stepNum + 1] + ' →';
        nextBtn.onclick = () => changeStep(1);
      } else {
        nextBtn.textContent = 'Restart Guided Tour ↺';
        nextBtn.onclick = () => goToStep(1);
      }
      window.scrollTo({ top: 0, behavior: 'smooth' });
    }

    function changeStep(delta) {
      const target = Math.min(4, Math.max(1, currentStep + delta));
      goToStep(target);
    }

    function formatBadge(cls, changeId) {
      if (cls === 'SPIKE_ANOMALY') return '<span class="badge badge-danger">CRITICAL SPIKE BREACH</span>';
      if (cls === 'SILENT_HOST_DROP_TO_ZERO') return '<span class="badge badge-info">SILENT FREEZE (0.0% DROP)</span>';
      if (cls === 'SUPPRESSED_MAINTENANCE_WINDOW') return '<span class="badge badge-warning">SUPPRESSED (' + (changeId || 'CHG0049281') + ')</span>';
      return '<span class="badge badge-success">HEALTHY</span>';
    }

    function renderAnomaliesTable(rows) {
      const tbody = document.getElementById('anomalies-tbody');
      if (!rows || rows.length === 0) {
        tbody.innerHTML = '<tr><td colspan="4">No matching server records.</td></tr>';
        return;
      }
      tbody.innerHTML = rows.slice(0, 7).map(r => `
        <tr>
          <td>
            <strong>${r.hostname}</strong>
            <div style="font-size:11px; color:var(--text-muted);">${r.site_location.replace(/_/g, ' ')} &bull; ${r.system_id}</div>
          </td>
          <td><strong>${Number(r.actual_cpu).toFixed(1)}%</strong></td>
          <td>${Number(r.expected_upper_bound).toFixed(1)}%</td>
          <td>${formatBadge(r.anomaly_classification, r.active_change_id)}</td>
        </tr>
      `).join('');
    }

    async function filterAnomalies(classification) {
      ['ALL', 'SUPPRESSED', 'ZERO'].forEach(k => {
        const el = document.getElementById('filter-card-' + k);
        if (el) el.classList.remove('selected');
      });
      if (classification === 'ALL') document.getElementById('filter-card-ALL').classList.add('selected');
      if (classification === 'SUPPRESSED_MAINTENANCE_WINDOW') document.getElementById('filter-card-SUPPRESSED').classList.add('selected');
      if (classification === 'SILENT_HOST_DROP_TO_ZERO') document.getElementById('filter-card-ZERO').classList.add('selected');

      if (classification === 'ALL') {
        renderAnomaliesTable(cachedAnomalies);
      } else {
        const filtered = cachedAnomalies.filter(r => r.anomaly_classification === classification);
        renderAnomaliesTable(filtered);
      }
    }

    function renderTopologyColumns(highlightIds, bannerText) {
      if (!cachedTopology) return;
      document.getElementById('topo-impact-text').innerHTML = bannerText;
      const byLabel = { Switch: [], Hypervisor: [], Host: [], Application: [] };
      (cachedTopology.nodes || []).forEach(n => {
        if (byLabel[n.label]) byLabel[n.label].push(n);
      });

      const renderCol = (nodes) => nodes.map(n => {
        const isHit = highlightIds.includes(n.id) || highlightIds.includes(n.hostname);
        return `
          <div class="topo-node ${isHit ? 'active-path' : ''}">
            <div style="display:flex; justify-content:space-between; align-items:center;">
              <span class="topo-node-title">${n.hostname || n.id}</span>
              ${isHit ? '<span class="badge badge-danger" style="font-size:10px; padding:1px 6px;">IMPACTED</span>' : ''}
            </div>
            <div class="topo-node-meta">${(n.site_location || n.tier || n.system_id || '').replace(/_/g, ' ')}</div>
          </div>
        `;
      }).join('');

      document.getElementById('topo-col-switch').innerHTML = renderCol(byLabel.Switch);
      document.getElementById('topo-col-hypervisor').innerHTML = renderCol(byLabel.Hypervisor);
      document.getElementById('topo-col-host').innerHTML = renderCol(byLabel.Host);
      document.getElementById('topo-col-app').innerHTML = renderCol(byLabel.Application);
    }

    function highlightTopology(scen) {
      if (scen === 'A') {
        renderTopologyColumns(
          ['sw-core-stv-01', 'esxi-cluster-04', 'srv-b-batch-02', 'app-ebrs-batch', 'EBRS-Batch-Engine', 'app-ebrs-web', 'EBRS-Web-Portal'],
          '<strong>Scenario A Path:</strong> Stevenage Switch (<code>sw-core-stv-01</code>) &rarr; VMware ESXi (<code>esxi-cluster-04</code>) &rarr; Batch Server (<code>srv-b-batch-02</code>) &rarr; <strong>EBRS-Batch-Engine Memory Crash</strong>'
        );
      } else if (scen === 'B') {
        renderTopologyColumns(
          ['sw-core-stv-01', 'esxi-cluster-04', 'srv-b-db-01', 'srv-a-web-01', 'srv-a-web-04', 'app-ebrs-db', 'EBRS-Database-Core', 'app-ebrs-web', 'EBRS-Web-Portal'],
          '<strong>Scenario B Cross-Site Blast Radius:</strong> Stevenage DB Server (<code>srv-b-db-01</code>) connection pool exhaustion (500/500) cascades upstream to London Web Servers (<code>srv-a-web-01</code>, <code>srv-a-web-04</code>) causing HTTP 504 timeouts on <strong>EBRS-Web-Portal</strong>.'
        );
      } else {
        renderTopologyColumns(
          ['sw-core-ware-01', 'aks-prod-ware-02', 'srv-c-ware-01', 'app-mes-sched', 'MES-Batch-Scheduler'],
          '<strong>Scenario C Path:</strong> Ware Switch (<code>sw-core-ware-01</code>) &rarr; AKS Cluster (<code>aks-prod-ware-02</code>) &rarr; Storage Stall on <code>srv-c-ware-01</code> (0.0% Silent Drop) &rarr; <strong>MES-Batch-Scheduler Evicted</strong>.'
        );
      }
    }

    async function loadScenario(id) {
      ['A', 'B', 'C'].forEach(k => {
        const card = document.getElementById('scen-card-' + k);
        if (card) card.classList.toggle('selected', k === id);
      });
      const resp = await fetch('/api/observability/scenarios/' + id);
      const data = await resp.json();
      const scen = data.scenario || data;

      document.getElementById('scen-title').textContent = scen.title;
      document.getElementById('scen-meta').textContent =
        `Primary Server: ${scen.primary_host} | Location: ${scen.site_location.replace(/_/g, ' ')} | Impacted App: ${scen.impacted_application}`;

      document.getElementById('scen-stages').innerHTML = (scen.stages || []).map((st, idx) => `
        <div class="stage-item stage-${idx + 1}">
          <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:4px;">
            <strong style="font-size:14px;">${st.name}</strong>
            <span style="font-size:11px; color:var(--text-muted); font-family:'JetBrains Mono', monospace;">${st.timestamp}</span>
          </div>
          <p style="font-size:13px; color:var(--text-secondary);">${st.description}</p>
        </div>
      `).join('');

      document.getElementById('scen-logs').innerHTML = (scen.correlated_logs || []).map(l => `
        <div style="background:#0f172a; color:#f8fafc; padding:10px 12px; border-radius:8px; font-family:'JetBrains Mono', monospace; font-size:11px;">
          <span style="color:#f87171; font-weight:700;">[${l.severity}]</span>
          <span style="color:#94a3b8;"> ${l.timestamp} (${l.hostname}):</span>
          <div style="margin-top:3px; color:#e2e8f0;">${l.message}</div>
        </div>
      `).join('');

      const rcaText = (scen.incident_root_cause_analysis && scen.incident_root_cause_analysis.gemini_root_cause_analysis) || '';
      const parts = rcaText.split(/(?=\d\.\s)/).map(s => s.trim()).filter(Boolean);
      const titles = ['1. Immediate Root Cause', '2. Business & Service Blast Radius', '3. Recommended Recovery Action'];
      document.getElementById('scen-rca-cards').innerHTML = parts.map((p, i) => `
        <div class="rca-point">
          <div class="rca-point-label">${titles[i] || 'AI Finding'}</div>
          <div style="font-size:13px; color:var(--text-primary);">${p.replace(/^\d\.\s*[^:]+:\s*/i, '')}</div>
        </div>
      `).join('');

      document.getElementById('scen-chain').textContent = scen.gql_topology_chain || '';
    }

    function askPreset(qText) {
      document.getElementById('neuro-input').value = qText;
      askNeuro();
    }

    async function askNeuro() {
      const q = document.getElementById('neuro-input').value;
      const resp = await fetch('/api/neuro/chat', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({question: q})
      });
      const data = await resp.json();
      const ex = data.executive_summary || {};
      const rcaParts = (ex.three_sentence_rca || '').split(/(?=\d\.\s)/).map(s => s.trim()).filter(Boolean);

      document.getElementById('neuro-executive-card').innerHTML = `
        <div style="border-top:1px solid var(--border-light); padding-top:18px;">
          <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:14px;">
            <div>
              <span class="badge badge-info">Neuro AI Verified Briefing (${data.query_type})</span>
              <h3 style="font-size:16px; font-weight:700; margin-top:6px;">Question: "${data.question}"</h3>
            </div>
            <span class="badge badge-success">${data.row_count || 0} Live Records Matched</span>
          </div>

          <div class="grid-3" style="margin-bottom:16px;">
            <div class="card" style="background:var(--bg-subtle); padding:14px;">
              <div style="font-size:11px; font-weight:700; text-transform:uppercase; color:var(--text-muted);">Impacted Server(s)</div>
              <div style="font-size:14px; font-weight:700; margin-top:4px;">${ex.hostname || 'N/A'}</div>
              <div style="font-size:12px; color:var(--text-secondary); margin-top:2px;">Site: ${ex.physical_site || 'N/A'}</div>
            </div>
            <div class="card" style="background:var(--bg-subtle); padding:14px;">
              <div style="font-size:11px; font-weight:700; text-transform:uppercase; color:var(--text-muted);">Impacted Manufacturing System</div>
              <div style="font-size:14px; font-weight:700; margin-top:4px;">${ex.impacted_application || 'N/A'}</div>
            </div>
            <div class="card" style="background:var(--bg-subtle); padding:14px;">
              <div style="font-size:11px; font-weight:700; text-transform:uppercase; color:var(--text-muted);">Measured Telemetry</div>
              <div style="font-size:13px; font-weight:700; color:var(--danger); margin-top:4px;">${ex.measured_utilization || 'N/A'}</div>
            </div>
          </div>

          <div class="grid-2" style="margin-bottom:12px;">
            <div>
              <h4 style="font-size:13px; font-weight:700; text-transform:uppercase; color:var(--text-secondary); margin-bottom:8px;">
                Plain-English Root Cause Summary (Gemini 2.5 Flash)
              </h4>
              ${rcaParts.map(p => `
                <div class="rca-point" style="margin-bottom:8px;">
                  <div style="font-size:13px; color:var(--text-primary);">${p}</div>
                </div>
              `).join('')}
            </div>
            <div>
              <h4 style="font-size:13px; font-weight:700; text-transform:uppercase; color:var(--text-secondary); margin-bottom:8px;">
                Actionable SRE Recovery Playbook
              </h4>
              <div class="card" style="background:#ecfdf5; border-color:#a7f3d0;">
                <div style="font-size:13px; font-weight:600; color:#065f46; line-height:1.6;">
                  ${ex.remediation_runbook || ''}
                </div>
              </div>
            </div>
          </div>
        </div>
      `;

      document.getElementById('neuro-sql-box').textContent =
        `-- Auto-Generated ${data.query_type} Query (Intent: ${data.intent})\n` +
        (data.generated_query || '') +
        `\n\n-- Matched Records (${data.row_count}):\n` +
        JSON.stringify(data.rows || [], null, 2);
    }

    async function initWorkbench() {
      const anomResp = await fetch('/api/observability/anomalies');
      const anomData = await anomResp.json();
      cachedAnomalies = anomData.anomalies || [];
      renderAnomaliesTable(cachedAnomalies);

      const topoResp = await fetch('/api/observability/topology');
      cachedTopology = await topoResp.json();
      highlightTopology('B');

      await loadScenario('A');
      await askNeuro();
    }

    initWorkbench();
  </script>
</body>
</html>
"""


class ObservabilityMVPRequestHandler(BaseHTTPRequestHandler):
  """HTTP Request Handler for standalone `observability_mvp_server.py`."""

  mvp_state: ObservabilityMVPState

  def log_message(self, format_str: str, *args: Any) -> None:
    return

  def _send_json(self, status_code: int, payload: dict[str, Any]) -> None:
    body = json.dumps(payload, indent=2).encode("utf-8")
    self.send_response(status_code)
    self.send_header("Content-Type", "application/json; charset=utf-8")
    self.send_header("Content-Length", str(len(body)))
    self.send_header("Cache-Control", "no-store")
    self.end_headers()
    self.wfile.write(body)

  def _send_html(self, status_code: int, html_text: str) -> None:
    body = html_text.encode("utf-8")
    self.send_response(status_code)
    self.send_header("Content-Type", "text/html; charset=utf-8")
    self.send_header("Content-Length", str(len(body)))
    self.end_headers()
    self.wfile.write(body)

  def do_GET(self) -> None:  # pylint: disable=invalid-name
    parsed = urlparse(self.path)
    path = parsed.path.rstrip("/") or "/"
    query_params = parse_qs(parsed.query)

    if path in ("/", "/observability"):
      self._send_html(200, OBSERVABILITY_WORKBENCH_HTML)
      return

    if path in ("/api/observability/status", "/api/status"):
      self._send_json(200, self.mvp_state.get_observability_status())
      return

    if path in ("/api/observability/anomalies", "/api/anomalies"):
      host_f = query_params.get("hostname", [None])[0]
      cls_f = query_params.get("classification", [None])[0]
      self._send_json(
          200,
          self.mvp_state.get_anomalies_payload(
              hostname_filter=host_f,
              classification_filter=cls_f,
          ),
      )
      return

    if path in (
        "/api/observability/topology",
        "/api/observability/gql_topology",
        "/api/topology",
    ):
      self._send_json(200, self.mvp_state.get_topology_payload())
      return

    if path in (
        "/api/observability/scenarios",
        "/api/observability/cascading_scenarios",
    ):
      scen_f = query_params.get("scenario", [None])[0]
      self._send_json(200, self.mvp_state.get_scenarios_payload(scen_f))
      return

    if path.startswith("/api/observability/scenarios/"):
      scen_id = path.split("/api/observability/scenarios/", 1)[1]
      try:
        self._send_json(200, self.mvp_state.get_scenarios_payload(scen_id))
      except ValueError as exc:
        self._send_json(400, {"status": "ERROR", "message": str(exc)})
      return

    if path == "/api/neuro/chat":
      q = query_params.get("question", [query_params.get("q", [""])[0]])[0]
      self._send_json(200, self.mvp_state.handle_neuro_chat(q))
      return

    self._send_json(404, {"status": "ERROR", "message": f"Not found: {path}"})

  def do_POST(self) -> None:  # pylint: disable=invalid-name
    content_len = int(self.headers.get("Content-Length", "0") or "0")
    raw_body = self.rfile.read(content_len) if content_len > 0 else b"{}"
    try:
      body_json = json.loads(raw_body.decode("utf-8") or "{}")
    except Exception:  # pylint: disable=broad-except
      body_json = {}

    parsed = urlparse(self.path)
    path = parsed.path.rstrip("/")

    if path == "/api/neuro/chat":
      question = (
          body_json.get("question")
          or body_json.get("prompt")
          or body_json.get("query")
          or body_json.get("message")
          or ""
      )
      self._send_json(200, self.mvp_state.handle_neuro_chat(str(question)))
      return

    if path.startswith("/api/observability/scenarios/"):
      scen_id = path.split("/api/observability/scenarios/", 1)[1]
      try:
        self._send_json(200, self.mvp_state.get_scenarios_payload(scen_id))
      except ValueError as exc:
        self._send_json(400, {"status": "ERROR", "message": str(exc)})
      return

    if path in (
        "/api/observability/scenarios",
        "/api/observability/cascading_scenarios",
    ):
      scen_id = body_json.get("scenario_id") or body_json.get("scenario")
      try:
        self._send_json(200, self.mvp_state.get_scenarios_payload(scen_id))
      except ValueError as exc:
        self._send_json(400, {"status": "ERROR", "message": str(exc)})
      return

    self._send_json(404, {"status": "ERROR", "message": f"Not found: {path}"})


def create_observability_mvp_server(
    host: str = "127.0.0.1",
    port: int = 0,
    project_id: str = DEFAULT_PROJECT_ID,
    dataset_id: str = DEFAULT_DATASET_ID,
    mirror_path: Optional[str] = None,
    live_bq: bool = False,
) -> ThreadingHTTPServer:
  """Factory creating a bound ThreadingHTTPServer for the GSK Observability MVP."""
  state = ObservabilityMVPState(
      project_id=project_id,
      dataset_id=dataset_id,
      mirror_path=mirror_path,
      live_bq=live_bq,
  )

  class BoundMVPRequestHandler(ObservabilityMVPRequestHandler):
    mvp_state = state

  return ThreadingHTTPServer((host, port), BoundMVPRequestHandler)


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
  parser = argparse.ArgumentParser(
      description="GSK Enterprise Observability Platform ('Neuro') — MVP Workbench Server & CLI"
  )
  parser.add_argument("--host", default="0.0.0.0", help="Bind host address (default: 0.0.0.0)")
  parser.add_argument("--port", type=int, default=8085, help="Bind TCP port")
  parser.add_argument(
      "--project",
      default=DEFAULT_PROJECT_ID,
      help=f"Target GCP Project ID (default: {DEFAULT_PROJECT_ID})",
  )
  parser.add_argument(
      "--dataset",
      default=DEFAULT_DATASET_ID,
      help=f"Target BigQuery Dataset ID (default: {DEFAULT_DATASET_ID})",
  )
  parser.add_argument(
      "--mirror-path",
      default=None,
      help="Optional path to local SQLite mirror DB",
  )
  parser.add_argument(
      "--live-bq",
      action="store_true",
      help="Attempt live BigQuery execution before mirror fallback",
  )
  parser.add_argument(
      "--verify",
      "--self-test",
      dest="verify",
      action="store_true",
      help="Run self-verification of all MVP & Neuro endpoints and exit",
  )
  parser.add_argument(
      "--query",
      default=None,
      help="Run a single natural-language SRE question through GSK 'Neuro' CLI and exit",
  )
  return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
  args = parse_args(argv)
  state = ObservabilityMVPState(
      project_id=args.project,
      dataset_id=args.dataset,
      mirror_path=args.mirror_path,
      live_bq=args.live_bq,
  )

  if args.query:
    res = state.handle_neuro_chat(args.query)
    print(json.dumps(res, indent=2))
    return 0

  if args.verify:
    status_res = state.get_observability_status()
    anom_res = state.get_anomalies_payload()
    topo_res = state.get_topology_payload()
    scen_res = state.get_scenarios_payload()
    neuro_sql = state.handle_neuro_chat(
        "What active service degradation is occurring on srv-b-batch-02?"
    )
    neuro_gql = state.handle_neuro_chat(
        "Trace the ISO GQL blast radius for PostgreSQL 500/500 saturation and HTTP 504 timeouts."
    )
    assert status_res["status"] == "HEALTHY"
    assert len(anom_res["anomalies"]) >= 5
    assert len(topo_res["graph_table_paths"]) >= 3
    assert set(scen_res["scenarios"].keys()) == {"A", "B", "C"}
    assert neuro_sql["query_type"] == "SQL"
    assert neuro_gql["query_type"] == "ISO_GQL"
    print(
        json.dumps(
            {
                "verification": "PASSED",
                "project_id": args.project,
                "dataset_id": args.dataset,
                "tables_verified": len(status_res["table_counts"]),
                "anomaly_points": len(anom_res["anomalies"]),
                "gql_paths": len(topo_res["graph_table_paths"]),
                "scenarios": list(scen_res["scenarios"].keys()),
                "neuro_intents_verified": [
                    neuro_sql["intent"],
                    neuro_gql["intent"],
                ],
            },
            indent=2,
        )
    )
    return 0

  server = create_observability_mvp_server(
      host=args.host,
      port=args.port,
      project_id=args.project,
      dataset_id=args.dataset,
      mirror_path=args.mirror_path,
      live_bq=args.live_bq,
  )
  actual_host, actual_port = server.server_address
  print("=" * 88)
  print("GSK ENTERPRISE OBSERVABILITY PLATFORM ('NEURO') — MVP WORKBENCH")
  print(f"Listening on: http://{actual_host}:{actual_port}")
  print(f"Target BigQuery Dataset: {args.project}.{args.dataset}")
  print("=" * 88)
  try:
    server.serve_forever()
  except KeyboardInterrupt:
    pass
  finally:
    server.shutdown()
    server.server_close()
  return 0


if __name__ == "__main__":
  sys.exit(main())
