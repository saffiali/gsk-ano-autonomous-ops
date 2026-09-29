#!/usr/bin/env python3
"""GSK Enterprise Observability Platform — EBRS Multi-Site & Cascading Failure Seeder.

Seeds realistic pharmaceutical manufacturing telemetry across `Site_A_London`,
`Site_B_Stevenage`, and `Site_C_Ware` (`EBRS`, `LIMS`, `MES_BATCH`; `Web_Frontend`,
`Database_Backend`, `Batch_Processing`) into both:
1. Live Google Cloud BigQuery dataset `gke-demos-363017.gsk_observability_demo` (`EU`)
2. Local SQLite analytical mirror `.cache/gsk_observability_mvp_mirror.db`

Covers all 14 canonical lakehouse & ISO GQL tables:
- Core Lakehouse Tables:
  1. `enterprise_telemetry_partitioned`
  2. `system_logs`
  3. `servicenow_maintenance_windows`
  4. `incident_root_cause_analysis`
  5. `structured_log_entities`
- 10-Step ISO GQL Property Graph Tables (`gsk_infrastructure_dependency_graph`):
  6. `nodes_switches`
  7. `nodes_hypervisors`
  8. `nodes_hosts`
  9. `nodes_applications`
  10. `edges_connected_to`
  11. `edges_hosts_vm`
  12. `edges_runs_app`
  13. `edges_app_communicates`
  14. `edges_network_flows`

Implements the 3 Multi-Stage Cascading Failure Waterfalls from Blueprint Section 7:
- Scenario A: JVM Memory Leak & OOM Collapse (`srv-b-batch-02` heap creep
  `55% -> 75% -> 92% -> 98.8%` + `FATAL: java.lang.OutOfMemoryError: Java heap space`).
- Scenario B: Database Saturation & Web Tier Blast Radius (`srv-b-db-01` /
  `srv-b-batch-02` `PostgreSQL connection pool exhausted 500/500` & deadlock on
  `clinical_trial_records` cascading to `srv-a-web-01` & `srv-a-web-04` HTTP 504
  timeouts across `sw-core-stv-01 -> esxi-cluster-04 -> Host -> Application`).
- Scenario C: Storage IO Saturation & Silent Host Drop to `0.0` (`/mnt/gsk_batch`
  on `srv-c-batch-03` IO wait `545ms -> 965ms` (`>940ms`) followed by heartbeat
  drop to `0.0` caught by `GAP_FILL()` as `SILENT_HOST_DROP_TO_ZERO`).
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import logging
import math
from pathlib import Path
import sqlite3
import subprocess
import sys
from typing import Any, Optional
import urllib.error
import urllib.request

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
  sys.path.insert(0, str(PROJECT_ROOT))

try:
  import google.auth  # type: ignore
except ImportError:  # pragma: no cover
  google = None  # type: ignore

try:
  from google.cloud import bigquery  # type: ignore
except ImportError:  # pragma: no cover
  bigquery = None  # type: ignore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("gsk_ebrs_seeder")

DEFAULT_PROJECT_ID = "gke-demos-363017"
DEFAULT_DATASET_ID = "gsk_observability_demo"
DEFAULT_LOCATION = "EU"
DEFAULT_MIRROR_PATH = PROJECT_ROOT / ".cache" / "gsk_observability_mvp_mirror.db"

REQUIRED_OBSERVABILITY_TABLES: list[str] = [
    "enterprise_telemetry_partitioned",
    "system_logs",
    "servicenow_maintenance_windows",
    "nodes_switches",
    "nodes_hypervisors",
    "nodes_hosts",
    "nodes_applications",
    "edges_connected_to",
    "edges_hosts_vm",
    "edges_runs_app",
    "edges_app_communicates",
    "edges_network_flows",
    "incident_root_cause_analysis",
    "structured_log_entities",
]


def _iso_utc(dt: datetime) -> str:
  """Formats a datetime object as an ISO-8601 UTC string."""
  return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_cascading_scenarios_catalog() -> dict[str, dict[str, Any]]:
  """Returns structured metadata for Cascading Failure Scenarios A, B, and C."""
  return {
      "A": {
          "scenario_id": "A",
          "name": "Scenario A: JVM Memory Leak & OOM Collapse (Batch Pipeline)",
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
                  "title": "Stage 1 (Creep)",
                  "description": (
                      "Spark genomics worker on srv-b-batch-02 leaks heap objects; "
                      "memory creeps 55.0% -> 75.0% -> 92.0% with GC pause times exceeding 1,200ms."
                  ),
                  "metrics": {"memory_usage_pct": [55.0, 75.0, 92.0], "gc_pause_ms": 1240},
              },
              {
                  "stage": 2,
                  "title": "Stage 2 (Breach & Early Warning)",
                  "description": (
                      "Heap utilization reaches 98.8% (CPU 94.5%), breaching ARIMA_PLUS "
                      "dynamic upper bound (P = 0.994). Predictive alert fires before crash."
                  ),
                  "metrics": {"memory_usage_pct": 98.8, "cpu_usage_pct": 94.5, "probability": 0.994},
              },
              {
                  "stage": 3,
                  "title": "Stage 3 (Process Termination)",
                  "description": (
                      "JVM throws FATAL: java.lang.OutOfMemoryError: Java heap space; "
                      "Linux OOM killer terminates pid 18422 with exit code 137."
                  ),
                  "log_signature": "FATAL: java.lang.OutOfMemoryError: Java heap space",
              },
          ],
          "gemini_rca_summary": (
              "1. Immediate root cause: Unbounded Spark genomics batch object retention on "
              "srv-b-batch-02 drove JVM Tenured Generation heap utilization from 55.0% to 98.8% "
              "(CPU 94.5%), culminating in java.lang.OutOfMemoryError: Java heap space and Linux "
              "OOM killer termination (exit code 137). "
              "2. Downstream service blast radius: EBRS-Batch-Engine electronic batch record "
              "validation jobs at Site_B_Stevenage are stalled, delaying upstream EBRS-Web-Portal "
              "batch sign-off workflows and MES-Batch-Scheduler dispatches. "
              "3. Immediate corrective action: Drain active Spark batch partitions on srv-b-batch-02, "
              "restart EBRS-Batch-Engine with -XX:+UseG1GC and -XX:MaxRAMPercentage=75.0, and replay "
              "checkpointed batch records from the last committed offset."
          ),
      },
      "B": {
          "scenario_id": "B",
          "name": "Scenario B: Database Saturation & Web Tier Blast Radius",
          "primary_host": "srv-b-db-01",
          "secondary_hosts": ["srv-b-batch-02", "srv-a-web-01", "srv-a-web-04"],
          "site_location": "Site_B_Stevenage -> Site_A_London",
          "system_id": "EBRS",
          "application_tier": "Database_Backend -> Web_Frontend",
          "impacted_application": "EBRS-Database-Core -> EBRS-Web-Portal",
          "anomaly_classification": "SPIKE_ANOMALY",
          "anomaly_probability": 0.997,
          "topology_path": (
              "sw-core-stv-01 (Switch) -> esxi-cluster-04 (Hypervisor) -> "
              "srv-b-db-01 / srv-b-batch-02 (Host) -> EBRS-Database-Core / EBRS-Batch-Engine "
              "(Application) <- EBRS-Web-Portal (srv-a-web-01, srv-a-web-04)"
          ),
          "stages": [
              {
                  "stage": 1,
                  "title": "Stage 1 (Load Surge)",
                  "description": (
                      "CPU utilization on srv-b-db-01 exceeds 95% (96.4%); active connection "
                      "pool surges to 350/500 with lock queues accumulating."
                  ),
                  "metrics": {"cpu_usage_pct": 96.4, "active_connections": 350},
              },
              {
                  "stage": 2,
                  "title": "Stage 2 (Deadlock & Exhaustion)",
                  "description": (
                      "Connection pool hits 500/500 (CRITICAL: PostgreSQL connection pool "
                      "exhausted 500/500) with write deadlocks on relation clinical_trial_records."
                  ),
                  "metrics": {"cpu_usage_pct": 98.2, "active_connections": 500},
                  "log_signature": "CRITICAL: PostgreSQL connection pool exhausted 500/500",
              },
              {
                  "stage": 3,
                  "title": "Stage 3 (Blast Radius Cascade)",
                  "description": (
                      "Connected Web Frontend nodes srv-a-web-01 and srv-a-web-04 in Site_A_London "
                      "exhaust thread pools, emitting ERROR: HTTP 504 Gateway Timeout while contacting backend database."
                  ),
                  "log_signature": "ERROR: HTTP 504 Gateway Timeout while contacting backend database",
              },
          ],
          "gemini_rca_summary": (
              "1. Immediate root cause: Unindexed concurrent batch updates on relation "
              "clinical_trial_records caused write deadlocks on srv-b-db-01 (CPU 98.2%), completely "
              "exhausting the PostgreSQL connection pool at 500/500 active connections. "
              "2. Downstream service blast radius: ISO GQL traversal from sw-core-stv-01 and "
              "esxi-cluster-04 confirms that saturation of EBRS-Database-Core cascaded upstream "
              "to EBRS-Web-Portal frontend nodes srv-a-web-01 and srv-a-web-04 in Site_A_London, "
              "triggering thread pool exhaustion and HTTP 504 Gateway Timeouts. "
              "3. Immediate corrective action: Terminate blocking PostgreSQL PIDs holding locks "
              "on clinical_trial_records via pg_terminate_backend(), route read queries through "
              "PgBouncer connection pooling, and throttle concurrent EBRS batch writers."
          ),
      },
      "C": {
          "scenario_id": "C",
          "name": "Scenario C: Storage IO Saturation & Silent Host Drop to 0.0",
          "primary_host": "srv-c-batch-03",
          "site_location": "Site_C_Ware",
          "system_id": "MES_BATCH",
          "application_tier": "Batch_Processing",
          "impacted_application": "MES-Batch-Scheduler",
          "anomaly_classification": "SILENT_HOST_DROP_TO_ZERO",
          "anomaly_probability": 0.998,
          "stages": [
              {
                  "stage": 1,
                  "title": "Stage 1 (IO Saturation)",
                  "description": (
                      "Shared NFS/SAN storage mount /mnt/gsk_batch on srv-c-batch-03 reaches "
                      "IOPS saturation; IO wait reaches 545.0ms (> 520ms threshold)."
                  ),
                  "metrics": {"io_wait_ms": 545.0, "cpu_usage_pct": 74.2},
              },
              {
                  "stage": 2,
                  "title": "Stage 2 (Storage Stall, Eviction & Silent Drop to 0.0)",
                  "description": (
                      "IO wait spikes to 965.0ms (> 940ms), blocking the node heartbeat daemon. "
                      "Cluster orchestrator evicts srv-c-batch-03 and telemetry drops to 0.0 "
                      "(STATUS='DOWN'), caught by GAP_FILL() + ARIMA_PLUS as SILENT_HOST_DROP_TO_ZERO."
                  ),
                  "metrics": {"io_wait_ms": 965.0, "cpu_usage_pct": 0.0, "status": "DOWN"},
                  "log_signature": "CRITICAL: Storage mount /mnt/gsk_batch stalled (io_wait=965ms); node heartbeat daemon blocked",
              },
          ],
          "gemini_rca_summary": (
              "1. Immediate root cause: Severe IOPS saturation on shared storage mount "
              "/mnt/gsk_batch drove disk IO wait on srv-c-batch-03 from 545.0ms to 965.0ms "
              "(>940ms), stalling kernel I/O threads and blocking the node heartbeat daemon. "
              "2. Downstream service blast radius: The cluster orchestrator evicted srv-c-batch-03 "
              "at Site_C_Ware, causing a silent telemetry drop to 0.0 CPU (classified via GAP_FILL "
              "as SILENT_HOST_DROP_TO_ZERO) and halting MES-Batch-Scheduler execution. "
              "3. Immediate corrective action: Unmount and fail over /mnt/gsk_batch to the redundant "
              "storage controller in Site_C_Ware, uncordon srv-c-batch-03 once IO wait falls below "
              "25ms, and reschedule evicted MES_BATCH workloads."
          ),
      },
  }


def build_ebrs_seed_dataset(
    reference_time: Optional[datetime] = None,
) -> dict[str, list[dict[str, Any]]]:
  """Generates deterministic rows for all 14 observability tables + 9 CMDB staging tables.

  Args:
    reference_time: Optional UTC datetime anchor (defaults to current UTC minute).

  Returns:
    Dictionary mapping table names to lists of row dictionaries.
  """
  now_utc = (
      reference_time.astimezone(timezone.utc)
      if reference_time is not None
      else datetime.now(timezone.utc)
  ).replace(second=0, microsecond=0)

  # -------------------------------------------------------------------------
  # 1. Topology Node Tables (Steps 1 - 4 of Property Graph)
  # -------------------------------------------------------------------------
  nodes_switches = [
      {
          "switch_id": "sw-core-stv-01",
          "hostname": "sw-core-stv-01",
          "site_location": "Site_B_Stevenage",
          "management_ip": "10.44.1.10",
          "model": "Cisco Nexus 9336C-FX2",
      },
      {
          "switch_id": "sw-core-lon-01",
          "hostname": "sw-core-lon-01",
          "site_location": "Site_A_London",
          "management_ip": "10.40.1.10",
          "model": "Cisco Nexus 9504",
      },
      {
          "switch_id": "sw-core-war-01",
          "hostname": "sw-core-war-01",
          "site_location": "Site_C_Ware",
          "management_ip": "10.48.1.10",
          "model": "Arista 7280R3",
      },
  ]

  nodes_hypervisors = [
      {
          "hypervisor_id": "esxi-cluster-04",
          "hostname": "esxi-cluster-04",
          "site_location": "Site_B_Stevenage",
          "cluster_name": "onprem-esxi-cluster-04",
          "esxi_version": "VMware ESXi 8.0 U2",
      },
      {
          "hypervisor_id": "esxi-cluster-lon-01",
          "hostname": "esxi-cluster-lon-01",
          "site_location": "Site_A_London",
          "cluster_name": "gke-prod-lon-01",
          "esxi_version": "VMware ESXi 8.0 U2",
      },
      {
          "hypervisor_id": "esxi-cluster-war-02",
          "hostname": "esxi-cluster-war-02",
          "site_location": "Site_C_Ware",
          "cluster_name": "aks-prod-ware-02",
          "esxi_version": "VMware ESXi 8.0 U1",
      },
  ]

  nodes_hosts = [
      {
          "host_id": "srv-b-batch-02",
          "hostname": "srv-b-batch-02",
          "site_location": "Site_B_Stevenage",
          "application_type": "Batch_Processing",
          "operating_system": "RHEL 9.3",
      },
      {
          "host_id": "srv-b-db-01",
          "hostname": "srv-b-db-01",
          "site_location": "Site_B_Stevenage",
          "application_type": "Database_Backend",
          "operating_system": "RHEL 9.3",
      },
      {
          "host_id": "srv-a-web-01",
          "hostname": "srv-a-web-01",
          "site_location": "Site_A_London",
          "application_type": "Web_Frontend",
          "operating_system": "RHEL 9.3",
      },
      {
          "host_id": "srv-a-web-04",
          "hostname": "srv-a-web-04",
          "site_location": "Site_A_London",
          "application_type": "Web_Frontend",
          "operating_system": "RHEL 9.3",
      },
      {
          "host_id": "srv-a-web-02",
          "hostname": "srv-a-web-02",
          "site_location": "Site_A_London",
          "application_type": "Web_Frontend",
          "operating_system": "RHEL 9.3",
      },
      {
          "host_id": "srv-c-batch-03",
          "hostname": "srv-c-batch-03",
          "site_location": "Site_C_Ware",
          "application_type": "Batch_Processing",
          "operating_system": "RHEL 9.2",
      },
      {
          "host_id": "srv-c-lims-01",
          "hostname": "srv-c-lims-01",
          "site_location": "Site_C_Ware",
          "application_type": "Database_Backend",
          "operating_system": "RHEL 9.2",
      },
  ]

  nodes_applications = [
      {
          "app_id": "app-ebrs-batch",
          "name": "EBRS-Batch-Engine",
          "tier": "Batch",
          "system_id": "EBRS",
          "criticality": "Tier-1-GXP",
      },
      {
          "app_id": "app-ebrs-db",
          "name": "EBRS-Database-Core",
          "tier": "Database",
          "system_id": "EBRS",
          "criticality": "Tier-1-GXP",
      },
      {
          "app_id": "app-ebrs-web",
          "name": "EBRS-Web-Portal",
          "tier": "Frontend",
          "system_id": "EBRS",
          "criticality": "Tier-1-GXP",
      },
      {
          "app_id": "app-lims-api",
          "name": "LIMS-Core-API",
          "tier": "Backend",
          "system_id": "LIMS",
          "criticality": "Tier-1-GXP",
      },
      {
          "app_id": "app-mes-sched",
          "name": "MES-Batch-Scheduler",
          "tier": "Batch",
          "system_id": "MES_BATCH",
          "criticality": "Tier-1-GXP",
      },
  ]

  # -------------------------------------------------------------------------
  # 2. Topology Edge Tables (Steps 5 - 9 of Property Graph)
  # -------------------------------------------------------------------------
  edges_connected_to = [
      {
          "edge_id": "edge-conn-stv-01",
          "switch_id": "sw-core-stv-01",
          "hypervisor_id": "esxi-cluster-04",
          "port_name": "Eth1/1",
          "speed_gbps": 100,
      },
      {
          "edge_id": "edge-conn-lon-01",
          "switch_id": "sw-core-lon-01",
          "hypervisor_id": "esxi-cluster-lon-01",
          "port_name": "Eth1/1",
          "speed_gbps": 100,
      },
      {
          "edge_id": "edge-conn-war-01",
          "switch_id": "sw-core-war-01",
          "hypervisor_id": "esxi-cluster-war-02",
          "port_name": "Eth1/2",
          "speed_gbps": 40,
      },
  ]

  edges_hosts_vm = [
      {
          "edge_id": "edge-vm-stv-batch-02",
          "hypervisor_id": "esxi-cluster-04",
          "host_id": "srv-b-batch-02",
          "allocated_vcpus": 32,
          "allocated_ram_gb": 128,
      },
      {
          "edge_id": "edge-vm-stv-db-01",
          "hypervisor_id": "esxi-cluster-04",
          "host_id": "srv-b-db-01",
          "allocated_vcpus": 64,
          "allocated_ram_gb": 256,
      },
      {
          "edge_id": "edge-vm-lon-web-01",
          "hypervisor_id": "esxi-cluster-lon-01",
          "host_id": "srv-a-web-01",
          "allocated_vcpus": 16,
          "allocated_ram_gb": 64,
      },
      {
          "edge_id": "edge-vm-lon-web-04",
          "hypervisor_id": "esxi-cluster-lon-01",
          "host_id": "srv-a-web-04",
          "allocated_vcpus": 16,
          "allocated_ram_gb": 64,
      },
      {
          "edge_id": "edge-vm-lon-web-02",
          "hypervisor_id": "esxi-cluster-lon-01",
          "host_id": "srv-a-web-02",
          "allocated_vcpus": 16,
          "allocated_ram_gb": 64,
      },
      {
          "edge_id": "edge-vm-war-batch-03",
          "hypervisor_id": "esxi-cluster-war-02",
          "host_id": "srv-c-batch-03",
          "allocated_vcpus": 32,
          "allocated_ram_gb": 128,
      },
      {
          "edge_id": "edge-vm-war-lims-01",
          "hypervisor_id": "esxi-cluster-war-02",
          "host_id": "srv-c-lims-01",
          "allocated_vcpus": 16,
          "allocated_ram_gb": 64,
      },
  ]

  edges_runs_app = [
      {
          "edge_id": "edge-run-b-batch-02",
          "host_id": "srv-b-batch-02",
          "app_id": "app-ebrs-batch",
          "process_id": 18422,
          "listen_port": 8443,
      },
      {
          "edge_id": "edge-run-b-db-01",
          "host_id": "srv-b-db-01",
          "app_id": "app-ebrs-db",
          "process_id": 29410,
          "listen_port": 5432,
      },
      {
          "edge_id": "edge-run-a-web-01",
          "host_id": "srv-a-web-01",
          "app_id": "app-ebrs-web",
          "process_id": 11040,
          "listen_port": 443,
      },
      {
          "edge_id": "edge-run-a-web-04",
          "host_id": "srv-a-web-04",
          "app_id": "app-ebrs-web",
          "process_id": 11088,
          "listen_port": 443,
      },
      {
          "edge_id": "edge-run-a-web-02",
          "host_id": "srv-a-web-02",
          "app_id": "app-ebrs-web",
          "process_id": 11055,
          "listen_port": 443,
      },
      {
          "edge_id": "edge-run-c-batch-03",
          "host_id": "srv-c-batch-03",
          "app_id": "app-mes-sched",
          "process_id": 22190,
          "listen_port": 9090,
      },
      {
          "edge_id": "edge-run-c-lims-01",
          "host_id": "srv-c-lims-01",
          "app_id": "app-lims-api",
          "process_id": 15320,
          "listen_port": 8080,
      },
  ]

  edges_app_communicates = [
      {
          "edge_id": "edge-appcomm-web-to-batch",
          "source_app_id": "app-ebrs-web",
          "target_app_id": "app-ebrs-batch",
          "protocol": "gRPC/TLS",
          "avg_latency_ms": 142.5,
      },
      {
          "edge_id": "edge-appcomm-web-to-db",
          "source_app_id": "app-ebrs-web",
          "target_app_id": "app-ebrs-db",
          "protocol": "PostgreSQL/TLS",
          "avg_latency_ms": 850.0,
      },
      {
          "edge_id": "edge-appcomm-batch-to-db",
          "source_app_id": "app-ebrs-batch",
          "target_app_id": "app-ebrs-db",
          "protocol": "PostgreSQL/TLS",
          "avg_latency_ms": 620.0,
      },
      {
          "edge_id": "edge-appcomm-lims-to-db",
          "source_app_id": "app-lims-api",
          "target_app_id": "app-ebrs-db",
          "protocol": "HTTPS/REST",
          "avg_latency_ms": 95.4,
      },
      {
          "edge_id": "edge-appcomm-mes-to-batch",
          "source_app_id": "app-mes-sched",
          "target_app_id": "app-ebrs-batch",
          "protocol": "gRPC/TLS",
          "avg_latency_ms": 310.0,
      },
  ]

  edges_network_flows = [
      {
          "edge_id": "edge-flow-web01-db01",
          "source_host_id": "srv-a-web-01",
          "destination_host_id": "srv-b-db-01",
          "avg_traffic": 14850000.0,
      },
      {
          "edge_id": "edge-flow-web04-db01",
          "source_host_id": "srv-a-web-04",
          "destination_host_id": "srv-b-db-01",
          "avg_traffic": 13920000.0,
      },
      {
          "edge_id": "edge-flow-web01-batch02",
          "source_host_id": "srv-a-web-01",
          "destination_host_id": "srv-b-batch-02",
          "avg_traffic": 9450000.0,
      },
      {
          "edge_id": "edge-flow-web04-batch02",
          "source_host_id": "srv-a-web-04",
          "destination_host_id": "srv-b-batch-02",
          "avg_traffic": 8820000.0,
      },
      {
          "edge_id": "edge-flow-batch02-db01",
          "source_host_id": "srv-b-batch-02",
          "destination_host_id": "srv-b-db-01",
          "avg_traffic": 28400000.0,
      },
      {
          "edge_id": "edge-flow-batch03-batch02",
          "source_host_id": "srv-c-batch-03",
          "destination_host_id": "srv-b-batch-02",
          "avg_traffic": 11250000.0,
      },
      {
          "edge_id": "edge-flow-lims01-db01",
          "source_host_id": "srv-c-lims-01",
          "destination_host_id": "srv-b-db-01",
          "avg_traffic": 6400000.0,
      },
  ]

  # -------------------------------------------------------------------------
  # 3. ServiceNow Planned Maintenance Windows
  # -------------------------------------------------------------------------
  servicenow_maintenance_windows = [
      {
          "change_id": "CHG0049281",
          "host_id": "srv-a-web-02",
          "hostname": "srv-a-web-02",
          "site_location": "Site_A_London",
          "system_id": "EBRS",
          "start_time": _iso_utc(now_utc - timedelta(minutes=45)),
          "end_time": _iso_utc(now_utc + timedelta(minutes=75)),
          "is_maintenance_window": 1,
          "change_type": "OS_KERNEL_PATCHING",
          "description": (
              "Scheduled RHEL 9.3 Kernel & Security Patching Window on srv-a-web-02 "
              "(Suppresses ARIMA_PLUS_XREG CPU Spike Alerts)"
          ),
      },
      {
          "change_id": "CHG0051024",
          "host_id": "srv-c-lims-01",
          "hostname": "srv-c-lims-01",
          "site_location": "Site_C_Ware",
          "system_id": "LIMS",
          "start_time": _iso_utc(now_utc - timedelta(minutes=30)),
          "end_time": _iso_utc(now_utc + timedelta(minutes=90)),
          "is_maintenance_window": 1,
          "change_type": "DB_INDEX_MAINTENANCE",
          "description": (
              "Planned LIMS PostgreSQL Vacuum & B-Tree Index Rebuild Window on srv-c-lims-01"
          ),
      },
  ]

  # -------------------------------------------------------------------------
  # 4. Multi-Site EBRS Telemetry (90 minutes per host: 75m baseline + 15m eval)
  # -------------------------------------------------------------------------
  host_configs = [
      {
          "host_id": "srv-b-batch-02",
          "hostname": "srv-b-batch-02",
          "enterprise_domain": "Pharma_Manufacturing",
          "site_location": "Site_B_Stevenage",
          "system_id": "EBRS",
          "cluster_id": "onprem-esxi-cluster-04",
          "application_tier": "Batch_Processing",
          "base_cpu": 38.0,
          "base_mem": 52.0,
          "base_io": 12.0,
          "base_net": 540000,
          "base_conn": 45,
      },
      {
          "host_id": "srv-b-db-01",
          "hostname": "srv-b-db-01",
          "enterprise_domain": "Pharma_Manufacturing",
          "site_location": "Site_B_Stevenage",
          "system_id": "EBRS",
          "cluster_id": "onprem-esxi-cluster-04",
          "application_tier": "Database_Backend",
          "base_cpu": 42.0,
          "base_mem": 60.0,
          "base_io": 18.0,
          "base_net": 920000,
          "base_conn": 110,
      },
      {
          "host_id": "srv-a-web-01",
          "hostname": "srv-a-web-01",
          "enterprise_domain": "Pharma_Manufacturing",
          "site_location": "Site_A_London",
          "system_id": "EBRS",
          "cluster_id": "gke-prod-lon-01",
          "application_tier": "Web_Frontend",
          "base_cpu": 31.0,
          "base_mem": 48.0,
          "base_io": 8.0,
          "base_net": 680000,
          "base_conn": 85,
      },
      {
          "host_id": "srv-a-web-04",
          "hostname": "srv-a-web-04",
          "enterprise_domain": "Pharma_Manufacturing",
          "site_location": "Site_A_London",
          "system_id": "EBRS",
          "cluster_id": "gke-prod-lon-01",
          "application_tier": "Web_Frontend",
          "base_cpu": 33.0,
          "base_mem": 49.0,
          "base_io": 8.5,
          "base_net": 695000,
          "base_conn": 88,
      },
      {
          "host_id": "srv-a-web-02",
          "hostname": "srv-a-web-02",
          "enterprise_domain": "Pharma_Manufacturing",
          "site_location": "Site_A_London",
          "system_id": "EBRS",
          "cluster_id": "gke-prod-lon-01",
          "application_tier": "Web_Frontend",
          "base_cpu": 30.0,
          "base_mem": 46.0,
          "base_io": 7.5,
          "base_net": 610000,
          "base_conn": 75,
      },
      {
          "host_id": "srv-c-batch-03",
          "hostname": "srv-c-batch-03",
          "enterprise_domain": "Pharma_Manufacturing",
          "site_location": "Site_C_Ware",
          "system_id": "MES_BATCH",
          "cluster_id": "aks-prod-ware-02",
          "application_tier": "Batch_Processing",
          "base_cpu": 54.0,
          "base_mem": 64.0,
          "base_io": 24.0,
          "base_net": 480000,
          "base_conn": 60,
      },
      {
          "host_id": "srv-c-lims-01",
          "hostname": "srv-c-lims-01",
          "enterprise_domain": "R_and_D",
          "site_location": "Site_C_Ware",
          "system_id": "LIMS",
          "cluster_id": "aks-prod-ware-02",
          "application_tier": "Database_Backend",
          "base_cpu": 36.0,
          "base_mem": 55.0,
          "base_io": 14.0,
          "base_net": 510000,
          "base_conn": 92,
      },
  ]

  telemetry_rows: list[dict[str, Any]] = []
  # Generate 90 1-minute steps: offset -90 to -1
  for offset_min in range(-90, 0):
    ts_dt = now_utc + timedelta(minutes=offset_min)
    ts_str = _iso_utc(ts_dt)
    wave = math.sin(offset_min * 0.25) * 1.2

    for cfg in host_configs:
      hid = cfg["host_id"]
      cpu = round(cfg["base_cpu"] + wave, 2)
      mem = round(cfg["base_mem"] + (wave * 0.5), 2)
      io_wait = round(cfg["base_io"] + abs(wave), 2)
      net = int(cfg["base_net"] + (wave * 8000))
      conn = int(cfg["base_conn"])
      status = "HEALTHY"

      # Apply cascading failure profiles in the trailing 15 minutes (-15..-1)
      if offset_min >= -15:
        if hid == "srv-b-batch-02":
          # Scenario A: JVM Memory Leak & OOM Collapse (55% -> 75% -> 92% -> 98.8%)
          if offset_min <= -11:
            mem, cpu, io_wait, status = 55.0, 48.5, 18.5, "HEALTHY"
          elif offset_min <= -7:
            mem, cpu, io_wait, status = 75.0, 64.2, 24.0, "HEALTHY"
          elif offset_min <= -4:
            mem, cpu, io_wait, status = 92.0, 82.5, 36.0, "ANOMALY"
          else:
            mem, cpu, io_wait, status = 98.8, 94.5, 48.0, "ANOMALY"
        elif hid == "srv-b-db-01":
          # Scenario B: Database Saturation (350/500 -> 500/500 connections, CPU >95%)
          if offset_min <= -8:
            cpu, mem, io_wait, conn, status = 96.4, 88.5, 115.0, 350, "ANOMALY"
          else:
            cpu, mem, io_wait, conn, status = 98.2, 93.4, 210.0, 500, "ANOMALY"
        elif hid in ("srv-a-web-01", "srv-a-web-04"):
          # Scenario B Stage 3: Upstream Web Tier Blast Radius (HTTP 504 timeouts)
          if offset_min <= -8:
            cpu, mem, conn, status = 52.5, 62.0, 240, "HEALTHY"
          else:
            cpu = 91.5 if hid == "srv-a-web-01" else 93.0
            mem = 86.2 if hid == "srv-a-web-01" else 87.4
            io_wait = 42.0
            conn = 500
            status = "ANOMALY"
        elif hid == "srv-c-batch-03":
          # Scenario C: Storage IO Saturation (>520ms -> >940ms) & Silent Host Drop to 0.0
          if offset_min <= -9:
            cpu, mem, io_wait, status = 74.2, 71.0, 545.0, "ANOMALY"
          elif offset_min <= -5:
            cpu, mem, io_wait, status = 89.5, 76.5, 965.0, "ANOMALY"
          elif offset_min in (-4, -3):
            # Omit raw heartbeat at -4 and -3 so GAP_FILL() demonstrates zero-filling!
            continue
          else:
            # Explicit 0.0 CPU and DOWN status at -2 and -1
            cpu, mem, io_wait, net, conn, status = 0.0, 0.0, 965.0, 0, 0, "DOWN"
        elif hid == "srv-a-web-02":
          # Planned Maintenance Window CHG0049281
          if offset_min >= -10:
            cpu, mem, status = 78.4, 68.0, "HEALTHY"
        elif hid == "srv-c-lims-01":
          # Planned Maintenance Window CHG0051024
          if offset_min >= -10:
            cpu, mem, status = 72.1, 66.5, "HEALTHY"

      telemetry_rows.append({
          "timestamp": ts_str,
          "enterprise_domain": cfg["enterprise_domain"],
          "site_location": cfg["site_location"],
          "system_id": cfg["system_id"],
          "cluster_id": cfg["cluster_id"],
          "application_tier": cfg["application_tier"],
          "host_id": hid,
          "hostname": cfg["hostname"],
          "cpu_usage_pct": float(cpu),
          "memory_usage_pct": float(mem),
          "io_wait_ms": float(io_wait),
          "network_bytes_sec": int(net),
          "status": status,
          "active_connections": int(conn),
      })

  # -------------------------------------------------------------------------
  # 5. System Logs (Aligned within 5-Minute Sliding Window of Anomalies)
  # -------------------------------------------------------------------------
  system_logs = [
      # Scenario A: srv-b-batch-02 JVM Memory Leak & OOM Collapse
      {
          "timestamp": _iso_utc(now_utc - timedelta(minutes=8)),
          "log_id": "log-scen-a-001",
          "host_id": "srv-b-batch-02",
          "hostname": "srv-b-batch-02",
          "site_location": "Site_B_Stevenage",
          "system_id": "EBRS",
          "application_tier": "Batch_Processing",
          "severity": "WARNING",
          "service_name": "EBRS-Batch-Engine",
          "message": (
              "WARNING: JVM GC pause time 1240ms exceeded threshold (1200ms) on "
              "srv-b-batch-02; heap utilization creeping 55% -> 75% -> 92%"
          ),
      },
      {
          "timestamp": _iso_utc(now_utc - timedelta(minutes=4)),
          "log_id": "log-scen-a-002",
          "host_id": "srv-b-batch-02",
          "hostname": "srv-b-batch-02",
          "site_location": "Site_B_Stevenage",
          "system_id": "EBRS",
          "application_tier": "Batch_Processing",
          "severity": "ERROR",
          "service_name": "EBRS-Batch-Engine",
          "message": (
              "ERROR: JVM Tenured Generation heap utilization reached 98.8% on "
              "srv-b-batch-02 (GC overhead limit exceeded, pause=1680ms)"
          ),
      },
      {
          "timestamp": _iso_utc(now_utc - timedelta(minutes=2)),
          "log_id": "log-scen-a-003",
          "host_id": "srv-b-batch-02",
          "hostname": "srv-b-batch-02",
          "site_location": "Site_B_Stevenage",
          "system_id": "EBRS",
          "application_tier": "Batch_Processing",
          "severity": "FATAL",
          "service_name": "EBRS-Batch-Engine",
          "message": (
              "FATAL: java.lang.OutOfMemoryError: Java heap space at "
              "com.gsk.ebrs.batch.GenomicsSparkWorker.processRecord(GenomicsSparkWorker.java:412)"
          ),
      },
      {
          "timestamp": _iso_utc(now_utc - timedelta(minutes=1)),
          "log_id": "log-scen-a-004",
          "host_id": "srv-b-batch-02",
          "hostname": "srv-b-batch-02",
          "site_location": "Site_B_Stevenage",
          "system_id": "EBRS",
          "application_tier": "Batch_Processing",
          "severity": "CRITICAL",
          "service_name": "kernel-oom-killer",
          "message": (
              "CRITICAL: Linux kernel OOM killer terminated process java (pid 18422) "
              "on srv-b-batch-02 with exit code 137"
          ),
      },
      # Scenario B: srv-b-db-01 Database Saturation & Deadlock
      {
          "timestamp": _iso_utc(now_utc - timedelta(minutes=9)),
          "log_id": "log-scen-b-001",
          "host_id": "srv-b-db-01",
          "hostname": "srv-b-db-01",
          "site_location": "Site_B_Stevenage",
          "system_id": "EBRS",
          "application_tier": "Database_Backend",
          "severity": "WARNING",
          "service_name": "EBRS-Database-Core",
          "message": (
              "WARNING: PostgreSQL active connection pool reached 350/500 on srv-b-db-01; "
              "CPU utilization at 96.4% with lock wait queues accumulating"
          ),
      },
      {
          "timestamp": _iso_utc(now_utc - timedelta(minutes=3)),
          "log_id": "log-scen-b-002",
          "host_id": "srv-b-db-01",
          "hostname": "srv-b-db-01",
          "site_location": "Site_B_Stevenage",
          "system_id": "EBRS",
          "application_tier": "Database_Backend",
          "severity": "CRITICAL",
          "service_name": "EBRS-Database-Core",
          "message": (
              "CRITICAL: PostgreSQL connection pool exhausted 500/500 on srv-b-db-01; "
              "rejecting new client connections from EBRS-Web-Portal"
          ),
      },
      {
          "timestamp": _iso_utc(now_utc - timedelta(minutes=2)),
          "log_id": "log-scen-b-003",
          "host_id": "srv-b-db-01",
          "hostname": "srv-b-db-01",
          "site_location": "Site_B_Stevenage",
          "system_id": "EBRS",
          "application_tier": "Database_Backend",
          "severity": "ERROR",
          "service_name": "EBRS-Database-Core",
          "message": (
              "ERROR: deadlock detected on relation clinical_trial_records "
              "(Process 29410 waits for ShareLock on transaction 884192)"
          ),
      },
      # Scenario B Stage 3: Upstream Web Frontend Blast Radius (srv-a-web-01 & srv-a-web-04)
      {
          "timestamp": _iso_utc(now_utc - timedelta(minutes=2)),
          "log_id": "log-scen-b-004",
          "host_id": "srv-a-web-01",
          "hostname": "srv-a-web-01",
          "site_location": "Site_A_London",
          "system_id": "EBRS",
          "application_tier": "Web_Frontend",
          "severity": "ERROR",
          "service_name": "EBRS-Web-Portal",
          "message": (
              "ERROR: HTTP 504 Gateway Timeout while contacting backend database "
              "srv-b-db-01:5432 (PostgreSQL connection pool exhausted 500/500)"
          ),
      },
      {
          "timestamp": _iso_utc(now_utc - timedelta(minutes=1)),
          "log_id": "log-scen-b-005",
          "host_id": "srv-a-web-01",
          "hostname": "srv-a-web-01",
          "site_location": "Site_A_London",
          "system_id": "EBRS",
          "application_tier": "Web_Frontend",
          "severity": "CRITICAL",
          "service_name": "EBRS-Web-Portal",
          "message": (
              "CRITICAL: HTTP worker thread pool exhausted (200/200 threads blocked) "
              "on srv-a-web-01 awaiting EBRS-Database-Core response"
          ),
      },
      {
          "timestamp": _iso_utc(now_utc - timedelta(minutes=2)),
          "log_id": "log-scen-b-006",
          "host_id": "srv-a-web-04",
          "hostname": "srv-a-web-04",
          "site_location": "Site_A_London",
          "system_id": "EBRS",
          "application_tier": "Web_Frontend",
          "severity": "ERROR",
          "service_name": "EBRS-Web-Portal",
          "message": (
              "ERROR: HTTP 504 Gateway Timeout while contacting backend database "
              "srv-b-db-01:5432 (PostgreSQL connection pool exhausted 500/500)"
          ),
      },
      {
          "timestamp": _iso_utc(now_utc - timedelta(minutes=1)),
          "log_id": "log-scen-b-007",
          "host_id": "srv-a-web-04",
          "hostname": "srv-a-web-04",
          "site_location": "Site_A_London",
          "system_id": "EBRS",
          "application_tier": "Web_Frontend",
          "severity": "CRITICAL",
          "service_name": "EBRS-Web-Portal",
          "message": (
              "CRITICAL: HTTP worker thread pool exhausted (200/200 threads blocked) "
              "on srv-a-web-04 awaiting EBRS-Database-Core response"
          ),
      },
      # Scenario C: srv-c-batch-03 Storage IO Saturation & Silent Drop to 0.0
      {
          "timestamp": _iso_utc(now_utc - timedelta(minutes=10)),
          "log_id": "log-scen-c-001",
          "host_id": "srv-c-batch-03",
          "hostname": "srv-c-batch-03",
          "site_location": "Site_C_Ware",
          "system_id": "MES_BATCH",
          "application_tier": "Batch_Processing",
          "severity": "ERROR",
          "service_name": "MES-Batch-Scheduler",
          "message": (
              "ERROR: Shared storage mount /mnt/gsk_batch reached IOPS saturation "
              "on srv-c-batch-03; io_wait_ms=545.0ms (> 520ms threshold)"
          ),
      },
      {
          "timestamp": _iso_utc(now_utc - timedelta(minutes=6)),
          "log_id": "log-scen-c-002",
          "host_id": "srv-c-batch-03",
          "hostname": "srv-c-batch-03",
          "site_location": "Site_C_Ware",
          "system_id": "MES_BATCH",
          "application_tier": "Batch_Processing",
          "severity": "CRITICAL",
          "service_name": "MES-Batch-Scheduler",
          "message": (
              "CRITICAL: Storage mount /mnt/gsk_batch stalled on srv-c-batch-03 "
              "(io_wait_ms=965.0ms > 940ms); node heartbeat daemon blocked"
          ),
      },
      {
          "timestamp": _iso_utc(now_utc - timedelta(minutes=5)),
          "log_id": "log-scen-c-003",
          "host_id": "srv-c-batch-03",
          "hostname": "srv-c-batch-03",
          "site_location": "Site_C_Ware",
          "system_id": "MES_BATCH",
          "application_tier": "Batch_Processing",
          "severity": "FATAL",
          "service_name": "kubelet-eviction-manager",
          "message": (
              "FATAL: Cluster orchestrator evicted host srv-c-batch-03 after "
              "/mnt/gsk_batch storage stall (io_wait_ms=965.0ms); host telemetry dropped to 0.0"
          ),
      },
      # ServiceNow Maintenance Logs (INFO)
      {
          "timestamp": _iso_utc(now_utc - timedelta(minutes=5)),
          "log_id": "log-maint-chg0049281",
          "host_id": "srv-a-web-02",
          "hostname": "srv-a-web-02",
          "site_location": "Site_A_London",
          "system_id": "EBRS",
          "application_tier": "Web_Frontend",
          "severity": "INFO",
          "service_name": "servicenow-change-agent",
          "message": (
              "INFO: Active ServiceNow maintenance window CHG0049281 on srv-a-web-02; "
              "ARIMA_PLUS_XREG external regressor is_maintenance=1 suppressing alerts"
          ),
      },
  ]

  # -------------------------------------------------------------------------
  # 6. Gemini 2.5 Flash Synthesis Outputs (`incident_root_cause_analysis` & `structured_log_entities`)
  # -------------------------------------------------------------------------
  scenarios = get_cascading_scenarios_catalog()
  incident_root_cause_analysis = [
      {
          "event_timestamp": _iso_utc(now_utc - timedelta(minutes=1)),
          "hostname": "srv-b-batch-02",
          "anomaly_score": 94.5,
          "prompt": (
              "You are an expert SRE triage assistant for GSK manufacturing and enterprise IT.\n"
              "Analyze the following telemetry incident:\n"
              "- Hostname: srv-b-batch-02\n"
              "- Application Tier: Batch_Processing\n"
              "- CPU Spike: 94.5%\n"
              "- Memory Saturation: 98.8%\n"
              "- Preceding Error Logs:\n"
              "FATAL: java.lang.OutOfMemoryError: Java heap space\n"
              "CRITICAL: Linux kernel OOM killer terminated process java (pid 18422) with exit code 137"
          ),
          "gemini_root_cause_analysis": scenarios["A"]["gemini_rca_summary"],
      },
      {
          "event_timestamp": _iso_utc(now_utc - timedelta(minutes=1)),
          "hostname": "srv-b-db-01",
          "anomaly_score": 98.2,
          "prompt": (
              "You are an expert SRE triage assistant for GSK manufacturing and enterprise IT.\n"
              "Analyze the following telemetry incident:\n"
              "- Hostname: srv-b-db-01\n"
              "- Application Tier: Database_Backend\n"
              "- CPU Spike: 98.2%\n"
              "- Memory Saturation: 93.4%\n"
              "- Preceding Error Logs:\n"
              "CRITICAL: PostgreSQL connection pool exhausted 500/500\n"
              "ERROR: deadlock detected on relation clinical_trial_records"
          ),
          "gemini_root_cause_analysis": scenarios["B"]["gemini_rca_summary"],
      },
      {
          "event_timestamp": _iso_utc(now_utc - timedelta(minutes=1)),
          "hostname": "srv-a-web-01",
          "anomaly_score": 91.5,
          "prompt": (
              "You are an expert SRE triage assistant for GSK manufacturing and enterprise IT.\n"
              "Analyze the following telemetry incident:\n"
              "- Hostname: srv-a-web-01\n"
              "- Application Tier: Web_Frontend\n"
              "- CPU Spike: 91.5%\n"
              "- Memory Saturation: 86.2%\n"
              "- Preceding Error Logs:\n"
              "ERROR: HTTP 504 Gateway Timeout while contacting backend database srv-b-db-01:5432"
          ),
          "gemini_root_cause_analysis": (
              "1. Immediate root cause: Upstream web frontend srv-a-web-01 exhausted its HTTP "
              "worker thread pool (200/200 blocked) due to downstream connection pool saturation "
              "(500/500) and deadlocks on backend database srv-b-db-01. "
              "2. Downstream service blast radius: EBRS-Web-Portal users at Site_A_London and "
              "Site_B_Stevenage experience HTTP 504 Gateway Timeouts across electronic batch "
              "record submission endpoints. "
              "3. Immediate corrective action: Resolve the root-cause deadlock on srv-b-db-01 "
              "(relation clinical_trial_records) and recycle stalled Tomcat/Nginx worker pools "
              "on srv-a-web-01 and srv-a-web-04."
          ),
      },
      {
          "event_timestamp": _iso_utc(now_utc - timedelta(minutes=5)),
          "hostname": "srv-c-batch-03",
          "anomaly_score": 89.5,
          "prompt": (
              "You are an expert SRE triage assistant for GSK manufacturing and enterprise IT.\n"
              "Analyze the following telemetry incident:\n"
              "- Hostname: srv-c-batch-03\n"
              "- Application Tier: Batch_Processing\n"
              "- CPU Spike: 89.5% -> 0.0% (SILENT_HOST_DROP_TO_ZERO)\n"
              "- IO Wait: 965.0ms\n"
              "- Preceding Error Logs:\n"
              "CRITICAL: Storage mount /mnt/gsk_batch stalled on srv-c-batch-03 (io_wait_ms=965.0ms)"
          ),
          "gemini_root_cause_analysis": scenarios["C"]["gemini_rca_summary"],
      },
  ]

  structured_log_entities = [
      {
          "timestamp": _iso_utc(now_utc - timedelta(minutes=2)),
          "hostname": "srv-b-batch-02",
          "root_cause_category": "JVM_MEMORY_EXHAUSTION_OOM",
          "failed_component": "EBRS-Batch-Engine (GenomicsSparkWorker)",
          "error_code": "EXIT_137_OOM_JAVA_HEAP_SPACE",
          "recommended_action": (
              "Restart EBRS-Batch-Engine with G1GC and MaxRAMPercentage=75.0; "
              "drain stuck Spark partitions on srv-b-batch-02."
          ),
          "confidence_score": 0.994,
      },
      {
          "timestamp": _iso_utc(now_utc - timedelta(minutes=3)),
          "hostname": "srv-b-db-01",
          "root_cause_category": "DATABASE_CONNECTION_POOL_EXHAUSTION",
          "failed_component": "EBRS-Database-Core (PostgreSQL clinical_trial_records)",
          "error_code": "PG_POOL_500_500_DEADLOCK_40P01",
          "recommended_action": (
              "Terminate deadlocked PostgreSQL backend PID 29410 on clinical_trial_records "
              "and enable PgBouncer transaction pooling."
          ),
          "confidence_score": 0.997,
      },
      {
          "timestamp": _iso_utc(now_utc - timedelta(minutes=2)),
          "hostname": "srv-a-web-01",
          "root_cause_category": "UPSTREAM_WEB_GATEWAY_TIMEOUT",
          "failed_component": "EBRS-Web-Portal (HTTP Ingress Worker Pool)",
          "error_code": "HTTP_504_GATEWAY_TIMEOUT",
          "recommended_action": (
              "Clear backend database deadlock on srv-b-db-01 and enable circuit-breaker "
              "fast-fail on srv-a-web-01 and srv-a-web-04."
          ),
          "confidence_score": 0.989,
      },
      {
          "timestamp": _iso_utc(now_utc - timedelta(minutes=2)),
          "hostname": "srv-a-web-04",
          "root_cause_category": "UPSTREAM_WEB_GATEWAY_TIMEOUT",
          "failed_component": "EBRS-Web-Portal (HTTP Ingress Worker Pool)",
          "error_code": "HTTP_504_GATEWAY_TIMEOUT",
          "recommended_action": (
              "Clear backend database deadlock on srv-b-db-01 and enable circuit-breaker "
              "fast-fail on srv-a-web-01 and srv-a-web-04."
          ),
          "confidence_score": 0.988,
      },
      {
          "timestamp": _iso_utc(now_utc - timedelta(minutes=5)),
          "hostname": "srv-c-batch-03",
          "root_cause_category": "STORAGE_IO_SATURATION_NODE_EVICTION",
          "failed_component": "/mnt/gsk_batch (MES-Batch-Scheduler)",
          "error_code": "IO_WAIT_965MS_HEARTBEAT_LOST",
          "recommended_action": (
              "Fail over /mnt/gsk_batch to secondary SAN controller in Site_C_Ware "
              "and uncordon srv-c-batch-03 once IO wait normalizes."
          ),
          "confidence_score": 0.998,
      },
  ]

  return {
      "enterprise_telemetry_partitioned": telemetry_rows,
      "system_logs": system_logs,
      "servicenow_maintenance_windows": servicenow_maintenance_windows,
      "nodes_switches": nodes_switches,
      "nodes_hypervisors": nodes_hypervisors,
      "nodes_hosts": nodes_hosts,
      "nodes_applications": nodes_applications,
      "edges_connected_to": edges_connected_to,
      "edges_hosts_vm": edges_hosts_vm,
      "edges_runs_app": edges_runs_app,
      "edges_app_communicates": edges_app_communicates,
      "edges_network_flows": edges_network_flows,
      "incident_root_cause_analysis": incident_root_cause_analysis,
      "structured_log_entities": structured_log_entities,
  }


def seed_sqlite_mirror(
    mirror_path: Optional[str | Path] = None,
    data: Optional[dict[str, list[dict[str, Any]]]] = None,
) -> dict[str, int]:
  """Creates and seeds the local SQLite mirror `.cache/gsk_observability_mvp_mirror.db`."""
  db_path = Path(mirror_path) if mirror_path is not None else DEFAULT_MIRROR_PATH
  db_path.parent.mkdir(parents=True, exist_ok=True)
  dataset = data if data is not None else build_ebrs_seed_dataset()

  conn = sqlite3.connect(str(db_path))
  cur = conn.cursor()

  ddl_statements = [
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
      )
      """,
      """
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
      )
      """,
      """
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
      )
      """,
      """
      CREATE TABLE IF NOT EXISTS nodes_switches (
        switch_id TEXT PRIMARY KEY,
        hostname TEXT,
        site_location TEXT,
        management_ip TEXT,
        model TEXT
      )
      """,
      """
      CREATE TABLE IF NOT EXISTS nodes_hypervisors (
        hypervisor_id TEXT PRIMARY KEY,
        hostname TEXT,
        site_location TEXT,
        cluster_name TEXT,
        esxi_version TEXT
      )
      """,
      """
      CREATE TABLE IF NOT EXISTS nodes_hosts (
        host_id TEXT PRIMARY KEY,
        hostname TEXT,
        site_location TEXT,
        application_type TEXT,
        operating_system TEXT
      )
      """,
      """
      CREATE TABLE IF NOT EXISTS nodes_applications (
        app_id TEXT PRIMARY KEY,
        name TEXT,
        tier TEXT,
        system_id TEXT,
        criticality TEXT
      )
      """,
      """
      CREATE TABLE IF NOT EXISTS edges_connected_to (
        edge_id TEXT PRIMARY KEY,
        switch_id TEXT,
        hypervisor_id TEXT,
        port_name TEXT,
        speed_gbps INTEGER
      )
      """,
      """
      CREATE TABLE IF NOT EXISTS edges_hosts_vm (
        edge_id TEXT PRIMARY KEY,
        hypervisor_id TEXT,
        host_id TEXT,
        allocated_vcpus INTEGER,
        allocated_ram_gb INTEGER
      )
      """,
      """
      CREATE TABLE IF NOT EXISTS edges_runs_app (
        edge_id TEXT PRIMARY KEY,
        host_id TEXT,
        app_id TEXT,
        process_id INTEGER,
        listen_port INTEGER
      )
      """,
      """
      CREATE TABLE IF NOT EXISTS edges_app_communicates (
        edge_id TEXT PRIMARY KEY,
        source_app_id TEXT,
        target_app_id TEXT,
        protocol TEXT,
        avg_latency_ms REAL
      )
      """,
      """
      CREATE TABLE IF NOT EXISTS edges_network_flows (
        edge_id TEXT PRIMARY KEY,
        source_host_id TEXT,
        destination_host_id TEXT,
        avg_traffic REAL
      )
      """,
      """
      CREATE TABLE IF NOT EXISTS incident_root_cause_analysis (
        event_timestamp TEXT,
        hostname TEXT,
        anomaly_score REAL,
        prompt TEXT,
        gemini_root_cause_analysis TEXT
      )
      """,
      """
      CREATE TABLE IF NOT EXISTS structured_log_entities (
        timestamp TEXT,
        hostname TEXT,
        root_cause_category TEXT,
        failed_component TEXT,
        error_code TEXT,
        recommended_action TEXT,
        confidence_score REAL
      )
      """,
  ]

  for stmt in ddl_statements:
    cur.execute(stmt)

  counts: dict[str, int] = {}
  for table_name, rows in dataset.items():
    cur.execute(f"DELETE FROM {table_name}")
    if rows:
      cols = list(rows[0].keys())
      placeholders = ", ".join(["?"] * len(cols))
      col_list = ", ".join(cols)
      values = [tuple(r[c] for c in cols) for r in rows]
      cur.executemany(
          f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})",
          values,
      )
    counts[table_name] = len(rows)

  conn.commit()
  conn.close()
  logger.info("Seeded local SQLite mirror at %s (%d tables).", db_path, len(counts))
  return counts


def _get_bq_access_token() -> str:
  """Obtains an OAuth bearer token via gcloud CLI or google.auth."""
  proc = subprocess.run(
      ["gcloud", "auth", "print-access-token"],
      capture_output=True,
      text=True,
      check=False,
  )
  token = proc.stdout.strip()
  if token:
    return token
  if google is not None and hasattr(google, "auth"):
    try:
      creds, _ = google.auth.default(
          scopes=["https://www.googleapis.com/auth/bigquery"]
      )
      if creds and getattr(creds, "token", None):
        return str(creds.token)
    except Exception:  # pylint: disable=broad-except
      pass
  return ""


def execute_bq_query(
    project_id: str,
    sql: str,
    location: str = DEFAULT_LOCATION,
    dry_run: bool = False,
    timeout_sec: int = 180,
    token: Optional[str] = None,
) -> dict[str, Any]:
  """Executes a Standard SQL / ISO GQL query in BigQuery via the REST jobs.query/insert API."""
  auth_token = token if token is not None else _get_bq_access_token()
  if not auth_token:
    raise RuntimeError("No active GCP access token available for BigQuery REST call.")

  headers = {
      "Authorization": f"Bearer {auth_token}",
      "Content-Type": "application/json",
  }
  url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/queries"
  payload: dict[str, Any] = {
      "query": sql,
      "useLegacySql": False,
      "location": location,
      "dryRun": dry_run,
      "timeoutMs": min(timeout_sec * 1000, 60000),
  }

  req = urllib.request.Request(
      url,
      data=json.dumps(payload).encode("utf-8"),
      headers=headers,
      method="POST",
  )
  try:
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
      body = json.loads(resp.read().decode("utf-8") or "{}")
  except urllib.error.HTTPError as exc:
    err_body = exc.read().decode("utf-8", errors="replace")
    raise RuntimeError(f"BigQuery HTTP {exc.code}: {err_body}") from exc

  if dry_run:
    return {"status": "DRY_RUN_OK", "totalBytesProcessed": body.get("totalBytesProcessed", "0"), "raw": body}

  # Poll if jobComplete is False
  job_ref = body.get("jobReference", {})
  job_id = job_ref.get("jobId")
  job_loc = job_ref.get("location", location)
  deadline = datetime.now(timezone.utc) + timedelta(seconds=timeout_sec)
  while not body.get("jobComplete", False) and job_id and datetime.now(timezone.utc) < deadline:
    poll_url = (
        f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}"
        f"/queries/{job_id}?location={job_loc}&timeoutMs=10000"
    )
    poll_req = urllib.request.Request(poll_url, headers=headers, method="GET")
    with urllib.request.urlopen(poll_req, timeout=30) as poll_resp:
      body = json.loads(poll_resp.read().decode("utf-8") or "{}")

  if body.get("errors"):
    raise RuntimeError(f"BigQuery execution error: {body['errors']}")

  schema_fields = body.get("schema", {}).get("fields", [])
  col_names = [f["name"] for f in schema_fields]
  parsed_rows: list[dict[str, Any]] = []
  for row_obj in body.get("rows", []):
    cells = row_obj.get("f", [])
    row_dict: dict[str, Any] = {}
    for idx, col_name in enumerate(col_names):
      val = cells[idx].get("v") if idx < len(cells) else None
      row_dict[col_name] = val
    parsed_rows.append(row_dict)

  return {
      "status": "SUCCESS",
      "job_id": job_id,
      "total_rows": int(body.get("totalRows", len(parsed_rows))),
      "rows": parsed_rows,
      "raw": body,
  }


def _sql_literal(val: Any, col_name: str = "") -> str:
  """Formats a Python value as a safe BigQuery Standard SQL literal."""
  if val is None:
    return "NULL"
  if isinstance(val, bool):
    return "TRUE" if val else "FALSE"
  if isinstance(val, int):
    return str(val)
  if isinstance(val, float):
    if math.isnan(val) or math.isinf(val):
      return "0.0"
    return repr(val)
  s = str(val)
  escaped = s.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n")
  if col_name in ("timestamp", "start_time", "end_time", "event_timestamp"):
    return f"TIMESTAMP('{escaped}')"
  return f"'{escaped}'"


def seed_live_bigquery(
    project_id: str = DEFAULT_PROJECT_ID,
    dataset_id: str = DEFAULT_DATASET_ID,
    location: str = DEFAULT_LOCATION,
    data: Optional[dict[str, list[dict[str, Any]]]] = None,
) -> dict[str, int]:
  """Creates and populates all 14 tables (+ 9 CMDB staging tables) in live BigQuery.

  Uses DDL `CREATE OR REPLACE TABLE` followed by batched `INSERT INTO` DML so
  that rows are immediately committed to BigQuery storage (not stuck in the
  streaming buffer) and immediately visible to `GRAPH_TABLE`, `GAP_FILL`,
  `AI.FORECAST`, and `AI.DETECT_ANOMALIES`.
  """
  dataset = data if data is not None else build_ebrs_seed_dataset()
  token = _get_bq_access_token()
  if not token:
    raise RuntimeError("No active GCP access token available to seed BigQuery.")

  # Ensure dataset exists
  ds_url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/datasets"
  ds_payload = {
      "datasetReference": {"projectId": project_id, "datasetId": dataset_id},
      "location": location,
      "friendlyName": "GSK Enterprise Observability Lakehouse",
      "description": "Unified dataset storing raw telemetry, metric rollups, and topology graphs",
  }
  req = urllib.request.Request(
      ds_url,
      data=json.dumps(ds_payload).encode("utf-8"),
      headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
      method="POST",
  )
  try:
    with urllib.request.urlopen(req, timeout=30):
      pass
  except urllib.error.HTTPError as exc:
    if exc.code != 409:
      raise

  fq_ds = f"`{project_id}.{dataset_id}"
  ddl_map: dict[str, str] = {
      "enterprise_telemetry_partitioned": f"""
        CREATE OR REPLACE TABLE {fq_ds}.enterprise_telemetry_partitioned` (
          timestamp TIMESTAMP NOT NULL,
          enterprise_domain STRING,
          site_location STRING,
          system_id STRING,
          cluster_id STRING,
          application_tier STRING,
          host_id STRING NOT NULL,
          hostname STRING NOT NULL,
          cpu_usage_pct FLOAT64,
          memory_usage_pct FLOAT64,
          io_wait_ms FLOAT64,
          network_bytes_sec INT64,
          status STRING,
          active_connections INT64
        )
        PARTITION BY DATE(timestamp)
        CLUSTER BY site_location, system_id, application_tier, host_id
        OPTIONS (
          partition_expiration_days = 90,
          description = "Partitioned and hierarchically clustered raw telemetry table for GSK enterprise monitoring"
        )
      """,
      "system_logs": f"""
        CREATE OR REPLACE TABLE {fq_ds}.system_logs` (
          timestamp TIMESTAMP NOT NULL,
          log_id STRING,
          host_id STRING NOT NULL,
          hostname STRING NOT NULL,
          site_location STRING,
          system_id STRING,
          application_tier STRING,
          severity STRING NOT NULL,
          service_name STRING,
          message STRING NOT NULL
        )
        PARTITION BY DATE(timestamp)
        CLUSTER BY host_id, severity
        OPTIONS (
          partition_expiration_days = 90,
          description = "Partitioned and clustered unstructured system & application error logs"
        )
      """,
      "servicenow_maintenance_windows": f"""
        CREATE OR REPLACE TABLE {fq_ds}.servicenow_maintenance_windows` (
          change_id STRING NOT NULL,
          host_id STRING,
          hostname STRING NOT NULL,
          site_location STRING,
          system_id STRING,
          start_time TIMESTAMP NOT NULL,
          end_time TIMESTAMP NOT NULL,
          is_maintenance_window INT64 NOT NULL,
          change_type STRING,
          description STRING
        )
        PARTITION BY DATE(start_time)
        CLUSTER BY hostname
      """,
      "nodes_switches": f"""
        CREATE OR REPLACE TABLE {fq_ds}.nodes_switches` (
          switch_id STRING NOT NULL,
          hostname STRING,
          site_location STRING,
          management_ip STRING,
          model STRING
        )
      """,
      "nodes_hypervisors": f"""
        CREATE OR REPLACE TABLE {fq_ds}.nodes_hypervisors` (
          hypervisor_id STRING NOT NULL,
          hostname STRING,
          site_location STRING,
          cluster_name STRING,
          esxi_version STRING
        )
      """,
      "nodes_hosts": f"""
        CREATE OR REPLACE TABLE {fq_ds}.nodes_hosts` (
          host_id STRING NOT NULL,
          hostname STRING,
          site_location STRING,
          application_type STRING,
          operating_system STRING
        )
      """,
      "nodes_applications": f"""
        CREATE OR REPLACE TABLE {fq_ds}.nodes_applications` (
          app_id STRING NOT NULL,
          name STRING,
          tier STRING,
          system_id STRING,
          criticality STRING
        )
      """,
      "edges_connected_to": f"""
        CREATE OR REPLACE TABLE {fq_ds}.edges_connected_to` (
          edge_id STRING NOT NULL,
          switch_id STRING NOT NULL,
          hypervisor_id STRING NOT NULL,
          port_name STRING,
          speed_gbps INT64
        )
      """,
      "edges_hosts_vm": f"""
        CREATE OR REPLACE TABLE {fq_ds}.edges_hosts_vm` (
          edge_id STRING NOT NULL,
          hypervisor_id STRING NOT NULL,
          host_id STRING NOT NULL,
          allocated_vcpus INT64,
          allocated_ram_gb INT64
        )
      """,
      "edges_runs_app": f"""
        CREATE OR REPLACE TABLE {fq_ds}.edges_runs_app` (
          edge_id STRING NOT NULL,
          host_id STRING NOT NULL,
          app_id STRING NOT NULL,
          process_id INT64,
          listen_port INT64
        )
      """,
      "edges_app_communicates": f"""
        CREATE OR REPLACE TABLE {fq_ds}.edges_app_communicates` (
          edge_id STRING NOT NULL,
          source_app_id STRING NOT NULL,
          target_app_id STRING NOT NULL,
          protocol STRING,
          avg_latency_ms FLOAT64
        )
      """,
      "edges_network_flows": f"""
        CREATE OR REPLACE TABLE {fq_ds}.edges_network_flows` (
          edge_id STRING NOT NULL,
          source_host_id STRING NOT NULL,
          destination_host_id STRING NOT NULL,
          avg_traffic FLOAT64
        )
      """,
      "incident_root_cause_analysis": f"""
        CREATE OR REPLACE TABLE {fq_ds}.incident_root_cause_analysis` (
          event_timestamp TIMESTAMP,
          hostname STRING,
          anomaly_score FLOAT64,
          prompt STRING,
          gemini_root_cause_analysis STRING
        )
      """,
      "structured_log_entities": f"""
        CREATE OR REPLACE TABLE {fq_ds}.structured_log_entities` (
          timestamp TIMESTAMP,
          hostname STRING,
          root_cause_category STRING,
          failed_component STRING,
          error_code STRING,
          recommended_action STRING,
          confidence_score FLOAT64
        )
      """,
  }

  seeded_counts: dict[str, int] = {}
  for table_name in REQUIRED_OBSERVABILITY_TABLES:
    ddl_sql = ddl_map[table_name]
    rows = dataset.get(table_name, [])
    logger.info("Creating & populating BigQuery table %s.%s.%s (%d rows)...", project_id, dataset_id, table_name, len(rows))
    execute_bq_query(project_id, ddl_sql, location=location, token=token)

    if rows:
      cols = list(rows[0].keys())
      col_list_sql = ", ".join(cols)
      # Insert in batches of 250 rows per DML statement
      for idx in range(0, len(rows), 250):
        batch = rows[idx : idx + 250]
        tuples_sql = []
        for r in batch:
          vals_sql = ", ".join(_sql_literal(r[c], c) for c in cols)
          tuples_sql.append(f"({vals_sql})")
        insert_sql = (
            f"INSERT INTO `{project_id}.{dataset_id}.{table_name}` ({col_list_sql}) "
            f"VALUES {', '.join(tuples_sql)}"
        )
        execute_bq_query(project_id, insert_sql, location=location, token=token)
    seeded_counts[table_name] = len(rows)

  # Also create the 9 CMDB source inventory views/tables from Blueprint Section 9.3
  # so any execution of Steps 1-9 in sql/05_property_graph_ddl_and_gql.sql succeeds natively.
  staging_ddls = [
      f"CREATE OR REPLACE VIEW `{project_id}.{dataset_id}.solarwinds_switch_inventory` AS SELECT * FROM `{project_id}.{dataset_id}.nodes_switches`",
      f"CREATE OR REPLACE VIEW `{project_id}.{dataset_id}.vmware_host_inventory` AS SELECT * FROM `{project_id}.{dataset_id}.nodes_hypervisors`",
      f"CREATE OR REPLACE VIEW `{project_id}.{dataset_id}.servicenow_server_inventory` AS SELECT * FROM `{project_id}.{dataset_id}.nodes_hosts`",
      f"CREATE OR REPLACE VIEW `{project_id}.{dataset_id}.cmdb_applications` AS SELECT * FROM `{project_id}.{dataset_id}.nodes_applications`",
      f"CREATE OR REPLACE VIEW `{project_id}.{dataset_id}.network_topology_links` AS SELECT switch_id, hypervisor_id, port_name, speed_gbps FROM `{project_id}.{dataset_id}.edges_connected_to`",
      f"CREATE OR REPLACE VIEW `{project_id}.{dataset_id}.vmware_vm_allocations` AS SELECT hypervisor_id, host_id, allocated_vcpus, allocated_ram_gb FROM `{project_id}.{dataset_id}.edges_hosts_vm`",
      f"CREATE OR REPLACE VIEW `{project_id}.{dataset_id}.process_bindings` AS SELECT host_id, app_id, process_id, listen_port FROM `{project_id}.{dataset_id}.edges_runs_app`",
      f"CREATE OR REPLACE VIEW `{project_id}.{dataset_id}.app_network_matrix` AS SELECT source_app_id, target_app_id, protocol, avg_latency_ms FROM `{project_id}.{dataset_id}.edges_app_communicates`",
      f"CREATE OR REPLACE VIEW `{project_id}.{dataset_id}.network_telemetry` AS SELECT source_host_id, destination_host_id, avg_traffic AS avg_traffic_bytes_sec FROM `{project_id}.{dataset_id}.edges_network_flows`",
  ]
  for view_sql in staging_ddls:
    execute_bq_query(project_id, view_sql, location=location, token=token)

  return seeded_counts


def seed_ebrs_observability(
    project_id: str = DEFAULT_PROJECT_ID,
    dataset_id: str = DEFAULT_DATASET_ID,
    location: str = DEFAULT_LOCATION,
    mirror_path: Optional[str | Path] = None,
    seed_live_bq: bool = True,
    reference_time: Optional[datetime] = None,
) -> dict[str, Any]:
  """Seeds EBRS multi-site telemetry, topology, maintenance windows & RCA into SQLite and BigQuery.

  Args:
    project_id: Target GCP project ID (default: `gke-demos-363017`).
    dataset_id: Target BigQuery dataset ID (default: `gsk_observability_demo`).
    location: Target BigQuery dataset location (default: `EU`).
    mirror_path: Optional path to SQLite mirror database.
    seed_live_bq: If True, also creates and populates all 14 tables in live BigQuery.
    reference_time: Optional reference UTC datetime.

  Returns:
    Summary dictionary containing SQLite counts, BigQuery counts, and scenario metadata.
  """
  data = build_ebrs_seed_dataset(reference_time=reference_time)
  sqlite_counts = seed_sqlite_mirror(mirror_path=mirror_path, data=data)

  bq_counts: dict[str, int] = {}
  bq_status = "SKIPPED_LOCAL_ONLY"
  if seed_live_bq:
    try:
      bq_counts = seed_live_bigquery(
          project_id=project_id,
          dataset_id=dataset_id,
          location=location,
          data=data,
      )
      bq_status = "SEEDED_LIVE_BIGQUERY"
    except Exception as exc:  # pylint: disable=broad-except
      logger.warning("Live BigQuery seeding encountered error: %s", exc)
      bq_status = f"FALLBACK_SQLITE_ONLY: {exc}"

  return {
      "status": "SUCCESS",
      "project_id": project_id,
      "dataset_id": dataset_id,
      "location": location,
      "mirror_db_path": str(Path(mirror_path) if mirror_path else DEFAULT_MIRROR_PATH),
      "bq_status": bq_status,
      "sqlite_table_counts": sqlite_counts,
      "bigquery_table_counts": bq_counts,
      "scenarios_seeded": ["A", "B", "C"],
      "maintenance_windows_seeded": ["CHG0049281", "CHG0051024"],
  }


def main(argv: Optional[list[str]] = None) -> int:
  """CLI entrypoint for `src/seed_ebrs_observability.py`."""
  parser = argparse.ArgumentParser(
      description="Seed GSK EBRS multi-site telemetry & 10-step ISO GQL topology."
  )
  parser.add_argument("--project", default=DEFAULT_PROJECT_ID, help="Target GCP project ID")
  parser.add_argument("--dataset", default=DEFAULT_DATASET_ID, help="Target BigQuery dataset ID")
  parser.add_argument("--location", default=DEFAULT_LOCATION, help="BigQuery location (EU)")
  parser.add_argument(
      "--mirror-path",
      default=str(DEFAULT_MIRROR_PATH),
      help="Filesystem path for local SQLite analytical mirror",
  )
  parser.add_argument(
      "--local-only",
      action="store_true",
      help="Seed only the local SQLite mirror without calling live BigQuery",
  )
  args = parser.parse_args(argv)

  summary = seed_ebrs_observability(
      project_id=args.project,
      dataset_id=args.dataset,
      location=args.location,
      mirror_path=args.mirror_path,
      seed_live_bq=not args.local_only,
  )
  print(json.dumps(summary, indent=2))
  return 0


if __name__ == "__main__":
  sys.exit(main())
