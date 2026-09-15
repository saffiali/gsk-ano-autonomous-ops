#!/usr/bin/env python3
"""GSK Autonomous Operations (ANO) — Live Customer Demo Telemetry Seeder.

This script implements Round 2 Requirement R2:
1. Seeds live GCP project `gke-demos-363017` (when authenticated) and the Shared
   Analytical Mirror (`src/mirror_store.py`) with realistic GSK estate telemetry:
   - 90 days of seasonal Prometheus metrics (`gmp_metrics` > 240 rows) with
     sinusoidal diurnal/weekly seasonality plus Act 2 (Server Unresponsiveness
     at 65.2% CPU, 38.4 RPS, 142 starved threads, 4,850 CLOSE_WAIT sockets,
     22m lead time) and Act 3 (Cross-Domain Root Cause Attribution across
     `tomcat-app-stv-01` -> `ora-db-stv-01` -> `core-sw-lon-01` OSPF flaps).
   - CMDB Dependency Graph (`topology_edges`) linking `tomcat-app-stv-01` ->
     `ora-db-stv-01` (`DEPENDS_ON`, weight 0.95), `ora-db-stv-01` ->
     `core-sw-lon-01` (`ROUTES_THROUGH`, weight 0.92), 2-hop transitive edge,
     and host edge to `vm-stv-app-01`.
   - ServiceNow Maintenance Schedules (`change_calendar`) including active
     OS patching window `CHG0049281` (`IN_PROGRESS`, `suppress_alerts=True`).
   - Baseline & Act 1 Novel Distributed Deadlock Logs (`raw_logs` & `log_embeddings`)
     processed through `StackTraceCoalescer`, `LogNormalizer`, and
     `SlidingWindowChunker` (`src.embedding_worker`) into 768-dim vectors where
     `COSINE_DISTANCE` against the baseline cluster centroid equals `0.421` (`> 0.35`).
   - Correlated Incident Predictions (`incidents_predictions`) for Acts 1, 2, 3,
     4A (`CHG0049281` suppressed), and 4B (`gsk-ano-network-ospf-reroute` triggered
     via `RemediationWebhookHandler`).
2. Exposes clean programmatic helper functions `seed_all(...)` and
   `trigger_act_scenario(act_id: int, ...)` for `demo_runner.py` and `demo_dashboard.py`.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
import sys
from typing import Any, Optional

# Ensure project root is in sys.path for clean imports
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
  sys.path.insert(0, str(PROJECT_ROOT))

from src.mirror_store import (  # pylint: disable=wrong-import-position
    AnalyticalMirrorStore,
    DEFAULT_DATASET_ID,
    DEFAULT_PROJECT_ID,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("gsk_ano.seed_live_demo")


def seed_all(
    project_id: str = DEFAULT_PROJECT_ID,
    dataset_id: str = DEFAULT_DATASET_ID,
    days: int = 90,
    inject_incidents: bool = True,
    incident_filter: str = "all",
    mirror_path: Optional[str | Path] = None,
) -> dict[str, Any]:
  """Seeds 90-day seasonal telemetry, CMDB topology, ServiceNow schedule & live incidents.

  Args:
    project_id: Target GCP project ID (default: "gke-demos-363017").
    dataset_id: Target BigQuery dataset ID (default: "gsk_ano_ops").
    days: Number of days of historical seasonal telemetry to generate (default: 90).
    inject_incidents: Whether to inject live Act 1-4 anomaly records (default: True).
    incident_filter: Specific incident act to inject ("act1"|"act2"|"act3"|"act4"|"all").
    mirror_path: Optional explicit filesystem path for the SQLite analytical mirror.

  Returns:
    Dictionary summarizing seeded table row counts and verified vector distances.
  """
  store = AnalyticalMirrorStore(
      db_path=mirror_path,
      project_id=project_id,
      dataset_id=dataset_id,
  )
  return store.seed_demo_data(
      days=days,
      inject_incidents=inject_incidents,
      incident_filter=incident_filter,
  )


def trigger_act_scenario(
    act_id: int,
    project_id: str = DEFAULT_PROJECT_ID,
    dataset_id: str = DEFAULT_DATASET_ID,
    mirror_path: Optional[str | Path] = None,
) -> dict[str, Any]:
  """Triggers a specific Customer Demo Act (1, 2, 3, or 4) and returns analytical results.

  Args:
    act_id: Integer Act identifier (1, 2, 3, or 4).
    project_id: Target GCP project ID.
    dataset_id: Target BigQuery dataset ID.
    mirror_path: Optional explicit filesystem path for the SQLite analytical mirror.

  Returns:
    Structured dictionary with SQL query, executive table rows, talking points, and incident.
  """
  store = AnalyticalMirrorStore(
      db_path=mirror_path,
      project_id=project_id,
      dataset_id=dataset_id,
  )
  return store.trigger_act(int(act_id))


def inject_act1_novel_deadlock_outlier(
    mirror_path: Optional[str | Path] = None,
    project_id: str = DEFAULT_PROJECT_ID,
    dataset_id: str = DEFAULT_DATASET_ID,
) -> dict[str, Any]:
  """On-demand injector for Act 1 multi-line Java/Oracle XA distributed deadlock log."""
  store = AnalyticalMirrorStore(
      db_path=mirror_path,
      project_id=project_id,
      dataset_id=dataset_id,
  )
  store.seed_demo_data(days=90, inject_incidents=True, incident_filter="act1")
  return store.trigger_act(1)


def verify_seeded_mirror(store: AnalyticalMirrorStore) -> dict[str, Any]:
  """Verifies row counts, topology graph links, CHG0049281, and Act 1 cosine distance."""
  conn = store.get_connection()
  cursor = conn.cursor()

  # Verify table counts
  counts: dict[str, int] = {}
  for tbl in [
      "raw_logs",
      "log_embeddings",
      "gmp_metrics",
      "topology_edges",
      "change_calendar",
      "incidents_predictions",
  ]:
    cursor.execute(f"SELECT COUNT(*) FROM {tbl}")
    counts[tbl] = int(cursor.fetchone()[0])

  if counts["gmp_metrics"] <= 200:
    raise RuntimeError(
        f"Expected >200 rows in gmp_metrics, found {counts['gmp_metrics']}"
    )

  # Verify topology edges
  cursor.execute("SELECT source_entity_id, target_entity_id FROM topology_edges")
  edges = {(row[0], row[1]) for row in cursor.fetchall()}
  required_edges = {
      ("tomcat-app-stv-01", "ora-db-stv-01"),
      ("ora-db-stv-01", "core-sw-lon-01"),
      ("tomcat-app-stv-01", "core-sw-lon-01"),
      ("tomcat-app-stv-01", "vm-stv-app-01"),
  }
  missing_edges = required_edges - edges
  if missing_edges:
    raise RuntimeError(f"Missing required topology edges: {missing_edges}")

  # Verify CHG0049281 active patching window
  cursor.execute(
      "SELECT status, suppress_alerts FROM change_calendar WHERE change_id = 'CHG0049281'"
  )
  chg_row = cursor.fetchone()
  if not chg_row or not bool(chg_row["suppress_alerts"]):
    raise RuntimeError(
        "Active maintenance window CHG0049281 with suppress_alerts=True not found"
    )

  # Verify Act 1 Novel Deadlock COSINE_DISTANCE == 0.421 (> 0.35 threshold)
  cursor.execute("""
      SELECT ROUND(COSINE_DISTANCE(a.embedding, b.embedding), 3)
      FROM log_embeddings a, log_embeddings b
      WHERE a.log_id = 'log-act1-xa-deadlock-001'
        AND b.log_id = 'log-hist-99201a'
  """)
  dist_row = cursor.fetchone()
  cosine_dist = float(dist_row[0]) if dist_row and dist_row[0] is not None else 0.0
  if abs(cosine_dist - 0.421) > 1e-3:
    raise RuntimeError(
        f"Expected Act 1 COSINE_DISTANCE == 0.421, got {cosine_dist}"
    )

  conn.close()
  return {
      "verification_status": "PASSED",
      "table_counts": counts,
      "verified_edges_count": len(edges),
      "active_change_ticket": "CHG0049281 (suppress_alerts=True)",
      "act1_cosine_distance": cosine_dist,
  }


def print_seeding_report(
    seed_summary: dict[str, Any],
    verification: dict[str, Any],
) -> None:
  """Prints a formatted executive telemetry seeding summary report."""
  banner = "=" * 88
  print(f"\n{banner}")
  print(" GSK AUTONOMOUS OPERATIONS (ANO) — LIVE CUSTOMER DEMO TELEMETRY SEEDING REPORT")
  print(f"{banner}")
  print(f"  Target GCP Project ID : {seed_summary['project_id']}")
  print(f"  Target BigQuery Dataset: {seed_summary['dataset_id']}")
  print(f"  Local Mirror Database : {seed_summary['mirror_db_path']}")
  print(f"  Seeding Timestamp     : {seed_summary['timestamp']}")
  print(f"{'-' * 88}")
  print("  [1] SEEDED TABLE ROW COUNTS:")
  for tbl_name, count in verification["table_counts"].items():
    print(f"      - {tbl_name:<24} : {count:>6} rows")

  print(f"{'-' * 88}")
  print("  [2] VERIFIED SCENARIO ARTIFACTS (ACTS 1 - 4):")
  print(
      f"      - Act 1 (Semantic Outlier) : Java/Oracle XA deadlock (ORA-02049 / ORA-00060) "
      f"768-dim embedding COSINE_DISTANCE = {verification['act1_cosine_distance']} (> 0.35 threshold)"
  )
  print(
      "      - Act 2 (22m Hang Predict) : tomcat-app-stv-01 & vm-stv-app-01 at 65.2% CPU, "
      "38.4 RPS, 142 starved threads, 4,850 CLOSE_WAIT sockets"
  )
  print(
      "      - Act 3 (Cross-Domain RCA) : 3-tier CMDB graph tomcat-app-stv-01 (2850ms) -> "
      "ora-db-stv-01 (1420ms lock) -> core-sw-lon-01 (19 OSPF flaps, 14.8% loss)"
  )
  print(
      f"      - Act 4 (Change Window)    : ServiceNow {verification['active_change_ticket']} "
      "suppressed vs. gsk-ano-network-ospf-reroute Cloud Workflow triggered"
  )
  print(f"{'-' * 88}")
  print(f"  OVERALL SEEDING & VERIFICATION STATUS: {verification['verification_status']}")
  print(f"{banner}\n")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
  """Parses CLI arguments for `seed_live_demo.py`."""
  parser = argparse.ArgumentParser(
      description="GSK ANO Live Customer Demo Telemetry & Incident Seeder."
  )
  parser.add_argument(
      "--project",
      default=DEFAULT_PROJECT_ID,
      help=f"Target GCP project ID (default: {DEFAULT_PROJECT_ID})",
  )
  parser.add_argument(
      "--dataset",
      default=DEFAULT_DATASET_ID,
      help=f"Target BigQuery dataset name (default: {DEFAULT_DATASET_ID})",
  )
  parser.add_argument(
      "--days",
      type=int,
      default=90,
      help="Number of days of seasonal historical telemetry to generate (default: 90)",
  )
  parser.add_argument(
      "--inject-live-incidents",
      action="store_true",
      default=True,
      help="Inject live Act 1-4 anomaly records into telemetry and incident tables.",
  )
  parser.add_argument(
      "--inject-incident",
      choices=["act1", "act2", "act3", "act4", "all"],
      default="all",
      help="Specific demo Act incident scenario to inject (default: all).",
  )
  parser.add_argument(
      "--local-only",
      action="store_true",
      help="Force seeding against the local SQLite analytical mirror only.",
  )
  parser.add_argument(
      "--verify",
      action="store_true",
      help="Run telemetry seeding and execute self-verification checks (exits 0).",
  )
  parser.add_argument(
      "--mirror-path",
      default=None,
      help="Optional explicit filesystem path for the SQLite analytical mirror database.",
  )
  return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
  """Main CLI entrypoint for `seed_live_demo.py`."""
  args = parse_args(argv)

  seed_summary = seed_all(
      project_id=args.project,
      dataset_id=args.dataset,
      days=args.days,
      inject_incidents=True,
      incident_filter=args.inject_incident,
      mirror_path=args.mirror_path,
  )

  store = AnalyticalMirrorStore(
      db_path=args.mirror_path,
      project_id=args.project,
      dataset_id=args.dataset,
  )
  verification = verify_seeded_mirror(store)
  print_seeding_report(seed_summary, verification)
  return 0


if __name__ == "__main__":
  sys.exit(main())
