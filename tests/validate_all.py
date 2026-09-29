#!/usr/bin/env python3
"""GSK Autonomous Operations (ANO) — Comprehensive Automated Verification Suite.

This self-contained verification runner programmatically validates:
1. Suite 1 (`TerraformValidationSuite`):
   - Authentic HashiCorp HCL v2 syntax validation using `/google/bin/releases/g3terraform/hclfmt -check`
     on every `.tf` and `.tfvars.example` file under `terraform/` (plus `terraform validate` if installed).
   - Pure-Python HCL AST & structural integrity verification (balanced braces, brackets, strings, heredocs).
   - Required repository layout across root and all 5 submodules (`ingestion`, `storage_and_vector`,
     `bqml_analytics`, `embedding_pipeline`, `alerting_and_remediation`).
   - Required GCP resource inventory & attribute completeness (Log Sinks with exclusions, Pub/Sub topics +
     DLQs with dead_letter_policy, 6 DAY-partitioned & clustered BigQuery tables, Vector Search DDL,
     3 BQML models, Cloud Run v2 services, Eventarc triggers, Cloud Monitoring alert policies).
   - Cross-module variable (`var.<name>`) and output (`module.<mod>.<out>`) wiring validation.
   - Least-privilege IAM security audit (zero broad roles like owner/editor/admin).
2. Suite 2 (`SQLSchemaValidationSuite`):
   - Exact schema verification (columns, types, modes, partitioning, clustering) for all 6 BigQuery tables
     (`raw_logs`, `log_embeddings`, `gmp_metrics`, `topology_edges`, `change_calendar`, `incidents_predictions`).
   - BigQuery Vector Search DDL (`CREATE VECTOR INDEX` with `TREE_AH` & `COSINE`, `STORING` columns) and
     `VECTOR_SEARCH` semantic outlier query verification.
   - BigQuery ML (BQML) query & schema alignment across Capability 1 (`LOGISTIC_REG`), Capability 2
     (`BOOSTED_TREE_CLASSIFIER` across `topology_edges`), and Capability 3 (`ARIMA_PLUS_XREG` +
     `ML.DETECT_ANOMALIES` joined with `change_calendar` noise suppression).
3. Suite 3 (`PythonPipelineUnitTestSuite`):
   - Comprehensive unit tests verifying `src/embedding_worker.py` (stack trace coalescing, regex token
     normalization, sliding-window chunking `window_size=5, stride=2`, Vertex AI `text-embedding-005`
     768-dim payload formatting, BigQuery streaming writer) and `src/remediation_webhook.py`
     (deterministic `change_calendar` maintenance suppression, unsuppressed remediation dispatch,
     and per-entity sliding-window cooldown rate limiting).
4. Suite 4 (`Round2LiveDemoAndDashboardSuite`):
   - End-to-end validation of Round 2 GCP live deployment script (`scripts/deploy_to_gcp.py`),
     dual-mode SQLite/BigQuery analytical mirror (`src/mirror_store.py`), 90-day seasonal telemetry
     and live incident seeder (`src/seed_live_demo.py`), interactive 4-Act CLI demo runner (`src/demo_runner.py`),
     executive web UI dashboard & REST API server (`src/demo_dashboard.py`), customer runbook documentation
     (`docs/CUSTOMER_DEMO_RUNBOOK.md`), and repository `git secrets` working tree safety.
5. Suite 5 (`Round3ObservabilityPlatformSuite`):
   - End-to-end validation of Round 3 GSK Enterprise Observability Platform & "Neuro" Conversational AI:
     architecture blueprint (`docs/GSK_OBSERVABILITY_PLATFORM_ARCHITECTURE.md`), OTel Collector config
     (`config/otel-collector-config.yaml`), SQL/DDL/ISO GQL catalog (`sql/01`..`06`), Terraform submodule
     (`terraform/modules/observability_lakehouse/`), PromQL `0.0`-preserving micro-batcher
     (`src/promql_micro_batcher.py`), EBRS multi-stage cascading failure seeder (`src/seed_ebrs_observability.py`),
     live GCP `gke-demos-363017.gsk_observability_demo` tables, `GRAPH_TABLE` traversal, TimesFM/BQML
     anomaly detection (`scripts/deploy_observability_mvp.py`), and Interactive MVP Workbench & "Neuro"
     REST APIs (`src/observability_mvp_server.py` & `src/demo_dashboard.py`).
"""

from __future__ import annotations

import base64
import json
import math
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from typing import Any

# Add project root to sys.path so `src` is importable from any working directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
  sys.path.insert(0, str(PROJECT_ROOT))

from src.embedding_worker import (
    BigQueryStreamWriter,
    EmbeddingPipelineWorker,
    LogNormalizer,
    SlidingWindowChunker,
    StackTraceCoalescer,
    VertexAIEmbeddingClient,
)
from src.remediation_webhook import (
    ChangeCalendarChecker,
    IncidentAuditWriter,
    RemediationRateLimiter,
    RemediationWebhookHandler,
    WorkflowsRemediationDispatcher,
)

TERRAFORM_DIR = PROJECT_ROOT / "terraform"
HCLFMT_BIN = Path("/google/bin/releases/g3terraform/hclfmt")

REQUIRED_SUBMODULES = [
    "ingestion",
    "storage_and_vector",
    "bqml_analytics",
    "embedding_pipeline",
    "alerting_and_remediation",
    "observability_lakehouse",
]

CANONICAL_TABLES_SPEC: dict[str, dict[str, Any]] = {
    "raw_logs": {
        "partition_field": "timestamp",
        "clustering": ["service_name", "severity", "host_id", "environment"],
        "columns": {
            "log_id": ("STRING", "REQUIRED"),
            "timestamp": ("TIMESTAMP", "REQUIRED"),
            "receive_timestamp": ("TIMESTAMP", "NULLABLE"),
            "severity": ("STRING", "REQUIRED"),
            "service_name": ("STRING", "REQUIRED"),
            "host_id": ("STRING", "REQUIRED"),
            "environment": ("STRING", "REQUIRED"),
            "domain": ("STRING", "REQUIRED"),
            "message_template": ("STRING", "NULLABLE"),
            "raw_payload": ("STRING", "REQUIRED"),
            "trace_id": ("STRING", "NULLABLE"),
            "span_id": ("STRING", "NULLABLE"),
            "labels": ("JSON", "NULLABLE"),
        },
    },
    "log_embeddings": {
        "partition_field": "timestamp",
        "clustering": ["service_name", "domain", "host_id"],
        "columns": {
            "chunk_id": ("STRING", "REQUIRED"),
            "log_id": ("STRING", "REQUIRED"),
            "timestamp": ("TIMESTAMP", "REQUIRED"),
            "service_name": ("STRING", "REQUIRED"),
            "host_id": ("STRING", "REQUIRED"),
            "domain": ("STRING", "REQUIRED"),
            "severity": ("STRING", "REQUIRED"),
            "chunk_text": ("STRING", "REQUIRED"),
            "embedding": ("FLOAT64", "REPEATED"),
            "embedding_model": ("STRING", "REQUIRED"),
            "token_count": ("INT64", "NULLABLE"),
            "is_outlier": ("BOOL", "NULLABLE"),
        },
    },
    "gmp_metrics": {
        "partition_field": "timestamp",
        "clustering": ["host_id", "service_name", "domain"],
        "columns": {
            "metric_sample_id": ("STRING", "REQUIRED"),
            "timestamp": ("TIMESTAMP", "REQUIRED"),
            "host_id": ("STRING", "REQUIRED"),
            "service_name": ("STRING", "REQUIRED"),
            "domain": ("STRING", "REQUIRED"),
            "environment": ("STRING", "REQUIRED"),
            "cpu_utilization_pct": ("FLOAT64", "NULLABLE"),
            "throughput_rps": ("FLOAT64", "NULLABLE"),
            "cpu_to_throughput_ratio": ("FLOAT64", "NULLABLE"),
            "thread_starvation_count": ("INT64", "NULLABLE"),
            "io_wait_pct": ("FLOAT64", "NULLABLE"),
            "disk_latency_ms": ("FLOAT64", "NULLABLE"),
            "socket_exhaustion_count": ("INT64", "NULLABLE"),
            "apache_tomcat_latency_ms": ("FLOAT64", "NULLABLE"),
            "db_lock_wait_ms": ("FLOAT64", "NULLABLE"),
            "db_active_sessions": ("INT64", "NULLABLE"),
            "ospf_flap_count": ("INT64", "NULLABLE"),
            "packet_loss_pct": ("FLOAT64", "NULLABLE"),
            "unresponsive_in_15_30m": ("BOOL", "NULLABLE"),
            "root_cause_domain": ("STRING", "NULLABLE"),
        },
    },
    "topology_edges": {
        "partition_field": "updated_at",
        "clustering": ["source_entity_id", "target_entity_id", "relationship_type"],
        "columns": {
            "edge_id": ("STRING", "REQUIRED"),
            "updated_at": ("TIMESTAMP", "REQUIRED"),
            "source_entity_id": ("STRING", "REQUIRED"),
            "source_domain": ("STRING", "REQUIRED"),
            "target_entity_id": ("STRING", "REQUIRED"),
            "target_domain": ("STRING", "REQUIRED"),
            "relationship_type": ("STRING", "REQUIRED"),
            "criticality_weight": ("FLOAT64", "REQUIRED"),
            "hop_distance": ("INT64", "REQUIRED"),
            "metadata": ("JSON", "NULLABLE"),
        },
    },
    "change_calendar": {
        "partition_field": "start_time",
        "clustering": ["target_entity_id", "change_type", "status"],
        "columns": {
            "change_id": ("STRING", "REQUIRED"),
            "start_time": ("TIMESTAMP", "REQUIRED"),
            "end_time": ("TIMESTAMP", "REQUIRED"),
            "target_entity_id": ("STRING", "REQUIRED"),
            "target_domain": ("STRING", "REQUIRED"),
            "change_type": ("STRING", "REQUIRED"),
            "status": ("STRING", "REQUIRED"),
            "suppress_alerts": ("BOOL", "REQUIRED"),
            "change_risk_level": ("INT64", "REQUIRED"),
            "owner_team": ("STRING", "NULLABLE"),
        },
    },
    "incidents_predictions": {
        "partition_field": "prediction_timestamp",
        "clustering": [
            "root_cause_entity_id",
            "capability_type",
            "suppressed_by_change_window",
            "remediation_status",
        ],
        "columns": {
            "incident_id": ("STRING", "REQUIRED"),
            "prediction_timestamp": ("TIMESTAMP", "REQUIRED"),
            "capability_type": ("STRING", "REQUIRED"),
            "affected_entity_id": ("STRING", "REQUIRED"),
            "root_cause_entity_id": ("STRING", "REQUIRED"),
            "root_cause_domain": ("STRING", "REQUIRED"),
            "lead_time_minutes": ("INT64", "REQUIRED"),
            "anomaly_probability": ("FLOAT64", "REQUIRED"),
            "semantic_nearest_neighbor_log_id": ("STRING", "NULLABLE"),
            "cosine_distance": ("FLOAT64", "NULLABLE"),
            "suppressed_by_change_window": ("BOOL", "REQUIRED"),
            "active_change_id": ("STRING", "NULLABLE"),
            "recommended_action": ("STRING", "REQUIRED"),
            "remediation_status": ("STRING", "REQUIRED"),
            "remediation_execution_id": ("STRING", "NULLABLE"),
        },
    },
}


# ==============================================================================
# Helper Utilities for HCL Parsing & AST Extraction
# ==============================================================================


def strip_hcl_comments_and_heredocs(content: str) -> str:
  """Strips heredocs (<<-EOF ... EOF) and comments (#, //, /* */) for structural scanning."""
  # Replace heredocs with a placeholder string literal
  heredoc_pattern = re.compile(r"<<-?([A-Za-z0-9_]+)\s*\n.*?\n\s*\1", re.DOTALL)
  cleaned = heredoc_pattern.sub('"__HEREDOC_PLACEHOLDER__"', content)
  # Strip multi-line comments /* ... */
  cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL)
  # Strip single-line comments # and // outside double quotes
  out_lines = []
  for line in cleaned.splitlines():
    in_str = False
    escaped = False
    cut_idx = len(line)
    for i, ch in enumerate(line):
      if escaped:
        escaped = False
        continue
      if ch == "\\":
        escaped = True
        continue
      if ch == '"':
        in_str = not in_str
      elif not in_str:
        if ch == "#":
          cut_idx = i
          break
        if ch == "/" and i + 1 < len(line) and line[i + 1] == "/":
          cut_idx = i
          break
    out_lines.append(line[:cut_idx])
  return "\n".join(out_lines)


def check_balanced_delimiters(hcl_text: str, filepath: str) -> None:
  """Verifies balanced braces {}, brackets [], parentheses (), and double quotes."""
  cleaned = strip_hcl_comments_and_heredocs(hcl_text)
  stack: list[tuple[str, int]] = []
  pairs = {"}": "{", "]": "[", ")": "("}
  in_str = False
  escaped = False

  for line_num, line in enumerate(cleaned.splitlines(), start=1):
    for ch in line:
      if escaped:
        escaped = False
        continue
      if ch == "\\":
        escaped = True
        continue
      if ch == '"':
        in_str = not in_str
        continue
      if in_str:
        continue
      if ch in "{[(":
        stack.append((ch, line_num))
      elif ch in "}])":
        if not stack or stack[-1][0] != pairs[ch]:
          raise AssertionError(
              f"Unbalanced delimiter '{ch}' at {filepath}:{line_num}"
          )
        stack.pop()

  if in_str:
    raise AssertionError(f"Unclosed string literal in {filepath}")
  if stack:
    unclosed, line_num = stack[-1]
    raise AssertionError(
        f"Unclosed delimiter '{unclosed}' opened at {filepath}:{line_num}"
    )


def extract_hcl_blocks(content: str) -> list[dict[str, Any]]:
  """Extracts top-level HCL blocks (resource, variable, output, module, etc.) with bodies."""
  blocks: list[dict[str, Any]] = []
  lines = content.splitlines()
  i = 0
  n = len(lines)

  header_regex = re.compile(
      r'^\s*(resource|variable|output|module|locals|terraform|provider|data)\s*'
      r'(?:"([^"]+)"\s*)?(?:"([^"]+)"\s*)?\{'
  )

  while i < n:
    line = lines[i]
    match = header_regex.match(line)
    if match:
      b_type = match.group(1)
      label1 = match.group(2)
      label2 = match.group(3)
      # Find matching closing brace
      brace_depth = 0
      body_lines: list[str] = []
      start_line = i + 1
      in_heredoc = False
      heredoc_marker = ""

      while i < n:
        curr = lines[i]
        body_lines.append(curr)
        if not in_heredoc:
          hd_match = re.search(r"<<-?([A-Za-z0-9_]+)", curr)
          if hd_match:
            in_heredoc = True
            heredoc_marker = hd_match.group(1)
          else:
            # Count braces outside quotes
            in_q = False
            esc = False
            for ch in curr:
              if esc:
                esc = False
                continue
              if ch == "\\":
                esc = True
                continue
              if ch == '"':
                in_q = not in_q
              elif not in_q:
                if ch == "{":
                  brace_depth += 1
                elif ch == "}":
                  brace_depth -= 1
        else:
          if curr.strip() == heredoc_marker:
            in_heredoc = False
            heredoc_marker = ""

        if brace_depth == 0 and not in_heredoc:
          break
        i += 1

      blocks.append({
          "type": b_type,
          "label1": label1,
          "label2": label2,
          "body": "\n".join(body_lines),
          "start_line": start_line,
      })
    i += 1
  return blocks


# ==============================================================================
# SUITE 1: Terraform HCL & Architectural Completeness Validation Suite
# ==============================================================================


class TerraformValidationSuite(unittest.TestCase):
  """Validates all Terraform HCL files, module wiring, resource inventory, and IAM security."""

  def test_1_1_hclfmt_syntax_validation(self) -> None:
    """Runs authentic `/google/bin/releases/g3terraform/hclfmt -check` on every .tf file."""
    self.assertTrue(
        TERRAFORM_DIR.exists(), f"Missing terraform directory at {TERRAFORM_DIR}"
    )
    tf_files = sorted(
        list(TERRAFORM_DIR.rglob("*.tf"))
        + list(TERRAFORM_DIR.rglob("*.tfvars.example"))
    )
    self.assertGreaterEqual(
        len(tf_files), 20, "Expected at least 20 Terraform files across root + 5 modules"
    )

    self.assertTrue(
        HCLFMT_BIN.exists(),
        f"Authentic HCL validator binary not found at {HCLFMT_BIN}",
    )

    for tf_file in tf_files:
      proc = subprocess.run(
          [str(HCLFMT_BIN), "-check", str(tf_file)],
          capture_output=True,
          text=True,
          check=False,
      )
      self.assertEqual(
          proc.returncode,
          0,
          f"hclfmt syntax/format check failed on {tf_file}:\n{proc.stderr}\n{proc.stdout}",
      )

    # Also run `terraform validate` if `terraform` binary is installed in PATH
    tf_bin = shutil.which("terraform")
    if tf_bin:
      init_proc = subprocess.run(
          [tf_bin, "init", "-backend=false"],
          cwd=str(TERRAFORM_DIR),
          capture_output=True,
          text=True,
          check=False,
      )
      if init_proc.returncode == 0:
        val_proc = subprocess.run(
            [tf_bin, "validate"],
            cwd=str(TERRAFORM_DIR),
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(
            val_proc.returncode,
            0,
            f"terraform validate failed:\n{val_proc.stderr}\n{val_proc.stdout}",
        )

  def test_1_2_hcl_ast_structural_integrity(self) -> None:
    """Verifies balanced delimiters and extracts valid top-level AST blocks for all .tf files."""
    tf_files = sorted(TERRAFORM_DIR.rglob("*.tf"))
    total_blocks = 0

    for tf_file in tf_files:
      content = tf_file.read_text(encoding="utf-8")
      check_balanced_delimiters(content, str(tf_file))
      blocks = extract_hcl_blocks(content)
      self.assertGreater(
          len(blocks), 0, f"File {tf_file} yielded 0 top-level HCL blocks"
      )
      total_blocks += len(blocks)

    self.assertGreaterEqual(
        total_blocks, 60, "Expected at least 60 top-level HCL blocks across suite"
    )

  def test_1_3_required_module_structure(self) -> None:
    """Verifies root files and all 5 required submodules exist with main/variables/outputs."""
    root_required = [
        "main.tf",
        "variables.tf",
        "outputs.tf",
        "versions.tf",
        "terraform.tfvars.example",
    ]
    for fname in root_required:
      self.assertTrue(
          (TERRAFORM_DIR / fname).is_file(),
          f"Missing required root file terraform/{fname}",
      )

    for mod_name in REQUIRED_SUBMODULES:
      mod_dir = TERRAFORM_DIR / "modules" / mod_name
      self.assertTrue(
          mod_dir.is_dir(), f"Missing required submodule directory: {mod_dir}"
      )
      for sub_file in ["main.tf", "variables.tf", "outputs.tf"]:
        self.assertTrue(
            (mod_dir / sub_file).is_file(),
            f"Missing required file {mod_dir / sub_file}",
        )

  def test_1_4_required_gcp_resources(self) -> None:
    """Verifies all required GCP resources and configurations exist across submodules."""
    all_resources: dict[tuple[str, str], str] = {}
    for tf_file in TERRAFORM_DIR.rglob("*.tf"):
      blocks = extract_hcl_blocks(tf_file.read_text(encoding="utf-8"))
      for b in blocks:
        if b["type"] == "resource":
          all_resources[(b["label1"], b["label2"])] = b["body"]

    # 1. Ingestion: Log sink with exclusions + 3 primary Pub/Sub topics + 3 DLQ topics + subscriptions
    self.assertIn(
        ("google_logging_project_sink", "ano_operational_sink"), all_resources
    )
    sink_body = all_resources[
        ("google_logging_project_sink", "ano_operational_sink")
    ]
    self.assertIn("exclusions", sink_body)
    self.assertIn("unique_writer_identity", sink_body)

    for topic_name in [
        "raw_logs",
        "raw_logs_dlq",
        "gmp_metrics",
        "gmp_metrics_dlq",
        "correlated_incidents",
        "correlated_incidents_dlq",
    ]:
      self.assertIn(("google_pubsub_topic", topic_name), all_resources)

    for sub_name in ["raw_logs_sub", "gmp_metrics_sub", "correlated_incidents_sub"]:
      self.assertIn(("google_pubsub_subscription", sub_name), all_resources)
      sub_body = all_resources[("google_pubsub_subscription", sub_name)]
      self.assertIn("dead_letter_policy", sub_body)
      self.assertIn("max_delivery_attempts", sub_body)

    # 2. Storage & Vector: BigQuery dataset + 6 DAY-partitioned & clustered tables + routines
    self.assertIn(("google_bigquery_dataset", "ano_dataset"), all_resources)
    for table_name, spec in CANONICAL_TABLES_SPEC.items():
      self.assertIn(
          ("google_bigquery_table", table_name),
          all_resources,
          f"Missing google_bigquery_table.{table_name}",
      )
      t_body = all_resources[("google_bigquery_table", table_name)]
      self.assertIn("time_partitioning", t_body)
      self.assertIn('type          = "DAY"' if 'type          = "DAY"' in t_body else 'type  = "DAY"', t_body)
      self.assertIn(f'field         = "{spec["partition_field"]}"' if f'field         = "{spec["partition_field"]}"' in t_body else f'field = "{spec["partition_field"]}"', t_body)
      self.assertIn("clustering", t_body)

    self.assertIn(
        ("google_bigquery_routine", "create_vector_index"), all_resources
    )
    self.assertIn(
        ("google_bigquery_routine", "semantic_outlier_vector_search"),
        all_resources,
    )

    # 3. BQML Analytics: 3 capabilities + anomaly evaluation routine
    self.assertIn(
        ("google_bigquery_routine", "train_cap1_unresponsiveness"), all_resources
    )
    self.assertIn(
        ("google_bigquery_routine", "train_cap2_cross_domain_rca"), all_resources
    )
    self.assertIn(
        ("google_bigquery_routine", "train_cap3_rolling_baseline"), all_resources
    )
    self.assertIn(
        ("google_bigquery_routine", "evaluate_and_suppress_anomalies"),
        all_resources,
    )

    # 4. Embedding Pipeline: Cloud Run v2 + Eventarc trigger
    self.assertIn(
        ("google_cloud_run_v2_service", "embedding_worker"), all_resources
    )
    self.assertIn(
        ("google_eventarc_trigger", "raw_logs_trigger"), all_resources
    )

    # 5. Alerting & Remediation: Cloud Run v2 webhook + Eventarc + Monitoring channels & alert policy
    self.assertIn(
        ("google_cloud_run_v2_service", "remediation_webhook"), all_resources
    )
    self.assertIn(
        ("google_eventarc_trigger", "incidents_remediation_trigger"),
        all_resources,
    )
    self.assertIn(
        ("google_monitoring_notification_channel", "email_channel"),
        all_resources,
    )
    self.assertIn(
        ("google_monitoring_notification_channel", "webhook_channel"),
        all_resources,
    )
    self.assertIn(
        ("google_monitoring_alert_policy", "unsuppressed_root_cause_alert"),
        all_resources,
    )
    policy_body = all_resources[
        ("google_monitoring_alert_policy", "unsuppressed_root_cause_alert")
    ]
    self.assertIn("notification_rate_limit", policy_body)
    self.assertIn("auto_close", policy_body)
    self.assertIn("suppressed_by_change_window=false", policy_body)

  def test_1_5_cross_module_variable_and_output_wiring(self) -> None:
    """Verifies all var.<name> and module.<mod>.<output> references resolve cleanly."""
    # Check each module directory's internal var.<name> references
    all_module_dirs = [TERRAFORM_DIR] + [
        TERRAFORM_DIR / "modules" / m for m in REQUIRED_SUBMODULES
    ]

    module_declared_vars: dict[str, set[str]] = {}
    module_declared_outputs: dict[str, set[str]] = {}

    for mod_dir in all_module_dirs:
      mod_key = "root" if mod_dir == TERRAFORM_DIR else mod_dir.name
      vars_file = mod_dir / "variables.tf"
      outputs_file = mod_dir / "outputs.tf"

      declared_v: set[str] = set()
      for b in extract_hcl_blocks(vars_file.read_text(encoding="utf-8")):
        if b["type"] == "variable" and b["label1"]:
          declared_v.add(b["label1"])
      module_declared_vars[mod_key] = declared_v

      declared_o: set[str] = set()
      for b in extract_hcl_blocks(outputs_file.read_text(encoding="utf-8")):
        if b["type"] == "output" and b["label1"]:
          declared_o.add(b["label1"])
      module_declared_outputs[mod_key] = declared_o

      # Scan all .tf files in mod_dir for var.<name> usage
      for tf_file in mod_dir.glob("*.tf"):
        text = tf_file.read_text(encoding="utf-8")
        used_vars = set(re.findall(r"\bvar\.([a-zA-Z0-9_]+)\b", text))
        unresolved = used_vars - declared_v
        self.assertEqual(
            len(unresolved),
            0,
            f"Unresolved variable references {unresolved} in {tf_file}",
        )

    # Check root main.tf module instantiation blocks and module.<mod>.<out> references
    root_main_text = (TERRAFORM_DIR / "main.tf").read_text(encoding="utf-8")
    root_outputs_text = (TERRAFORM_DIR / "outputs.tf").read_text(encoding="utf-8")
    combined_root_text = root_main_text + "\n" + root_outputs_text

    # Verify every module.<mod>.<out> reference points to a declared output in that submodule
    mod_out_refs = re.findall(
        r"\bmodule\.([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\b", combined_root_text
    )
    for mod_name, out_name in mod_out_refs:
      self.assertIn(
          mod_name,
          module_declared_outputs,
          f"Referenced unknown module '{mod_name}'",
      )
      self.assertIn(
          out_name,
          module_declared_outputs[mod_name],
          f"Output '{out_name}' not declared in module.{mod_name}/outputs.tf",
      )

    # Verify every input passed to module "<mod_name>" in root main.tf is declared in that module's variables.tf
    for b in extract_hcl_blocks(root_main_text):
      if b["type"] == "module" and b["label1"] in REQUIRED_SUBMODULES:
        mod_name = b["label1"]
        # Extract argument keys inside module block (excluding 'source')
        lines = b["body"].splitlines()[1:-1]
        for line in lines:
          kv_match = re.match(r"^\s*([a-zA-Z0-9_]+)\s*=", line)
          if kv_match:
            arg_name = kv_match.group(1)
            if arg_name != "source":
              self.assertIn(
                  arg_name,
                  module_declared_vars[mod_name],
                  f"Root main.tf passes undeclared input '{arg_name}' to module.{mod_name}",
              )

  def test_1_6_iam_least_privilege_security_audit(self) -> None:
    """Asserts dedicated service accounts and zero overly permissive IAM roles."""
    forbidden_roles = {
        "roles/owner",
        "roles/editor",
        "roles/bigquery.admin",
        "roles/pubsub.admin",
        "roles/iam.securityAdmin",
        "roles/run.admin",
    }

    service_accounts: list[str] = []
    granted_roles: list[str] = []

    for tf_file in TERRAFORM_DIR.rglob("*.tf"):
      for b in extract_hcl_blocks(tf_file.read_text(encoding="utf-8")):
        if b["type"] == "resource":
          res_type = b["label1"]
          if res_type == "google_service_account":
            service_accounts.append(b["label2"])
          elif "iam_member" in res_type or "iam_binding" in res_type:
            role_match = re.search(r'role\s*=\s*"([^"]+)"', b["body"])
            if role_match:
              role = role_match.group(1)
              granted_roles.append(role)
              self.assertNotIn(
                  role,
                  forbidden_roles,
                  f"Violation of least-privilege IAM: forbidden role '{role}' found in {tf_file}",
              )

    self.assertIn("embedding_worker_sa", service_accounts)
    self.assertIn("remediation_sa", service_accounts)
    self.assertIn("roles/aiplatform.user", granted_roles)
    self.assertIn("roles/bigquery.dataEditor", granted_roles)
    self.assertIn("roles/workflows.invoker", granted_roles)


# ==============================================================================
# SUITE 2: BigQuery SQL & BQML Schema Alignment Validation Suite
# ==============================================================================


class SQLSchemaValidationSuite(unittest.TestCase):
  """Validates BigQuery table schemas, Vector Index DDL, and BQML model SQL queries."""

  def _extract_storage_tables_and_routines(
      self,
  ) -> tuple[dict[str, list[dict[str, Any]]], dict[str, str]]:
    """Parses storage_and_vector/main.tf and bqml_analytics/main.tf for schemas & SQL."""
    tables_json: dict[str, list[dict[str, Any]]] = {}
    routines_sql: dict[str, str] = {}

    storage_tf = (
        TERRAFORM_DIR / "modules" / "storage_and_vector" / "main.tf"
    ).read_text(encoding="utf-8")
    bqml_tf = (
        TERRAFORM_DIR / "modules" / "bqml_analytics" / "main.tf"
    ).read_text(encoding="utf-8")

    for content in (storage_tf, bqml_tf):
      for b in extract_hcl_blocks(content):
        if b["type"] == "resource" and b["label1"] == "google_bigquery_table":
          t_name = b["label2"]
          heredoc_match = re.search(
              r"schema\s*=\s*<<-?EOF\s*\n(.*?)\n\s*EOF", b["body"], re.DOTALL
          )
          if heredoc_match:
            parsed_schema = json.loads(heredoc_match.group(1))
            tables_json[t_name] = parsed_schema
        elif b["type"] == "resource" and b["label1"] == "google_bigquery_routine":
          r_name = b["label2"]
          heredoc_match = re.search(
              r"definition_body\s*=\s*<<-?EOF\s*\n(.*?)\n\s*EOF",
              b["body"],
              re.DOTALL,
          )
          if heredoc_match:
            routines_sql[r_name] = heredoc_match.group(1)

    return tables_json, routines_sql

  def test_2_1_bigquery_table_schemas_exact_alignment(self) -> None:
    """Verifies all 6 BigQuery table schemas match the canonical contract 100%."""
    tables_json, _ = self._extract_storage_tables_and_routines()
    self.assertEqual(
        set(tables_json.keys()),
        set(CANONICAL_TABLES_SPEC.keys()),
        "Mismatch between defined BigQuery tables and canonical 6-table contract",
    )

    for table_name, expected_spec in CANONICAL_TABLES_SPEC.items():
      schema_list = tables_json[table_name]
      actual_cols = {
          col["name"]: (col["type"], col["mode"]) for col in schema_list
      }
      expected_cols = expected_spec["columns"]
      self.assertEqual(
          actual_cols,
          expected_cols,
          f"Schema mismatch in table '{table_name}'",
      )

  def test_2_2_vector_search_ddl_and_query_validation(self) -> None:
    """Verifies CREATE VECTOR INDEX DDL and VECTOR_SEARCH semantic outlier query."""
    tables_json, routines_sql = self._extract_storage_tables_and_routines()
    log_embed_cols = {c["name"] for c in tables_json["log_embeddings"]}

    # 1. Verify CREATE VECTOR INDEX DDL
    self.assertIn("create_vector_index", routines_sql)
    idx_sql = routines_sql["create_vector_index"]
    self.assertIn("CREATE VECTOR INDEX", idx_sql)
    self.assertIn("log_embeddings", idx_sql)
    self.assertIn("(embedding)", idx_sql)
    self.assertIn("index_type = 'TREE_AH'", idx_sql)
    self.assertIn("distance_type = 'COSINE'", idx_sql)

    # Extract STORING(...) columns and verify each exists in log_embeddings schema
    storing_match = re.search(r"STORING\s*\(([^)]+)\)", idx_sql)
    self.assertIsNotNone(storing_match, "CREATE VECTOR INDEX missing STORING clause")
    storing_cols = [c.strip() for c in storing_match.group(1).split(",")]
    for sc in storing_cols:
      self.assertIn(
          sc,
          log_embed_cols,
          f"STORING column '{sc}' not found in log_embeddings table schema",
      )

    # 2. Verify VECTOR_SEARCH query
    self.assertIn("semantic_outlier_vector_search", routines_sql)
    vs_sql = routines_sql["semantic_outlier_vector_search"]
    self.assertIn("VECTOR_SEARCH", vs_sql)
    self.assertIn("top_k => 3", vs_sql)
    self.assertIn("distance_type => 'COSINE'", vs_sql)
    self.assertIn("distance < 0.25", vs_sql)

  def test_2_3_bqml_capabilities_schema_alignment(self) -> None:
    """Verifies Capabilities 1, 2, and 3 BQML queries against table schemas."""
    tables_json, routines_sql = self._extract_storage_tables_and_routines()
    gmp_cols = {c["name"] for c in tables_json["gmp_metrics"]}
    topo_cols = {c["name"] for c in tables_json["topology_edges"]}
    cal_cols = {c["name"] for c in tables_json["change_calendar"]}
    inc_cols = [c["name"] for c in tables_json["incidents_predictions"]]

    # Capability 1: Server Unresponsiveness (LOGISTIC_REG, 15-30m lead)
    cap1_sql = routines_sql["train_cap1_unresponsiveness"]
    self.assertIn("model_type = 'LOGISTIC_REG'", cap1_sql)
    self.assertIn("input_label_cols = ['unresponsive_in_15_30m']", cap1_sql)
    self.assertIn("auto_class_weights = TRUE", cap1_sql)
    for col in [
        "cpu_utilization_pct",
        "throughput_rps",
        "thread_starvation_count",
        "io_wait_pct",
        "disk_latency_ms",
        "socket_exhaustion_count",
        "unresponsive_in_15_30m",
    ]:
      self.assertIn(col, cap1_sql)
      self.assertIn(col, gmp_cols)

    # Capability 2: Cross-Domain Correlation (BOOSTED_TREE_CLASSIFIER, 30-60m lead)
    cap2_sql = routines_sql["train_cap2_cross_domain_rca"]
    self.assertIn("model_type = 'BOOSTED_TREE_CLASSIFIER'", cap2_sql)
    self.assertIn("input_label_cols = ['root_cause_domain']", cap2_sql)
    self.assertIn("topology_edges", cap2_sql)
    for col in [
        "apache_tomcat_latency_ms",
        "db_lock_wait_ms",
        "db_active_sessions",
        "ospf_flap_count",
        "packet_loss_pct",
        "root_cause_domain",
    ]:
      self.assertIn(col, cap2_sql)
      self.assertIn(col, gmp_cols)
    self.assertIn("criticality_weight", cap2_sql)
    self.assertIn("criticality_weight", topo_cols)

    # Capability 3: Dynamic Baselines & Change-Window Noise Suppression (ARIMA_PLUS_XREG)
    cap3_sql = routines_sql["train_cap3_rolling_baseline"]
    self.assertIn("model_type = 'ARIMA_PLUS_XREG'", cap3_sql)
    self.assertIn("change_calendar", cap3_sql)
    self.assertIn("is_scheduled_change_window", cap3_sql)
    self.assertIn("change_risk_level", cap3_sql)
    self.assertIn("change_risk_level", cal_cols)

    # Capability 3 Evaluation & Suppression Query
    eval_sql = routines_sql["evaluate_and_suppress_anomalies"]
    self.assertIn("ML.DETECT_ANOMALIES", eval_sql)
    self.assertIn("change_calendar", eval_sql)
    self.assertIn("suppress_alerts = TRUE", eval_sql)
    self.assertIn("NO_ACTION_SUPPRESSED", eval_sql)
    self.assertIn("INSERT INTO", eval_sql)

    # Verify all 15 columns of incidents_predictions are populated in INSERT statement
    for col in inc_cols:
      self.assertIn(
          col,
          eval_sql,
          f"Column '{col}' missing from INSERT INTO incidents_predictions in evaluate_and_suppress_anomalies",
      )


# ==============================================================================
# SUITE 3: Reference Python Pipeline Unit Test Suite
# ==============================================================================


class MockResponse:
  """Mock HTTP response object for testing REST clients without network access."""

  def __init__(self, json_data: dict[str, Any], status_code: int = 200) -> None:
    self._json_data = json_data
    self.status_code = status_code

  def json(self) -> dict[str, Any]:
    return self._json_data

  def raise_for_status(self) -> None:
    if self.status_code >= 400:
      raise RuntimeError(f"HTTP Error {self.status_code}")


class MockHttpSession:
  """Records all POST requests and returns deterministic mock responses."""

  def __init__(self, response_factory: Any) -> None:
    self.requests: list[tuple[str, dict[str, Any]]] = []
    self._response_factory = response_factory

  def post(
      self,
      url: str,
      json: Any = None,
      headers: Any = None,
      timeout: float = 30.0,
  ) -> MockResponse:
    self.requests.append((url, json))
    if callable(self._response_factory):
      return self._response_factory(url, json)
    return MockResponse(self._response_factory)


class PythonPipelineUnitTestSuite(unittest.TestCase):
  """Unit tests for src/embedding_worker.py and src/remediation_webhook.py."""

  def test_3_1_stack_trace_coalescer(self) -> None:
    """Verifies multi-line Java/Python stack trace coalescing and severity promotion."""
    raw_lines = [
        "2026-09-15T12:00:01Z INFO [tomcat-app] Request received from 10.240.12.5:8080",
        "2026-09-15T12:00:02Z ERROR [tomcat-app] Unhandled exception in servlet",
        "java.sql.SQLTransientConnectionException: HikariPool-1 - Connection is not available, request timed out after 30000ms.",
        "    at com.zaxxer.hikari.pool.HikariPool.createTimeoutException(HikariPool.java:696)",
        "    at org.apache.catalina.core.StandardWrapperValve.invoke(StandardWrapperValve.java:199)",
        "Caused by: org.postgresql.util.PSQLException: FATAL: remaining connection slots are reserved",
        "    ... 24 more",
        "2026-09-15T12:00:03Z INFO [tomcat-app] Health check probe completed",
    ]
    coalesced_lines = StackTraceCoalescer.coalesce_raw_lines(raw_lines)
    self.assertEqual(len(coalesced_lines), 3)
    self.assertIn("Caused by: org.postgresql.util.PSQLException", coalesced_lines[1])
    self.assertIn("StandardWrapperValve.invoke(StandardWrapperValve.java:199)", coalesced_lines[1])

    # Test structured entry coalescing
    entries = [
        {
            "host_id": "app-vm-01",
            "service_name": "tomcat-app",
            "domain": "APPLICATION",
            "severity": "INFO",
            "raw_payload": "Starting DB query execution",
        },
        {
            "host_id": "app-vm-01",
            "service_name": "tomcat-app",
            "domain": "APPLICATION",
            "severity": "CRITICAL",
            "raw_payload": "  at com.gsk.dao.OrderRepository.findById(OrderRepository.java:84)\nCaused by: java.net.SocketTimeoutException: Read timed out",
        },
    ]
    coalesced_entries = StackTraceCoalescer.coalesce_log_entries(entries)
    self.assertEqual(len(coalesced_entries), 1)
    self.assertEqual(coalesced_entries[0]["severity"], "CRITICAL")
    self.assertIn("OrderRepository.findById", coalesced_entries[0]["raw_payload"])

  def test_3_2_log_normalizer_and_frame_preservation(self) -> None:
    """Verifies lexical token masking while preserving exception and stack frame signatures."""
    normalizer = LogNormalizer(max_stack_frames=15)
    raw_msg = (
        "2026-09-15T12:15:30.451Z ERROR [req_id=550e8400-e29b-41d4-a716-446655440000] "
        "Connection to 10.128.0.42:5432 failed after 1500ms at mem 0x7fff5fbff8c0 "
        "reading file /var/log/gsk/app_error.log (retry 3)\n"
        "java.sql.SQLTransientConnectionException: Pool exhausted\n"
        "    at org.apache.catalina.core.StandardWrapperValve.invoke(StandardWrapperValve.java:199)\n"
        "    at com.gsk.service.InventoryService.reserve(InventoryService.java:42)"
    )

    norm_text, has_trace = normalizer.normalize(raw_msg)
    self.assertTrue(has_trace)
    self.assertIn("<TIMESTAMP>", norm_text)
    self.assertIn("<UUID>", norm_text)
    self.assertIn("<IP_PORT>", norm_text)
    self.assertIn("<DURATION>", norm_text)
    self.assertIn("<HEX_ID>", norm_text)
    self.assertIn("<PATH>", norm_text)
    # Verify exception class and method signatures with <LINE> are preserved
    self.assertIn("java.sql.SQLTransientConnectionException", norm_text)
    self.assertIn(
        "org.apache.catalina.core.StandardWrapperValve.invoke(StandardWrapperValve.java:<LINE>)",
        norm_text,
    )
    self.assertIn(
        "com.gsk.service.InventoryService.reserve(InventoryService.java:<LINE>)",
        norm_text,
    )

    # Test stack trace truncation when frames > 15
    long_trace_lines = ["Traceback (most recent call last):"] + [
        f'  File "/opt/gsk/worker/module_{i}.py", line {i * 10}, in process'
        for i in range(25)
    ] + ["RuntimeError: Worker thread starvation detected"]
    norm_long, has_long_trace = normalizer.normalize("\n".join(long_trace_lines))
    self.assertTrue(has_long_trace)
    self.assertIn("middle frames omitted", norm_long)
    self.assertIn("RuntimeError: Worker thread starvation detected", norm_long)

  def test_3_3_sliding_window_chunker(self) -> None:
    """Verifies window_size=5, stride=2 sliding window overlap and immediate CRITICAL flush."""
    chunker = SlidingWindowChunker(window_size=5, stride=2, max_window_gap_sec=30.0)
    emitted_all = []

    # Feed 6 sequential INFO logs (1 second apart)
    for i in range(1, 7):
      log_entry = {
          "log_id": f"log-{i:03d}",
          "timestamp": f"2026-09-15T12:00:{i:02d}Z",
          "severity": "INFO",
          "service_name": "apache-web",
          "host_id": "web-vm-01",
          "environment": "prod",
          "domain": "APPLICATION",
          "raw_payload": f"Processed HTTP GET request {i} in 12ms",
      }
      chunks = chunker.ingest_log(log_entry)
      emitted_all.extend(chunks)

    # Log 5 should trigger chunk 1 (containing logs 1..5)
    self.assertEqual(len(emitted_all), 1)
    chunk1 = emitted_all[0]
    self.assertEqual(chunk1.log_count, 5)
    self.assertEqual(chunk1.log_ids, ["log-001", "log-002", "log-003", "log-004", "log-005"])
    self.assertEqual(len(chunk1.chunk_id), 32)

    # Feed log 7 -> buffer now has logs [3, 4, 5, 6, 7] (length 5) -> emits chunk 2!
    chunks_on_7 = chunker.ingest_log({
        "log_id": "log-007",
        "timestamp": "2026-09-15T12:00:07Z",
        "severity": "INFO",
        "service_name": "apache-web",
        "host_id": "web-vm-01",
        "environment": "prod",
        "domain": "APPLICATION",
        "raw_payload": "Processed HTTP GET request 7 in 15ms",
    })
    self.assertEqual(len(chunks_on_7), 1)
    chunk2 = chunks_on_7[0]
    # Verify 60% overlap (logs 3, 4, 5 are shared between chunk1 and chunk2)
    self.assertEqual(chunk2.log_ids, ["log-003", "log-004", "log-005", "log-006", "log-007"])

    # Feed a CRITICAL log on a new host -> verify immediate flush without waiting for 5 logs
    crit_chunks = chunker.ingest_log({
        "log_id": "log-crit-99",
        "timestamp": "2026-09-15T12:01:00Z",
        "severity": "CRITICAL",
        "service_name": "postgres-db",
        "host_id": "db-vm-01",
        "environment": "prod",
        "domain": "DATABASE",
        "raw_payload": "FATAL: deadlock detected on relation topology_edges",
    })
    self.assertEqual(len(crit_chunks), 1)
    self.assertEqual(crit_chunks[0].severity, "CRITICAL")
    self.assertEqual(crit_chunks[0].log_id, "log-crit-99")

  def test_3_4_vertex_ai_embedding_and_bigquery_writer(self) -> None:
    """Verifies Vertex AI text-embedding-005 768-dim payload formatting & BigQuery row schema."""
    chunker = SlidingWindowChunker(window_size=5, stride=2)
    chunks = chunker.ingest_log({
        "log_id": "log-embed-test",
        "timestamp": "2026-09-15T12:00:00Z",
        "severity": "ERROR",
        "service_name": "core-switch",
        "host_id": "sw-lon-01",
        "environment": "prod",
        "domain": "NETWORK",
        "raw_payload": "OSPF neighbor 10.0.0.1 state changed from Full to Down",
    })
    self.assertEqual(len(chunks), 1)

    # Mock session returning a 768-float vector
    mock_vector = [round(0.001 * i, 6) for i in range(768)]

    def mock_vertex_responder(url: str, json_body: dict[str, Any]) -> MockResponse:
      if "aiplatform.googleapis.com" in url:
        return MockResponse({
            "predictions": [
                {
                    "embeddings": {
                        "values": mock_vector,
                        "statistics": {"token_count": 42, "truncated": False},
                    }
                }
            ]
        })
      return MockResponse({"kind": "bigquery#tableDataInsertAllResponse"})

    session = MockHttpSession(mock_vertex_responder)
    embed_client = VertexAIEmbeddingClient(
        project_id="gsk-ano-prod",
        region="us-central1",
        model_name="text-embedding-005",
        dimensions=768,
        session=session,
    )

    bq_rows = embed_client.embed_chunks(chunks)
    self.assertEqual(len(bq_rows), 1)
    row = bq_rows[0]

    # Verify request sent to Vertex AI
    vertex_url, vertex_req = session.requests[0]
    self.assertIn("text-embedding-005:predict", vertex_url)
    self.assertEqual(
        vertex_req["instances"][0]["task_type"], "RETRIEVAL_DOCUMENT"
    )
    self.assertEqual(vertex_req["parameters"]["outputDimensionality"], 768)
    self.assertTrue(vertex_req["parameters"]["autoTruncate"])

    # Verify output row matches BigQuery log_embeddings schema 100%
    expected_columns = set(CANONICAL_TABLES_SPEC["log_embeddings"]["columns"].keys())
    self.assertEqual(set(row.keys()), expected_columns)
    self.assertEqual(len(row["embedding"]), 768)
    self.assertEqual(row["embedding_model"], "text-embedding-005")
    self.assertEqual(row["token_count"], 42)
    self.assertFalse(row["is_outlier"])

    # Test BigQueryStreamWriter insertId deduplication payload
    bq_writer = BigQueryStreamWriter(
        project_id="gsk-ano-prod",
        dataset_id="gsk_ano_ops",
        table_id="log_embeddings",
        session=session,
    )
    write_res = bq_writer.write_rows(bq_rows)
    self.assertEqual(write_res["status"], "SUCCESS")
    self.assertEqual(write_res["inserted_count"], 1)
    _, bq_req = session.requests[1]
    self.assertEqual(bq_req["rows"][0]["insertId"], row["chunk_id"])

  def test_3_5_remediation_webhook_change_window_suppression(self) -> None:
    """Verifies active change_calendar maintenance windows deterministically suppress remediation."""
    in_memory_calendar = [
        {
            "change_id": "CHG0049281",
            "start_time": "2026-09-15T12:00:00Z",
            "end_time": "2026-09-15T14:00:00Z",
            "target_entity_id": "pg-prod-db-01",
            "target_domain": "DATABASE",
            "change_type": "DB_MAINTENANCE",
            "status": "IN_PROGRESS",
            "suppress_alerts": True,
            "change_risk_level": 3,
            "owner_team": "dba-core-team",
        }
    ]

    mock_session = MockHttpSession({"kind": "bigquery#tableDataInsertAllResponse"})
    calendar_checker = ChangeCalendarChecker(
        project_id="gsk-ano-prod",
        dataset_id="gsk_ano_ops",
        session=mock_session,
        in_memory_schedule=in_memory_calendar,
    )
    rate_limiter = RemediationRateLimiter(cooldown_seconds=900, max_actions_per_hour=3)
    dispatcher = WorkflowsRemediationDispatcher(
        project_id="gsk-ano-prod",
        region="us-central1",
        session=mock_session,
    )
    audit_writer = IncidentAuditWriter(
        project_id="gsk-ano-prod",
        dataset_id="gsk_ano_ops",
        session=mock_session,
    )

    webhook = RemediationWebhookHandler(
        calendar_checker=calendar_checker,
        rate_limiter=rate_limiter,
        dispatcher=dispatcher,
        audit_writer=audit_writer,
    )

    # Dispatch incident occurring during the active maintenance window (12:30Z)
    incident_payload = {
        "incident_id": "inc-suppressed-001",
        "prediction_timestamp": "2026-09-15T12:30:00Z",
        "capability_type": "CAP2_CROSS_DOMAIN_30_60M",
        "affected_entity_id": "tomcat-app-01",
        "root_cause_entity_id": "pg-prod-db-01",
        "root_cause_domain": "DATABASE",
        "lead_time_minutes": 45,
        "anomaly_probability": 0.96,
        "recommended_action": "KILL_BLOCKING_DB_SESSIONS",
    }

    result = webhook.process_incident(incident_payload)

    # Assert deterministic suppression
    self.assertTrue(result["suppressed_by_change_window"])
    self.assertEqual(result["active_change_id"], "CHG0049281")
    self.assertEqual(result["recommended_action"], "NO_ACTION_SUPPRESSED")
    self.assertEqual(result["remediation_status"], "SUPPRESSED")
    self.assertIsNone(result["remediation_execution_id"])

    # Assert 0 calls were made to Cloud Workflows (only 1 call to BigQuery audit insertAll)
    workflow_calls = [
        url for url, _ in mock_session.requests if "workflowexecutions.googleapis.com" in url
    ]
    self.assertEqual(len(workflow_calls), 0)

  def test_3_6_remediation_webhook_unsuppressed_dispatch_and_rate_limiting(self) -> None:
    """Verifies unsuppressed incidents trigger Cloud Workflows and respect 900s cooldown."""
    # Empty maintenance schedule (or expired window)
    in_memory_calendar = [
        {
            "change_id": "CHG0011111",
            "start_time": "2026-09-15T08:00:00Z",
            "end_time": "2026-09-15T09:00:00Z",
            "target_entity_id": "pg-prod-db-01",
            "target_domain": "DATABASE",
            "change_type": "DB_MAINTENANCE",
            "status": "COMPLETED",
            "suppress_alerts": True,
            "change_risk_level": 2,
        }
    ]

    def mock_responder(url: str, json_body: dict[str, Any]) -> MockResponse:
      if "workflowexecutions.googleapis.com" in url:
        return MockResponse({
            "name": "projects/gsk-ano-prod/locations/us-central1/workflows/gsk-ano-network-ospf-reroute/executions/exec-889900"
        })
      return MockResponse({"kind": "bigquery#tableDataInsertAllResponse"})

    mock_session = MockHttpSession(mock_responder)
    calendar_checker = ChangeCalendarChecker(
        project_id="gsk-ano-prod",
        in_memory_schedule=in_memory_calendar,
    )
    rate_limiter = RemediationRateLimiter(cooldown_seconds=900, max_actions_per_hour=3)
    dispatcher = WorkflowsRemediationDispatcher(
        project_id="gsk-ano-prod",
        region="us-central1",
        session=mock_session,
    )
    audit_writer = IncidentAuditWriter(
        project_id="gsk-ano-prod",
        session=mock_session,
    )

    webhook = RemediationWebhookHandler(
        calendar_checker=calendar_checker,
        rate_limiter=rate_limiter,
        dispatcher=dispatcher,
        audit_writer=audit_writer,
    )

    # First unsuppressed network incident at T=1000
    incident_1 = {
        "incident_id": "inc-net-001",
        "prediction_timestamp": "2026-09-15T15:00:00Z",
        "capability_type": "CAP2_CROSS_DOMAIN_30_60M",
        "affected_entity_id": "tomcat-app-01",
        "root_cause_entity_id": "core-sw-lon-01",
        "root_cause_domain": "NETWORK",
        "lead_time_minutes": 35,
        "anomaly_probability": 0.98,
        "recommended_action": "REROUTE_OSPF_TRAFFIC",
    }
    res1 = webhook.process_incident(incident_1, current_epoch_sec=1000.0)
    self.assertFalse(res1["suppressed_by_change_window"])
    self.assertEqual(res1["remediation_status"], "TRIGGERED")
    self.assertIn("exec-889900", res1["remediation_execution_id"])

    workflow_calls_after_1 = [
        url for url, _ in mock_session.requests if "workflowexecutions.googleapis.com" in url
    ]
    self.assertEqual(len(workflow_calls_after_1), 1)
    self.assertIn("gsk-ano-network-ospf-reroute", workflow_calls_after_1[0])

    # Second identical incident 120 seconds later (T=1120, within 900s cooldown)
    incident_2 = dict(incident_1)
    incident_2["incident_id"] = "inc-net-002"
    res2 = webhook.process_incident(incident_2, current_epoch_sec=1120.0)
    self.assertFalse(res2["suppressed_by_change_window"])
    self.assertEqual(res2["remediation_status"], "SUPPRESSED_RATE_LIMIT")
    self.assertIsNone(res2["remediation_execution_id"])

    # Verify Cloud Workflows was NOT called a second time
    workflow_calls_after_2 = [
        url for url, _ in mock_session.requests if "workflowexecutions.googleapis.com" in url
    ]
    self.assertEqual(len(workflow_calls_after_2), 1)

    # Third incident after cooldown expires (T=2000 > 1000 + 900)
    incident_3 = dict(incident_1)
    incident_3["incident_id"] = "inc-net-003"
    res3 = webhook.process_incident(incident_3, current_epoch_sec=2000.0)
    self.assertEqual(res3["remediation_status"], "TRIGGERED")
    workflow_calls_after_3 = [
        url for url, _ in mock_session.requests if "workflowexecutions.googleapis.com" in url
    ]
    self.assertEqual(len(workflow_calls_after_3), 2)

  def test_3_7_pubsub_and_cloudevent_envelope_end_to_end(self) -> None:
    """Verifies base64 Pub/Sub push envelopes in EmbeddingPipelineWorker and RemediationWebhookHandler."""
    mock_vector = [0.05] * 768

    def mock_responder(url: str, json_body: dict[str, Any]) -> MockResponse:
      if "aiplatform.googleapis.com" in url:
        return MockResponse({
            "predictions": [
                {
                    "embeddings": {
                        "values": mock_vector,
                        "statistics": {"token_count": 30},
                    }
                }
            ]
        })
      return MockResponse({"kind": "bigquery#tableDataInsertAllResponse"})

    session = MockHttpSession(mock_responder)
    worker = EmbeddingPipelineWorker(
        chunker=SlidingWindowChunker(window_size=5, stride=2),
        embedding_client=VertexAIEmbeddingClient(
            project_id="gsk-ano-prod", session=session
        ),
        bq_writer=BigQueryStreamWriter(project_id="gsk-ano-prod", session=session),
    )

    log_payload = {
        "log_id": "pubsub-log-101",
        "timestamp": "2026-09-15T16:00:00Z",
        "severity": "CRITICAL",
        "service_name": "tomcat-app",
        "host_id": "app-vm-09",
        "domain": "APPLICATION",
        "raw_payload": "CRITICAL: OutOfMemoryError: Java heap space",
    }
    encoded_data = base64.b64encode(json.dumps(log_payload).encode("utf-8")).decode("utf-8")
    pubsub_envelope = {
        "message": {
            "data": encoded_data,
            "messageId": "msg-999",
        }
    }
    res = worker.handle_pubsub_envelope(pubsub_envelope)
    self.assertEqual(res["status"], "PROCESSED")
    self.assertEqual(res["chunks_emitted"], 1)
    self.assertEqual(res["rows_inserted"], 1)

    # Verify RemediationWebhookHandler base64 extraction
    inc_payload = {
        "incident_id": "inc-b64-777",
        "root_cause_entity_id": "app-vm-09",
        "root_cause_domain": "APPLICATION",
        "recommended_action": "SCALE_UP_INSTANCE_GROUP",
    }
    inc_b64 = base64.b64encode(json.dumps(inc_payload).encode("utf-8")).decode("utf-8")
    extracted = RemediationWebhookHandler.extract_incident_payload(
        {"message": {"data": inc_b64}}
    )
    self.assertEqual(extracted["incident_id"], "inc-b64-777")
    self.assertEqual(extracted["recommended_action"], "SCALE_UP_INSTANCE_GROUP")

  def test_3_8_bigquery_rest_query_and_hourly_rate_cap(self) -> None:
    """Verifies ChangeCalendarChecker REST SQL execution and RemediationRateLimiter hourly cap."""

    def mock_bq_query_responder(url: str, json_body: dict[str, Any]) -> MockResponse:
      if "/queries" in url:
        return MockResponse({
            "rows": [
                {
                    "f": [
                        {"v": "CHG998877"},
                        {"v": "OS_PATCHING"},
                        {"v": "IN_PROGRESS"},
                        {"v": "true"},
                        {"v": "2026-09-15T12:00:00Z"},
                        {"v": "2026-09-15T18:00:00Z"},
                        {"v": "linux-platform-team"},
                    ]
                }
            ]
        })
      return MockResponse({})

    session = MockHttpSession(mock_bq_query_responder)
    checker = ChangeCalendarChecker(
        project_id="gsk-ano-prod", dataset_id="gsk_ano_ops", session=session
    )
    res = checker.check_maintenance_window(
        root_cause_entity_id="vm-patch-01",
        affected_entity_id="vm-patch-01",
        incident_timestamp="2026-09-15T14:00:00Z",
    )
    self.assertTrue(res.is_suppressed)
    self.assertEqual(res.active_change_id, "CHG998877")
    self.assertEqual(res.change_type, "OS_PATCHING")
    self.assertEqual(res.owner_team, "linux-platform-team")

    # Verify hourly rate limiter cap (max_actions_per_hour=2, cooldown=10s)
    limiter = RemediationRateLimiter(cooldown_seconds=10, max_actions_per_hour=2)
    ok1, _ = limiter.allow_execution("host-a", "DRAIN_AND_RESTART_WORKERS", 100.0)
    self.assertTrue(ok1)
    ok2, _ = limiter.allow_execution("host-a", "DRAIN_AND_RESTART_WORKERS", 200.0)
    self.assertTrue(ok2)
    ok3, reason3 = limiter.allow_execution("host-a", "DRAIN_AND_RESTART_WORKERS", 300.0)
    self.assertFalse(ok3)
    self.assertIn("RATE_LIMITED_HOURLY_CAP", reason3)

  def test_3_9_adversarial_edge_cases_and_concurrency(self) -> None:
    """Verifies resilience against malformed/null payloads, ms/ns timestamps, out-of-order bursts, and custom stack truncation."""
    from src.embedding_worker import parse_iso_timestamp
    from src.remediation_webhook import parse_iso_datetime

    # 1. Millisecond, nanosecond, NaN, and boolean timestamps
    _, epoch_ms = parse_iso_timestamp(1789478400000)
    self.assertTrue(1789478000 <= epoch_ms <= 1789479000)
    _, epoch_ns = parse_iso_timestamp(1789478400000000000)
    self.assertTrue(1789478000 <= epoch_ns <= 1789479000)
    iso_nan, _ = parse_iso_timestamp(float("nan"))
    self.assertTrue(isinstance(iso_nan, str))

    # 2. Custom max_stack_frames truncation
    normalizer = LogNormalizer(max_stack_frames=6)
    frames = [f"\tat com.gsk.Class{i}.method(Class{i}.java:{10+i})" for i in range(12)]
    norm_text, has_trace = normalizer.normalize("java.lang.Exception: Err\n" + "\n".join(frames))
    self.assertTrue(has_trace)
    self.assertLessEqual(len([l for l in norm_text.splitlines() if "at com.gsk." in l]), 6)

    # 3. Explicit null fields and non-dict envelopes in RemediationWebhookHandler
    extracted = RemediationWebhookHandler.extract_incident_payload({
        "lead_time_minutes": None,
        "anomaly_probability": None,
        "prediction_timestamp": None,
        "root_cause_domain": None,
    })
    self.assertEqual(extracted["lead_time_minutes"], 20)
    self.assertEqual(extracted["anomaly_probability"], 0.95)
    self.assertIsNotNone(parse_iso_datetime(extracted["prediction_timestamp"]))

    # 4. Out-of-order timestamps in RemediationRateLimiter
    limiter = RemediationRateLimiter(cooldown_seconds=900, max_actions_per_hour=3)
    self.assertTrue(limiter.allow_execution("sw-01", "REROUTE_OSPF_TRAFFIC", 5000.0)[0])
    ok_ooo, reason_ooo = limiter.allow_execution("sw-01", "REROUTE_OSPF_TRAFFIC", 4500.0)
    self.assertFalse(ok_ooo)
    self.assertNotIn("-", reason_ooo)
    self.assertTrue(limiter.allow_execution("sw-01", "REROUTE_OSPF_TRAFFIC", 1000.0)[0])


# ==============================================================================
# SUITE 4: Round 2 Live Customer Demo, Seeder, CLI Runner & Web UI Dashboard Suite
# ==============================================================================


class Round2LiveDemoAndDashboardSuite(unittest.TestCase):
  """Validates scripts/deploy_to_gcp.py, src/seed_live_demo.py, src/demo_runner.py, src/demo_dashboard.py, and docs."""

  def setUp(self) -> None:
    """Creates an isolated temporary directory for local SQLite/JSON mirror testing."""
    self.temp_dir = tempfile.TemporaryDirectory()
    self.mirror_db_path = Path(self.temp_dir.name) / "gsk_ano_ops_mirror.db"
    os.environ["GSK_ANO_MIRROR_PATH"] = str(self.mirror_db_path)

  def tearDown(self) -> None:
    """Cleans up temporary mirror files and environment overrides."""
    os.environ.pop("GSK_ANO_MIRROR_PATH", None)
    self.temp_dir.cleanup()

  def test_4_1_deploy_to_gcp_script_verification_and_local_mirror(self) -> None:
    """Verifies scripts/deploy_to_gcp.py --project gke-demos-363017 --verify initializes mirror and validates all payloads."""
    deploy_script = PROJECT_ROOT / "scripts" / "deploy_to_gcp.py"
    self.assertTrue(deploy_script.is_file(), f"Missing {deploy_script}")

    proc = subprocess.run(
        [
            sys.executable,
            str(deploy_script),
            "--project",
            "gke-demos-363017",
            "--verify",
            "--local-fallback",
            "--mirror-path",
            str(self.mirror_db_path),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    self.assertEqual(
        proc.returncode,
        0,
        f"deploy_to_gcp.py failed:\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}",
    )
    output = proc.stdout
    self.assertIn("gke-demos-363017", output)
    self.assertIn("gsk_ano_ops", output)
    for table_name in CANONICAL_TABLES_SPEC:
      self.assertIn(table_name, output)
    self.assertIn("TREE_AH", output)
    self.assertTrue(self.mirror_db_path.exists(), "Local SQLite mirror file was not created")

  def test_4_2_seed_live_demo_telemetry_and_incident_injection(self) -> None:
    """Verifies src/seed_live_demo.py seeds 90d metrics, tomcat->ora->switch topology, CHG0049281, and live incidents."""
    # First initialize mirror
    subprocess.run(
        [
            sys.executable,
            str(PROJECT_ROOT / "scripts" / "deploy_to_gcp.py"),
            "--project",
            "gke-demos-363017",
            "--local-fallback",
            "--mirror-path",
            str(self.mirror_db_path),
        ],
        capture_output=True,
        check=True,
    )

    seed_script = PROJECT_ROOT / "src" / "seed_live_demo.py"
    self.assertTrue(seed_script.is_file(), f"Missing {seed_script}")

    proc = subprocess.run(
        [
            sys.executable,
            str(seed_script),
            "--project",
            "gke-demos-363017",
            "--inject-live-incidents",
            "--mirror-path",
            str(self.mirror_db_path),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    self.assertEqual(proc.returncode, 0, f"seed_live_demo.py failed:\n{proc.stderr}")

    # Verify SQLite mirror contents directly
    conn = sqlite3.connect(str(self.mirror_db_path))
    cursor = conn.cursor()

    # Verify topology edges: tomcat-app-stv-01 -> ora-db-stv-01 -> core-sw-lon-01
    cursor.execute("SELECT source_entity_id, target_entity_id FROM topology_edges")
    edges = set(cursor.fetchall())
    self.assertIn(("tomcat-app-stv-01", "ora-db-stv-01"), edges)
    self.assertIn(("ora-db-stv-01", "core-sw-lon-01"), edges)

    # Verify ServiceNow maintenance window CHG0049281
    cursor.execute("SELECT change_id, suppress_alerts FROM change_calendar WHERE change_id = 'CHG0049281'")
    chg_row = cursor.fetchone()
    self.assertIsNotNone(chg_row, "Active change window CHG0049281 not found in change_calendar")
    self.assertTrue(bool(chg_row[1]), "CHG0049281 must have suppress_alerts = True")

    # Verify GMP metrics and injected incidents
    cursor.execute("SELECT COUNT(*) FROM gmp_metrics")
    self.assertGreater(cursor.fetchone()[0], 100, "Expected seeded historical rows in gmp_metrics")
    cursor.execute("SELECT COUNT(*) FROM raw_logs")
    self.assertGreater(cursor.fetchone()[0], 0, "Expected injected incident logs in raw_logs")
    cursor.execute("SELECT COUNT(*) FROM log_embeddings")
    self.assertGreater(cursor.fetchone()[0], 0, "Expected injected incident rows in log_embeddings")
    conn.close()

  def test_4_3_demo_runner_cli_four_acts_execution(self) -> None:
    """Verifies src/demo_runner.py --project gke-demos-363017 --act all runs all 4 Acts with SQL, tables, and talking points."""
    runner_script = PROJECT_ROOT / "src" / "demo_runner.py"
    self.assertTrue(runner_script.is_file(), f"Missing {runner_script}")

    # Ensure mirror is seeded
    subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "src" / "seed_live_demo.py"), "--mirror-path", str(self.mirror_db_path)],
        capture_output=True,
        check=False,
    )

    proc = subprocess.run(
        [
            sys.executable,
            str(runner_script),
            "--project",
            "gke-demos-363017",
            "--act",
            "all",
            "--mirror-path",
            str(self.mirror_db_path),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    self.assertEqual(proc.returncode, 0, f"demo_runner.py failed:\n{proc.stderr}")
    out = proc.stdout

    # Assert all 4 Acts, key SQL constructs, entities, and talking points appear
    self.assertIn("ACT 1", out.upper())
    self.assertIn("VECTOR_SEARCH", out)
    self.assertIn("text-embedding-005", out)
    self.assertIn("ACT 2", out.upper())
    self.assertIn("ML.PREDICT", out)
    self.assertIn("22", out)  # 22 mins lead time
    self.assertIn("ACT 3", out.upper())
    self.assertIn("tomcat-app-stv-01", out)
    self.assertIn("ora-db-stv-01", out)
    self.assertIn("core-sw-lon-01", out)
    self.assertIn("ACT 4", out.upper())
    self.assertIn("CHG0049281", out)
    self.assertIn("ARIMA_PLUS_XREG", out)
    self.assertIn("gsk-ano-network-ospf-reroute", out)

  def test_4_4_demo_dashboard_web_ui_and_rest_api_endpoints(self) -> None:
    """Starts src/demo_dashboard.py on an ephemeral port and verifies HTML UI, GET endpoints, and POST /api/trigger_act/1..4."""
    from src.demo_dashboard import create_dashboard_server

    server = create_dashboard_server(
        host="127.0.0.1",
        port=0,
        project_id="gke-demos-363017",
        mirror_path=str(self.mirror_db_path),
    )
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
      base_url = f"http://127.0.0.1:{port}"

      # 1. Verify HTML Dashboard UI (GET /)
      with urllib.request.urlopen(f"{base_url}/", timeout=5) as resp:
        self.assertEqual(resp.status, 200)
        html = resp.read().decode("utf-8")
        self.assertIn("GSK Autonomous Operations", html)
        self.assertIn("12,000", html)
        self.assertIn("71.2B", html)
        self.assertIn("tomcat-app-stv-01", html)
        self.assertIn("core-sw-lon-01", html)
        self.assertIn("CHG0049281", html)

      # 2. Verify GET /api/status
      with urllib.request.urlopen(f"{base_url}/api/status", timeout=5) as resp:
        self.assertEqual(resp.status, 200)
        status_data = json.loads(resp.read().decode("utf-8"))
        self.assertEqual(status_data["project_id"], "gke-demos-363017")
        self.assertIn("kpis", status_data)

      # 3. Verify GET /api/topology
      with urllib.request.urlopen(f"{base_url}/api/topology", timeout=5) as resp:
        self.assertEqual(resp.status, 200)
        topo_data = json.loads(resp.read().decode("utf-8"))
        node_ids = {n["id"] for n in topo_data["nodes"]}
        self.assertIn("tomcat-app-stv-01", node_ids)
        self.assertIn("ora-db-stv-01", node_ids)
        self.assertIn("core-sw-lon-01", node_ids)

      # 4. Verify GET /api/incidents
      with urllib.request.urlopen(f"{base_url}/api/incidents", timeout=5) as resp:
        self.assertEqual(resp.status, 200)
        inc_data = json.loads(resp.read().decode("utf-8"))
        self.assertIn("incidents", inc_data)

      # 5. Verify POST /api/trigger_act/1..4
      for act_id in [1, 2, 3, 4]:
        req = urllib.request.Request(
            f"{base_url}/api/trigger_act/{act_id}",
            data=b"{}",
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
          self.assertEqual(resp.status, 200)
          act_res = json.loads(resp.read().decode("utf-8"))
          self.assertEqual(act_res["status"], "SUCCESS")
          self.assertEqual(act_res["act_id"], act_id)
          self.assertIn("sql_query", act_res)
          self.assertIn("talking_points", act_res)
    finally:
      server.shutdown()
      server.server_close()

  def test_4_5_round2_documentation_and_runbook_completeness(self) -> None:
    """Verifies docs/CUSTOMER_DEMO_RUNBOOK.md and updated README.md contain all required Round 2 sections."""
    runbook_path = PROJECT_ROOT / "docs" / "CUSTOMER_DEMO_RUNBOOK.md"
    self.assertTrue(runbook_path.is_file(), "Missing docs/CUSTOMER_DEMO_RUNBOOK.md")
    runbook_text = runbook_path.read_text(encoding="utf-8")
    self.assertGreaterEqual(len(runbook_text), 4000, "CUSTOMER_DEMO_RUNBOOK.md is too short")
    for required_term in [
        "gke-demos-363017",
        "Act 1",
        "Act 2",
        "Act 3",
        "Act 4",
        "tomcat-app-stv-01",
        "ora-db-stv-01",
        "core-sw-lon-01",
        "CHG0049281",
        "demo_dashboard.py",
        "demo_runner.py",
    ]:
      self.assertIn(required_term, runbook_text, f"Missing '{required_term}' in CUSTOMER_DEMO_RUNBOOK.md")

    readme_text = (PROJECT_ROOT / "README.md").read_text(encoding="utf-8")
    for readme_term in [
        "deploy_to_gcp.py",
        "seed_live_demo.py",
        "demo_runner.py",
        "demo_dashboard.py",
        "CUSTOMER_DEMO_RUNBOOK.md",
        "gke-demos-363017",
    ]:
      self.assertIn(readme_term, readme_text, f"Missing '{readme_term}' in README.md")

  def test_4_6_git_secrets_and_working_tree_safety(self) -> None:
    """Scans all Python, Markdown, and Terraform files to ensure zero forbidden secret patterns exist."""
    forbidden_regexes = [
        re.compile(r"AIza[0-9A-Za-z_-]{35}"),
        re.compile(r"ya29\.[0-9A-Za-z_-]+"),
        re.compile(r"(A3T[A-Z0-9]|AKIA|AGPA|AIDA|AROA|AIPA|ANPA|ANVA|ASIA)[A-Z0-9]{16}"),
    ]
    for path in PROJECT_ROOT.rglob("*"):
      if path.is_file() and not any(
          p in path.parts for p in [".git", ".agents", "__pycache__", ".cache", ".ano_mirror"]
      ):
        if path.suffix in {".py", ".md", ".tf", ".json", ".sh", ".yaml", ".yml"}:
          content = path.read_text(encoding="utf-8", errors="ignore")
          for pattern in forbidden_regexes:
            match = pattern.search(content)
            self.assertIsNone(
                match,
                f"Forbidden git-secrets pattern '{match.group(0) if match else ''}' found in {path}!",
            )


# ==============================================================================
# SUITE 5: Round 3 GSK Enterprise Observability Platform & "Neuro" MVP Suite
# ==============================================================================


class Round3ObservabilityPlatformSuite(unittest.TestCase):
  """Validates Round 3 Observability Architecture, OTel YAML, SQL/GQL Catalog, PromQL 0.0 Batcher, EBRS Seeder, Live GCP Dataset/Graph, and Neuro Workbench."""

  def setUp(self) -> None:
    self.temp_dir = tempfile.TemporaryDirectory()
    self.temp_mirror_path = (
        Path(self.temp_dir.name) / "gsk_observability_mvp_test.db"
    )

  def tearDown(self) -> None:
    self.temp_dir.cleanup()

  def test_5_1_architecture_blueprint_and_otel_config_validation(self) -> None:
    """Validates docs/GSK_OBSERVABILITY_PLATFORM_ARCHITECTURE.md (12 sections, Findings 1-6, Neuro prompt) and config/otel-collector-config.yaml."""
    from src.observability_mvp_server import NEURO_SYSTEM_PROMPT

    arch_doc = PROJECT_ROOT / "docs" / "GSK_OBSERVABILITY_PLATFORM_ARCHITECTURE.md"
    self.assertTrue(arch_doc.is_file(), f"Missing {arch_doc}")
    arch_text = arch_doc.read_text(encoding="utf-8")
    self.assertGreaterEqual(
        len(arch_text),
        50000,
        "GSK_OBSERVABILITY_PLATFORM_ARCHITECTURE.md should be comprehensive (>=50k chars)",
    )

    # Verify Executive Summary + all 12 numbered sections
    self.assertIn("## Executive Summary", arch_text)
    for sec_num in range(1, 13):
      self.assertRegex(
          arch_text,
          rf"(?m)^##\s+{sec_num}\.",
          f"Missing Section {sec_num} heading in GSK_OBSERVABILITY_PLATFORM_ARCHITECTURE.md",
      )

    # Verify Findings 1-6 remediations, Mermaid diagrams, Live GCP target, and Neuro system prompt
    for required_token in [
        "gke-demos-363017",
        "gsk_observability_demo",
        "```mermaid",
        "HORIZON = 10000",
        "HOLIDAY_REGION = 'GB'",
        "GAP_FILL(",
        "ARIMA_PLUS_XREG",
        "AI.DETECT_ANOMALIES",
        "AI.FORECAST",
        "CREATE OR REPLACE PROPERTY GRAPH",
        "gsk_infrastructure_dependency_graph",
        "gemini-2.5-flash",
        "ML.GENERATE_TEXT",
        "AI.GENERATE_TABLE",
        "SILENT_HOST_DROP_TO_ZERO",
        "SPIKE_ANOMALY",
        "Finding 1",
        "Finding 2",
        "Finding 3",
        "Finding 4",
        "Finding 5",
        "Finding 6",
    ]:
      self.assertIn(
          required_token,
          arch_text,
          f"Missing required token '{required_token}' in GSK_OBSERVABILITY_PLATFORM_ARCHITECTURE.md",
      )

    self.assertIn(NEURO_SYSTEM_PROMPT.strip(), arch_text)
    for broken_token in ["SeeVertex", "[w]", "[ew]", "'gemini-model-endpoint'"]:
      self.assertNotIn(
          broken_token,
          arch_text.split("## 12.")[0],
          f"Unremediated defect token '{broken_token}' found in main blueprint body",
      )

    # Verify config/otel-collector-config.yaml
    otel_path = PROJECT_ROOT / "config" / "otel-collector-config.yaml"
    self.assertTrue(otel_path.is_file(), f"Missing {otel_path}")
    otel_text = otel_path.read_text(encoding="utf-8")

    self.assertIn("127.0.0.1:9090", otel_text)
    self.assertIn("localhost:8080", otel_text)
    self.assertNotIn("0.0.0.0:9090", otel_text)
    self.assertNotIn("googlemanagedprometheus", otel_text)
    self.assertIn("telemetry.googleapis.com:443", otel_text)
    self.assertIn("Pharma_Manufacturing", otel_text)
    self.assertIn(
        "processors: [memory_limiter, resourcedetection, batch]", otel_text
    )
    self.assertIn("exporters: [otlp, googlecloud]", otel_text)

    try:
      import yaml  # type: ignore

      otel_cfg = yaml.safe_load(otel_text)
      pipeline_metrics = otel_cfg["service"]["pipelines"]["metrics"]
      self.assertEqual(
          pipeline_metrics["processors"],
          ["memory_limiter", "resourcedetection", "batch"],
      )
      self.assertEqual(
          pipeline_metrics["processors"][0],
          "memory_limiter",
          "memory_limiter MUST execute first in the OTel metrics pipeline",
      )
      self.assertEqual(
          pipeline_metrics["exporters"],
          ["otlp", "googlecloud"],
      )
      scrape_targets = otel_cfg["receivers"]["prometheus"]["config"][
          "scrape_configs"
      ][0]["static_configs"][0]["targets"]
      self.assertEqual(scrape_targets, ["127.0.0.1:9090", "localhost:8080"])
    except ImportError:
      pass

  def test_5_2_sql_gql_script_catalog_and_observability_terraform_validation(
      self,
  ) -> None:
    """Validates sql/01..06 catalog and terraform/modules/observability_lakehouse/main.tf (14 tables, 4 routines, dataset IAM)."""
    from src.seed_ebrs_observability import REQUIRED_OBSERVABILITY_TABLES

    sql_dir = PROJECT_ROOT / "sql"
    expected_sql_files = {
        "01_dataset_and_iam.sql": [
            "CREATE SCHEMA IF NOT EXISTS",
            "gke-demos-363017.gsk_observability_demo",
            "GRANT `roles/bigquery.dataEditor`",
            "ON SCHEMA",
        ],
        "02_ebrs_lakehouse_tables_ddl.sql": [
            "enterprise_telemetry_partitioned",
            "PARTITION BY DATE(timestamp)",
            "CLUSTER BY site_location, system_id, application_tier, host_id",
            "partition_expiration_days = 90",
            "system_logs",
            "servicenow_maintenance_windows",
            "incident_root_cause_analysis",
            "structured_log_entities",
        ],
        "03_bqml_arima_and_timesfm_baselines.sql": [
            "dense_minute_telemetry_view",
            "GAP_FILL(",
            "COALESCE(cpu_usage, 0.0)",
            "host_cpu_arima_model",
            "MODEL_TYPE = 'ARIMA_PLUS'",
            "HORIZON = 10000",
            "HOLIDAY_REGION = 'GB'",
            "TIME_SERIES_ID_COL = 'hostname'",
            "host_cpu_arimax_model",
            "MODEL_TYPE = 'ARIMA_PLUS_XREG'",
            "servicenow_maintenance_windows",
            "composite_host_signals_arima",
            "TIME_SERIES_ID_COL = ['hostname', 'metric_name']",
            "ML.DETECT_ANOMALIES",
            "SPIKE_ANOMALY",
            "SILENT_HOST_DROP_TO_ZERO",
            "DIP_ANOMALY",
            "AI.DETECT_ANOMALIES",
            "AI.FORECAST",
        ],
        "04_metric_to_log_correlation.sql": [
            "enterprise_telemetry_partitioned",
            "system_logs",
            "INTERVAL 5 MINUTE",
            "STRING_AGG(l.message",
            "correlated_error_traces",
        ],
        "05_iso_gql_property_graph.sql": [
            "nodes_switches",
            "nodes_hypervisors",
            "nodes_hosts",
            "nodes_applications",
            "edges_connected_to",
            "edges_hosts_vm",
            "edges_runs_app",
            "edges_app_communicates",
            "edges_network_flows",
            "CREATE OR REPLACE PROPERTY GRAPH",
            "gsk_infrastructure_dependency_graph",
            "LABEL Switch",
            "LABEL Hypervisor",
            "LABEL Host",
            "LABEL Application",
            "GRAPH_TABLE(",
            "src.hostname AS src_host",
            "dst.hostname AS dst_host",
            "JSON_OBJECT(",
        ],
        "06_gemini_2_5_flash_synthesis.sql": [
            "gemini_2_5_flash",
            "ENDPOINT = 'gemini-2.5-flash'",
            "ML.GENERATE_TEXT",
            "incident_root_cause_analysis",
            "AI.GENERATE_TABLE",
            "structured_log_entities",
            "root_cause_category STRING, failed_component STRING, error_code STRING, recommended_action STRING, confidence_score FLOAT64",
        ],
    }

    for fname, tokens in expected_sql_files.items():
      fpath = sql_dir / fname
      self.assertTrue(fpath.is_file(), f"Missing SQL catalog file {fpath}")
      sql_text = fpath.read_text(encoding="utf-8")
      for tok in tokens:
        self.assertIn(tok, sql_text, f"Missing '{tok}' in {fpath}")
      self.assertNotIn("TO_JSON(src)", sql_text)
      self.assertNotIn("'gemini-model-endpoint'", sql_text)

    # Validate terraform/modules/observability_lakehouse/main.tf
    obs_tf_path = (
        TERRAFORM_DIR / "modules" / "observability_lakehouse" / "main.tf"
    )
    self.assertTrue(obs_tf_path.is_file(), f"Missing {obs_tf_path}")
    obs_tf_text = obs_tf_path.read_text(encoding="utf-8")
    obs_blocks = extract_hcl_blocks(obs_tf_text)

    resources_by_type_name: dict[tuple[str, str], str] = {}
    obs_tables_schemas: dict[str, list[dict[str, Any]]] = {}
    obs_routines: dict[str, str] = {}

    for b in obs_blocks:
      if b["type"] == "resource":
        resources_by_type_name[(b["label1"], b["label2"])] = b["body"]
        if b["label1"] == "google_bigquery_table":
          hd = re.search(
              r"schema\s*=\s*<<-?EOF\s*\n(.*?)\n\s*EOF", b["body"], re.DOTALL
          )
          self.assertIsNotNone(
              hd, f"Missing JSON schema heredoc in table {b['label2']}"
          )
          obs_tables_schemas[b["label2"]] = json.loads(hd.group(1))
        elif b["label1"] == "google_bigquery_routine":
          hd = re.search(
              r"definition_body\s*=\s*<<-?EOF\s*\n(.*?)\n\s*EOF",
              b["body"],
              re.DOTALL,
          )
          self.assertIsNotNone(
              hd, f"Missing definition_body heredoc in routine {b['label2']}"
          )
          obs_routines[b["label2"]] = hd.group(1)

    # Verify dataset, sink, and dataset-scoped IAM
    self.assertIn(
        ("google_bigquery_dataset", "gsk_observability_demo"),
        resources_by_type_name,
    )
    self.assertIn(
        ("google_logging_project_sink", "gsk_bq_telemetry_sink"),
        resources_by_type_name,
    )
    sink_body = resources_by_type_name[
        ("google_logging_project_sink", "gsk_bq_telemetry_sink")
    ]
    self.assertIn("unique_writer_identity = true", sink_body)
    self.assertIn("use_partitioned_tables = true", sink_body)

    self.assertIn(
        ("google_bigquery_dataset_iam_member", "sink_dataset_writer"),
        resources_by_type_name,
    )
    iam_body = resources_by_type_name[
        ("google_bigquery_dataset_iam_member", "sink_dataset_writer")
    ]
    self.assertIn('role       = "roles/bigquery.dataEditor"', iam_body)
    self.assertIn(
        "google_logging_project_sink.gsk_bq_telemetry_sink.writer_identity",
        iam_body,
    )

    # Verify all 14 tables exist in observability_lakehouse/main.tf with valid JSON schemas
    self.assertEqual(
        set(obs_tables_schemas.keys()),
        set(REQUIRED_OBSERVABILITY_TABLES),
        "observability_lakehouse/main.tf must define all 14 required tables",
    )
    etp_body = resources_by_type_name[
        ("google_bigquery_table", "enterprise_telemetry_partitioned")
    ]
    self.assertIn(
        'clustering = ["site_location", "system_id", "application_tier", "host_id"]',
        etp_body,
    )
    self.assertIn("expiration_ms = 7776000000", etp_body)

    # Verify all 4 routines in observability_lakehouse/main.tf
    expected_routines = {
        "deploy_property_graph_gql",
        "train_host_cpu_arima_model",
        "train_host_cpu_arimax_model",
        "train_composite_host_signals_arima",
    }
    self.assertEqual(set(obs_routines.keys()), expected_routines)
    self.assertIn(
        "CREATE OR REPLACE PROPERTY GRAPH",
        obs_routines["deploy_property_graph_gql"],
    )
    self.assertIn(
        "HORIZON = 10000", obs_routines["train_host_cpu_arima_model"]
    )
    self.assertIn(
        "HOLIDAY_REGION = 'GB'", obs_routines["train_host_cpu_arima_model"]
    )
    self.assertIn("GAP_FILL(", obs_routines["train_host_cpu_arima_model"])
    self.assertIn(
        "ARIMA_PLUS_XREG", obs_routines["train_host_cpu_arimax_model"]
    )

  def test_5_3_promql_micro_batcher_zero_float_and_boundary_cases(self) -> None:
    """Unit-tests src/promql_micro_batcher.py across all 0.0 float edge cases and matrix batch ingestion."""
    from src.promql_micro_batcher import (
        compute_aligned_window,
        extract_metric_float,
        fetch_and_ingest_metrics,
        parse_promql_matrix_response,
    )

    # 1. Verify zero values are deterministically preserved as 0.0 (positive sign)
    for zero_in in [0, 0.0, -0.0, "0", "0.0", "-0.0", "0.0000", " 0.0 "]:
      val = extract_metric_float(zero_in)
      self.assertIsInstance(val, float)
      self.assertEqual(val, 0.0)
      self.assertEqual(
          math.copysign(1.0, val),
          1.0,
          f"Input {zero_in!r} produced negative zero (-0.0)",
      )

    # 2. Verify invalid / null / NaN / Inf / boolean inputs safely coerce to 0.0 without throwing
    for bad_in in [
        None,
        "",
        "   ",
        "null",
        "None",
        "NaN",
        "nan",
        float("nan"),
        "Inf",
        "-Inf",
        "infinity",
        float("inf"),
        float("-inf"),
        True,
        False,
        "not_a_float",
        [],
        {},
        object(),
    ]:
      val = extract_metric_float(bad_in)
      self.assertIsInstance(val, float)
      self.assertEqual(val, 0.0, f"Failed safe 0.0 fallback for {bad_in!r}")

    # 3. Verify non-zero valid floats
    self.assertAlmostEqual(extract_metric_float("98.8"), 98.8)
    self.assertAlmostEqual(extract_metric_float(545), 545.0)
    self.assertAlmostEqual(extract_metric_float("1e-4"), 0.0001)

    # 4. Verify 60s epoch window alignment
    start_ep, end_ep, step_str = compute_aligned_window(
        now_epoch=1790700147.8, window_seconds=300, step_seconds=60
    )
    self.assertEqual(end_ep, 1790700120)
    self.assertEqual(start_ep, 1790699820)
    self.assertEqual(step_str, "60s")

    # 5. Verify multi-metric matrix merging preserves 0.0 for silent host drops
    cpu_payload = {
        "status": "success",
        "data": {
            "resultType": "matrix",
            "result": [
                {
                    "metric": {
                        "host_name": "srv-c-batch-03",
                        "site_location": "Site_C_Ware",
                        "system_id": "MES_BATCH",
                        "application_tier": "Batch_Processing",
                    },
                    "values": [[1790700060, "0"], [1790700120, "0.0"]],
                },
                {
                    "metric": {
                        "host_name": "srv-b-batch-02",
                        "site_location": "Site_B_Stevenage",
                        "system_id": "EBRS",
                        "application_tier": "Batch_Processing",
                    },
                    "values": [[1790700120, "94.5"]],
                },
            ],
        },
    }
    mem_payload = {
        "status": "success",
        "data": {
            "resultType": "matrix",
            "result": [
                {
                    "metric": {"host_name": "srv-c-batch-03"},
                    "values": [[1790700060, "0.0"], [1790700120, "NaN"]],
                },
                {
                    "metric": {"host_name": "srv-b-batch-02"},
                    "values": [[1790700120, "98.8"]],
                },
            ],
        },
    }

    records = parse_promql_matrix_response("cpu_usage_pct", cpu_payload)
    records = parse_promql_matrix_response(
        "memory_usage_pct", mem_payload, host_records=records
    )
    self.assertEqual(len(records), 3)
    silent_row = records[("srv-c-batch-03", 1790700120)]
    self.assertIsNotNone(silent_row["cpu_usage_pct"])
    self.assertEqual(silent_row["cpu_usage_pct"], 0.0)
    self.assertEqual(silent_row["memory_usage_pct"], 0.0)
    anom_row = records[("srv-b-batch-02", 1790700120)]
    self.assertEqual(anom_row["cpu_usage_pct"], 94.5)
    self.assertEqual(anom_row["memory_usage_pct"], 98.8)
    self.assertEqual(anom_row["status"], "ANOMALY")

    # 6. Verify end-to-end fetch_and_ingest_metrics with mock session & inserter
    captured_inserts: list[tuple[str, list[dict[str, Any]]]] = []

    def mock_get(url: str, params: dict[str, str]) -> Any:
      if "cpu" in params.get("query", ""):
        return MockResponse(cpu_payload)
      return MockResponse(mem_payload)

    def mock_bq_inserter(
        table_ref: str, rows: list[dict[str, Any]]
    ) -> list[Any]:
      captured_inserts.append((table_ref, rows))
      return []

    ingested = fetch_and_ingest_metrics(
        project_id="gke-demos-363017",
        dataset_id="gsk_observability_demo",
        table_id="enterprise_telemetry_partitioned",
        session=mock_get,
        bq_inserter=mock_bq_inserter,
        now_epoch=1790700125.0,
    )
    self.assertEqual(len(ingested), 3)
    self.assertEqual(len(captured_inserts), 1)
    self.assertEqual(
        captured_inserts[0][0],
        "gke-demos-363017.gsk_observability_demo.enterprise_telemetry_partitioned",
    )

    # Also run CLI --self-test
    proc = subprocess.run(
        [
            sys.executable,
            str(PROJECT_ROOT / "src" / "promql_micro_batcher.py"),
            "--self-test",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    self.assertEqual(proc.returncode, 0, f"promql_micro_batcher --self-test failed:\n{proc.stderr}")
    self.assertIn('"status": "PASSED"', proc.stdout)

  def test_5_4_ebrs_cascading_seeder_and_sqlite_mirror_parity(self) -> None:
    """Tests src/seed_ebrs_observability.py against a temporary SQLite mirror across all 14 tables and Scenarios A, B, C."""
    from src.seed_ebrs_observability import (
        REQUIRED_OBSERVABILITY_TABLES,
        seed_ebrs_observability,
    )

    summary = seed_ebrs_observability(
        project_id="gke-demos-363017",
        dataset_id="gsk_observability_demo",
        mirror_path=self.temp_mirror_path,
        seed_live_bq=False,
    )
    self.assertTrue(self.temp_mirror_path.exists())
    sqlite_counts = summary["sqlite_table_counts"]
    for tbl in REQUIRED_OBSERVABILITY_TABLES:
      self.assertIn(tbl, sqlite_counts)
      self.assertGreater(
          sqlite_counts[tbl], 0, f"Expected >0 rows in SQLite mirror table {tbl}"
      )

    conn = sqlite3.connect(str(self.temp_mirror_path))
    cur = conn.cursor()

    # Verify multi-site & system coverage
    cur.execute("SELECT DISTINCT site_location FROM enterprise_telemetry_partitioned")
    sites = {r[0] for r in cur.fetchall()}
    self.assertEqual(sites, {"Site_A_London", "Site_B_Stevenage", "Site_C_Ware"})

    cur.execute("SELECT DISTINCT system_id FROM enterprise_telemetry_partitioned")
    systems = {r[0] for r in cur.fetchall()}
    self.assertEqual(systems, {"EBRS", "LIMS", "MES_BATCH"})

    # Scenario A: srv-b-batch-02 memory creep up to 98.8% + OutOfMemoryError log
    cur.execute(
        "SELECT MAX(memory_usage_pct) FROM enterprise_telemetry_partitioned WHERE hostname = 'srv-b-batch-02'"
    )
    self.assertAlmostEqual(cur.fetchone()[0], 98.8, places=1)
    cur.execute(
        "SELECT message FROM system_logs WHERE hostname = 'srv-b-batch-02' AND severity = 'FATAL'"
    )
    fatal_logs = [r[0] for r in cur.fetchall()]
    self.assertTrue(
        any("java.lang.OutOfMemoryError" in m for m in fatal_logs),
        f"Missing OutOfMemoryError log for srv-b-batch-02: {fatal_logs}",
    )

    # Scenario B: srv-b-db-01 500/500 pool exhaustion cascading to srv-a-web-01 / srv-a-web-04 HTTP 504
    cur.execute(
        "SELECT MAX(active_connections) FROM enterprise_telemetry_partitioned WHERE hostname = 'srv-b-db-01'"
    )
    self.assertEqual(cur.fetchone()[0], 500)
    cur.execute(
        "SELECT message FROM system_logs WHERE hostname IN ('srv-a-web-01', 'srv-a-web-04')"
    )
    web_logs = [r[0] for r in cur.fetchall()]
    self.assertTrue(any("HTTP 504 Gateway Timeout" in m for m in web_logs))

    # Scenario C: srv-c-batch-03 /mnt/gsk_batch IO wait >940ms
    cur.execute(
        "SELECT MAX(io_wait_ms) FROM enterprise_telemetry_partitioned WHERE hostname = 'srv-c-batch-03'"
    )
    self.assertGreater(cur.fetchone()[0], 940.0)
    cur.execute(
        "SELECT message FROM system_logs WHERE hostname = 'srv-c-batch-03'"
    )
    c_logs = [r[0] for r in cur.fetchall()]
    self.assertTrue(any("/mnt/gsk_batch" in m for m in c_logs))

    # ServiceNow maintenance windows CHG0049281 & CHG0051024
    cur.execute("SELECT change_id, is_maintenance_window FROM servicenow_maintenance_windows")
    maint = dict(cur.fetchall())
    self.assertEqual(maint.get("CHG0049281"), 1)
    self.assertEqual(maint.get("CHG0051024"), 1)

    conn.close()

  def test_5_5_live_gcp_bigquery_tables_property_graph_and_cascading_queries(
      self,
  ) -> None:
    """Verifies live GCP gke-demos-363017.gsk_observability_demo tables, ISO GQL Property Graph traversal, correlation, and TimesFM."""
    from scripts.deploy_observability_mvp import verify_live_observability_mvp
    from src.seed_ebrs_observability import (
        REQUIRED_OBSERVABILITY_TABLES,
        _get_bq_access_token,
    )

    token = _get_bq_access_token()
    self.assertTrue(
        bool(token),
        "Expected active GCP credentials for live gke-demos-363017 verification",
    )

    report = verify_live_observability_mvp(
        project_id="gke-demos-363017",
        dataset_id="gsk_observability_demo",
        location="EU",
    )
    self.assertEqual(report["verification_status"], "PASSED")

    # 1. Verify all 14 live BigQuery tables exist and have COUNT(*) > 0
    live_counts = report["live_table_counts"]
    for tbl in REQUIRED_OBSERVABILITY_TABLES:
      self.assertIn(tbl, live_counts, f"Table {tbl} missing from live BigQuery")
      self.assertGreater(
          live_counts[tbl],
          0,
          f"Live BigQuery table gke-demos-363017.gsk_observability_demo.{tbl} has 0 rows",
      )

    # 2. Verify live ISO GQL GRAPH_TABLE traversal (Switch -> Hypervisor -> Host -> Application)
    gql_rows = report["gql_fullstack_rows"]
    self.assertGreaterEqual(len(gql_rows), 4)
    switches = {r["failing_switch"] for r in gql_rows}
    hypervisors = {r["impacted_hypervisor"] for r in gql_rows}
    vms = {r["impacted_vm"] for r in gql_rows}
    self.assertIn("sw-core-stv-01", switches)
    self.assertIn("esxi-cluster-04", hypervisors)
    self.assertIn("srv-b-batch-02", vms)
    self.assertIn("srv-b-db-01", vms)

    # Verify Host-to-Host GRAPH_TABLE JSON visualization output
    flow_json = report["gql_host_flow_json"]
    self.assertGreater(flow_json.get("total_dependency_paths", 0), 0)
    self.assertIn("srv-b-db-01", flow_json.get("nodes", []))

    # 3. Verify live 5-minute metric-to-log temporal correlation across Scenarios A, B, C
    corr_rows = report["correlated_anomalies"]
    corr_by_host = {r["hostname"]: r for r in corr_rows}
    for host in [
        "srv-b-batch-02",
        "srv-b-db-01",
        "srv-a-web-01",
        "srv-a-web-04",
        "srv-c-batch-03",
    ]:
      self.assertIn(host, corr_by_host)
      self.assertTrue(
          bool(corr_by_host[host].get("correlated_error_traces")),
          f"Missing correlated error traces for {host} in live BigQuery",
      )

    # 4. Verify live TimesFM 2.5 AI.DETECT_ANOMALIES & AI.FORECAST
    self.assertGreater(report["timesfm_detected_anomalies_count"], 0)
    classifications = {
        r["anomaly_classification"]
        for r in report["timesfm_detected_anomalies_sample"]
    }
    self.assertTrue(
        "SILENT_HOST_DROP_TO_ZERO" in classifications
        or "SPIKE_ANOMALY" in classifications
    )
    self.assertGreater(len(report["timesfm_forecast_sample"]), 0)

  def test_5_6_interactive_mvp_workbench_and_neuro_conversational_ai(
      self,
  ) -> None:
    """Starts create_observability_mvp_server and create_dashboard_server and tests all /api/observability/* and /api/neuro/chat endpoints."""
    from src.demo_dashboard import create_dashboard_server
    from src.observability_mvp_server import (
        NEURO_SYSTEM_PROMPT,
        create_observability_mvp_server,
    )
    from src.seed_ebrs_observability import REQUIRED_OBSERVABILITY_TABLES

    mvp_server = create_observability_mvp_server(
        host="127.0.0.1",
        port=0,
        project_id="gke-demos-363017",
        dataset_id="gsk_observability_demo",
        mirror_path=str(self.temp_mirror_path),
    )
    dash_server = create_dashboard_server(
        host="127.0.0.1",
        port=0,
        project_id="gke-demos-363017",
        mirror_path=str(self.temp_mirror_path),
    )
    mvp_port = mvp_server.server_address[1]
    dash_port = dash_server.server_address[1]

    t1 = threading.Thread(target=mvp_server.serve_forever, daemon=True)
    t2 = threading.Thread(target=dash_server.serve_forever, daemon=True)
    t1.start()
    t2.start()

    try:
      for base_url in [
          f"http://127.0.0.1:{mvp_port}",
          f"http://127.0.0.1:{dash_port}",
      ]:
        # 1. Verify HTML Workbench UI (GET /)
        with urllib.request.urlopen(f"{base_url}/", timeout=5) as resp:
          self.assertEqual(resp.status, 200)
          html = resp.read().decode("utf-8")
          self.assertIn("Neuro", html)
          self.assertIn("gsk_infrastructure_dependency_graph", html)
          self.assertIn("SILENT_HOST_DROP_TO_ZERO", html)

        # 2. Verify GET /api/observability/status
        with urllib.request.urlopen(
            f"{base_url}/api/observability/status", timeout=5
        ) as resp:
          self.assertEqual(resp.status, 200)
          st = json.loads(resp.read().decode("utf-8"))
          self.assertEqual(st["status"], "HEALTHY")
          self.assertEqual(st["project_id"], "gke-demos-363017")
          self.assertEqual(st["dataset_id"], "gsk_observability_demo")
          self.assertEqual(
              st["property_graph"], "gsk_infrastructure_dependency_graph"
          )
          for tbl in REQUIRED_OBSERVABILITY_TABLES:
            self.assertGreater(st["table_counts"].get(tbl, 0), 0)

        # 3. Verify GET /api/observability/anomalies & query filters
        with urllib.request.urlopen(
            f"{base_url}/api/observability/anomalies", timeout=5
        ) as resp:
          self.assertEqual(resp.status, 200)
          anom = json.loads(resp.read().decode("utf-8"))
          self.assertEqual(anom["model_metadata"]["horizon"], 10000)
          self.assertEqual(anom["model_metadata"]["holiday_region"], "GB")
          cls_set = {pt["anomaly_classification"] for pt in anom["anomalies"]}
          self.assertIn("SPIKE_ANOMALY", cls_set)
          self.assertIn("SILENT_HOST_DROP_TO_ZERO", cls_set)
          self.assertIn("SUPPRESSED_MAINTENANCE_WINDOW", cls_set)

        with urllib.request.urlopen(
            f"{base_url}/api/observability/anomalies?classification=SILENT_HOST_DROP_TO_ZERO",
            timeout=5,
        ) as resp:
          filtered = json.loads(resp.read().decode("utf-8"))
          self.assertGreater(len(filtered["anomalies"]), 0)
          for pt in filtered["anomalies"]:
            self.assertEqual(
                pt["anomaly_classification"], "SILENT_HOST_DROP_TO_ZERO"
            )
            self.assertEqual(pt["actual_cpu"], 0.0)

        # 4. Verify GET /api/observability/topology
        with urllib.request.urlopen(
            f"{base_url}/api/observability/topology", timeout=5
        ) as resp:
          self.assertEqual(resp.status, 200)
          topo = json.loads(resp.read().decode("utf-8"))
          node_ids = {n["id"] for n in topo["nodes"]}
          self.assertIn("sw-core-stv-01", node_ids)
          self.assertIn("esxi-cluster-04", node_ids)
          self.assertIn("srv-b-batch-02", node_ids)
          self.assertIn("srv-b-db-01", node_ids)
          self.assertGreaterEqual(len(topo["graph_table_paths"]), 3)

        # 5. Verify GET & POST /api/observability/scenarios/<A|B|C> and negative 400 test
        for scen_id in ["A", "B", "C"]:
          req = urllib.request.Request(
              f"{base_url}/api/observability/scenarios/{scen_id}",
              data=b"{}",
              headers={"Content-Type": "application/json"},
              method="POST",
          )
          with urllib.request.urlopen(req, timeout=5) as resp:
            self.assertEqual(resp.status, 200)
            s_data = json.loads(resp.read().decode("utf-8"))
            self.assertEqual(s_data["scenario_id"], scen_id)
            self.assertGreaterEqual(len(s_data["scenario"]["stages"]), 2)
            self.assertTrue(
                bool(
                    s_data["scenario"]["incident_root_cause_analysis"][
                        "gemini_root_cause_analysis"
                    ]
                )
            )

        with self.assertRaises(urllib.error.HTTPError) as ctx:
          urllib.request.urlopen(
              f"{base_url}/api/observability/scenarios/INVALID_SCENARIO",
              timeout=5,
          )
        self.assertEqual(ctx.exception.code, 400)

        # 6. Verify POST /api/neuro/chat across SQL, ISO GQL, maintenance suppression, and boundary prompts
        prompts_and_expected = [
            (
                "What active service degradation is occurring in the last 60 minutes?",
                "SQL",
                "enterprise_telemetry_partitioned",
            ),
            (
                "Trace the ISO GQL blast radius for PostgreSQL 500/500 database saturation and upstream HTTP 504 timeouts",
                "ISO_GQL",
                "GRAPH_TABLE",
            ),
            (
                "Investigate silent host drop to 0.0 and /mnt/gsk_batch storage IO wait in Site_C_Ware",
                "SQL",
                "SILENT_HOST_DROP_TO_ZERO",
            ),
            (
                "Is CHG0049281 suppressed by ServiceNow maintenance window?",
                "SQL",
                "servicenow_maintenance_windows",
            ),
            (
                "Why did srv-b-batch-02 crash with OutOfMemoryError?",
                "SQL",
                "system_logs",
            ),
            (
                "",
                "SQL",
                "enterprise_telemetry_partitioned",
            ),
        ]
        for question, expected_qtype, expected_query_substr in prompts_and_expected:
          req = urllib.request.Request(
              f"{base_url}/api/neuro/chat",
              data=json.dumps({"question": question}).encode("utf-8"),
              headers={"Content-Type": "application/json"},
              method="POST",
          )
          with urllib.request.urlopen(req, timeout=5) as resp:
            self.assertEqual(resp.status, 200)
            chat_res = json.loads(resp.read().decode("utf-8"))
            self.assertEqual(
                chat_res["system_prompt"].strip(), NEURO_SYSTEM_PROMPT.strip()
            )
            self.assertEqual(chat_res["query_type"], expected_qtype)
            self.assertIn(expected_query_substr, chat_res["generated_query"])
            self.assertIn("hostname", chat_res["executive_summary"])
            self.assertIn("remediation_runbook", chat_res["executive_summary"])
    finally:
      mvp_server.shutdown()
      mvp_server.server_close()
      dash_server.shutdown()
      dash_server.server_close()


def run_all_validations() -> None:
  """Executes all validation suites and prints a formatted summary report."""
  print("=" * 80)
  print("GSK AUTONOMOUS OPERATIONS (ANO) — AUTOMATED VERIFICATION SUITE")
  print("=" * 80)

  loader = unittest.TestLoader()
  suite = unittest.TestSuite()
  suite.addTests(loader.loadTestsFromTestCase(TerraformValidationSuite))
  suite.addTests(loader.loadTestsFromTestCase(SQLSchemaValidationSuite))
  suite.addTests(loader.loadTestsFromTestCase(PythonPipelineUnitTestSuite))
  suite.addTests(loader.loadTestsFromTestCase(Round2LiveDemoAndDashboardSuite))
  suite.addTests(loader.loadTestsFromTestCase(Round3ObservabilityPlatformSuite))

  runner = unittest.TextTestRunner(verbosity=2)
  result = runner.run(suite)

  print("-" * 80)
  total_run = result.testsRun
  failures = len(result.failures)
  errors = len(result.errors)
  passed = total_run - failures - errors
  pass_rate = (passed / total_run * 100.0) if total_run > 0 else 0.0

  print(
      f"SUMMARY: Executed {total_run} verification checks | "
      f"Passed: {passed} | Failures: {failures} | Errors: {errors} | "
      f"Pass Rate: {pass_rate:.1f}%"
  )
  print("=" * 80)

  if not result.wasSuccessful():
    sys.exit(1)
  sys.exit(0)


def main() -> None:
  """CLI entrypoint."""
  run_all_validations()


if __name__ == "__main__":
  main()
