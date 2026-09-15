#!/usr/bin/env python3
"""GSK Autonomous Operations (ANO) — Preemptive Self-Healing Remediation Webhook.

This module implements Pillar 5 of the GSK ANO architecture:
1. Cloud Run v2 / Eventarc webhook handler receiving correlated incident predictions
   from the `correlated_incidents` Pub/Sub topic.
2. Deterministic maintenance window suppression check querying/evaluating BigQuery
   `change_calendar` (`status IN ('SCHEDULED', 'IN_PROGRESS')`, `suppress_alerts = TRUE`,
   `prediction_timestamp BETWEEN start_time AND end_time`).
3. When an active maintenance window matches:
   - Sets `suppressed_by_change_window = True`
   - Sets `active_change_id = <change_id>`
   - Sets `remediation_status = 'SUPPRESSED'`
   - Sets `recommended_action = 'NO_ACTION_SUPPRESSED'`
   - Halts workflow execution (0 remediation calls) and records audit trail.
4. Per-entity sliding-window rate limiter (`cooldown_seconds = 900`, max 3/hr)
   preventing remediation thrashing on flapping entities.
5. Cloud Workflows execution dispatcher for unsuppressed, rate-allowed incidents:
   - `DRAIN_AND_RESTART_WORKERS` -> `gsk-ano-compute-drain-restart`
   - `KILL_BLOCKING_DB_SESSIONS` -> `gsk-ano-db-session-terminator`
   - `REROUTE_OSPF_TRAFFIC`      -> `gsk-ano-network-ospf-reroute`
   - `SCALE_UP_INSTANCE_GROUP`   -> `gsk-ano-compute-mig-scaleup`
6. BigQuery `incidents_predictions` audit trail writer with clean dependency injection.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
import math
import os
import threading
import uuid
from typing import Any, Optional, Protocol

# Configure structured logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("gsk_ano.remediation_webhook")

# Action to Cloud Workflows mapping
ACTION_TO_WORKFLOW_MAP: dict[str, str] = {
    "DRAIN_AND_RESTART_WORKERS": "gsk-ano-compute-drain-restart",
    "KILL_BLOCKING_DB_SESSIONS": "gsk-ano-db-session-terminator",
    "REROUTE_OSPF_TRAFFIC": "gsk-ano-network-ospf-reroute",
    "SCALE_UP_INSTANCE_GROUP": "gsk-ano-compute-mig-scaleup",
}

# Default recommended action per root_cause_domain if not explicitly provided
DOMAIN_DEFAULT_ACTION: dict[str, str] = {
    "COMPUTE": "DRAIN_AND_RESTART_WORKERS",
    "APPLICATION": "DRAIN_AND_RESTART_WORKERS",
    "DATABASE": "KILL_BLOCKING_DB_SESSIONS",
    "NETWORK": "REROUTE_OSPF_TRAFFIC",
}


class HttpSessionProtocol(Protocol):
  """Protocol for injectable HTTP/REST sessions (requests or AuthorizedSession)."""

  def post(
      self,
      url: str,
      json: Optional[dict[str, Any]] = None,
      headers: Optional[dict[str, str]] = None,
      timeout: float = 30.0,
  ) -> Any:
    ...


@dataclass
class SuppressionCheckResult:
  """Result of checking BigQuery `change_calendar` for active maintenance windows."""

  is_suppressed: bool
  active_change_id: Optional[str] = None
  change_type: Optional[str] = None
  owner_team: Optional[str] = None
  reason: str = "NO_ACTIVE_MAINTENANCE_WINDOW"


def parse_iso_datetime(ts_val: Any) -> datetime:
  """Parses an ISO-8601 timestamp string or numeric epoch into a timezone-aware UTC datetime."""
  if isinstance(ts_val, datetime):
    return ts_val.astimezone(timezone.utc) if ts_val.tzinfo else ts_val.replace(tzinfo=timezone.utc)
  if isinstance(ts_val, (int, float)) and not isinstance(ts_val, bool):
    val = float(ts_val)
    if not (math.isnan(val) or math.isinf(val)):
      if abs(val) > 1e16:
        val /= 1e9
      elif abs(val) > 1e11:
        val /= 1e3
      try:
        return datetime.fromtimestamp(val, tz=timezone.utc)
      except (ValueError, OverflowError, OSError):
        pass
  if isinstance(ts_val, str) and ts_val.strip() and ts_val.strip().lower() != "none":
    clean = ts_val.strip()
    try:
      if clean.endswith(("Z", "z")):
        clean = clean[:-1] + "+00:00"
      dt = datetime.fromisoformat(clean)
      if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
      return dt.astimezone(timezone.utc)
    except ValueError:
      try:
        return parse_iso_datetime(float(clean))
      except ValueError:
        pass
  return datetime.now(timezone.utc)


class ChangeCalendarChecker:
  """Verifies whether an entity is currently under an active maintenance window."""

  # Canonical SQL query executed in BigQuery production mode
  CHANGE_CALENDAR_SQL = """
  SELECT
    change_id,
    change_type,
    status,
    suppress_alerts,
    start_time,
    end_time,
    owner_team
  FROM `{project_id}.{dataset_id}.change_calendar`
  WHERE target_entity_id IN (@root_cause_entity_id, @affected_entity_id)
    AND status IN ('SCHEDULED', 'IN_PROGRESS')
    AND suppress_alerts = TRUE
    AND TIMESTAMP(@incident_timestamp) BETWEEN start_time AND end_time
  ORDER BY change_risk_level DESC
  LIMIT 1
  """

  def __init__(
      self,
      project_id: str,
      dataset_id: str = "gsk_ano_ops",
      session: Optional[HttpSessionProtocol] = None,
      in_memory_schedule: Optional[list[dict[str, Any]]] = None,
  ) -> None:
    self.project_id = project_id
    self.dataset_id = dataset_id
    self._session = session
    self._in_memory_schedule = in_memory_schedule

  def _get_session(self) -> HttpSessionProtocol:
    if self._session is not None:
      return self._session
    import google.auth
    from google.auth.transport.requests import AuthorizedSession

    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/bigquery.readonly"]
    )
    self._session = AuthorizedSession(credentials)
    return self._session

  def set_in_memory_schedule(self, schedule: list[dict[str, Any]]) -> None:
    """Updates the in-memory change_calendar schedule for deterministic testing."""
    self._in_memory_schedule = schedule

  def _check_in_memory(
      self,
      root_cause_entity_id: str,
      affected_entity_id: str,
      incident_timestamp: str,
  ) -> SuppressionCheckResult:
    """Evaluates maintenance window overlap against in-memory schedule records."""
    incident_dt = parse_iso_datetime(incident_timestamp)
    target_entities = {root_cause_entity_id, affected_entity_id}
    matching_rows: list[dict[str, Any]] = []

    for row in self._in_memory_schedule or []:
      target_id = str(row.get("target_entity_id", ""))
      status = str(row.get("status", "")).upper()
      suppress = bool(row.get("suppress_alerts", False))
      if target_id not in target_entities:
        continue
      if status not in ("SCHEDULED", "IN_PROGRESS"):
        continue
      if not suppress:
        continue

      if row.get("start_time") is None or row.get("end_time") is None:
        continue
      start_dt = parse_iso_datetime(row["start_time"])
      end_dt = parse_iso_datetime(row["end_time"])
      if start_dt <= incident_dt <= end_dt:
        matching_rows.append(row)

    if not matching_rows:
      return SuppressionCheckResult(is_suppressed=False)

    # Sort by change_risk_level DESC as in BigQuery query (null-safe)
    def _safe_risk(r: dict[str, Any]) -> int:
      val = r.get("change_risk_level")
      if val is None:
        return 1
      try:
        return int(val)
      except (ValueError, TypeError):
        return 1

    matching_rows.sort(key=_safe_risk, reverse=True)
    winner = matching_rows[0]
    owner_val = winner.get("owner_team")
    return SuppressionCheckResult(
        is_suppressed=True,
        active_change_id=str(winner.get("change_id") or "UNKNOWN_CHANGE"),
        change_type=str(winner.get("change_type") or "MAINTENANCE"),
        owner_team=str(owner_val) if owner_val is not None else "ops-team",
        reason=f"Active change window {winner.get('change_id')} ({winner.get('status')})",
    )

  def check_maintenance_window(
      self,
      root_cause_entity_id: str,
      affected_entity_id: str,
      incident_timestamp: str,
  ) -> SuppressionCheckResult:
    """Checks BigQuery change_calendar for an active alert-suppressing maintenance window."""
    if self._in_memory_schedule is not None:
      return self._check_in_memory(
          root_cause_entity_id=root_cause_entity_id,
          affected_entity_id=affected_entity_id,
          incident_timestamp=incident_timestamp,
      )

    # Execute BigQuery REST API jobs.query
    url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{self.project_id}/queries"
    query_sql = self.CHANGE_CALENDAR_SQL.format(
        project_id=self.project_id, dataset_id=self.dataset_id
    )
    payload = {
        "query": query_sql,
        "useLegacySql": False,
        "parameterMode": "NAMED",
        "queryParameters": [
            {
                "name": "root_cause_entity_id",
                "parameterType": {"type": "STRING"},
                "parameterValue": {"value": root_cause_entity_id},
            },
            {
                "name": "affected_entity_id",
                "parameterType": {"type": "STRING"},
                "parameterValue": {"value": affected_entity_id},
            },
            {
                "name": "incident_timestamp",
                "parameterType": {"type": "STRING"},
                "parameterValue": {"value": incident_timestamp},
            },
        ],
    }

    session = self._get_session()
    response = session.post(url, json=payload, timeout=15.0)
    if hasattr(response, "raise_for_status"):
      response.raise_for_status()

    resp_json = response.json() if callable(getattr(response, "json", None)) else response
    rows = resp_json.get("rows", [])
    if not rows:
      return SuppressionCheckResult(is_suppressed=False)

    # Extract fields from first returned row (null-safe for BigQuery {"v": None})
    fields = rows[0].get("f", [])
    def _extract_field(idx: int, default: str) -> str:
      if idx < len(fields):
        val = fields[idx].get("v")
        if val is not None:
          return str(val)
      return default

    change_id = _extract_field(0, "UNKNOWN_CHANGE")
    change_type = _extract_field(1, "MAINTENANCE")
    owner_team = _extract_field(6, "ops-team")

    return SuppressionCheckResult(
        is_suppressed=True,
        active_change_id=change_id,
        change_type=change_type,
        owner_team=owner_team,
        reason=f"Active BigQuery change window {change_id}",
    )


class RemediationRateLimiter:
  """Per-entity sliding-window rate limiter preventing remediation thrashing."""

  def __init__(
      self, cooldown_seconds: int = 900, max_actions_per_hour: int = 3
  ) -> None:
    if cooldown_seconds < 0:
      raise ValueError("cooldown_seconds must be >= 0")
    self.cooldown_seconds = cooldown_seconds
    self.max_actions_per_hour = max_actions_per_hour
    # Keyed by (entity_id, recommended_action) -> list of epoch execution timestamps
    self._history: dict[tuple[str, str], list[float]] = {}
    self._lock = threading.Lock()

  def allow_execution(
      self, entity_id: str, action: str, current_epoch_sec: float
  ) -> tuple[bool, str]:
    """Determines whether a remediation action is allowed under cooldown & hourly limits.

    Args:
      entity_id: Target root_cause_entity_id.
      action: Recommended self-healing action name.
      current_epoch_sec: Current timestamp in epoch seconds.

    Returns:
      Tuple of (is_allowed, reason_code).
    """
    key = (entity_id, action)
    with self._lock:
      timestamps = self._history.get(key, [])

      # Retain timestamps within 1 hour (3600s) of either the latest seen event or current_epoch_sec
      anchor_ts = max([current_epoch_sec] + timestamps)
      cutoff = anchor_ts - 3600.0
      recent_timestamps = [ts for ts in timestamps if ts > cutoff]
      self._history[key] = recent_timestamps

      # Check cooldown against any existing execution within cooldown_seconds distance
      for exec_ts in recent_timestamps:
        dist = abs(current_epoch_sec - exec_ts)
        if dist < self.cooldown_seconds:
          remaining = int(self.cooldown_seconds - dist)
          return (
              False,
              f"RATE_LIMITED_COOLDOWN: last executed {int(dist)}s ago "
              f"(cooldown={self.cooldown_seconds}s, {remaining}s remaining)",
          )

      # Check hourly cap within 3600s window of current_epoch_sec
      hourly_window = [
          ts for ts in recent_timestamps if abs(current_epoch_sec - ts) < 3600.0
      ]
      if len(hourly_window) >= self.max_actions_per_hour:
        return (
            False,
            f"RATE_LIMITED_HOURLY_CAP: {len(hourly_window)} executions in last hour "
            f"(max={self.max_actions_per_hour})",
        )

      # Record execution timestamp in sorted order
      recent_timestamps.append(current_epoch_sec)
      recent_timestamps.sort()
      self._history[key] = recent_timestamps
      return True, "ALLOWED"


class WorkflowsRemediationDispatcher:
  """Dispatches self-healing remediation workflows via Google Cloud Workflows REST API."""

  def __init__(
      self,
      project_id: str,
      region: str = "us-central1",
      session: Optional[HttpSessionProtocol] = None,
  ) -> None:
    self.project_id = project_id
    self.region = region
    self._session = session

  def _get_session(self) -> HttpSessionProtocol:
    if self._session is not None:
      return self._session
    import google.auth
    from google.auth.transport.requests import AuthorizedSession

    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    self._session = AuthorizedSession(credentials)
    return self._session

  def resolve_workflow_name(self, recommended_action: str, domain: str) -> str:
    """Resolves the target Cloud Workflow name from action or domain fallback."""
    if recommended_action in ACTION_TO_WORKFLOW_MAP:
      return ACTION_TO_WORKFLOW_MAP[recommended_action]
    fallback_action = DOMAIN_DEFAULT_ACTION.get(
        domain.upper(), "DRAIN_AND_RESTART_WORKERS"
    )
    return ACTION_TO_WORKFLOW_MAP[fallback_action]

  def dispatch_workflow(
      self,
      incident_id: str,
      root_cause_entity_id: str,
      affected_entity_id: str,
      root_cause_domain: str,
      recommended_action: str,
      anomaly_probability: float,
      lead_time_minutes: int,
  ) -> dict[str, Any]:
    """Invokes a Google Cloud Workflow execution for automated self-healing."""
    workflow_id = self.resolve_workflow_name(recommended_action, root_cause_domain)
    url = (
        f"https://workflowexecutions.googleapis.com/v1/projects/{self.project_id}/"
        f"locations/{self.region}/workflows/{workflow_id}/executions"
    )
    workflow_args = {
        "incident_id": incident_id,
        "root_cause_entity_id": root_cause_entity_id,
        "affected_entity_id": affected_entity_id,
        "root_cause_domain": root_cause_domain,
        "recommended_action": recommended_action,
        "anomaly_probability": float(anomaly_probability),
        "lead_time_minutes": int(lead_time_minutes),
        "dispatched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    payload = {
        "argument": json.dumps(workflow_args),
    }

    session = self._get_session()
    response = session.post(url, json=payload, timeout=20.0)
    if hasattr(response, "raise_for_status"):
      response.raise_for_status()

    resp_json = response.json() if callable(getattr(response, "json", None)) else response
    execution_name = resp_json.get(
        "name",
        f"projects/{self.project_id}/locations/{self.region}/workflows/{workflow_id}/executions/exec-{uuid.uuid4().hex[:12]}",
    )
    return {
        "workflow_id": workflow_id,
        "execution_id": execution_name,
        "payload": workflow_args,
    }


class IncidentAuditWriter:
  """Streams audit rows into BigQuery `incidents_predictions` table."""

  def __init__(
      self,
      project_id: str,
      dataset_id: str = "gsk_ano_ops",
      table_id: str = "incidents_predictions",
      session: Optional[HttpSessionProtocol] = None,
  ) -> None:
    self.project_id = project_id
    self.dataset_id = dataset_id
    self.table_id = table_id
    self._session = session

  def _get_session(self) -> HttpSessionProtocol:
    if self._session is not None:
      return self._session
    import google.auth
    from google.auth.transport.requests import AuthorizedSession

    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/bigquery.insertdata"]
    )
    self._session = AuthorizedSession(credentials)
    return self._session

  @property
  def endpoint_url(self) -> str:
    return (
        f"https://bigquery.googleapis.com/bigquery/v2/projects/{self.project_id}/"
        f"datasets/{self.dataset_id}/tables/{self.table_id}/insertAll"
    )

  def write_incident_prediction(self, record: dict[str, Any]) -> dict[str, Any]:
    """Formats and writes a single incident audit row to BigQuery."""
    payload = {
        "kind": "bigquery#tableDataInsertAllRequest",
        "skipInvalidRows": False,
        "ignoreUnknownValues": False,
        "rows": [
            {
                "insertId": str(record["incident_id"]),
                "json": record,
            }
        ],
    }
    session = self._get_session()
    response = session.post(self.endpoint_url, json=payload, timeout=15.0)
    if hasattr(response, "raise_for_status"):
      response.raise_for_status()

    resp_json = response.json() if callable(getattr(response, "json", None)) else response
    insert_errors = resp_json.get("insertErrors", [])
    if insert_errors:
      raise RuntimeError(f"BigQuery audit insert error: {json.dumps(insert_errors)}")
    return {"status": "AUDIT_RECORDED", "incident_id": record["incident_id"]}


class RemediationWebhookHandler:
  """Core orchestrator for Eventarc incident processing, suppression, and remediation."""

  def __init__(
      self,
      calendar_checker: ChangeCalendarChecker,
      rate_limiter: RemediationRateLimiter,
      dispatcher: WorkflowsRemediationDispatcher,
      audit_writer: IncidentAuditWriter,
  ) -> None:
    self.calendar_checker = calendar_checker
    self.rate_limiter = rate_limiter
    self.dispatcher = dispatcher
    self.audit_writer = audit_writer

  @staticmethod
  def extract_incident_payload(envelope: Any) -> dict[str, Any]:
    """Extracts structured incident fields from Pub/Sub push or Eventarc CloudEvent."""
    data: dict[str, Any] = {}
    if isinstance(envelope, dict):
      if "message" in envelope and isinstance(envelope["message"], dict):
        msg = envelope["message"]
        b64_data = msg.get("data", "")
        if b64_data and isinstance(b64_data, str):
          try:
            decoded = base64.b64decode(b64_data).decode("utf-8", errors="replace")
            parsed = json.loads(decoded)
            if isinstance(parsed, dict):
              data = parsed
          except Exception:  # pylint: disable=broad-except
            data = msg
        else:
          data = msg
      elif "data" in envelope and isinstance(envelope["data"], dict):
        data = envelope["data"]
      else:
        data = envelope

    def _get_or(key: str, default: Any) -> Any:
      val = data.get(key) if isinstance(data, dict) else None
      return default if val is None else val

    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    domain = str(_get_or("root_cause_domain", "COMPUTE")).upper()
    default_action = DOMAIN_DEFAULT_ACTION.get(domain, "DRAIN_AND_RESTART_WORKERS")

    root_id = str(
        _get_or("root_cause_entity_id", _get_or("affected_entity_id", "unknown-entity"))
    )
    affected_id = str(_get_or("affected_entity_id", root_id))

    try:
      lead_time = int(_get_or("lead_time_minutes", 20))
    except (ValueError, TypeError):
      lead_time = 20

    try:
      prob = float(_get_or("anomaly_probability", 0.95))
    except (ValueError, TypeError):
      prob = 0.95

    raw_cosine = data.get("cosine_distance") if isinstance(data, dict) else None
    try:
      cosine_dist = float(raw_cosine) if raw_cosine is not None else None
    except (ValueError, TypeError):
      cosine_dist = None

    return {
        "incident_id": str(_get_or("incident_id", f"inc-{uuid.uuid4().hex[:12]}")),
        "prediction_timestamp": str(_get_or("prediction_timestamp", now_iso)),
        "capability_type": str(
            _get_or("capability_type", "CAP1_UNRESPONSIVENESS_15_30M")
        ),
        "affected_entity_id": affected_id,
        "root_cause_entity_id": root_id,
        "root_cause_domain": domain,
        "lead_time_minutes": lead_time,
        "anomaly_probability": prob,
        "semantic_nearest_neighbor_log_id": (
            data.get("semantic_nearest_neighbor_log_id")
            if isinstance(data, dict)
            else None
        ),
        "cosine_distance": cosine_dist,
        "recommended_action": str(_get_or("recommended_action", default_action)),
    }

  def process_incident(
      self,
      envelope: dict[str, Any],
      current_epoch_sec: Optional[float] = None,
  ) -> dict[str, Any]:
    """Processes a correlated incident through change-window suppression and remediation."""
    incident = self.extract_incident_payload(envelope)
    incident_id = incident["incident_id"]
    root_entity = incident["root_cause_entity_id"]
    affected_entity = incident["affected_entity_id"]
    pred_ts = incident["prediction_timestamp"]
    action = incident["recommended_action"]
    domain = incident["root_cause_domain"]

    if current_epoch_sec is None:
      current_epoch_sec = parse_iso_datetime(pred_ts).timestamp()

    # Step 1: Deterministic BigQuery change_calendar suppression check
    suppression = self.calendar_checker.check_maintenance_window(
        root_cause_entity_id=root_entity,
        affected_entity_id=affected_entity,
        incident_timestamp=pred_ts,
    )

    if suppression.is_suppressed:
      logger.info(
          "Incident %s suppressed by active maintenance window %s",
          incident_id,
          suppression.active_change_id,
      )
      audit_record = {
          "incident_id": incident_id,
          "prediction_timestamp": pred_ts,
          "capability_type": incident["capability_type"],
          "affected_entity_id": affected_entity,
          "root_cause_entity_id": root_entity,
          "root_cause_domain": domain,
          "lead_time_minutes": incident["lead_time_minutes"],
          "anomaly_probability": incident["anomaly_probability"],
          "semantic_nearest_neighbor_log_id": incident[
              "semantic_nearest_neighbor_log_id"
          ],
          "cosine_distance": incident["cosine_distance"],
          "suppressed_by_change_window": True,
          "active_change_id": suppression.active_change_id,
          "recommended_action": "NO_ACTION_SUPPRESSED",
          "remediation_status": "SUPPRESSED",
          "remediation_execution_id": None,
      }
      self.audit_writer.write_incident_prediction(audit_record)
      return audit_record

    # Step 2: Check per-entity sliding-window cooldown rate limiter
    allowed, rate_reason = self.rate_limiter.allow_execution(
        entity_id=root_entity,
        action=action,
        current_epoch_sec=current_epoch_sec,
    )

    if not allowed:
      logger.warning(
          "Incident %s rate-limited for entity %s (%s): %s",
          incident_id,
          root_entity,
          action,
          rate_reason,
      )
      audit_record = {
          "incident_id": incident_id,
          "prediction_timestamp": pred_ts,
          "capability_type": incident["capability_type"],
          "affected_entity_id": affected_entity,
          "root_cause_entity_id": root_entity,
          "root_cause_domain": domain,
          "lead_time_minutes": incident["lead_time_minutes"],
          "anomaly_probability": incident["anomaly_probability"],
          "semantic_nearest_neighbor_log_id": incident[
              "semantic_nearest_neighbor_log_id"
          ],
          "cosine_distance": incident["cosine_distance"],
          "suppressed_by_change_window": False,
          "active_change_id": None,
          "recommended_action": action,
          "remediation_status": "SUPPRESSED_RATE_LIMIT",
          "remediation_execution_id": None,
      }
      self.audit_writer.write_incident_prediction(audit_record)
      return audit_record

    # Step 3: Dispatch Cloud Workflows self-healing remediation
    dispatch_res = self.dispatcher.dispatch_workflow(
        incident_id=incident_id,
        root_cause_entity_id=root_entity,
        affected_entity_id=affected_entity,
        root_cause_domain=domain,
        recommended_action=action,
        anomaly_probability=incident["anomaly_probability"],
        lead_time_minutes=incident["lead_time_minutes"],
    )

    execution_id = dispatch_res["execution_id"]
    logger.info(
        "Dispatched remediation workflow %s for incident %s (execution=%s)",
        dispatch_res["workflow_id"],
        incident_id,
        execution_id,
    )

    audit_record = {
        "incident_id": incident_id,
        "prediction_timestamp": pred_ts,
        "capability_type": incident["capability_type"],
        "affected_entity_id": affected_entity,
        "root_cause_entity_id": root_entity,
        "root_cause_domain": domain,
        "lead_time_minutes": incident["lead_time_minutes"],
        "anomaly_probability": incident["anomaly_probability"],
        "semantic_nearest_neighbor_log_id": incident[
            "semantic_nearest_neighbor_log_id"
        ],
        "cosine_distance": incident["cosine_distance"],
        "suppressed_by_change_window": False,
        "active_change_id": None,
        "recommended_action": action,
        "remediation_status": "TRIGGERED",
        "remediation_execution_id": execution_id,
    }
    self.audit_writer.write_incident_prediction(audit_record)
    return audit_record


def create_http_handler(
    webhook_handler: RemediationWebhookHandler,
) -> type[BaseHTTPRequestHandler]:
  """Creates a Cloud Run v2 HTTP handler bound to the provided RemediationWebhookHandler."""

  class WebhookHTTPRequestHandler(BaseHTTPRequestHandler):
    """HTTP request handler for Eventarc incident notifications."""

    def do_POST(self) -> None:  # pylint: disable=invalid-name
      content_length = int(self.headers.get("Content-Length", 0))
      body = self.rfile.read(content_length).decode("utf-8") if content_length > 0 else "{}"
      try:
        envelope = json.loads(body)
        result = webhook_handler.process_incident(envelope)
        response_bytes = json.dumps(result).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response_bytes)))
        self.end_headers()
        self.wfile.write(response_bytes)
      except Exception as exc:  # pylint: disable=broad-except
        logger.exception("Error processing remediation webhook: %s", exc)
        err_bytes = json.dumps({"error": str(exc)}).encode("utf-8")
        self.send_response(500)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(err_bytes)))
        self.end_headers()
        self.wfile.write(err_bytes)

    def do_GET(self) -> None:  # pylint: disable=invalid-name
      """Health check endpoint for Cloud Run v2 probes."""
      health_bytes = json.dumps(
          {"status": "HEALTHY", "service": "gsk-ano-remediation-webhook"}
      ).encode("utf-8")
      self.send_response(200)
      self.send_header("Content-Type", "application/json")
      self.send_header("Content-Length", str(len(health_bytes)))
      self.end_headers()
      self.wfile.write(health_bytes)

  return WebhookHTTPRequestHandler


def main() -> None:
  """Production entrypoint for Cloud Run v2 container execution."""
  project_id = os.environ.get("PROJECT_ID", "gsk-ano-prod")
  region = os.environ.get("REGION", "us-central1")
  dataset_id = os.environ.get("DATASET_ID", "gsk_ano_ops")
  cooldown_sec = int(os.environ.get("COOLDOWN_SECONDS", "900"))
  port = int(os.environ.get("PORT", "8080"))

  calendar_checker = ChangeCalendarChecker(
      project_id=project_id,
      dataset_id=dataset_id,
  )
  rate_limiter = RemediationRateLimiter(
      cooldown_seconds=cooldown_sec,
      max_actions_per_hour=3,
  )
  dispatcher = WorkflowsRemediationDispatcher(
      project_id=project_id,
      region=region,
  )
  audit_writer = IncidentAuditWriter(
      project_id=project_id,
      dataset_id=dataset_id,
      table_id="incidents_predictions",
  )
  webhook_handler = RemediationWebhookHandler(
      calendar_checker=calendar_checker,
      rate_limiter=rate_limiter,
      dispatcher=dispatcher,
      audit_writer=audit_writer,
  )

  handler_cls = create_http_handler(webhook_handler)
  server = ThreadingHTTPServer(("0.0.0.0", port), handler_cls)
  logger.info("Starting GSK ANO Remediation Webhook on port %d...", port)
  server.serve_forever()


if __name__ == "__main__":
  main()
