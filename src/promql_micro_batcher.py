#!/usr/bin/env python3
"""GSK Enterprise Observability Platform — Serverless PromQL query_range Micro-Batcher.

Queries Google Cloud Monitoring's PromQL `query_range` HTTP API (`step="60s"`)
over a 5-minute window aligned to 60-second epoch boundaries and streams evaluated
metric series into BigQuery (`enterprise_telemetry_partitioned`) with deterministic
`0.0` float preservation (`extract_metric_float`).

Key Remediations Implemented (Blueprint Section 2.2 Method 2 & Finding 2):
1. `extract_metric_float(raw_val)` deterministically preserves `0.0` (`0`, `0.0`,
   `"0"`, `"0.0"`, `"-0.0"` -> `0.0`) and never treats `0.0` as falsy, while safely
   returning `0.0` for `None`, `NaN`, `Inf`, `-Inf`, booleans, or invalid strings.
2. Guards optional third-party imports (`google.cloud.bigquery`,
   `google.cloud.bigquery_storage`, `requests`) with `try ... except ImportError`
   and provides native Python stdlib HTTP (`urllib.request` + `gcloud auth` /
   `google.auth`) and injectable `session` / `bq_inserter` callbacks for offline
   unit testing.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
import math
import os
import subprocess
import sys
import time
from typing import Any, Callable, Optional
import urllib.error
import urllib.parse
import urllib.request

try:
  import google.auth  # type: ignore
except ImportError:  # pragma: no cover
  google = None  # type: ignore

try:
  from google.auth.transport.requests import AuthorizedSession  # type: ignore
except ImportError:  # pragma: no cover
  AuthorizedSession = None  # type: ignore

try:
  from google.cloud import bigquery  # type: ignore
except ImportError:  # pragma: no cover
  bigquery = None  # type: ignore

try:
  from google.cloud import bigquery_storage  # type: ignore
except ImportError:  # pragma: no cover
  bigquery_storage = None  # type: ignore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("gsk_promql_batcher")

DEFAULT_PROJECT_ID = os.getenv("GCP_PROJECT", "gke-demos-363017")
DEFAULT_DATASET_ID = os.getenv("BQ_DATASET", "gsk_observability_demo")
DEFAULT_TABLE_ID = os.getenv("BQ_METRIC_TABLE", "enterprise_telemetry_partitioned")

PROJECT_ID = DEFAULT_PROJECT_ID
DATASET_ID = DEFAULT_DATASET_ID
TABLE_ID = DEFAULT_TABLE_ID

PROMQL_QUERIES: dict[str, str] = {
    "cpu_usage_pct": (
        "avg by (host_name)"
        " (rate(compute_googleapis_com:instance_cpu_utilization[1m])) * 100"
    ),
    "memory_usage_pct": (
        "avg by (host_name)"
        " (compute_googleapis_com:instance_memory_balloon_ram_used)"
    ),
    "network_bytes_sec": (
        "sum by (host_name)"
        " (rate(compute_googleapis_com:instance_network_received_bytes_count[1m]))"
    ),
}


def extract_metric_float(raw_val: Any) -> float:
  """Safely extracts numeric metric values, preserving 0.0 deterministically.

  Handles None, NaN, Inf, -Inf, booleans, and non-numeric representations
  without dropping zero states or raising exceptions.

  Args:
    raw_val: Raw metric value from PromQL matrix vector `[ts_epoch, raw_val]`.

  Returns:
    Deterministic Python `float` (`0.0` for zero, null, NaN, Inf, or invalid).
  """
  if raw_val is None or isinstance(raw_val, bool):
    return 0.0
  try:
    val = float(raw_val)
    if math.isnan(val) or math.isinf(val):
      return 0.0
    if val == 0.0:
      return 0.0
    return val
  except (ValueError, TypeError, OverflowError):
    return 0.0


def compute_aligned_window(
    now_epoch: Optional[float] = None,
    window_seconds: int = 300,
    step_seconds: int = 60,
) -> tuple[int, int, str]:
  """Computes a 60-second epoch-aligned PromQL query_range window.

  Args:
    now_epoch: Optional reference UNIX epoch timestamp (defaults to `time.time()`).
    window_seconds: Trailing window duration in seconds (default: 300s / 5m).
    step_seconds: PromQL evaluation step in seconds (default: 60s).

  Returns:
    Tuple of `(start_epoch, end_epoch, step_str)` where `step_str` is `"60s"`.
  """
  current = float(time.time() if now_epoch is None else now_epoch)
  step = max(1, int(step_seconds))
  end_epoch = int(current // step) * step
  start_epoch = end_epoch - max(step, int(window_seconds))
  return start_epoch, end_epoch, f"{step}s"


def _format_utc_iso(ts_epoch: float | int | str) -> str:
  """Formats a UNIX epoch timestamp into an ISO-8601 UTC string."""
  epoch_val = extract_metric_float(ts_epoch)
  return datetime.fromtimestamp(epoch_val, tz=timezone.utc).strftime(
      "%Y-%m-%dT%H:%M:%SZ"
  )


def parse_promql_matrix_response(
    metric_col: str,
    payload: dict[str, Any],
    host_records: Optional[dict[tuple[str, int], dict[str, Any]]] = None,
    enterprise_domain: str = "Pharma_Manufacturing",
) -> dict[tuple[str, int], dict[str, Any]]:
  """Parses a PromQL `query_range` matrix response into per-(host, minute) records.

  Preserves `0.0` values deterministically and merges multiple metric columns
  (`cpu_usage_pct`, `memory_usage_pct`, `network_bytes_sec`, `io_wait_ms`)
  into unified rows keyed by `(host_name, int(ts_epoch))`.

  Args:
    metric_col: Target column name (`"cpu_usage_pct"`, `"memory_usage_pct"`,
      `"network_bytes_sec"`, or `"io_wait_ms"`).
    payload: Decoded JSON response from PromQL `query_range` endpoint.
    host_records: Optional mutable dictionary accumulating `(host_name, ts_epoch)`
      records across multiple metric queries.
    enterprise_domain: Default enterprise domain label (`"Pharma_Manufacturing"`).

  Returns:
    Updated `host_records` mapping `(host_name, int_epoch)` to BigQuery row dicts.
  """
  if host_records is None:
    host_records = {}

  if not isinstance(payload, dict):
    return host_records

  data_block = payload.get("data")
  if not isinstance(data_block, dict):
    return host_records

  results = data_block.get("result")
  if not isinstance(results, list):
    return host_records

  for series in results:
    if not isinstance(series, dict):
      continue
    metric_labels = series.get("metric") or {}
    if not isinstance(metric_labels, dict):
      metric_labels = {}

    host_name = (
        metric_labels.get("host_name")
        or metric_labels.get("hostname")
        or metric_labels.get("instance")
        or "unknown_host"
    )
    host_id = metric_labels.get("host_id") or host_name
    site_loc = metric_labels.get("site_location", "Site_B_Stevenage")
    system_id = metric_labels.get("system_id", "EBRS")
    cluster_id = metric_labels.get("cluster_id", "aks-prod-stv-01")
    app_tier = metric_labels.get("application_tier", "Batch_Processing")
    domain = metric_labels.get("enterprise_domain", enterprise_domain)

    values = series.get("values")
    if values is None and "value" in series and isinstance(series["value"], (list, tuple)):
      values = [series["value"]]
    if not isinstance(values, list):
      continue

    for point in values:
      if not isinstance(point, (list, tuple)) or len(point) < 2:
        continue
      ts_raw, raw_val = point[0], point[1]
      try:
        ts_epoch = int(float(ts_raw))
      except (ValueError, TypeError):
        continue

      numeric_val = extract_metric_float(raw_val)
      iso_timestamp = _format_utc_iso(ts_epoch)
      record_key = (str(host_name), ts_epoch)

      if record_key not in host_records:
        host_records[record_key] = {
            "timestamp": iso_timestamp,
            "enterprise_domain": domain,
            "site_location": site_loc,
            "system_id": system_id,
            "cluster_id": cluster_id,
            "application_tier": app_tier,
            "host_id": str(host_id),
            "hostname": str(host_name),
            "cpu_usage_pct": 0.0,
            "memory_usage_pct": 0.0,
            "io_wait_ms": 0.0,
            "network_bytes_sec": 0,
            "status": "HEALTHY",
            "active_connections": 0,
        }

      row = host_records[record_key]
      if metric_col == "network_bytes_sec":
        row["network_bytes_sec"] = int(round(numeric_val))
      elif metric_col == "active_connections":
        row["active_connections"] = int(round(numeric_val))
      elif metric_col in ("cpu_usage_pct", "memory_usage_pct", "io_wait_ms"):
        row[metric_col] = float(numeric_val)
      else:
        row[metric_col] = float(numeric_val)

      # Flag status automatically if CPU or memory breaches critical saturation
      if row["cpu_usage_pct"] >= 90.0 or row["memory_usage_pct"] >= 95.0 or row["io_wait_ms"] >= 520.0:
        row["status"] = "ANOMALY"

  return host_records


def _get_access_token() -> str:
  """Retrieves an OAuth2 access token via google.auth or gcloud CLI."""
  if google is not None and hasattr(google, "auth"):
    try:
      creds, _ = google.auth.default(
          scopes=[
              "https://www.googleapis.com/auth/monitoring.read",
              "https://www.googleapis.com/auth/bigquery",
              "https://www.googleapis.com/auth/cloud-platform",
          ]
      )
      if creds and getattr(creds, "token", None):
        return str(creds.token)
    except Exception:  # pylint: disable=broad-except
      pass

  proc = subprocess.run(
      ["gcloud", "auth", "print-access-token"],
      capture_output=True,
      text=True,
      check=False,
  )
  return proc.stdout.strip()


class _StdlibHttpResponse:
  """Minimal requests.Response-compatible wrapper around urllib response."""

  def __init__(self, status_code: int, text: str) -> None:
    self.status_code = status_code
    self.text = text

  def json(self) -> dict[str, Any]:
    return json.loads(self.text or "{}")


class StdlibAuthorizedSession:
  """Zero-dependency HTTP session using urllib.request + OAuth bearer token."""

  def __init__(self, token: Optional[str] = None) -> None:
    self._token = token if token is not None else _get_access_token()

  def get(
      self,
      url: str,
      params: Optional[dict[str, str]] = None,
      timeout: int = 30,
  ) -> _StdlibHttpResponse:
    """Executes an authenticated HTTP GET request with URL query parameters."""
    full_url = url
    if params:
      query_string = urllib.parse.urlencode(params)
      sep = "&" if "?" in url else "?"
      full_url = f"{url}{sep}{query_string}"
    headers = {"Accept": "application/json"}
    if self._token:
      headers["Authorization"] = f"Bearer {self._token}"
    req = urllib.request.Request(full_url, headers=headers, method="GET")
    try:
      with urllib.request.urlopen(req, timeout=timeout) as resp:
        return _StdlibHttpResponse(resp.status, resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
      return _StdlibHttpResponse(
          exc.code, exc.read().decode("utf-8", errors="replace")
      )
    except Exception as exc:  # pylint: disable=broad-except
      return _StdlibHttpResponse(500, json.dumps({"error": str(exc)}))


def _insert_rows_via_bq_rest(
    project_id: str,
    dataset_id: str,
    table_id: str,
    rows: list[dict[str, Any]],
    token: Optional[str] = None,
) -> list[dict[str, Any]]:
  """Streams rows into BigQuery using the REST `tabledata.insertAll` API."""
  if not rows:
    return []
  auth_token = token if token is not None else _get_access_token()
  if not auth_token:
    return [{"error": "No active GCP OAuth access token available"}]

  url = (
      f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}"
      f"/datasets/{dataset_id}/tables/{table_id}/insertAll"
  )
  headers = {
      "Authorization": f"Bearer {auth_token}",
      "Content-Type": "application/json",
  }
  errors: list[dict[str, Any]] = []
  for idx in range(0, len(rows), 250):
    batch = rows[idx : idx + 250]
    payload = {
        "kind": "bigquery#tableDataInsertAllRequest",
        "rows": [
            {
                "insertId": f"{r.get('host_id', 'host')}-{r.get('timestamp', idx)}-{j}",
                "json": r,
            }
            for j, r in enumerate(batch)
        ],
    }
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
      with urllib.request.urlopen(req, timeout=30) as resp:
        body = json.loads(resp.read().decode("utf-8") or "{}")
        if body.get("insertErrors"):
          errors.extend(body["insertErrors"])
    except urllib.error.HTTPError as exc:
      err_text = exc.read().decode("utf-8", errors="replace")
      errors.append({"httpStatus": exc.code, "message": err_text})
    except Exception as exc:  # pylint: disable=broad-except
      errors.append({"error": str(exc)})
  return errors


def fetch_and_ingest_metrics(
    project_id: str = DEFAULT_PROJECT_ID,
    dataset_id: str = DEFAULT_DATASET_ID,
    table_id: str = DEFAULT_TABLE_ID,
    session: Any = None,
    bq_inserter: Optional[Callable[[str, list[dict[str, Any]]], list[Any]] | Any] = None,
    now_epoch: Optional[float] = None,
    queries: Optional[dict[str, str]] = None,
    enterprise_domain: str = "Pharma_Manufacturing",
) -> list[dict[str, Any]]:
  """Queries Cloud Monitoring PromQL `query_range` (step=60s) and streams into BigQuery.

  Supports both live Cloud Monitoring + BigQuery REST/Storage Write API execution
  and dependency-injected `session` and `bq_inserter` objects for unit testing.

  Args:
    project_id: Target Google Cloud project ID.
    dataset_id: Target BigQuery dataset ID (`gsk_observability_demo`).
    table_id: Target BigQuery table ID (`enterprise_telemetry_partitioned`).
    session: Optional HTTP session with `.get(url, params=..., timeout=...)` or
      callable `session(url, params)`.
    bq_inserter: Optional callable `bq_inserter(table_ref, rows)` or BigQuery
      client object exposing `.insert_rows_json(table_ref, rows)`.
    now_epoch: Optional reference epoch timestamp for deterministic window alignment.
    queries: Optional override mapping metric column names to PromQL expressions.
    enterprise_domain: Default `enterprise_domain` column value.

  Returns:
    List of evaluated telemetry row dictionaries prepared/streamed into BigQuery.

  Raises:
    RuntimeError: If `bq_inserter` returns insertion errors.
  """
  start_epoch, end_epoch, step_str = compute_aligned_window(
      now_epoch=now_epoch, window_seconds=300, step_seconds=60
  )
  active_queries = queries if queries is not None else PROMQL_QUERIES

  if session is None:
    if AuthorizedSession is not None and google is not None:
      try:
        creds, _ = google.auth.default(
            scopes=[
                "https://www.googleapis.com/auth/monitoring.read",
                "https://www.googleapis.com/auth/bigquery",
            ]
        )
        session = AuthorizedSession(creds)
      except Exception:  # pylint: disable=broad-except
        session = StdlibAuthorizedSession()
    else:
      session = StdlibAuthorizedSession()

  api_url = (
      f"https://monitoring.googleapis.com/v1/projects/{project_id}"
      "/location/global/prometheus/api/v1/query_range"
  )
  host_records: dict[tuple[str, int], dict[str, Any]] = {}

  for metric_name, promql_query in active_queries.items():
    logger.info("Executing PromQL query_range for %s (step=%s)...", metric_name, step_str)
    params = {
        "query": promql_query,
        "start": str(start_epoch),
        "end": str(end_epoch),
        "step": step_str,
    }
    try:
      if hasattr(session, "get"):
        resp = session.get(api_url, params=params, timeout=30)
      elif callable(session):
        resp = session(api_url, params)
      else:
        raise TypeError(f"Unsupported session type: {type(session)}")

      status_code = getattr(resp, "status_code", 200)
      if status_code != 200:
        logger.error(
            "PromQL API error (%s) for %s: %s",
            status_code,
            metric_name,
            getattr(resp, "text", ""),
        )
        continue

      payload = resp.json() if hasattr(resp, "json") and callable(resp.json) else resp
      if isinstance(payload, dict):
        parse_promql_matrix_response(
            metric_col=metric_name,
            payload=payload,
            host_records=host_records,
            enterprise_domain=enterprise_domain,
        )
    except Exception as exc:  # pylint: disable=broad-except
      logger.error("Failed to query %s: %s", metric_name, exc)

  rows_to_insert = [
      host_records[key] for key in sorted(host_records.keys())
  ]

  if rows_to_insert:
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    logger.info(
        "Streaming %d evaluated points into BigQuery table: %s",
        len(rows_to_insert),
        table_ref,
    )
    if bq_inserter is not None:
      if hasattr(bq_inserter, "insert_rows_json"):
        errors = bq_inserter.insert_rows_json(table_ref, rows_to_insert)
      elif callable(bq_inserter):
        errors = bq_inserter(table_ref, rows_to_insert)
      else:
        raise TypeError(f"Unsupported bq_inserter type: {type(bq_inserter)}")
    elif bigquery is not None:
      bq_client = bigquery.Client(project=project_id)
      errors = bq_client.insert_rows_json(table_ref, rows_to_insert)
    else:
      errors = _insert_rows_via_bq_rest(
          project_id=project_id,
          dataset_id=dataset_id,
          table_id=table_id,
          rows=rows_to_insert,
      )

    if errors:
      logger.error("BigQuery streaming errors encountered: %s", errors)
      raise RuntimeError(f"BigQuery streaming errors encountered: {errors}")
    logger.info("Successfully ingested %d metric rows into %s.", len(rows_to_insert), table_ref)
  else:
    logger.warning("No metric data points retrieved in this 5-minute window.")

  return rows_to_insert


# Alias matching blueprint Section 2.2 entrypoint name
sync_gmp_to_bigquery = fetch_and_ingest_metrics


def run_self_test() -> dict[str, Any]:
  """Runs deterministic self-tests on `extract_metric_float` and matrix parsing."""
  zero_inputs = [0, 0.0, -0.0, "0", "0.0", "-0.0", "0.0000"]
  for z in zero_inputs:
    val = extract_metric_float(z)
    assert isinstance(val, float) and val == 0.0, f"Failed zero preservation for {z!r}: {val!r}"

  invalid_inputs = [
      None,
      float("nan"),
      float("inf"),
      float("-inf"),
      "NaN",
      "nan",
      "Inf",
      "-Inf",
      "null",
      "non_numeric",
      "",
      True,
      False,
      {},
      [],
  ]
  for inv in invalid_inputs:
    val = extract_metric_float(inv)
    assert isinstance(val, float) and val == 0.0, f"Failed invalid fallback for {inv!r}: {val!r}"

  assert abs(extract_metric_float("94.5") - 94.5) < 1e-6
  assert abs(extract_metric_float(55.25) - 55.25) < 1e-6

  # Test matrix parsing & fetch_and_ingest_metrics with mock session + mock inserter
  mock_payloads = {
      "cpu_usage_pct": {
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
                      "values": [
                          [1759161600, "74.2"],
                          [1759161660, "0.0"],
                          [1759161720, "NaN"],
                      ],
                  }
              ],
          },
      },
      "memory_usage_pct": {
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
                      "values": [
                          [1759161600, "68.5"],
                          [1759161660, "0"],
                          [1759161720, None],
                      ],
                  }
              ],
          },
      },
      "network_bytes_sec": {
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
                      "values": [
                          [1759161600, "125000"],
                          [1759161660, "0.0"],
                          [1759161720, "0"],
                      ],
                  }
              ],
          },
      },
  }

  class _MockSession:
    def get(self, url: str, params: dict[str, str], timeout: int = 30) -> _StdlibHttpResponse:
      del url, timeout
      q = params.get("query", "")
      if "cpu_utilization" in q:
        return _StdlibHttpResponse(200, json.dumps(mock_payloads["cpu_usage_pct"]))
      if "memory_balloon" in q:
        return _StdlibHttpResponse(200, json.dumps(mock_payloads["memory_usage_pct"]))
      return _StdlibHttpResponse(200, json.dumps(mock_payloads["network_bytes_sec"]))

  captured_batches: list[tuple[str, list[dict[str, Any]]]] = []

  def _mock_inserter(table_ref: str, rows: list[dict[str, Any]]) -> list[Any]:
    captured_batches.append((table_ref, rows))
    return []

  rows = fetch_and_ingest_metrics(
      project_id="gke-demos-363017",
      dataset_id="gsk_observability_demo",
      table_id="enterprise_telemetry_partitioned",
      session=_MockSession(),
      bq_inserter=_mock_inserter,
      now_epoch=1759161780.0,
  )
  assert len(rows) == 3, f"Expected 3 merged rows, got {len(rows)}"
  assert rows[0]["cpu_usage_pct"] == 74.2
  assert rows[1]["cpu_usage_pct"] == 0.0 and isinstance(rows[1]["cpu_usage_pct"], float)
  assert rows[2]["cpu_usage_pct"] == 0.0 and isinstance(rows[2]["cpu_usage_pct"], float)
  assert len(captured_batches) == 1
  return {
      "status": "PASSED",
      "tested_zero_inputs": len(zero_inputs),
      "tested_invalid_inputs": len(invalid_inputs),
      "ingested_rows": len(rows),
  }


def main(argv: Optional[list[str]] = None) -> int:
  """CLI entrypoint for PromQL query_range micro-batcher."""
  parser = argparse.ArgumentParser(
      description="GSK Enterprise Observability PromQL query_range Micro-Batcher."
  )
  parser.add_argument("--project", default=DEFAULT_PROJECT_ID, help="GCP Project ID")
  parser.add_argument("--dataset", default=DEFAULT_DATASET_ID, help="BigQuery Dataset ID")
  parser.add_argument("--table", default=DEFAULT_TABLE_ID, help="BigQuery Table ID")
  parser.add_argument(
      "--self-test",
      action="store_true",
      help="Run deterministic unit tests for 0.0 float preservation and matrix parsing.",
  )
  args = parser.parse_args(argv)

  if args.self_test:
    summary = run_self_test()
    print(json.dumps(summary, indent=2))
    return 0

  fetch_and_ingest_metrics(
      project_id=args.project,
      dataset_id=args.dataset,
      table_id=args.table,
  )
  return 0


if __name__ == "__main__":
  sys.exit(main())
