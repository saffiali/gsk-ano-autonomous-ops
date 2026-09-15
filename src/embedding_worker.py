#!/usr/bin/env python3
"""GSK Autonomous Operations (ANO) — Real-Time Log Chunker & Embedding Worker.

This module implements Pillar 2 of the GSK ANO architecture:
1. Multi-line stack trace coalescing (Java, Python, and network OSPF/BGP dumps).
2. Lexical shape normalization & dynamic token masking (<TIMESTAMP>, <UUID>,
   <IP_PORT>, <HEX_ID>, <PATH>, <DURATION>, <NUM>) while preserving exception
   class names and stack frame signatures (ClassName.method(File.java:<LINE>)).
3. Per-entity sliding-window log chunking (window_size=5, stride=2, 60% overlap)
   keyed by (host_id, service_name, domain) with immediate flush on ERROR/CRITICAL
   or time gaps > 30 seconds.
4. Vertex AI `text-embedding-005` (768 dimensions, RETRIEVAL_DOCUMENT) payload
   formatting and response parsing.
5. BigQuery `log_embeddings` streaming row formatting with deterministic SHA-256
   `chunk_id` insertId deduplication.
6. Clean dependency injection supporting both GCP Cloud Run v2 production
   execution (via google-auth REST sessions) and 100% deterministic offline unit
   testing without missing-package ImportErrors.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
import math
import os
import re
import threading
from typing import Any, Callable, Optional, Protocol

# Configure structured logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("gsk_ano.embedding_worker")

# Severity ordering for determining maximum severity across a sliding window
SEVERITY_RANK: dict[str, int] = {
    "DEBUG": 10,
    "INFO": 20,
    "NOTICE": 25,
    "WARNING": 30,
    "WARN": 30,
    "ERROR": 40,
    "CRITICAL": 50,
    "FATAL": 50,
    "ALERT": 60,
    "EMERGENCY": 70,
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
class NormalizedLogEntry:
  """Represents a single coalesced and normalized operational log entry."""

  log_id: str
  timestamp: str
  epoch_seconds: float
  severity: str
  service_name: str
  host_id: str
  environment: str
  domain: str
  raw_payload: str
  normalized_text: str
  message_template: str
  has_stack_trace: bool
  trace_id: Optional[str] = None
  span_id: Optional[str] = None
  labels: Optional[dict[str, Any]] = None


@dataclass
class LogChunk:
  """Represents a sliding-window chunk of log entries ready for embedding."""

  chunk_id: str
  log_id: str  # Anchor log_id (first log in window)
  timestamp: str  # Anchor timestamp (ISO-8601)
  service_name: str
  host_id: str
  domain: str
  severity: str  # Highest severity present in the window
  chunk_text: str
  log_count: int
  has_stack_trace: bool
  log_ids: list[str] = field(default_factory=list)


def parse_iso_timestamp(ts_val: Any) -> tuple[str, float]:
  """Parses an ISO-8601 string or numeric timestamp into (iso_str, epoch_sec)."""
  if isinstance(ts_val, (int, float)) and not isinstance(ts_val, bool):
    val = float(ts_val)
    if not (math.isnan(val) or math.isinf(val)):
      # Auto-scale nanosecond (>1e16) or millisecond (>1e11) timestamps to seconds
      if abs(val) > 1e16:
        val /= 1e9
      elif abs(val) > 1e11:
        val /= 1e3
      try:
        dt = datetime.fromtimestamp(val, tz=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ"), val
      except (ValueError, OverflowError, OSError):
        pass
  if isinstance(ts_val, str) and ts_val.strip():
    clean_ts = ts_val.strip()
    try:
      if clean_ts.endswith(("Z", "z")):
        dt = datetime.fromisoformat(clean_ts[:-1] + "+00:00")
      else:
        dt = datetime.fromisoformat(clean_ts)
      if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
      return dt.strftime("%Y-%m-%dT%H:%M:%SZ"), dt.timestamp()
    except ValueError:
      pass
  now = datetime.now(timezone.utc)
  return now.strftime("%Y-%m-%dT%H:%M:%SZ"), now.timestamp()


class StackTraceCoalescer:
  """Coalesces multi-line Java/Python stack traces and network dumps into atomic logs."""

  # Patterns indicating a line is a continuation of a preceding log/stack trace
  CONTINUATION_REGEX = re.compile(
      r"^(?:"
      r"\s+at\s+[\w.$<>]+\([^\)]*\)"  # Java stack frame: "  at org.apache..."
      r"|\s*Caused\s+by:\s+.*"  # Java root cause: "Caused by: ..."
      r"|\s*\.\.\.\s+\d+\s+more"  # Java elided frames: "  ... 15 more"
      r"|\s*Traceback\s+\(most\s+recent\s+call\s+last\):"  # Python traceback header
      r"|\s+File\s+\"[^\"]+\",\s+line\s+\d+.*"  # Python stack frame
      r"|\s*[\w.$]+(?:Exception|Error|Throwable):.*"  # Exception header line
      r"|\t+.*"  # Tab-indented continuation line
      r"|\s{2,}\S+.*"  # 2+ leading spaces continuation
      r")"
  )

  # Pattern identifying start of a new timestamped log line
  NEW_LOG_LINE_REGEX = re.compile(
      r"^(?:\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}|[A-Z][a-z]{2}\s+\d+\s+\d{2}:\d{2}:\d{2}|\[\d{4}-\d{2}-\d{2})"
  )

  @classmethod
  def is_continuation_line(cls, line: str) -> bool:
    """Returns True if the line is part of a stack trace or multi-line payload."""
    if not line:
      return False
    if cls.NEW_LOG_LINE_REGEX.match(line):
      return False
    return bool(cls.CONTINUATION_REGEX.match(line))

  @classmethod
  def coalesce_raw_lines(cls, lines: list[str]) -> list[str]:
    """Merges multi-line stack traces from a list of raw text log lines."""
    if not lines:
      return []
    coalesced: list[str] = []
    current_block: list[str] = []

    for line in lines:
      if not current_block:
        current_block.append(line)
      elif cls.is_continuation_line(line):
        current_block.append(line)
      else:
        coalesced.append("\n".join(current_block))
        current_block = [line]

    if current_block:
      coalesced.append("\n".join(current_block))
    return coalesced

  @classmethod
  def coalesce_log_entries(
      cls, entries: list[dict[str, Any]]
  ) -> list[dict[str, Any]]:
    """Coalesces sequential structured log entries belonging to the same entity."""
    if not entries or not isinstance(entries, list):
      return []
    coalesced: list[dict[str, Any]] = []

    for raw_entry in entries:
      if isinstance(raw_entry, dict):
        entry = raw_entry
      elif isinstance(raw_entry, str):
        entry = {"raw_payload": raw_entry}
      else:
        continue
      payload = str(entry.get("raw_payload", entry.get("message", "")))
      if not coalesced:
        entry_copy = dict(entry)
        entry_copy["raw_payload"] = payload
        coalesced.append(entry_copy)
        continue

      prev = coalesced[-1]
      same_entity = (
          prev.get("host_id") == entry.get("host_id")
          and prev.get("service_name") == entry.get("service_name")
          and prev.get("domain") == entry.get("domain")
      )
      first_line = payload.splitlines()[0] if payload else ""
      if same_entity and cls.is_continuation_line(first_line):
        prev["raw_payload"] = f"{prev['raw_payload']}\n{payload}"
        # Upgrade severity if continuation has higher rank
        prev_rank = SEVERITY_RANK.get(str(prev.get("severity", "INFO")).upper(), 20)
        curr_rank = SEVERITY_RANK.get(str(entry.get("severity", "INFO")).upper(), 20)
        if curr_rank > prev_rank:
          prev["severity"] = entry.get("severity")
      else:
        entry_copy = dict(entry)
        entry_copy["raw_payload"] = payload
        coalesced.append(entry_copy)

    return coalesced


class LogNormalizer:
  """Normalizes operational log messages while preserving stack trace structure."""

  # Stack trace detection regex
  STACK_TRACE_DETECTOR = re.compile(
      r"(?:Traceback\s+\(most\s+recent\s+call\s+last\):|Caused\s+by:|^\s+at\s+[\w.$<>]+\(|^\s+File\s+\"[^\"]+\",\s+line\s+\d+)",
      re.MULTILINE,
  )

  # Java stack frame line number pattern: (FileName.java:123) -> (FileName.java:<LINE>)
  JAVA_FRAME_LINE_REGEX = re.compile(r"\(([A-Za-z0-9_$]+\.[a-zA-Z]+):(\d+)\)")

  # Python stack frame pattern: File "/path/to/script.py", line 123 -> File "<PATH>/script.py", line <LINE>
  PYTHON_FRAME_LINE_REGEX = re.compile(
      r'(File\s+")([^"]*?)([^"/\\]+\.py)(",\s+line\s+)(\d+)'
  )

  # Ordered lexical replacement rules
  REGEX_RULES: list[tuple[re.Pattern[str], str]] = [
      # 1. ISO-8601 & syslog timestamps
      (
          re.compile(
              r"\b\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:[.,]\d{1,9})?(?:Z|[+-]\d{2}:?\d{2})?\b"
          ),
          "<TIMESTAMP>",
      ),
      (
          re.compile(r"\b[A-Z][a-z]{2}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\b"),
          "<TIMESTAMP>",
      ),
      # 2. UUIDs
      (
          re.compile(
              r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
          ),
          "<UUID>",
      ),
      # 3. IPv4 / IPv6 addresses with optional port
      (
          re.compile(
              r"\b(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d\d?)(?::\d{1,5})?\b"
          ),
          "<IP_PORT>",
      ),
      # 4. Hexadecimal IDs and memory addresses (0x... or 8+ hex chars with digits)
      (re.compile(r"\b0x[0-9a-fA-F]+\b"), "<HEX_ID>"),
      (
          re.compile(r"\b(?=[0-9a-fA-F]*\d)(?=[0-9a-fA-F]*[a-fA-F])[0-9a-fA-F]{8,}\b"),
          "<HEX_ID>",
      ),
      # 5. Duration literals (e.g. 142ms, 3.5s, 500us)
      (
          re.compile(r"\b\d+(?:\.\d+)?\s*(?:ms|us|ns|s|sec|seconds)\b"),
          "<DURATION>",
      ),
      # 6. Unix file paths (excluding URLs or simple words; starts with / and at least 2 segments)
      (
          re.compile(r"(?<![\w/])/(?:[a-zA-Z0-9_.-]+/)+[a-zA-Z0-9_.-]+"),
          "<PATH>",
      ),
      # 7. Standalone numeric literals (excluding numbers inside identifiers or <LINE>)
      (re.compile(r"(?<![\w<>])\d+(?:\.\d+)?(?![\w<>])"), "<NUM>"),
  ]

  def __init__(self, max_stack_frames: int = 15) -> None:
    self.max_stack_frames = max(2, int(max_stack_frames))

  def _truncate_stack_frames(self, text: str) -> str:
    """Preserves top 5 and bottom 10 stack frames (or proportional split) if trace exceeds max_stack_frames."""
    lines = text.splitlines()
    frame_indices = [
        i
        for i, line in enumerate(lines)
        if re.match(r"^\s*(?:at\s+[\w.$<>]+\(|File\s+\")", line)
    ]
    if len(frame_indices) <= self.max_stack_frames:
      return text

    if self.max_stack_frames == 15:
      top_keep = 5
      bottom_keep = 10
    else:
      top_keep = max(1, self.max_stack_frames // 3)
      bottom_keep = max(1, self.max_stack_frames - top_keep)

    omitted_count = len(frame_indices) - (top_keep + bottom_keep)
    if omitted_count <= 0:
      return text

    first_omit_idx = frame_indices[top_keep]
    last_omit_idx = frame_indices[-bottom_keep] - 1

    new_lines = (
        lines[:first_omit_idx]
        + [f"    ... [{omitted_count} middle frames omitted] ..."]
        + lines[last_omit_idx + 1 :]
    )
    return "\n".join(new_lines)

  def normalize(self, raw_message: str) -> tuple[str, bool]:
    """Normalizes volatile tokens while preserving exception and stack trace semantics.

    Args:
      raw_message: Raw multi-line operational log message.

    Returns:
      Tuple of (normalized_text, has_stack_trace).
    """
    if not raw_message:
      return "", False

    has_stack_trace = bool(self.STACK_TRACE_DETECTOR.search(raw_message))
    text = raw_message

    if has_stack_trace:
      text = self._truncate_stack_frames(text)

    # First protect Java/Python stack frame file+line signatures
    # e.g., StandardWrapperValve.invoke(StandardWrapperValve.java:199)
    # -> StandardWrapperValve.invoke(StandardWrapperValve.java:__FRAME_LINE__)
    text = self.JAVA_FRAME_LINE_REGEX.sub(r"(\1:__FRAME_LINE__)", text)
    text = self.PYTHON_FRAME_LINE_REGEX.sub(
        r"\1<PATH>/\3\4__FRAME_LINE__", text
    )

    # Apply ordered lexical rules
    for pattern, replacement in self.REGEX_RULES:
      text = pattern.sub(replacement, text)

    # Restore frame line marker as <LINE>
    text = text.replace("__FRAME_LINE__", "<LINE>")
    return text, has_stack_trace

  def extract_message_template(self, raw_message: str) -> str:
    """Extracts single-line normalized message template for raw_logs table."""
    first_line = raw_message.splitlines()[0] if raw_message else ""
    normalized, _ = self.normalize(first_line)
    return normalized


class SlidingWindowChunker:
  """Maintains per-entity sliding window buffers and emits overlapping LogChunks."""

  def __init__(
      self,
      window_size: int = 5,
      stride: int = 2,
      max_window_gap_sec: float = 30.0,
      max_chunk_chars: int = 6000,
      normalizer: Optional[LogNormalizer] = None,
  ) -> None:
    if window_size < 1:
      raise ValueError("window_size must be >= 1")
    if stride < 1 or stride > window_size:
      raise ValueError("stride must be between 1 and window_size")
    self.window_size = window_size
    self.stride = stride
    self.max_window_gap_sec = max_window_gap_sec
    self.max_chunk_chars = max_chunk_chars
    self.normalizer = normalizer or LogNormalizer()
    # Keyed by (host_id, service_name, domain)
    self._buffers: dict[tuple[str, str, str], list[NormalizedLogEntry]] = {}
    self._lock = threading.Lock()

  @staticmethod
  def compute_chunk_id(
      host_id: str,
      service_name: str,
      domain: str,
      anchor_log_id: str,
      window_start_ts: str,
  ) -> str:
    """Computes a deterministic 32-character SHA-256 hex chunk_id."""
    raw_key = f"{host_id}:{service_name}:{domain}:{anchor_log_id}:{window_start_ts}"
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()[:32]

  def _cap_chunk_text(self, text: str) -> str:
    """Enforces max_chunk_chars limit using head+tail preservation."""
    if len(text) <= self.max_chunk_chars:
      return text
    half = (self.max_chunk_chars - 45) // 2
    return (
        text[:half]
        + "\n... [CHUNK TRUNCATED FOR TOKEN LIMIT] ...\n"
        + text[-half:]
    )

  def _build_chunk(
      self, key: tuple[str, str, str], window: list[NormalizedLogEntry]
  ) -> LogChunk:
    """Constructs a LogChunk from a slice of NormalizedLogEntry objects."""
    host_id, service_name, domain = key
    anchor = window[0]
    highest_sev = "INFO"
    highest_rank = -1
    has_trace = False
    formatted_lines: list[str] = []
    log_ids: list[str] = []

    for item in window:
      log_ids.append(item.log_id)
      rank = SEVERITY_RANK.get(item.severity.upper(), 20)
      if rank > highest_rank:
        highest_rank = rank
        highest_sev = item.severity.upper()
      if item.has_stack_trace:
        has_trace = True
      formatted_lines.append(
          f"[{item.timestamp}] [{item.severity.upper()}]"
          f" [{item.service_name}@{item.host_id}] {item.normalized_text}"
      )

    combined_text = self._cap_chunk_text("\n".join(formatted_lines))
    chunk_id = self.compute_chunk_id(
        host_id=host_id,
        service_name=service_name,
        domain=domain,
        anchor_log_id=anchor.log_id,
        window_start_ts=anchor.timestamp,
    )
    return LogChunk(
        chunk_id=chunk_id,
        log_id=anchor.log_id,
        timestamp=anchor.timestamp,
        service_name=service_name,
        host_id=host_id,
        domain=domain,
        severity=highest_sev,
        chunk_text=combined_text,
        log_count=len(window),
        has_stack_trace=has_trace,
        log_ids=log_ids,
    )

  def ingest_log(self, log_entry: dict[str, Any]) -> list[LogChunk]:
    """Ingests a single structured log entry and returns any emitted LogChunks."""
    host_id = str(log_entry.get("host_id", "unknown-host"))
    service_name = str(log_entry.get("service_name", "unknown-service"))
    domain = str(log_entry.get("domain", "COMPUTE")).upper()
    severity = str(log_entry.get("severity", "INFO")).upper()
    log_id = str(log_entry.get("log_id", hashlib.md5(str(log_entry).encode()).hexdigest()))
    raw_payload = str(log_entry.get("raw_payload", log_entry.get("message", "")))
    iso_ts, epoch_sec = parse_iso_timestamp(log_entry.get("timestamp"))

    norm_text, has_trace = self.normalizer.normalize(raw_payload)
    msg_template = self.normalizer.extract_message_template(raw_payload)

    norm_entry = NormalizedLogEntry(
        log_id=log_id,
        timestamp=iso_ts,
        epoch_seconds=epoch_sec,
        severity=severity,
        service_name=service_name,
        host_id=host_id,
        environment=str(log_entry.get("environment", "prod")),
        domain=domain,
        raw_payload=raw_payload,
        normalized_text=norm_text,
        message_template=msg_template,
        has_stack_trace=has_trace,
        trace_id=log_entry.get("trace_id"),
        span_id=log_entry.get("span_id"),
        labels=log_entry.get("labels"),
    )

    key = (host_id, service_name, domain)
    emitted: list[LogChunk] = []

    with self._lock:
      buffer = self._buffers.setdefault(key, [])

      # Check if time gap between first buffered log and current log exceeds max_window_gap_sec
      if buffer and (epoch_sec - buffer[0].epoch_seconds > self.max_window_gap_sec):
        emitted.append(self._build_chunk(key, list(buffer)))
        buffer.clear()

      buffer.append(norm_entry)

      # Check if buffer reached full window_size
      if len(buffer) >= self.window_size:
        window_slice = buffer[: self.window_size]
        emitted.append(self._build_chunk(key, window_slice))
        # Advance buffer by stride
        self._buffers[key] = buffer[self.stride :]
        buffer = self._buffers[key]
      elif severity in ("ERROR", "CRITICAL", "FATAL") or has_trace:
        # Immediate flush on critical error / stack trace so detection is never delayed
        emitted.append(self._build_chunk(key, list(buffer)))
        # Advance buffer by stride (or clear if smaller than stride) to avoid duplicate identical emits
        self._buffers[key] = buffer[self.stride :] if len(buffer) > self.stride else []

    return emitted

  def flush_all(self) -> list[LogChunk]:
    """Flushes all remaining partial buffers across all entities."""
    emitted: list[LogChunk] = []
    with self._lock:
      for key, buffer in list(self._buffers.items()):
        if buffer:
          emitted.append(self._build_chunk(key, list(buffer)))
        self._buffers[key] = []
    return emitted


class VertexAIEmbeddingClient:
  """Formats requests and parses 768-dim embeddings from Vertex AI text-embedding-005."""

  def __init__(
      self,
      project_id: str,
      region: str = "us-central1",
      model_name: str = "text-embedding-005",
      dimensions: int = 768,
      session: Optional[HttpSessionProtocol] = None,
  ) -> None:
    self.project_id = project_id
    self.region = region
    self.model_name = model_name
    self.dimensions = dimensions
    self._session = session

  def _get_session(self) -> HttpSessionProtocol:
    """Lazily initializes an AuthorizedSession using google.auth if none injected."""
    if self._session is not None:
      return self._session
    try:
      import google.auth
      from google.auth.transport.requests import AuthorizedSession

      credentials, _ = google.auth.default(
          scopes=["https://www.googleapis.com/auth/cloud-platform"]
      )
      self._session = AuthorizedSession(credentials)
      return self._session
    except Exception as exc:
      raise RuntimeError(
          f"Failed to initialize Google Cloud AuthorizedSession: {exc}"
      ) from exc

  @property
  def endpoint_url(self) -> str:
    return (
        f"https://{self.region}-aiplatform.googleapis.com/v1/projects/"
        f"{self.project_id}/locations/{self.region}/publishers/google/models/"
        f"{self.model_name}:predict"
    )

  def format_request_payload(self, chunks: list[LogChunk]) -> dict[str, Any]:
    """Formats a batch of LogChunks into the exact Vertex AI text-embedding-005 payload."""
    instances = []
    for chunk in chunks:
      instances.append({
          "content": chunk.chunk_text,
          "task_type": "RETRIEVAL_DOCUMENT",
          "title": f"{chunk.service_name}:{chunk.domain}:{chunk.severity}",
      })
    return {
        "instances": instances,
        "parameters": {
            "outputDimensionality": self.dimensions,
            "autoTruncate": True,
        },
    }

  def embed_chunks(self, chunks: list[LogChunk]) -> list[dict[str, Any]]:
    """Invokes Vertex AI text-embedding-005 and formats rows for BigQuery log_embeddings."""
    if not chunks:
      return []

    payload = self.format_request_payload(chunks)
    session = self._get_session()
    response = session.post(self.endpoint_url, json=payload, timeout=30.0)

    if hasattr(response, "raise_for_status"):
      response.raise_for_status()

    resp_json = response.json() if callable(getattr(response, "json", None)) else response
    predictions = resp_json.get("predictions", [])

    if len(predictions) != len(chunks):
      raise ValueError(
          f"Vertex AI prediction count mismatch: expected {len(chunks)}, got {len(predictions)}"
      )

    bq_rows: list[dict[str, Any]] = []
    for chunk, pred in zip(chunks, predictions):
      embeddings_obj = pred.get("embeddings", {})
      raw_values = embeddings_obj.get("values", [])
      vector = [float(val) for val in raw_values]

      if len(vector) != self.dimensions:
        raise ValueError(
            f"Invalid embedding dimension for chunk {chunk.chunk_id}: "
            f"expected {self.dimensions}, got {len(vector)}"
        )

      stats = embeddings_obj.get("statistics", {})
      token_count = int(stats.get("token_count", max(1, len(chunk.chunk_text) // 4)))

      bq_rows.append({
          "chunk_id": chunk.chunk_id,
          "log_id": chunk.log_id,
          "timestamp": chunk.timestamp,
          "service_name": chunk.service_name,
          "host_id": chunk.host_id,
          "domain": chunk.domain,
          "severity": chunk.severity,
          "chunk_text": chunk.chunk_text,
          "embedding": vector,
          "embedding_model": self.model_name,
          "token_count": token_count,
          "is_outlier": False,
      })

    return bq_rows


class BigQueryStreamWriter:
  """Streams formatted log_embeddings rows into BigQuery using tabledata.insertAll."""

  def __init__(
      self,
      project_id: str,
      dataset_id: str = "gsk_ano_ops",
      table_id: str = "log_embeddings",
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

  def format_insert_payload(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Formats rows with insertId = chunk_id for BigQuery streaming deduplication."""
    return {
        "kind": "bigquery#tableDataInsertAllRequest",
        "skipInvalidRows": False,
        "ignoreUnknownValues": False,
        "rows": [
            {
                "insertId": str(row["chunk_id"]),
                "json": row,
            }
            for row in rows
        ],
    }

  def write_rows(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Streams rows to BigQuery and checks for row-level insertErrors."""
    if not rows:
      return {"status": "NO_OP", "inserted_count": 0}

    payload = self.format_insert_payload(rows)
    session = self._get_session()
    response = session.post(self.endpoint_url, json=payload, timeout=30.0)

    if hasattr(response, "raise_for_status"):
      response.raise_for_status()

    resp_json = response.json() if callable(getattr(response, "json", None)) else response
    insert_errors = resp_json.get("insertErrors", [])
    if insert_errors:
      raise RuntimeError(f"BigQuery streaming insert errors: {json.dumps(insert_errors)}")

    return {
        "status": "SUCCESS",
        "inserted_count": len(rows),
        "table": f"{self.project_id}.{self.dataset_id}.{self.table_id}",
    }


class EmbeddingPipelineWorker:
  """End-to-end orchestrator combining coalescing, chunking, embedding, and BQ streaming."""

  def __init__(
      self,
      chunker: SlidingWindowChunker,
      embedding_client: VertexAIEmbeddingClient,
      bq_writer: BigQueryStreamWriter,
  ) -> None:
    self.chunker = chunker
    self.embedding_client = embedding_client
    self.bq_writer = bq_writer

  def process_log_batch(
      self, raw_entries: list[dict[str, Any]], flush_remaining: bool = False
  ) -> dict[str, Any]:
    """Processes a batch of structured log entries through the embedding pipeline."""
    coalesced_entries = StackTraceCoalescer.coalesce_log_entries(raw_entries)
    emitted_chunks: list[LogChunk] = []

    for entry in coalesced_entries:
      chunks = self.chunker.ingest_log(entry)
      emitted_chunks.extend(chunks)

    if flush_remaining:
      emitted_chunks.extend(self.chunker.flush_all())

    if not emitted_chunks:
      return {
          "status": "BUFFERED",
          "input_logs": len(raw_entries),
          "coalesced_logs": len(coalesced_entries),
          "chunks_emitted": 0,
          "rows_inserted": 0,
      }

    bq_rows = self.embedding_client.embed_chunks(emitted_chunks)
    write_result = self.bq_writer.write_rows(bq_rows)

    return {
        "status": "PROCESSED",
        "input_logs": len(raw_entries),
        "coalesced_logs": len(coalesced_entries),
        "chunks_emitted": len(emitted_chunks),
        "rows_inserted": write_result.get("inserted_count", 0),
        "chunk_ids": [c.chunk_id for c in emitted_chunks],
    }

  def handle_pubsub_envelope(self, envelope: Any) -> dict[str, Any]:
    """Parses a Pub/Sub push or Eventarc HTTP request envelope and processes logs."""
    entries: list[dict[str, Any]] = []

    if isinstance(envelope, list):
      for item in envelope:
        if isinstance(item, dict):
          entries.append(item)
        elif isinstance(item, str):
          entries.append({"raw_payload": item})
    elif isinstance(envelope, dict):
      if "message" in envelope and isinstance(envelope["message"], dict):
        msg = envelope["message"]
        data_b64 = msg.get("data", "")
        if data_b64 and isinstance(data_b64, str):
          try:
            decoded_bytes = base64.b64decode(data_b64)
            decoded_str = decoded_bytes.decode("utf-8", errors="replace")
            try:
              parsed = json.loads(decoded_str)
              if isinstance(parsed, list):
                for item in parsed:
                  if isinstance(item, dict):
                    entries.append(item)
                  elif isinstance(item, str):
                    entries.append({"raw_payload": item})
              elif isinstance(parsed, dict):
                entries.append(parsed)
              elif parsed is not None:
                entries.append({"raw_payload": str(parsed)})
            except json.JSONDecodeError:
              entries.append({"raw_payload": decoded_str})
          except Exception:  # pylint: disable=broad-except
            entries.append({"raw_payload": str(data_b64)})
      elif "entries" in envelope and isinstance(envelope["entries"], list):
        for item in envelope["entries"]:
          if isinstance(item, dict):
            entries.append(item)
          elif isinstance(item, str):
            entries.append({"raw_payload": item})
      elif "raw_payload" in envelope or "message" in envelope:
        entries.append(envelope)
    elif isinstance(envelope, str) and envelope.strip():
      entries.append({"raw_payload": envelope})

    return self.process_log_batch(entries, flush_remaining=False)


def create_http_handler(worker: EmbeddingPipelineWorker) -> type[BaseHTTPRequestHandler]:
  """Creates a Cloud Run v2 HTTP handler bound to the provided worker instance."""

  class WorkerRequestHandler(BaseHTTPRequestHandler):
    """HTTP request handler for Eventarc / Pub/Sub push messages."""

    def do_POST(self) -> None:  # pylint: disable=invalid-name
      content_length = int(self.headers.get("Content-Length", 0))
      body = self.rfile.read(content_length).decode("utf-8") if content_length > 0 else "{}"
      try:
        envelope = json.loads(body)
        result = worker.handle_pubsub_envelope(envelope)
        response_bytes = json.dumps(result).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response_bytes)))
        self.end_headers()
        self.wfile.write(response_bytes)
      except Exception as exc:  # pylint: disable=broad-except
        logger.exception("Error processing embedding request: %s", exc)
        err_bytes = json.dumps({"error": str(exc)}).encode("utf-8")
        self.send_response(500)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(err_bytes)))
        self.end_headers()
        self.wfile.write(err_bytes)

    def do_GET(self) -> None:  # pylint: disable=invalid-name
      """Health check endpoint for Cloud Run v2 startup/liveness probes."""
      health_bytes = json.dumps({"status": "HEALTHY", "service": "gsk-ano-embedding-worker"}).encode("utf-8")
      self.send_response(200)
      self.send_header("Content-Type", "application/json")
      self.send_header("Content-Length", str(len(health_bytes)))
      self.end_headers()
      self.wfile.write(health_bytes)

  return WorkerRequestHandler


def main() -> None:
  """Production entrypoint for Cloud Run v2 container execution."""
  project_id = os.environ.get("PROJECT_ID", "gsk-ano-prod")
  region = os.environ.get("REGION", "us-central1")
  dataset_id = os.environ.get("DATASET_ID", "gsk_ano_ops")
  table_id = os.environ.get("TABLE_ID", "log_embeddings")
  model_name = os.environ.get("EMBEDDING_MODEL", "text-embedding-005")
  port = int(os.environ.get("PORT", "8080"))

  chunker = SlidingWindowChunker(window_size=5, stride=2)
  embedding_client = VertexAIEmbeddingClient(
      project_id=project_id,
      region=region,
      model_name=model_name,
      dimensions=768,
  )
  bq_writer = BigQueryStreamWriter(
      project_id=project_id,
      dataset_id=dataset_id,
      table_id=table_id,
  )
  worker = EmbeddingPipelineWorker(
      chunker=chunker,
      embedding_client=embedding_client,
      bq_writer=bq_writer,
  )

  handler_cls = create_http_handler(worker)
  server = ThreadingHTTPServer(("0.0.0.0", port), handler_cls)
  logger.info("Starting GSK ANO Embedding Worker on port %d...", port)
  server.serve_forever()


if __name__ == "__main__":
  main()
