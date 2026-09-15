"""Typed Cloud Logging ``LogEntry`` model, serialiser and parser.

Contract **C7** (``PROJECT.md``) fixes the public surface::

    LogEntry                              # typed record
    parse_log_entries(src)  -> Iterator[LogEntry]
    serialize_log_entry(e)  -> dict
    validate_log_entry(obj) -> None

Fidelity rules honoured here, each of which a naive implementation gets wrong
(``LOGENTRY_SPEC.md`` §1):

* JSON keys are **lowerCamelCase** (``logName``, not ``log_name``).
* ``int64`` fields serialise as **quoted JSON strings** (``"requestSize":
  "2481"``); ``int32`` fields serialise as **numbers** (``"status": 503``).
* ``severity`` is the enum **name** (``"ERROR"``), never the number ``500``.
* ``google.protobuf.Timestamp`` renders RFC 3339 with nanosecond precision and
  a ``Z`` suffix; ``google.protobuf.Duration`` renders as decimal seconds with
  an ``s`` suffix (``"0.253s"``).
* ``resource.labels`` keys are fixed by ``resource.type``. A hostname goes in
  the top-level ``labels`` map, never in ``resource.labels``.
* ``errorGroups`` is **not** emitted: the proto states it is excluded from
  Cloud Storage / BigQuery / Pub/Sub exports, and ANO models an export path.
* Unset optional fields are **omitted** rather than emitted as nulls, which is
  what proto3 JSON does.

Key order in the emitted dict is fixed (not sorted) so that two runs of the
generator produce byte-identical JSON, and so the result reads like a real
export rather than an alphabetised dump.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any

from ano.telemetry.errors import LogEntryError
from ano.telemetry.logentry_validator import (
    SEVERITIES,
    resource_label_keys,
    validate_log_entry,
)
from ano.telemetry.timefmt import format_rfc3339, parse_rfc3339

__all__ = [
    "MonitoredResource",
    "HttpRequest",
    "LogEntryOperation",
    "LogEntrySourceLocation",
    "LogSplit",
    "LogEntry",
    "serialize_log_entry",
    "parse_log_entry",
    "parse_log_entries",
    "log_entries_to_jsonl",
    "encode_log_id",
    "build_log_name",
]

_FROZEN: dict[str, Any] = {"frozen": True, "slots": True}


def encode_log_id(log_id: str) -> str:
    """Percent-encode a LOG_ID for embedding in ``logName``.

    ``LOGENTRY_SPEC.md`` §3: *"[LOG_ID] must be URL-encoded within log_name"*.
    In practice the only character in the permitted LOG_ID charset that needs
    encoding is ``/`` (the proto's own example is
    ``cloudresourcemanager.googleapis.com%2Factivity``). Encoding just that
    keeps ``.``, ``-`` and ``_`` literal, which is how real log names look.
    """
    return log_id.replace("%", "%25").replace("/", "%2F")


def build_log_name(project_id: str, log_id: str, parent: str = "projects") -> str:
    """Assemble ``projects/<project_id>/logs/<url-encoded log_id>``."""
    return f"{parent}/{project_id}/logs/{encode_log_id(log_id)}"


@dataclass(**_FROZEN)
class MonitoredResource:
    """``google.api.MonitoredResource`` — ``{type, labels}``.

    Validates its own label key set against the descriptor registry on
    construction, so an object with an illegal key (the classic
    ``gce_instance`` + ``hostname`` mistake) cannot be built at all.
    """

    type: str
    labels: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        try:
            permitted = set(resource_label_keys(self.type))
        except KeyError:
            raise LogEntryError(
                "L05", f"unknown monitored-resource type {self.type!r}", "resource.type"
            ) from None
        illegal = sorted(set(self.labels) - permitted)
        if illegal:
            raise LogEntryError(
                "L05",
                f"resource.type {self.type!r} does not define label(s) {illegal}; "
                f"its label set is exactly {sorted(permitted)}",
                "resource.labels",
            )
        object.__setattr__(self, "labels", dict(self.labels))

    def to_dict(self) -> dict[str, Any]:
        return {"type": self.type, "labels": dict(self.labels)}


@dataclass(**_FROZEN)
class HttpRequest:
    """``google.logging.type.HttpRequest``.

    ``request_size`` / ``response_size`` / ``cache_fill_bytes`` are ``int64``
    and therefore serialise as **strings**; ``status`` is ``int32`` and
    serialises as a **number**. ``latency_s`` is a ``Duration``.
    """

    request_method: str | None = None
    request_url: str | None = None
    request_size: int | None = None
    status: int | None = None
    response_size: int | None = None
    user_agent: str | None = None
    remote_ip: str | None = None
    server_ip: str | None = None
    referer: str | None = None
    latency_s: float | None = None
    cache_lookup: bool | None = None
    cache_hit: bool | None = None
    cache_validated_with_origin_server: bool | None = None
    cache_fill_bytes: int | None = None
    protocol: str | None = None

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        _put(out, "requestMethod", self.request_method)
        _put(out, "requestUrl", self.request_url)
        _put_int64(out, "requestSize", self.request_size)
        _put(out, "status", self.status)
        _put_int64(out, "responseSize", self.response_size)
        _put(out, "userAgent", self.user_agent)
        _put(out, "remoteIp", self.remote_ip)
        _put(out, "serverIp", self.server_ip)
        _put(out, "referer", self.referer)
        if self.latency_s is not None:
            out["latency"] = format_duration_seconds(self.latency_s)
        _put(out, "cacheLookup", self.cache_lookup)
        _put(out, "cacheHit", self.cache_hit)
        _put(out, "cacheValidatedWithOriginServer", self.cache_validated_with_origin_server)
        _put_int64(out, "cacheFillBytes", self.cache_fill_bytes)
        _put(out, "protocol", self.protocol)
        return out


@dataclass(**_FROZEN)
class LogEntryOperation:
    """``LogEntryOperation`` — ``id`` + ``producer`` must be globally unique."""

    id: str | None = None
    producer: str | None = None
    first: bool | None = None
    last: bool | None = None

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        _put(out, "id", self.id)
        _put(out, "producer", self.producer)
        _put(out, "first", self.first)
        _put(out, "last", self.last)
        return out


@dataclass(**_FROZEN)
class LogEntrySourceLocation:
    """``LogEntrySourceLocation``; ``line`` is ``int64`` → a JSON string."""

    file: str | None = None
    line: int | None = None
    function: str | None = None

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        _put(out, "file", self.file)
        _put_int64(out, "line", self.line)
        _put(out, "function", self.function)
        return out


@dataclass(**_FROZEN)
class LogSplit:
    """``LogSplit``; both counters are ``int32`` → JSON numbers."""

    uid: str
    index: int
    total_splits: int

    def to_dict(self) -> dict[str, Any]:
        return {"uid": self.uid, "index": self.index, "totalSplits": self.total_splits}


@dataclass(**_FROZEN)
class LogEntry:
    """One Cloud Logging ``LogEntry``.

    Times are held as **integer nanoseconds since the Unix epoch**, not as
    ``datetime``: ``datetime`` resolves only to microseconds and would silently
    truncate the nanosecond precision the wire format carries (see
    :mod:`ano.telemetry.timefmt`).

    Exactly one payload field may be set — they are members of a single proto
    ``oneof``. This is checked on construction.
    """

    log_name: str
    resource: MonitoredResource
    insert_id: str
    timestamp_nanos: int | None = None
    receive_timestamp_nanos: int | None = None
    severity: str = "DEFAULT"
    labels: Mapping[str, str] = field(default_factory=dict)
    json_payload: Mapping[str, Any] | None = None
    text_payload: str | None = None
    proto_payload: Mapping[str, Any] | None = None
    http_request: HttpRequest | None = None
    operation: LogEntryOperation | None = None
    trace: str | None = None
    span_id: str | None = None
    trace_sampled: bool | None = None
    source_location: LogEntrySourceLocation | None = None
    split: LogSplit | None = None

    def __post_init__(self) -> None:
        payloads = [
            name
            for name, value in (
                ("json_payload", self.json_payload),
                ("text_payload", self.text_payload),
                ("proto_payload", self.proto_payload),
            )
            if value is not None
        ]
        if len(payloads) != 1:
            raise LogEntryError(
                "L06",
                "exactly one of json_payload / text_payload / proto_payload must "
                f"be set (they share one oneof); got {payloads or 'none'}",
            )
        if self.severity not in SEVERITIES:
            raise LogEntryError(
                "L04",
                f"{self.severity!r} is not a LogSeverity name; permitted: "
                f"{list(SEVERITIES)}",
                "severity",
            )
        if not self.insert_id:
            raise LogEntryError("L18", "insert_id must be non-empty", "insertId")
        object.__setattr__(self, "labels", dict(self.labels))

    # -- serialisation ---------------------------------------------------
    def to_dict(self) -> dict[str, Any]:
        """Return the genuine Cloud Logging ``LogEntry`` JSON shape."""
        return serialize_log_entry(self)

    def to_json(self) -> str:
        """Return one compact JSON line (no trailing newline)."""
        return json.dumps(self.to_dict(), separators=(",", ":"), ensure_ascii=False)


def format_duration_seconds(seconds: float) -> str:
    """Render a ``google.protobuf.Duration`` as proto3 JSON.

    Nine fractional digits, ``s`` suffix — e.g. ``50.218374000s``. Fixed width
    keeps generator output byte-stable; ``float`` repr would not be.
    """
    total_nanos = round(seconds * 1_000_000_000)
    sign = "-" if total_nanos < 0 else ""
    whole, fraction = divmod(abs(total_nanos), 1_000_000_000)
    return f"{sign}{whole}.{fraction:09d}s"


def _put(out: dict[str, Any], key: str, value: Any) -> None:
    if value is not None:
        out[key] = value


def _put_int64(out: dict[str, Any], key: str, value: int | None) -> None:
    """int64 → JSON **string**. This is the proto3 JSON mapping, not a choice."""
    if value is not None:
        out[key] = str(int(value))


#: Emission order. Fixed rather than sorted so output is byte-stable across
#: runs and reads like a real export.
_FIELD_ORDER = (
    "logName",
    "resource",
    "timestamp",
    "receiveTimestamp",
    "severity",
    "insertId",
    "labels",
    "jsonPayload",
    "textPayload",
    "protoPayload",
    "httpRequest",
    "operation",
    "trace",
    "spanId",
    "traceSampled",
    "sourceLocation",
    "split",
)


def serialize_log_entry(entry: LogEntry) -> dict[str, Any]:
    """Serialise a :class:`LogEntry` to its Cloud Logging JSON representation.

    Returns:
        A plain ``dict`` whose keys are lowerCamelCase JSON names, with unset
        optional fields omitted (proto3 JSON semantics) and ``int64`` fields
        rendered as quoted strings.
    """
    out: dict[str, Any] = {}
    out["logName"] = entry.log_name
    out["resource"] = entry.resource.to_dict()
    if entry.timestamp_nanos is not None:
        out["timestamp"] = format_rfc3339(entry.timestamp_nanos)
    if entry.receive_timestamp_nanos is not None:
        out["receiveTimestamp"] = format_rfc3339(entry.receive_timestamp_nanos)
    # proto3 JSON omits a field holding its default; DEFAULT is LogSeverity's.
    if entry.severity != "DEFAULT":
        out["severity"] = entry.severity
    out["insertId"] = entry.insert_id
    if entry.labels:
        out["labels"] = dict(entry.labels)
    if entry.json_payload is not None:
        out["jsonPayload"] = _plain(entry.json_payload)
    if entry.text_payload is not None:
        out["textPayload"] = entry.text_payload
    if entry.proto_payload is not None:
        out["protoPayload"] = _plain(entry.proto_payload)
    if entry.http_request is not None:
        out["httpRequest"] = entry.http_request.to_dict()
    if entry.operation is not None:
        out["operation"] = entry.operation.to_dict()
    _put(out, "trace", entry.trace)
    _put(out, "spanId", entry.span_id)
    _put(out, "traceSampled", entry.trace_sampled)
    if entry.source_location is not None:
        out["sourceLocation"] = entry.source_location.to_dict()
    if entry.split is not None:
        out["split"] = entry.split.to_dict()
    # Re-key in the canonical order.
    return {key: out[key] for key in _FIELD_ORDER if key in out}


def _plain(value: Any) -> Any:
    """Deep-copy mappings/sequences into plain JSON containers."""
    if isinstance(value, Mapping):
        return {str(k): _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    return value


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------
def parse_log_entry(obj: Mapping[str, Any] | str, *, validate: bool = True) -> LogEntry:
    """Build a :class:`LogEntry` from a JSON object or a single JSON line."""
    if isinstance(obj, str):
        obj = json.loads(obj)
    if not isinstance(obj, dict):
        raise LogEntryError("L01", f"expected an object, got {type(obj).__name__}")
    if validate:
        validate_log_entry(obj)

    resource = obj["resource"]
    http = obj.get("httpRequest")
    operation = obj.get("operation")
    location = obj.get("sourceLocation")
    split = obj.get("split")
    return LogEntry(
        log_name=obj["logName"],
        resource=MonitoredResource(
            type=resource["type"], labels=dict(resource.get("labels", {}))
        ),
        insert_id=obj["insertId"],
        timestamp_nanos=(
            parse_rfc3339(obj["timestamp"]) if "timestamp" in obj else None
        ),
        receive_timestamp_nanos=(
            parse_rfc3339(obj["receiveTimestamp"]) if "receiveTimestamp" in obj else None
        ),
        severity=obj.get("severity", "DEFAULT"),
        labels=dict(obj.get("labels", {})),
        json_payload=obj.get("jsonPayload"),
        text_payload=obj.get("textPayload"),
        proto_payload=obj.get("protoPayload"),
        http_request=_parse_http_request(http) if http is not None else None,
        operation=(
            LogEntryOperation(
                id=operation.get("id"),
                producer=operation.get("producer"),
                first=operation.get("first"),
                last=operation.get("last"),
            )
            if operation is not None
            else None
        ),
        trace=obj.get("trace"),
        span_id=obj.get("spanId"),
        trace_sampled=obj.get("traceSampled"),
        source_location=(
            LogEntrySourceLocation(
                file=location.get("file"),
                line=int(location["line"]) if "line" in location else None,
                function=location.get("function"),
            )
            if location is not None
            else None
        ),
        split=(
            LogSplit(
                uid=split["uid"], index=split["index"], total_splits=split["totalSplits"]
            )
            if split is not None
            else None
        ),
    )


def _parse_http_request(obj: Mapping[str, Any]) -> HttpRequest:
    latency = obj.get("latency")
    return HttpRequest(
        request_method=obj.get("requestMethod"),
        request_url=obj.get("requestUrl"),
        request_size=int(obj["requestSize"]) if "requestSize" in obj else None,
        status=obj.get("status"),
        response_size=int(obj["responseSize"]) if "responseSize" in obj else None,
        user_agent=obj.get("userAgent"),
        remote_ip=obj.get("remoteIp"),
        server_ip=obj.get("serverIp"),
        referer=obj.get("referer"),
        latency_s=float(latency[:-1]) if isinstance(latency, str) else None,
        cache_lookup=obj.get("cacheLookup"),
        cache_hit=obj.get("cacheHit"),
        cache_validated_with_origin_server=obj.get("cacheValidatedWithOriginServer"),
        cache_fill_bytes=int(obj["cacheFillBytes"]) if "cacheFillBytes" in obj else None,
        protocol=obj.get("protocol"),
    )


def parse_log_entries(
    src: str | os.PathLike[str] | Iterable[str], *, validate: bool = True
) -> Iterator[LogEntry]:
    """Parse ``LogEntry`` JSON Lines.

    Args:
        src: A filesystem path to a ``.jsonl`` file, **or** any iterable of
            strings (lines, or whole JSON documents). Blank lines are skipped,
            which is what makes a hand-edited fixture usable.
        validate: Run :func:`validate_log_entry` on each decoded object before
            building the typed record. Leave this on unless you are
            deliberately round-tripping known-bad input.

    Yields:
        :class:`LogEntry` records, in file order.
    """
    if isinstance(src, (str, os.PathLike)):
        with open(src, "r", encoding="utf-8") as handle:
            # Materialise inside the ``with`` so the file is not left open by a
            # partially-consumed generator.
            lines = handle.read().splitlines()
        yield from _iter_lines(lines, validate)
    else:
        yield from _iter_lines(src, validate)


def _iter_lines(lines: Iterable[str], validate: bool) -> Iterator[LogEntry]:
    for index, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            obj = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise LogEntryError(
                "L01", f"line {index + 1} is not valid JSON: {exc}"
            ) from exc
        yield parse_log_entry(obj, validate=validate)


def log_entries_to_jsonl(entries: Iterable[LogEntry]) -> str:
    """Render entries as JSON Lines, one compact object per line, LF-terminated."""
    return "".join(entry.to_json() + "\n" for entry in entries)
