"""Structural validator for Cloud Logging ``LogEntry`` JSON.

Approach (``VALIDATION_STRATEGY.md`` §2.3, option C): a small generic
interpreter over a **declarative schema held as data** in
``ano/telemetry/schemas/``. The schema is diffable against the vendored
Apache-2.0 ``log_entry.proto`` by eye, and is pinned to it mechanically by the
drift test in ``tests/m2_generator/test_logentry_drift.py``.

Deliberately *not* a JSON Schema evaluator: several of the rules that carry the
most fidelity value cannot be expressed in JSON Schema at all — the
``resource.labels`` key set being a function of ``resource.type``, the
``logName`` LOG_ID needing to be URL-**decoded** before its charset is checked,
and ``split.index < split.totalSplits`` being a cross-field comparison. See
``VALIDATION_STRATEGY.md`` §2.2.

Rule ids (raised as :class:`~ano.telemetry.errors.LogEntryError.rule`) are the
``L01``..``L18`` set enumerated in ``VALIDATION_STRATEGY.md`` §2.5:

===== ==========================================================================
 L01   unknown / non-lowerCamelCase key
 L02   ``logName`` and ``resource`` are REQUIRED
 L03   ``logName`` grammar; LOG_ID URL-decoded charset; no leading slash
 L04   ``severity`` is one of the nine enum **names**
 L05   ``resource.type`` known and ``resource.labels`` keys within descriptor
 L06   exactly one of ``textPayload`` / ``jsonPayload`` / ``protoPayload``
 L07   ``protoPayload["@type"]`` is one of the two supported Any types
 L08   int64 fields are JSON **strings** of canonical integers
 L09   int32 fields are JSON **numbers**
 L10   timestamps are RFC 3339 ending in ``Z``, <= 9 fractional digits
 L11   ``httpRequest.latency`` is a Duration string ending in ``s``
 L12   ``spanId`` is 16 lowercase hex chars and not all zero
 L13   ``split``: ``0 <= index < totalSplits``
 L14   ``sourceLocation.line`` >= 0
 L15   ``labels`` key <= 512 B, value <= 64 KB
 L16   all strings are valid UTF-8 with no lone surrogates
 L17   export-excluded / internal fields absent (notably ``errorGroups``)
 L18   ``insertId`` present, and unique within a batch
===== ==========================================================================
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Iterable, Mapping
from typing import Any
from urllib.parse import unquote

from ano.telemetry.errors import LogEntryError
from ano.telemetry.timefmt import RFC3339_NANOS_RE, parse_rfc3339

__all__ = [
    "SCHEMA_DIR",
    "LOGENTRY_SCHEMA",
    "MONITORED_RESOURCES",
    "SEVERITIES",
    "resource_label_keys",
    "validate_log_entry",
    "validate_log_entry_batch",
]

SCHEMA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schemas")


def _load(name: str) -> dict[str, Any]:
    with open(os.path.join(SCHEMA_DIR, name), "r", encoding="utf-8") as handle:
        return json.load(handle)


LOGENTRY_SCHEMA: dict[str, Any] = _load("logentry_schema.json")
MONITORED_RESOURCES: dict[str, Any] = _load("monitored_resources.json")

#: The nine LogSeverity enum names, in ascending severity order.
SEVERITIES: tuple[str, ...] = tuple(LOGENTRY_SCHEMA["enums"]["LogSeverity"]["values"])

_LOG_ID_CHARSET = re.compile(LOGENTRY_SCHEMA["log_id_charset_regex"])
_LOG_NAME_RE = re.compile(
    r"^(%s)/([^/]+)/logs/(.+)$" % "|".join(LOGENTRY_SCHEMA["log_name_parents"])
)
#: int64-as-JSON-string: canonical, so no leading zeros and no ``+``.
_INT64_STRING_RE = re.compile(r"^-?(0|[1-9][0-9]*)$")
_SPAN_ID_RE = re.compile(r"^[0-9a-f]{16}$")
#: proto3 Duration JSON: decimal seconds with up to nine fractional digits.
_DURATION_RE = re.compile(r"^-?(0|[1-9][0-9]*)(\.[0-9]{1,9})?s$")

_MUST_NOT_EMIT_JSON_NAMES: frozenset[str] = frozenset(
    LOGENTRY_SCHEMA["must_not_emit"]["fields"].values()
)
_SUPPORTED_ANY_TYPES: frozenset[str] = frozenset(
    LOGENTRY_SCHEMA["supported_any_types"]
)
_INT32_MIN, _INT32_MAX = -(2**31), 2**31 - 1
_INT64_MIN, _INT64_MAX = -(2**63), 2**63 - 1


def resource_label_keys(resource_type: str) -> tuple[str, ...]:
    """Return the exact label key set a monitored-resource type declares.

    Raises:
        KeyError: if the type is not in the registry.
    """
    return tuple(MONITORED_RESOURCES["types"][resource_type]["labels"])


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
def _fail(rule: str, message: str, path: str) -> None:
    raise LogEntryError(rule, message, path)


def _check_utf8_string(value: str, path: str) -> None:
    """Rule L16 — the proto forbids non-UTF-8 content anywhere in a LogEntry."""
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:  # lone surrogate from e.g. "\ud800"
        _fail("L16", f"string is not encodable as UTF-8 ({exc.reason})", path)


def _check_timestamp(value: Any, path: str) -> None:
    """Rule L10."""
    if not isinstance(value, str):
        _fail("L10", f"timestamp must be a JSON string, got {type(value).__name__}", path)
    if RFC3339_NANOS_RE.match(value) is None:
        _fail(
            "L10",
            "timestamp must be RFC 3339 UTC ending in 'Z' with at most nine "
            f"fractional digits, got {value!r}",
            path,
        )
    try:
        parse_rfc3339(value)
    except ValueError as exc:
        _fail("L10", str(exc), path)


def _check_int64_string(value: Any, path: str, minimum: int | None) -> None:
    """Rule L08 — and rule L14 when ``minimum`` is supplied."""
    if isinstance(value, bool) or not isinstance(value, str):
        _fail(
            "L08",
            "int64 fields serialise as JSON strings in proto3 JSON; got "
            f"{type(value).__name__}",
            path,
        )
    if _INT64_STRING_RE.match(value) is None:
        _fail("L08", f"not a canonical decimal integer string: {value!r}", path)
    number = int(value)
    if not _INT64_MIN <= number <= _INT64_MAX:
        _fail("L08", f"value {number} is outside the int64 range", path)
    if minimum is not None and number < minimum:
        _fail("L14", f"value {number} is below the permitted minimum {minimum}", path)


def _check_int32_number(value: Any, path: str, minimum: int | None) -> None:
    """Rule L09 — and rule L13 bounds when ``minimum`` is supplied."""
    if isinstance(value, bool) or not isinstance(value, int):
        _fail(
            "L09",
            f"int32 fields serialise as JSON numbers; got {type(value).__name__}",
            path,
        )
    if not _INT32_MIN <= value <= _INT32_MAX:
        _fail("L09", f"value {value} is outside the int32 range", path)
    if minimum is not None and value < minimum:
        _fail("L13", f"value {value} is below the permitted minimum {minimum}", path)


def _check_log_name(value: Any, path: str) -> None:
    """Rule L03."""
    if not isinstance(value, str):
        _fail("L03", f"logName must be a string, got {type(value).__name__}", path)
    if value.startswith("/"):
        _fail(
            "L03",
            "logName must not begin with '/': Logging strips it and the entry "
            "becomes unfilterable",
            path,
        )
    match = _LOG_NAME_RE.match(value)
    if match is None:
        parents = "|".join(LOGENTRY_SCHEMA["log_name_parents"])
        _fail("L03", f"logName must match ({parents})/<id>/logs/<LOG_ID>: {value!r}", path)
    encoded_log_id = match.group(3)
    decoded = unquote(encoded_log_id)
    if _LOG_ID_CHARSET.match(decoded) is None:
        _fail(
            "L03",
            "URL-decoded LOG_ID must be 1..511 chars of [A-Za-z0-9/_.-], got "
            f"{decoded!r}",
            path,
        )
    if "/" in encoded_log_id:
        _fail(
            "L03",
            "a literal '/' inside LOG_ID must be percent-encoded as %2F "
            f"(got {encoded_log_id!r})",
            path,
        )


def _check_resource(value: Any, path: str) -> None:
    """Rule L05."""
    if not isinstance(value, dict):
        _fail("L05", f"resource must be an object, got {type(value).__name__}", path)
    unknown = set(value) - {"type", "labels"}
    if unknown:
        _fail("L05", f"unknown key(s) in resource: {sorted(unknown)}", path)
    if "type" not in value:
        _fail("L05", "resource.type is required", path)
    resource_type = value["type"]
    if not isinstance(resource_type, str):
        _fail("L05", "resource.type must be a string", f"{path}.type")
    registry = MONITORED_RESOURCES["types"]
    if resource_type not in registry:
        _fail(
            "L05",
            f"unknown monitored-resource type {resource_type!r}; known types: "
            f"{sorted(registry)}",
            f"{path}.type",
        )
    labels = value.get("labels", {})
    if not isinstance(labels, dict):
        _fail("L05", "resource.labels must be an object", f"{path}.labels")
    permitted = set(registry[resource_type]["labels"])
    illegal = sorted(set(labels) - permitted)
    if illegal:
        _fail(
            "L05",
            f"resource.type {resource_type!r} does not define label(s) {illegal}; "
            f"its label set is exactly {sorted(permitted)}. Free-text host "
            "identifiers belong in the top-level 'labels' map",
            f"{path}.labels",
        )
    for key, label_value in labels.items():
        if not isinstance(label_value, str):
            _fail("L05", "resource.labels values must be strings", f"{path}.labels.{key}")
        _check_utf8_string(label_value, f"{path}.labels.{key}")


def _check_message(value: Any, message_name: str, path: str, constraint: str | None) -> None:
    """Recursive check for a nested message described in the schema."""
    if not isinstance(value, dict):
        _fail("L01", f"{message_name} must be an object, got {type(value).__name__}", path)
    spec = LOGENTRY_SCHEMA["messages"][message_name]
    by_json_name = {f["json_name"]: f for f in spec["fields"]}
    for key, item in value.items():
        _reject_forbidden_key(key, f"{path}.{key}")
        field = by_json_name.get(key)
        if field is None:
            _fail(
                "L01",
                f"unknown field {key!r} on {message_name}; permitted: "
                f"{sorted(by_json_name)}",
                path,
            )
        _check_value(item, field, f"{path}.{key}")
    if constraint == "split_index":
        index = value.get("index")
        total = value.get("totalSplits")
        if isinstance(index, int) and isinstance(total, int):
            if not 0 <= index < total:
                _fail(
                    "L13",
                    f"split requires 0 <= index < totalSplits, got index={index}, "
                    f"totalSplits={total}",
                    path,
                )


def _reject_forbidden_key(key: str, path: str) -> None:
    """Rule L17 — export-excluded, deprecated and Google-internal field names."""
    if key in _MUST_NOT_EMIT_JSON_NAMES:
        _fail(
            "L17",
            f"field {key!r} is absent from the public LogEntry schema (it is "
            "OUTPUT_ONLY, deprecated, internal, or excluded from log exports); "
            "emitting it makes export-shaped data less faithful, not more",
            path,
        )


def _check_value(value: Any, field: Mapping[str, Any], path: str) -> None:
    """Dispatch on the declared field type."""
    kind = field["type"]
    if kind == "string":
        if not isinstance(value, str):
            _fail("L01", f"expected a JSON string, got {type(value).__name__}", path)
        _check_utf8_string(value, path)
        constraint = field.get("constraint")
        if constraint == "log_name":
            _check_log_name(value, path)
        elif constraint == "span_id":
            _check_span_id(value, path)
    elif kind == "bool":
        if not isinstance(value, bool):
            _fail("L01", f"expected a JSON boolean, got {type(value).__name__}", path)
    elif kind == "int64_string":
        _check_int64_string(value, path, field.get("min_value"))
    elif kind == "int32_number":
        _check_int32_number(value, path, field.get("min_value"))
    elif kind == "timestamp":
        _check_timestamp(value, path)
    elif kind == "duration":
        _check_duration(value, path)
    elif kind == "struct":
        _check_struct(value, path)
    elif kind == "any":
        _check_any(value, path)
    elif kind == "map_string_string":
        _check_label_map(value, field, path)
    elif kind == "monitored_resource":
        _check_resource(value, path)
    elif kind.startswith("enum:"):
        _check_enum(value, kind.split(":", 1)[1], path)
    elif kind.startswith("message:"):
        _check_message(value, kind.split(":", 1)[1], path, field.get("constraint"))
    else:  # pragma: no cover - schema authoring error
        raise AssertionError(f"schema declares unknown field type {kind!r}")


def _check_span_id(value: str, path: str) -> None:
    """Rule L12."""
    if _SPAN_ID_RE.match(value) is None:
        _fail("L12", f"spanId must be 16 lowercase hex characters, got {value!r}", path)
    if set(value) == {"0"}:
        _fail("L12", "spanId must not be all zeros", path)


def _check_duration(value: Any, path: str) -> None:
    """Rule L11."""
    if not isinstance(value, str):
        _fail("L11", f"Duration must be a JSON string, got {type(value).__name__}", path)
    if _DURATION_RE.match(value) is None:
        _fail(
            "L11",
            f"Duration must be decimal seconds with an 's' suffix, got {value!r}",
            path,
        )


def _check_enum(value: Any, enum_name: str, path: str) -> None:
    """Rule L04."""
    names = LOGENTRY_SCHEMA["enums"][enum_name]["values"]
    if not isinstance(value, str):
        _fail(
            "L04",
            f"{enum_name} serialises as the enum NAME string in proto3 JSON, not "
            f"as its number; got {type(value).__name__} {value!r}",
            path,
        )
    if value not in names:
        _fail("L04", f"{value!r} is not a {enum_name} name; permitted: {list(names)}", path)


def _check_struct(value: Any, path: str) -> None:
    """``google.protobuf.Struct`` — an arbitrary JSON object."""
    if not isinstance(value, dict):
        _fail("L01", f"Struct must be a JSON object, got {type(value).__name__}", path)
    _walk_json_strings(value, path)


def _check_any(value: Any, path: str) -> None:
    """Rules L01 / L07."""
    if not isinstance(value, dict):
        _fail("L01", f"Any must be a JSON object, got {type(value).__name__}", path)
    type_url = value.get("@type")
    if type_url is None:
        _fail("L07", "protoPayload must carry an '@type' key", path)
    if type_url not in _SUPPORTED_ANY_TYPES:
        _fail(
            "L07",
            f"protoPayload @type {type_url!r} is not supported by Cloud Logging; "
            f"only {sorted(_SUPPORTED_ANY_TYPES)} are",
            f"{path}.@type",
        )
    _walk_json_strings(value, path)


def _walk_json_strings(value: Any, path: str) -> None:
    """Rule L16 over an arbitrary JSON subtree."""
    if isinstance(value, str):
        _check_utf8_string(value, path)
    elif isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):  # pragma: no cover - json keys are str
                _fail("L01", "object keys must be strings", path)
            _check_utf8_string(key, path)
            _walk_json_strings(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _walk_json_strings(item, f"{path}[{index}]")


def _check_label_map(value: Any, field: Mapping[str, Any], path: str) -> None:
    """Rules L01 / L15 / L16 for ``map<string,string>``."""
    if not isinstance(value, dict):
        _fail("L01", f"expected a JSON object, got {type(value).__name__}", path)
    max_key = field.get("max_key_bytes")
    max_value = field.get("max_value_bytes")
    for key, item in value.items():
        item_path = f"{path}.{key}"
        if not isinstance(item, str):
            _fail("L01", f"map values must be strings, got {type(item).__name__}", item_path)
        _check_utf8_string(key, item_path)
        _check_utf8_string(item, item_path)
        if max_key is not None and len(key.encode("utf-8")) > max_key:
            _fail("L15", f"label key exceeds {max_key} bytes", item_path)
        if max_value is not None and len(item.encode("utf-8")) > max_value:
            _fail("L15", f"label value exceeds {max_value} bytes", item_path)


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------
def validate_log_entry(obj: Any, path: str = "") -> None:
    """Validate one ``LogEntry`` JSON object. Returns ``None``; raises on failure.

    Args:
        obj: A ``dict`` decoded from one line of ``LogEntry`` JSON. Anything
            exposing ``to_dict()`` (such as
            :class:`ano.telemetry.logentry.LogEntry`) is serialised first, so
            callers may pass either representation.
        path: Optional path prefix used in error messages, e.g. ``"entries[7]"``.

    Raises:
        ano.telemetry.errors.LogEntryError: on any non-conformance. The
            exception's ``rule`` attribute names the violated rule (``L01``..
            ``L18``).
    """
    if hasattr(obj, "to_dict") and not isinstance(obj, dict):
        obj = obj.to_dict()
    if not isinstance(obj, dict):
        _fail("L01", f"a LogEntry must be a JSON object, got {type(obj).__name__}", path)

    spec = LOGENTRY_SCHEMA["messages"]["LogEntry"]
    by_json_name = {f["json_name"]: f for f in spec["fields"]}

    # L01 / L17 — key vocabulary.
    for key in obj:
        key_path = f"{path}.{key}" if path else key
        _reject_forbidden_key(key, key_path)
        if key not in by_json_name:
            _fail(
                "L01",
                f"unknown LogEntry field {key!r}. Cloud Logging JSON uses "
                "lowerCamelCase names drawn from the published schema",
                key_path,
            )

    # L02 — required fields.
    for field in spec["fields"]:
        if field.get("required") and field["json_name"] not in obj:
            _fail("L02", f"{field['json_name']} is REQUIRED", path)

    # L06 — the payload oneof.
    payload_names = [f["json_name"] for f in spec["fields"] if f.get("oneof") == "payload"]
    present = [name for name in payload_names if name in obj]
    if len(present) != 1:
        _fail(
            "L06",
            "exactly one payload must be set (they are members of a single "
            f"oneof); found {present or 'none'} of {payload_names}",
            path,
        )

    # L18 — insertId presence. (Uniqueness is a batch property; see
    # validate_log_entry_batch.)
    insert_id = obj.get("insertId")
    if not isinstance(insert_id, str) or not insert_id:
        _fail(
            "L18",
            "insertId must be present and non-empty: it is the de-duplication "
            "key and the tiebreaker ordering key in queries",
            path,
        )

    # Per-field checks.
    for key, value in obj.items():
        _check_value(value, by_json_name[key], f"{path}.{key}" if path else key)


def validate_log_entry_batch(entries: Iterable[Any]) -> int:
    """Validate a sequence of entries and enforce batch-level rule L18.

    Args:
        entries: An iterable of ``LogEntry`` dicts (or objects with ``to_dict``).

    Returns:
        The number of entries validated.

    Raises:
        ano.telemetry.errors.LogEntryError: on the first non-conformance, or if
            two entries share an ``insertId``.
    """
    seen: dict[str, int] = {}
    count = 0
    for index, entry in enumerate(entries):
        path = f"entries[{index}]"
        validate_log_entry(entry, path)
        payload = entry.to_dict() if hasattr(entry, "to_dict") and not isinstance(entry, dict) else entry
        insert_id = payload["insertId"]
        if insert_id in seen:
            _fail(
                "L18",
                f"insertId {insert_id!r} is duplicated (first seen at index "
                f"{seen[insert_id]})",
                path,
            )
        seen[insert_id] = index
        count += 1
    return count
