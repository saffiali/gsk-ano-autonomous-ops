"""Shared primitives for every ANO contract: errors, time, JSON, validation.

This module is the foundation the C1 / C2 / C5 contracts are built on. It is
deliberately small and dependency-free (standard library only) because every
other milestone imports it.

Three things live here:

1.  :class:`ValidationError` — the single exception type every strict validator
    raises. It always carries a JSON-pointer-ish ``path`` so a malformed record
    can be located precisely.
2.  RFC 3339 timestamp handling — :func:`parse_rfc3339` and
    :func:`format_rfc3339`. All contract timestamps are timezone-aware UTC
    ``datetime`` objects in Python and canonical RFC 3339 strings on the wire.
3.  A tiny strict-validation toolkit (:func:`require_mapping`,
    :func:`take_str`, ...) used by the contract parsers. It is *strict*: unknown
    keys are rejected, ``None`` is not silently coerced, and numbers are not
    silently stringified.

Canonical JSON
--------------
:func:`dumps_canonical` produces byte-stable JSON (sorted keys, no insignificant
whitespace, no NaN/Infinity). Determinism is an acceptance criterion, so every
artifact this project writes for machine consumption goes through it.
"""

from __future__ import annotations

import datetime as _dt
import json
import math
import re
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

__all__ = [
    "ValidationError",
    "UTC",
    "parse_rfc3339",
    "format_rfc3339",
    "ensure_utc",
    "dumps_canonical",
    "dumps_pretty",
    "loads",
    "require_mapping",
    "reject_unknown_keys",
    "take_str",
    "take_bool",
    "take_int",
    "take_float",
    "take_timestamp",
    "take_list",
    "take_mapping",
    "take_optional_str",
    "check_enum",
    "join_path",
]

#: Convenience alias so callers do not have to import ``datetime`` themselves.
UTC = _dt.timezone.utc

# RFC 3339 date-time. Fractional seconds optional; offset may be 'Z' or +/-HH:MM.
_RFC3339_RE = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})"
    r"[Tt]"
    r"(?P<time>\d{2}:\d{2}:\d{2})"
    r"(?P<frac>\.\d{1,9})?"
    r"(?P<offset>[Zz]|[+-]\d{2}:\d{2})$"
)


class ValidationError(ValueError):
    """Raised when a record does not satisfy its contract.

    Attributes:
        message: Human-readable description of what is wrong.
        path: Location of the offending value, e.g. ``events[3].root_cause.domain``.
            Empty string means "the record as a whole".
    """

    def __init__(self, message: str, path: str = "") -> None:
        self.message = message
        self.path = path
        super().__init__(f"{path}: {message}" if path else message)


def join_path(path: str, part: str | int) -> str:
    """Append a field name or list index to a validation path."""
    if isinstance(part, int):
        return f"{path}[{part}]"
    return f"{path}.{part}" if path else str(part)


# --------------------------------------------------------------------------
# Time
# --------------------------------------------------------------------------
def parse_rfc3339(value: str, path: str = "") -> _dt.datetime:
    """Parse an RFC 3339 timestamp into a timezone-aware UTC ``datetime``.

    Accepts the forms real Google Cloud APIs emit, e.g.
    ``2026-09-13T12:00:00Z``, ``2026-09-13T12:00:00.123456Z``,
    ``2026-09-13T12:00:00+01:00``. Nanosecond precision (Cloud Logging emits up
    to 9 fractional digits) is accepted and truncated to microseconds, which is
    all ``datetime`` can represent; the truncation is documented rather than
    silent-but-unmentioned.

    Raises:
        ValidationError: if ``value`` is not a string in RFC 3339 form.
    """
    if not isinstance(value, str):
        raise ValidationError(
            f"expected an RFC3339 timestamp string, got {type(value).__name__}", path
        )
    match = _RFC3339_RE.match(value)
    if match is None:
        raise ValidationError(f"not a valid RFC3339 timestamp: {value!r}", path)
    frac = match.group("frac") or ""
    # datetime.fromisoformat in 3.13 handles 'Z' and offsets but only 0/3/6
    # fractional digits, so normalise the fraction to exactly 6 digits.
    micros = (frac[1:] + "000000")[:6] if frac else "000000"
    offset = match.group("offset")
    offset = "+00:00" if offset in ("Z", "z") else offset
    iso = f"{match.group('date')}T{match.group('time')}.{micros}{offset}"
    try:
        parsed = _dt.datetime.fromisoformat(iso)
    except ValueError as exc:  # pragma: no cover - regex already constrains this
        raise ValidationError(f"not a valid RFC3339 timestamp: {value!r}", path) from exc
    return parsed.astimezone(UTC)


def format_rfc3339(value: _dt.datetime, path: str = "") -> str:
    """Render a timezone-aware ``datetime`` as canonical RFC 3339 UTC text.

    Canonical form: ``YYYY-MM-DDTHH:MM:SSZ``, or ``YYYY-MM-DDTHH:MM:SS.ffffffZ``
    when the microsecond component is non-zero. Choosing one fixed rendering
    (rather than ``isoformat()``'s several) is what makes serialised records
    byte-comparable between runs.

    Raises:
        ValidationError: if ``value`` is naive (no tzinfo).
    """
    if not isinstance(value, _dt.datetime):
        raise ValidationError(
            f"expected a datetime, got {type(value).__name__}", path
        )
    if value.tzinfo is None:
        raise ValidationError(
            "naive datetime is not allowed; timestamps must be timezone-aware UTC",
            path,
        )
    utc = value.astimezone(UTC)
    if utc.microsecond:
        return utc.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
    return utc.strftime("%Y-%m-%dT%H:%M:%S") + "Z"


def ensure_utc(value: _dt.datetime, path: str = "") -> _dt.datetime:
    """Return ``value`` converted to UTC, rejecting naive datetimes."""
    if not isinstance(value, _dt.datetime):
        raise ValidationError(f"expected a datetime, got {type(value).__name__}", path)
    if value.tzinfo is None:
        raise ValidationError(
            "naive datetime is not allowed; timestamps must be timezone-aware UTC",
            path,
        )
    return value.astimezone(UTC)


# --------------------------------------------------------------------------
# JSON
# --------------------------------------------------------------------------
def dumps_canonical(obj: Any) -> str:
    """Serialise ``obj`` to byte-stable JSON.

    Sorted keys, compact separators, ``allow_nan=False``. Used for hashing and
    for any artifact whose bytes are compared between runs.
    """
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), allow_nan=False)


def dumps_pretty(obj: Any, indent: int = 2) -> str:
    """Serialise ``obj`` to human-readable but still deterministic JSON.

    Keys are sorted, so two runs producing equal data produce equal text. A
    trailing newline is included so the files are well-formed for line tools.
    """
    return json.dumps(obj, sort_keys=True, indent=indent, allow_nan=False) + "\n"


def loads(text: str, path: str = "") -> Any:
    """Parse JSON text, converting parse failures into :class:`ValidationError`."""
    if not isinstance(text, str):
        raise ValidationError(f"expected JSON text, got {type(text).__name__}", path)
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValidationError(f"invalid JSON: {exc}", path) from exc


# --------------------------------------------------------------------------
# Strict validation toolkit
# --------------------------------------------------------------------------
def require_mapping(obj: Any, path: str = "") -> Mapping[str, Any]:
    """Assert ``obj`` is a JSON object and return it."""
    if not isinstance(obj, Mapping):
        raise ValidationError(
            f"expected a JSON object, got {type(obj).__name__}", path
        )
    for key in obj:
        if not isinstance(key, str):
            raise ValidationError(f"object keys must be strings, got {key!r}", path)
    return obj


def reject_unknown_keys(
    obj: Mapping[str, Any], allowed: Iterable[str], path: str = ""
) -> None:
    """Reject any key not in ``allowed``.

    The contracts are a *published API*: an unexpected key almost always means a
    typo or a private extension that the consumer will silently ignore. Both are
    worse than a loud failure, so both fail.
    """
    allowed_set = set(allowed)
    unknown = sorted(k for k in obj if k not in allowed_set)
    if unknown:
        raise ValidationError(
            "unknown key(s) "
            + ", ".join(repr(k) for k in unknown)
            + "; allowed keys are "
            + ", ".join(repr(k) for k in sorted(allowed_set)),
            path,
        )


def _present(obj: Mapping[str, Any], key: str, path: str) -> Any:
    if key not in obj:
        raise ValidationError(f"missing required key {key!r}", path)
    return obj[key]


def take_str(
    obj: Mapping[str, Any], key: str, path: str = "", *, allow_empty: bool = False
) -> str:
    """Extract a required string field."""
    value = _present(obj, key, path)
    field = join_path(path, key)
    if not isinstance(value, str):
        raise ValidationError(
            f"expected a string, got {type(value).__name__}", field
        )
    if not allow_empty and not value:
        raise ValidationError("must not be empty", field)
    return value


def take_optional_str(
    obj: Mapping[str, Any], key: str, path: str = "", *, allow_empty: bool = False
) -> str | None:
    """Extract a string field that may be explicitly ``null``.

    The key must still be present — an absent key is a malformed record, because
    a consumer cannot distinguish "not set" from "the producer forgot".
    """
    value = _present(obj, key, path)
    field = join_path(path, key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValidationError(
            f"expected a string or null, got {type(value).__name__}", field
        )
    if not allow_empty and not value:
        raise ValidationError("must not be empty (use null instead)", field)
    return value


def take_bool(obj: Mapping[str, Any], key: str, path: str = "") -> bool:
    """Extract a required boolean field. ``0``/``1`` are *not* accepted."""
    value = _present(obj, key, path)
    field = join_path(path, key)
    if not isinstance(value, bool):
        raise ValidationError(
            f"expected a boolean, got {type(value).__name__}", field
        )
    return value


def take_int(
    obj: Mapping[str, Any],
    key: str,
    path: str = "",
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    """Extract a required integer field. ``True``/``False`` are rejected."""
    value = _present(obj, key, path)
    field = join_path(path, key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(
            f"expected an integer, got {type(value).__name__}", field
        )
    if minimum is not None and value < minimum:
        raise ValidationError(f"must be >= {minimum}, got {value}", field)
    if maximum is not None and value > maximum:
        raise ValidationError(f"must be <= {maximum}, got {value}", field)
    return value


def take_float(
    obj: Mapping[str, Any],
    key: str,
    path: str = "",
    *,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float:
    """Extract a required finite number field, returned as ``float``.

    Integers are accepted and widened (JSON has one number type); ``bool`` is
    not. NaN and infinities are rejected: they poison every downstream
    comparison and are never a legitimate metric value.
    """
    value = _present(obj, key, path)
    field = join_path(path, key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(f"expected a number, got {type(value).__name__}", field)
    value = float(value)
    if not math.isfinite(value):
        raise ValidationError(f"must be a finite number, got {value!r}", field)
    if minimum is not None and value < minimum:
        raise ValidationError(f"must be >= {minimum}, got {value}", field)
    if maximum is not None and value > maximum:
        raise ValidationError(f"must be <= {maximum}, got {value}", field)
    return value


def take_timestamp(
    obj: Mapping[str, Any], key: str, path: str = ""
) -> _dt.datetime:
    """Extract a required RFC 3339 timestamp field as an aware UTC datetime."""
    value = _present(obj, key, path)
    return parse_rfc3339(value, join_path(path, key))


def take_list(obj: Mapping[str, Any], key: str, path: str = "") -> Sequence[Any]:
    """Extract a required JSON array field. Strings are not arrays."""
    value = _present(obj, key, path)
    field = join_path(path, key)
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ValidationError(
            f"expected an array, got {type(value).__name__}", field
        )
    return value


def take_mapping(obj: Mapping[str, Any], key: str, path: str = "") -> Mapping[str, Any]:
    """Extract a required JSON object field."""
    value = _present(obj, key, path)
    return require_mapping(value, join_path(path, key))


def check_enum(value: str, allowed: Iterable[str], path: str = "") -> str:
    """Assert ``value`` is one of ``allowed`` and return it."""
    allowed_sorted = sorted(allowed)
    if value not in allowed_sorted:
        raise ValidationError(
            f"{value!r} is not one of " + ", ".join(repr(a) for a in allowed_sorted),
            path,
        )
    return value
