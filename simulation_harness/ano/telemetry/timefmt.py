"""RFC 3339 timestamps with **nanosecond** accuracy.

Why this module exists
----------------------
``LOGENTRY_SPEC.md`` §2: ``timestamp`` and ``receiveTimestamp`` are
``google.protobuf.Timestamp``, whose proto3 JSON mapping is an RFC 3339 string
with up to nine fractional digits and a ``Z`` suffix. The proto comment says
plainly that *"timestamps have nanosecond accuracy"*.

Python's :class:`datetime.datetime` resolves only to **microseconds**. Storing
event time in a ``datetime`` therefore silently truncates the last three
digits, and a round-trip
``parse -> datetime -> render`` would not reproduce the input. Since the
project's determinism bar is *byte-identical* output, that truncation is not
cosmetic — it is a correctness bug.

This module therefore represents wire time as an ``int`` count of nanoseconds
since the Unix epoch. Integers are exact, cheap, orderable and trivially
deterministic. Conversion to/from ``datetime`` is offered for the boundaries
where the rest of the project speaks ``datetime`` (``ano.contracts`` does), and
is explicitly lossy in one direction only — which is documented at the call
site rather than hidden.
"""

from __future__ import annotations

import datetime as _dt
import re

__all__ = [
    "NANOS_PER_SECOND",
    "RFC3339_NANOS_RE",
    "format_rfc3339",
    "parse_rfc3339",
    "datetime_to_nanos",
    "nanos_to_datetime",
    "is_rfc3339_z",
]

NANOS_PER_SECOND = 1_000_000_000

#: Canonical accepted wire form: RFC 3339, UTC, ``Z`` suffix, at most nine
#: fractional digits. A numeric offset such as ``+01:00`` is legal RFC 3339 but
#: is *not* what Cloud Logging emits, so the validator rejects it (rule L10).
RFC3339_NANOS_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?Z$"
)

_EPOCH = _dt.datetime(1970, 1, 1, tzinfo=_dt.timezone.utc)


def is_rfc3339_z(value: object) -> bool:
    """Return True if ``value`` is a UTC RFC 3339 string with a ``Z`` suffix.

    Purely lexical. :func:`parse_rfc3339` additionally proves the calendar date
    is real (it rejects ``2026-02-30T00:00:00Z``, which the regex accepts).
    """
    return isinstance(value, str) and RFC3339_NANOS_RE.match(value) is not None


def parse_rfc3339(value: str) -> int:
    """Parse an RFC 3339 UTC timestamp into nanoseconds since the epoch.

    Args:
        value: e.g. ``"2026-09-13T09:42:17.482913571Z"``.

    Returns:
        Integer nanoseconds since 1970-01-01T00:00:00Z. May be negative.

    Raises:
        ValueError: if the string is not RFC 3339 UTC with a ``Z`` suffix, or
            if it names a date that does not exist.
    """
    match = RFC3339_NANOS_RE.match(value) if isinstance(value, str) else None
    if match is None:
        raise ValueError(f"not an RFC 3339 UTC timestamp with 'Z' suffix: {value!r}")
    year, month, day, hour, minute, second = (int(g) for g in match.groups()[:6])
    fraction = match.group(7) or ""
    # Right-pad to nine digits: ".5" means 500000000 ns, not 5 ns.
    nanos = int(fraction.ljust(9, "0")) if fraction else 0
    try:
        moment = _dt.datetime(
            year, month, day, hour, minute, second, tzinfo=_dt.timezone.utc
        )
    except ValueError as exc:  # e.g. 2026-02-30, or hour 24
        raise ValueError(f"impossible date/time in {value!r}: {exc}") from exc
    whole_seconds = int((moment - _EPOCH).total_seconds())
    return whole_seconds * NANOS_PER_SECOND + nanos


def format_rfc3339(nanos: int, *, fractional_digits: int = 9) -> str:
    """Render nanoseconds since the epoch as an RFC 3339 UTC timestamp.

    Args:
        nanos: Integer nanoseconds since the epoch.
        fractional_digits: How many fractional digits to emit, 0..9. Nine is
            the default because it is what full-fidelity Cloud Logging export
            data carries, and because a fixed width keeps ``insertId`` ordering
            and lexical timestamp ordering in agreement.

    Returns:
        e.g. ``"2026-09-13T09:42:17.482913571Z"``.
    """
    if not isinstance(nanos, int) or isinstance(nanos, bool):
        raise TypeError(f"nanos must be int, got {type(nanos).__name__}")
    if not 0 <= fractional_digits <= 9:
        raise ValueError("fractional_digits must be in 0..9")
    # Floor division keeps the fraction non-negative for pre-epoch instants.
    whole_seconds, fraction_ns = divmod(nanos, NANOS_PER_SECOND)
    moment = _EPOCH + _dt.timedelta(seconds=whole_seconds)
    stamp = moment.strftime("%Y-%m-%dT%H:%M:%S")
    if fractional_digits == 0:
        return stamp + "Z"
    digits = f"{fraction_ns:09d}"[:fractional_digits]
    return f"{stamp}.{digits}Z"


def datetime_to_nanos(moment: _dt.datetime) -> int:
    """Convert a timezone-aware ``datetime`` to nanoseconds since the epoch.

    Raises:
        ValueError: if ``moment`` is naive. Naive datetimes are rejected
            project-wide (M1 convention) because their UTC offset is a guess.
    """
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError("naive datetime rejected; supply a timezone-aware value")
    delta = moment.astimezone(_dt.timezone.utc) - _EPOCH
    return (
        delta.days * 86_400 * NANOS_PER_SECOND
        + delta.seconds * NANOS_PER_SECOND
        + delta.microseconds * 1_000
    )


def nanos_to_datetime(nanos: int) -> _dt.datetime:
    """Convert nanoseconds since the epoch to a UTC ``datetime``.

    **Lossy**: ``datetime`` resolves to microseconds, so the final three digits
    are truncated (toward negative infinity). Use this only at boundaries that
    genuinely require a ``datetime``; keep wire values as nanos.
    """
    micros, _remainder = divmod(nanos, 1_000)
    return _EPOCH + _dt.timedelta(microseconds=micros)
