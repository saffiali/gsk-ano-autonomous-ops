"""Errors raised by :mod:`ano.telemetry`.

Both error classes derive from :class:`ano.contracts.common.ValidationError`
(itself a ``ValueError``) so that every contract-layer failure in the project
has one catchable base, per the M1 published API. They add the one thing a
format validator needs and a generic contract error does not: a **rule id**.

Rule ids are stable, greppable and traceable to a numbered section of the
format specifications:

* ``L01``..``L18`` — ``LOGENTRY_SPEC.md`` rules, as enumerated in
  ``VALIDATION_STRATEGY.md`` §2.5.
* Prometheus codes mirror the reference parser's own error conditions listed
  in ``PROMETHEUS_SPEC.md`` §1.12 and the consolidated rule list in Part 4.

Carrying the rule id in the exception is what makes the negative-test corpus
meaningful: a test asserts not merely "it was rejected" but "it was rejected
*for the right reason*". A validator that rejects everything for the wrong
reason is not a validator.
"""

from __future__ import annotations

from ano.contracts.common import ValidationError

__all__ = [
    "TelemetryError",
    "LogEntryError",
    "ExpositionError",
]


class TelemetryError(ValidationError):
    """Base class for every ``ano.telemetry`` conformance failure."""

    #: Short stable identifier for the violated rule, e.g. ``"L05"``.
    rule: str

    def __init__(self, rule: str, message: str, path: str = "") -> None:
        self.rule = rule
        super().__init__(f"[{rule}] {message}", path)


class LogEntryError(TelemetryError):
    """A JSON object does not conform to the Cloud Logging ``LogEntry`` schema."""


class ExpositionError(TelemetryError):
    """Text does not conform to Prometheus text exposition format 0.0.4 / GMP."""

    #: 1-based line number the failure was detected on, or 0 if whole-document.
    line: int

    def __init__(self, rule: str, message: str, line: int = 0) -> None:
        self.line = line
        super().__init__(rule, message, f"line {line}" if line else "")
