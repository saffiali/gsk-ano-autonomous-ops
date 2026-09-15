"""``AlertSink`` — the Cloud Monitoring adapter.

Stands in for
-------------
**Cloud Monitoring** alerting. In production the ``gcp`` backend creates
incidents against alert policies via ``google-cloud-monitoring``
(``AlertPolicyServiceClient`` / notification channels), so an emitted alert
reaches the on-call rotation through the channel named by
``Config.notification_channel``.

What changes on deployment
--------------------------
Configuration only: ``alerting_backend`` becomes ``"gcp"``, ``project_id`` is
set and ``notification_channel`` names the channel. ``alert_path`` stops being
used. Callers still build an :class:`Alert` and call :meth:`AlertSink.emit`.

Why the alert store matters to the scoring
------------------------------------------
Three acceptance criteria are counted off this surface:

* **AC-11** compares alert volume during patching windows against the
  static-threshold baseline;
* **AC-12** compares recall with suppression on and off;
* **AC-13** requires one incident per upstream fault, not one alert per
  affected downstream entity.

So the sink records **suppressed alerts too**, marked rather than discarded.
Dropping them would make the suppression numbers unverifiable — you cannot
report what you never recorded. :meth:`LocalAlertSink.alerts` returns
everything; :meth:`LocalAlertSink.delivered` returns only what an operator would
actually have been paged for.

Determinism
-----------
``alert_id`` is a blake2b digest of the alert's canonical content, so the same
incident produces the same id on every run and an alert artifact is
byte-reproducible. Emission order is preserved and is the artifact's order.
"""

from __future__ import annotations

import abc
import datetime as _dt
import json
import os
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from ano.config import LOCAL, Config
from ano.contracts.common import dumps_canonical, format_rfc3339
from ano.contracts.determinism import stable_id
from ano.gcp.registry import register_backend

__all__ = [
    "SEVERITIES",
    "Alert",
    "AlertSink",
    "LocalAlertSink",
    "build_alert_sink",
]

#: Alert severities, ordered from least to most urgent. These mirror the
#: severity vocabulary Cloud Monitoring notifications carry.
SEVERITIES: tuple[str, ...] = ("INFO", "WARNING", "ERROR", "CRITICAL")


@dataclass(frozen=True, slots=True)
class Alert:
    """One alert as it would reach an operator.

    Attributes:
        incident_id: The incident this alert belongs to. Several predictions
            collapsed into one incident share it (AC-13).
        entity: The subject entity.
        severity: One of :data:`SEVERITIES`.
        summary: One-line description an operator reads first.
        created_at: Emission time. ``None`` when the caller has no meaningful
            clock; the offline pipeline deliberately does not invent one.
        suppressed: True if change-awareness or noise suppression held this
            alert back. It is still recorded (see the module docstring).
        suppression_reason: Why, when ``suppressed``.
        prediction_ids: The C2 predictions this alert was raised from.
        payload: Any additional structured detail for the demo surface.
    """

    incident_id: str
    entity: str
    severity: str
    summary: str
    created_at: _dt.datetime | None = None
    suppressed: bool = False
    suppression_reason: str | None = None
    prediction_ids: tuple[str, ...] = ()
    payload: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        """Raise ``ValueError``/``TypeError`` if this alert is malformed."""
        for attr in ("incident_id", "entity", "summary"):
            value = getattr(self, attr)
            if not isinstance(value, str) or not value:
                raise ValueError(f"{attr} must be a non-empty string")
        if self.severity not in SEVERITIES:
            raise ValueError(
                f"severity must be one of {list(SEVERITIES)}, got "
                f"{self.severity!r}"
            )
        if self.created_at is not None:
            format_rfc3339(self.created_at, "created_at")
        if not isinstance(self.suppressed, bool):
            raise TypeError("suppressed must be a boolean")
        if self.suppressed and not self.suppression_reason:
            raise ValueError(
                "suppression_reason is required when an alert is suppressed"
            )
        if not self.suppressed and self.suppression_reason is not None:
            raise ValueError(
                "suppression_reason must be None when an alert is not suppressed"
            )
        if not isinstance(self.prediction_ids, tuple):
            raise TypeError("prediction_ids must be a tuple of strings")
        for item in self.prediction_ids:
            if not isinstance(item, str) or not item:
                raise ValueError("prediction ids must be non-empty strings")
        if not isinstance(self.payload, Mapping):
            raise TypeError("payload must be a mapping")

    def content(self) -> dict[str, Any]:
        """The alert's identity-bearing content, used to derive ``alert_id``."""
        return {
            "incident_id": self.incident_id,
            "entity": self.entity,
            "severity": self.severity,
            "summary": self.summary,
            "created_at": (
                None if self.created_at is None else format_rfc3339(self.created_at)
            ),
            "suppressed": self.suppressed,
            "suppression_reason": self.suppression_reason,
            "prediction_ids": sorted(self.prediction_ids),
            "payload": dict(self.payload),
        }

    def alert_id(self) -> str:
        """Deterministic id derived from the alert's content."""
        return stable_id(
            dumps_canonical(self.content()), "alerting", prefix="alert", length=16
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialise, including the derived ``alert_id``."""
        record = self.content()
        record["alert_id"] = self.alert_id()
        return record

    @classmethod
    def from_dict(cls, obj: Mapping[str, Any]) -> "Alert":
        """Rebuild an alert from its serialised form (``alert_id`` ignored)."""
        from ano.contracts.common import parse_rfc3339

        created = obj.get("created_at")
        return cls(
            incident_id=obj["incident_id"],
            entity=obj["entity"],
            severity=obj["severity"],
            summary=obj["summary"],
            created_at=None if created is None else parse_rfc3339(created),
            suppressed=bool(obj.get("suppressed", False)),
            suppression_reason=obj.get("suppression_reason"),
            prediction_ids=tuple(obj.get("prediction_ids") or ()),
            payload=dict(obj.get("payload") or {}),
        )


class AlertSink(abc.ABC):
    """Emit alerts to the alerting plane."""

    @abc.abstractmethod
    def emit(self, alert: Alert) -> str:
        """Emit one alert.

        Returns:
            The alert id.
        """

    def emit_many(self, alerts: Iterable[Alert]) -> list[str]:
        """Emit a batch of alerts.

        Returns:
            Their ids, in emission order.
        """
        return [self.emit(alert) for alert in alerts]

    @abc.abstractmethod
    def count(self) -> int:
        """Number of alerts emitted, including suppressed ones."""

    def flush(self) -> None:
        """Flush buffered alerts. No-op unless overridden."""

    def close(self) -> None:
        """Flush and release resources."""
        self.flush()

    def __enter__(self) -> "AlertSink":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


class LocalAlertSink(AlertSink):
    """Offline Cloud Monitoring substitute backed by a JSONL file.

    Args:
        path: Destination file, or ``None`` for in-memory only.
        keep_in_memory: Retain alerts for :meth:`alerts`.
        truncate: Truncate an existing file on open.
    """

    def __init__(
        self,
        path: str | os.PathLike[str] | None,
        *,
        keep_in_memory: bool = True,
        truncate: bool = True,
    ) -> None:
        self._path = os.fspath(path) if path is not None else None
        self._keep = keep_in_memory or self._path is None
        self._alerts: list[Alert] = []
        self._count = 0
        self._handle = None
        if self._path is not None:
            parent = os.path.dirname(self._path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            self._handle = open(
                self._path, "w" if truncate else "a", encoding="utf-8"
            )

    @property
    def path(self) -> str | None:
        """The JSONL destination, or ``None``."""
        return self._path

    def emit(self, alert: Alert) -> str:
        """Record an alert, suppressed or not, and return its id."""
        if not isinstance(alert, Alert):
            raise TypeError(f"expected an Alert, got {type(alert).__name__}")
        if self._keep:
            self._alerts.append(alert)
        if self._handle is not None:
            self._handle.write(dumps_canonical(alert.to_dict()) + "\n")
        self._count += 1
        return alert.alert_id()

    def count(self) -> int:
        """Number of alerts emitted, including suppressed ones."""
        return self._count

    def flush(self) -> None:
        """Flush the underlying file handle."""
        if self._handle is not None:
            self._handle.flush()

    def close(self) -> None:
        """Flush and close the underlying file handle."""
        if self._handle is not None:
            self._handle.flush()
            self._handle.close()
            self._handle = None

    # -- offline read-back -------------------------------------------------
    def alerts(self) -> tuple[Alert, ...]:
        """Every alert in emission order, including suppressed ones."""
        if not self._keep:
            raise RuntimeError(
                "this sink was created with keep_in_memory=False; read the "
                f"artifact at {self._path!r} instead"
            )
        return tuple(self._alerts)

    def delivered(self) -> tuple[Alert, ...]:
        """Only the alerts an operator would actually have been paged for."""
        return tuple(alert for alert in self.alerts() if not alert.suppressed)

    def suppressed(self) -> tuple[Alert, ...]:
        """Only the alerts that were held back."""
        return tuple(alert for alert in self.alerts() if alert.suppressed)

    def incidents(self) -> tuple[str, ...]:
        """Distinct incident ids among delivered alerts, sorted.

        The collapse ratio (AC-13) is the relationship between the number of
        delivered alerts and the size of this set.
        """
        return tuple(sorted({alert.incident_id for alert in self.delivered()}))

    @classmethod
    def from_config(cls, config: Config) -> "LocalAlertSink":
        """Build the offline sink from a :class:`ano.config.Config`."""
        return cls(config.resolved_alert_path())


def read_alerts(path: str | os.PathLike[str]) -> list[Alert]:
    """Read alerts back from a JSONL artifact written by this sink."""
    out: list[Alert] = []
    with open(path, "r", encoding="utf-8") as handle:
        for number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                payload = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{number}: invalid JSON: {exc}") from exc
            out.append(Alert.from_dict(payload))
    return out


def build_alert_sink(config: Config) -> AlertSink:
    """Build the configured alert sink (see :func:`ano.gcp.registry.build`)."""
    from ano.gcp.registry import build

    return build("alerting", config)


def severity_at_least(severity: str, minimum: str) -> bool:
    """True if ``severity`` is at least as urgent as ``minimum``."""
    if severity not in SEVERITIES:
        raise ValueError(f"unknown severity {severity!r}")
    if minimum not in SEVERITIES:
        raise ValueError(f"unknown severity {minimum!r}")
    return SEVERITIES.index(severity) >= SEVERITIES.index(minimum)


def sorted_alerts(alerts: Sequence[Alert]) -> tuple[Alert, ...]:
    """Alerts in a total, reproducible order (created_at, incident, entity)."""
    return tuple(
        sorted(
            alerts,
            key=lambda alert: (
                ""
                if alert.created_at is None
                else format_rfc3339(alert.created_at),
                alert.incident_id,
                alert.entity,
                alert.alert_id(),
            ),
        )
    )


register_backend("alerting", LOCAL, LocalAlertSink.from_config)
