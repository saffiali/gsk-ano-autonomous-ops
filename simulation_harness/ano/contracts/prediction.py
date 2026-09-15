"""Contract C2 — detector output (Prediction records).

Direction of travel: anything under ``ano`` that predicts **writes** these;
``harness`` and ``ano.demo`` **read** them. This is the only channel by which
detector output reaches the scorer, which is what keeps requirement R5's
separation honest — the harness never imports a detector.

Wire format (from ``PROJECT.md``)::

    { "prediction_id": "...", "emitted_at": "RFC3339",
      "track": "unresponsiveness|cross_domain",
      "entity": "...", "predicted_onset": "RFC3339", "time_to_failure_s": 1200,
      "confidence": 0.0,
      "signals": [{"name": "...", "value": 0.0, "baseline": 0.0,
                   "contribution": 0.0}],
      "root_cause": {"domain": "...", "entity": "...",
                     "evidence": [{"kind": "...", "detail": "..."}]},
      "suppressed": false, "suppression_reason": null,
      "incident_id": "...", "collapsed_from": ["..."],
      "recommended_remediation": "..." }

Field semantics that the acceptance criteria depend on
------------------------------------------------------
``emitted_at``
    When the detector emitted the prediction. **Lead time is measured from
    this** (ratified definition RD-02: ``lead_time = true_onset -
    emitted_at_of_first_matched_prediction``), so it must be the model's
    decision time, not the time the file was written.
``predicted_onset`` / ``time_to_failure_s``
    The forecast. ``predicted_onset`` must not precede ``emitted_at`` — a
    prediction is about the future. ``time_to_failure_s`` must equal the gap
    between the two to the second; a mismatch means one of the two is stale and
    the harness cannot tell which. :meth:`Prediction.from_parts` derives one
    from the other so producers cannot get this wrong.
``signals``
    Why the model fired, per requirement R2 ("the specific signals that drove
    it"). Must be non-empty: a prediction with no evidence is not actionable and
    the demo surface has nothing to render.
``suppressed`` / ``suppression_reason``
    Suppression is *reported, not deleted*: a suppressed prediction is still
    emitted with ``suppressed=true`` and a non-null reason. This is what lets
    the harness compute recall with suppression on and off (AC-12) from one run
    of the pipeline plus one toggle, and lets AC-11 count alert volume both ways.
    Invariant: ``suppression_reason`` is non-null **iff** ``suppressed`` is true.
``incident_id``
    The collapse key. N downstream predictions that share one ``incident_id``
    count as one incident (AC-13). A prediction that was not collapsed still
    carries an ``incident_id`` — its own.
``collapsed_from``
    Ids of the predictions this one absorbed. Must not contain
    ``prediction_id`` itself.
"""

from __future__ import annotations

import datetime as _dt
import os
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any

from ano.contracts.common import (
    ValidationError,
    check_enum,
    dumps_canonical,
    format_rfc3339,
    join_path,
    loads,
    reject_unknown_keys,
    require_mapping,
    take_bool,
    take_float,
    take_int,
    take_list,
    take_mapping,
    take_optional_str,
    take_str,
    take_timestamp,
)
from ano.contracts.labels import DOMAINS

__all__ = [
    "PREDICTION_TRACKS",
    "DOMAINS",
    "Signal",
    "Evidence",
    "Attribution",
    "Prediction",
    "parse_prediction",
    "parse_predictions_jsonl",
    "predictions_to_jsonl",
    "read_predictions",
    "iter_predictions",
    "write_predictions",
]

#: The two predictive tracks. Scored independently (AC-08).
PREDICTION_TRACKS: frozenset[str] = frozenset({"unresponsiveness", "cross_domain"})


@dataclass(frozen=True, slots=True)
class Signal:
    """One driver of a prediction.

    Attributes:
        name: Signal name, e.g. ``"thread_pool_utilisation"``. Non-empty.
        value: The observed value at decision time.
        baseline: The learned baseline the value is being compared against
            (requirement R4 — dynamic baselines, not static thresholds).
        contribution: How much this signal contributed to the confidence, in
            ``[0, 1]``. Contributions across a prediction's signals are *not*
            required to sum to 1: models may attribute non-additively, and
            forcing normalisation would invite fudging.
    """

    name: str
    value: float
    baseline: float
    contribution: float
    novelty_score: float | None = None

    WIRE_KEYS = ("name", "value", "baseline", "contribution", "novelty_score")

    def __post_init__(self) -> None:
        self.validate()

    def validate(self, path: str = "") -> None:
        """Raise :class:`ValidationError` if this signal is malformed."""
        if not isinstance(self.name, str) or not self.name:
            raise ValidationError(
                "name must be a non-empty string", join_path(path, "name")
            )
        for attr in ("value", "baseline", "contribution"):
            number = getattr(self, attr)
            if isinstance(number, bool) or not isinstance(number, (int, float)):
                raise ValidationError(
                    f"{attr} must be a number", join_path(path, attr)
                )
            if number != number or number in (float("inf"), float("-inf")):
                raise ValidationError(
                    f"{attr} must be finite", join_path(path, attr)
                )
        if not 0.0 <= self.contribution <= 1.0:
            raise ValidationError(
                f"contribution must be in [0, 1], got {self.contribution}",
                join_path(path, "contribution"),
            )
        if self.novelty_score is not None:
            if isinstance(self.novelty_score, bool) or not isinstance(self.novelty_score, (int, float)):
                raise ValidationError(
                    "novelty_score must be a number", join_path(path, "novelty_score")
                )

    def to_dict(self) -> dict[str, Any]:
        """Serialise to the C2 wire form."""
        payload: dict[str, Any] = {
            "name": self.name,
            "value": float(self.value),
            "baseline": float(self.baseline),
            "contribution": float(self.contribution),
        }
        if self.novelty_score is not None:
            payload["novelty_score"] = float(self.novelty_score)
        return payload

    @classmethod
    def from_dict(cls, obj: Any, path: str = "") -> "Signal":
        """Parse and strictly validate one signal record."""
        mapping = require_mapping(obj, path)
        reject_unknown_keys(mapping, cls.WIRE_KEYS, path)
        return cls(
            name=take_str(mapping, "name", path),
            value=take_float(mapping, "value", path),
            baseline=take_float(mapping, "baseline", path),
            contribution=take_float(
                mapping, "contribution", path, minimum=0.0, maximum=1.0
            ),
            novelty_score=take_float(mapping, "novelty_score", path) if "novelty_score" in mapping else None,
        )


@dataclass(frozen=True, slots=True)
class Evidence:
    """One piece of correlation evidence supporting a root-cause attribution.

    Attributes:
        kind: Evidence category, e.g. ``"topology_path"``, ``"lock_wait_spike"``,
            ``"temporal_precedence"``. Non-empty.
        detail: Human-readable specifics an operator can check, e.g.
            ``"db-ora-03 lock waits 18x baseline 40s before app latency rose"``.
    """

    kind: str
    detail: str

    WIRE_KEYS = ("kind", "detail")

    def __post_init__(self) -> None:
        self.validate()

    def validate(self, path: str = "") -> None:
        """Raise :class:`ValidationError` if this evidence record is malformed."""
        if not isinstance(self.kind, str) or not self.kind:
            raise ValidationError(
                "kind must be a non-empty string", join_path(path, "kind")
            )
        if not isinstance(self.detail, str) or not self.detail:
            raise ValidationError(
                "detail must be a non-empty string", join_path(path, "detail")
            )

    def to_dict(self) -> dict[str, Any]:
        """Serialise to the C2 wire form."""
        return {"kind": self.kind, "detail": self.detail}

    @classmethod
    def from_dict(cls, obj: Any, path: str = "") -> "Evidence":
        """Parse and strictly validate one evidence record."""
        mapping = require_mapping(obj, path)
        reject_unknown_keys(mapping, cls.WIRE_KEYS, path)
        return cls(
            kind=take_str(mapping, "kind", path),
            detail=take_str(mapping, "detail", path),
        )


@dataclass(frozen=True, slots=True)
class Attribution:
    """The root cause a detector attributes a prediction to.

    Attributes:
        domain: ``application``, ``database`` or ``network``. Scored by AC-09.
        entity: The specific responsible entity. Scored by requirement R3's
            "name the responsible entity".
        evidence: Why. May be empty for a same-entity application attribution
            where the signals are the evidence, but cross-domain attributions
            should always carry at least one item — requirement R3 asks for
            "the correlation evidence".
    """

    domain: str
    entity: str
    evidence: tuple[Evidence, ...] = ()

    WIRE_KEYS = ("domain", "entity", "evidence")

    def __post_init__(self) -> None:
        self.validate()

    def validate(self, path: str = "") -> None:
        """Raise :class:`ValidationError` if this attribution is malformed."""
        check_enum(self.domain, DOMAINS, join_path(path, "domain"))
        if not isinstance(self.entity, str) or not self.entity:
            raise ValidationError(
                "entity must be a non-empty string", join_path(path, "entity")
            )
        if not isinstance(self.evidence, tuple):
            raise ValidationError(
                "evidence must be a tuple of Evidence", join_path(path, "evidence")
            )
        for index, item in enumerate(self.evidence):
            item.validate(join_path(join_path(path, "evidence"), index))

    def to_dict(self) -> dict[str, Any]:
        """Serialise to the C2 wire form."""
        return {
            "domain": self.domain,
            "entity": self.entity,
            "evidence": [item.to_dict() for item in self.evidence],
        }

    @classmethod
    def from_dict(cls, obj: Any, path: str = "") -> "Attribution":
        """Parse and strictly validate one attribution record."""
        mapping = require_mapping(obj, path)
        reject_unknown_keys(mapping, cls.WIRE_KEYS, path)
        evidence_raw = take_list(mapping, "evidence", path)
        evidence = tuple(
            Evidence.from_dict(item, join_path(join_path(path, "evidence"), index))
            for index, item in enumerate(evidence_raw)
        )
        return cls(
            domain=check_enum(
                take_str(mapping, "domain", path), DOMAINS, join_path(path, "domain")
            ),
            entity=take_str(mapping, "entity", path),
            evidence=evidence,
        )


@dataclass(frozen=True, slots=True)
class Prediction:
    """A single C2 prediction record. See the module docstring for semantics."""

    prediction_id: str
    emitted_at: _dt.datetime
    track: str
    entity: str
    predicted_onset: _dt.datetime
    time_to_failure_s: int
    confidence: float
    signals: tuple[Signal, ...]
    root_cause: Attribution
    incident_id: str
    recommended_remediation: str
    suppressed: bool = False
    suppression_reason: str | None = None
    collapsed_from: tuple[str, ...] = ()

    #: Keys allowed on the wire.
    WIRE_KEYS = (
        "prediction_id",
        "emitted_at",
        "track",
        "entity",
        "predicted_onset",
        "time_to_failure_s",
        "confidence",
        "signals",
        "root_cause",
        "suppressed",
        "suppression_reason",
        "incident_id",
        "collapsed_from",
        "recommended_remediation",
    )

    def __post_init__(self) -> None:
        self.validate()

    def validate(self, path: str = "") -> None:
        """Raise :class:`ValidationError` if this prediction is malformed."""
        for attr in (
            "prediction_id",
            "entity",
            "incident_id",
            "recommended_remediation",
        ):
            value = getattr(self, attr)
            if not isinstance(value, str) or not value:
                raise ValidationError(
                    f"{attr} must be a non-empty string", join_path(path, attr)
                )
        check_enum(self.track, PREDICTION_TRACKS, join_path(path, "track"))
        format_rfc3339(self.emitted_at, join_path(path, "emitted_at"))
        format_rfc3339(self.predicted_onset, join_path(path, "predicted_onset"))
        if self.predicted_onset < self.emitted_at:
            raise ValidationError(
                f"predicted_onset ({format_rfc3339(self.predicted_onset)}) must not "
                f"precede emitted_at ({format_rfc3339(self.emitted_at)}); a "
                "prediction is about the future",
                path,
            )
        if isinstance(self.time_to_failure_s, bool) or not isinstance(
            self.time_to_failure_s, int
        ):
            raise ValidationError(
                "time_to_failure_s must be an integer number of seconds",
                join_path(path, "time_to_failure_s"),
            )
        if self.time_to_failure_s < 0:
            raise ValidationError(
                f"time_to_failure_s must be >= 0, got {self.time_to_failure_s}",
                join_path(path, "time_to_failure_s"),
            )
        gap = int(round((self.predicted_onset - self.emitted_at).total_seconds()))
        if gap != self.time_to_failure_s:
            raise ValidationError(
                f"time_to_failure_s ({self.time_to_failure_s}) disagrees with "
                f"predicted_onset - emitted_at ({gap}s); use "
                "Prediction.from_parts() to derive one from the other",
                path,
            )
        if isinstance(self.confidence, bool) or not isinstance(
            self.confidence, (int, float)
        ):
            raise ValidationError(
                "confidence must be a number", join_path(path, "confidence")
            )
        if not 0.0 <= self.confidence <= 1.0:
            raise ValidationError(
                f"confidence must be in [0, 1], got {self.confidence}",
                join_path(path, "confidence"),
            )
        if not isinstance(self.signals, tuple) or not self.signals:
            raise ValidationError(
                "signals must be a non-empty tuple; requirement R2 requires a "
                "prediction to name the signals that drove it",
                join_path(path, "signals"),
            )
        for index, signal in enumerate(self.signals):
            signal.validate(join_path(join_path(path, "signals"), index))
        self.root_cause.validate(join_path(path, "root_cause"))
        if not isinstance(self.suppressed, bool):
            raise ValidationError(
                "suppressed must be a boolean", join_path(path, "suppressed")
            )
        if self.suppressed and not self.suppression_reason:
            raise ValidationError(
                "suppression_reason is required when suppressed is true",
                join_path(path, "suppression_reason"),
            )
        if not self.suppressed and self.suppression_reason is not None:
            raise ValidationError(
                "suppression_reason must be null when suppressed is false",
                join_path(path, "suppression_reason"),
            )
        if not isinstance(self.collapsed_from, tuple):
            raise ValidationError(
                "collapsed_from must be a tuple of prediction ids",
                join_path(path, "collapsed_from"),
            )
        seen: set[str] = set()
        for index, other in enumerate(self.collapsed_from):
            item_path = join_path(join_path(path, "collapsed_from"), index)
            if not isinstance(other, str) or not other:
                raise ValidationError("must be a non-empty string", item_path)
            if other == self.prediction_id:
                raise ValidationError(
                    "a prediction may not list itself in collapsed_from", item_path
                )
            if other in seen:
                raise ValidationError(f"duplicate id {other!r}", item_path)
            seen.add(other)

    # -- derived helpers ---------------------------------------------------
    def lead_time_s(self, true_onset: _dt.datetime) -> float:
        """Lead time in seconds against a ground-truth onset.

        Positive means the prediction preceded the incident. Ratified definition
        RD-03: only strictly positive lead times count toward recall.

        The harness owns the matching rules (entity match, 120-minute horizon);
        this helper only does the subtraction, so a detector cannot use it to
        infer anything about the ground truth it is not allowed to see.
        """
        return (true_onset - self.emitted_at).total_seconds()

    def is_collapsed(self) -> bool:
        """True if this prediction absorbed at least one other."""
        return bool(self.collapsed_from)

    def to_dict(self) -> dict[str, Any]:
        """Serialise to the C2 wire form."""
        return {
            "prediction_id": self.prediction_id,
            "emitted_at": format_rfc3339(self.emitted_at),
            "track": self.track,
            "entity": self.entity,
            "predicted_onset": format_rfc3339(self.predicted_onset),
            "time_to_failure_s": self.time_to_failure_s,
            "confidence": float(self.confidence),
            "signals": [signal.to_dict() for signal in self.signals],
            "root_cause": self.root_cause.to_dict(),
            "suppressed": self.suppressed,
            "suppression_reason": self.suppression_reason,
            "incident_id": self.incident_id,
            "collapsed_from": list(self.collapsed_from),
            "recommended_remediation": self.recommended_remediation,
        }

    def to_json(self) -> str:
        """Serialise to a single canonical JSON line (no newline)."""
        return dumps_canonical(self.to_dict())

    @classmethod
    def from_dict(cls, obj: Any, path: str = "") -> "Prediction":
        """Parse and strictly validate one prediction record."""
        mapping = require_mapping(obj, path)
        reject_unknown_keys(mapping, cls.WIRE_KEYS, path)
        signals_raw = take_list(mapping, "signals", path)
        signals = tuple(
            Signal.from_dict(item, join_path(join_path(path, "signals"), index))
            for index, item in enumerate(signals_raw)
        )
        collapsed_raw = take_list(mapping, "collapsed_from", path)
        collapsed: list[str] = []
        for index, item in enumerate(collapsed_raw):
            item_path = join_path(join_path(path, "collapsed_from"), index)
            if not isinstance(item, str) or not item:
                raise ValidationError("must be a non-empty string", item_path)
            collapsed.append(item)
        return cls(
            prediction_id=take_str(mapping, "prediction_id", path),
            emitted_at=take_timestamp(mapping, "emitted_at", path),
            track=check_enum(
                take_str(mapping, "track", path),
                PREDICTION_TRACKS,
                join_path(path, "track"),
            ),
            entity=take_str(mapping, "entity", path),
            predicted_onset=take_timestamp(mapping, "predicted_onset", path),
            time_to_failure_s=take_int(mapping, "time_to_failure_s", path, minimum=0),
            confidence=take_float(
                mapping, "confidence", path, minimum=0.0, maximum=1.0
            ),
            signals=signals,
            root_cause=Attribution.from_dict(
                take_mapping(mapping, "root_cause", path), join_path(path, "root_cause")
            ),
            suppressed=take_bool(mapping, "suppressed", path),
            suppression_reason=take_optional_str(mapping, "suppression_reason", path),
            incident_id=take_str(mapping, "incident_id", path),
            collapsed_from=tuple(collapsed),
            recommended_remediation=take_str(mapping, "recommended_remediation", path),
        )

    @classmethod
    def from_parts(
        cls,
        *,
        prediction_id: str,
        emitted_at: _dt.datetime,
        track: str,
        entity: str,
        time_to_failure_s: int,
        confidence: float,
        signals: Iterable[Signal],
        root_cause: Attribution,
        recommended_remediation: str,
        incident_id: str | None = None,
        suppressed: bool = False,
        suppression_reason: str | None = None,
        collapsed_from: Iterable[str] = (),
    ) -> "Prediction":
        """Build a prediction, deriving ``predicted_onset`` from the lead time.

        This is the constructor detectors should use: it makes the
        ``predicted_onset`` / ``time_to_failure_s`` consistency invariant
        impossible to violate, and defaults ``incident_id`` to
        ``prediction_id`` (an uncollapsed prediction is its own incident).
        """
        onset = emitted_at + _dt.timedelta(seconds=int(time_to_failure_s))
        return cls(
            prediction_id=prediction_id,
            emitted_at=emitted_at,
            track=track,
            entity=entity,
            predicted_onset=onset,
            time_to_failure_s=int(time_to_failure_s),
            confidence=float(confidence),
            signals=tuple(signals),
            root_cause=root_cause,
            incident_id=incident_id if incident_id is not None else prediction_id,
            recommended_remediation=recommended_remediation,
            suppressed=suppressed,
            suppression_reason=suppression_reason,
            collapsed_from=tuple(collapsed_from),
        )

    def with_suppression(self, reason: str) -> "Prediction":
        """Return a copy marked suppressed with ``reason``.

        Suppression never deletes a prediction — see the module docstring.
        """
        if not reason:
            raise ValidationError("suppression reason must not be empty")
        data = self.to_dict()
        data["suppressed"] = True
        data["suppression_reason"] = reason
        return Prediction.from_dict(data)

    def with_incident(
        self, incident_id: str, collapsed_from: Iterable[str] = ()
    ) -> "Prediction":
        """Return a copy assigned to ``incident_id``, absorbing ``collapsed_from``."""
        data = self.to_dict()
        data["incident_id"] = incident_id
        data["collapsed_from"] = sorted(set(collapsed_from))
        return Prediction.from_dict(data)


def parse_prediction(text: str) -> Prediction:
    """Parse one JSON object of C2 text into a validated :class:`Prediction`."""
    return Prediction.from_dict(loads(text))


def parse_predictions_jsonl(text: str) -> list[Prediction]:
    """Parse newline-delimited C2 JSON. Blank lines are ignored.

    Raises:
        ValidationError: on the first malformed record, with the line number in
            the error path.
    """
    predictions: list[Prediction] = []
    for number, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if not stripped:
            continue
        predictions.append(
            Prediction.from_dict(loads(stripped, f"line {number}"), f"line {number}")
        )
    return predictions


def predictions_to_jsonl(predictions: Iterable[Prediction]) -> str:
    """Serialise predictions to newline-delimited canonical JSON.

    Emission order is preserved — it is part of the artifact and must be
    reproducible.
    """
    return "".join(prediction.to_json() + "\n" for prediction in predictions)


def read_predictions(path: str | os.PathLike[str]) -> list[Prediction]:
    """Read and validate a C2 JSONL file from disk."""
    with open(path, "r", encoding="utf-8") as handle:
        return parse_predictions_jsonl(handle.read())


def write_predictions(
    path: str | os.PathLike[str], predictions: Iterable[Prediction]
) -> int:
    """Write predictions as C2 JSONL, creating parent directories.

    Returns:
        The number of records written.
    """
    items = list(predictions)
    parent = os.path.dirname(os.fspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(predictions_to_jsonl(items))
    return len(items)


def iter_predictions(path: str | os.PathLike[str]) -> Iterator[Prediction]:
    """Stream predictions from a C2 JSONL file one record at a time."""
    with open(path, "r", encoding="utf-8") as handle:
        for number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            yield Prediction.from_dict(
                loads(stripped, f"line {number}"), f"line {number}"
            )
