"""Matching rules: which contract-C2 prediction counts against which C1 event.

Every scoring decision in the harness reduces to a matching question, so the
rules live in one place, are pure functions over the two contracts, and are unit
tested against hand-computed cases.

The rules, and the ratified definition each one implements
(``.agents/teamwork_preview_orchestrator/RATIFIED_DEFINITIONS.md``):

RD-02 — a prediction matches a genuine incident only if
  * **the entity matches**: the prediction names either the incident's
    ``root_cause.entity`` or one of its ``affected_entities``; and
  * ``emitted_at`` falls inside the association window
    ``(onset - MAX_PREDICTION_HORIZON_S, onset)`` — strictly before onset, and
    no earlier than the declared 7200-second horizon.
  A firing earlier than the horizon earns **no** lead-time credit. It is not
  silently dropped: it lands in ``fpr_strict`` and in ``unmatched_alert_count``.

RD-03 — only strictly-positive-lead matches count toward recall. A firing at or
  after onset (but no later than the incident's end) is a **reactive detection**:
  reported separately, excluded from the recall numerator.

A-03 — an *alert* is an operator-deliverable notification for one entity,
  counted after change-suppression and before collapse. On the wire a
  suppressed prediction is still emitted (contract C2 forbids deleting it), so
  the harness treats ``suppressed is False`` as "delivered to the operator".
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

from ano.contracts.labels import GroundTruthEvent, LabelFile
from ano.contracts.prediction import Prediction
from ano.contracts.results import MAX_PREDICTION_HORIZON_S

__all__ = [
    "MAX_PREDICTION_HORIZON_S",
    "MAINTENANCE_KINDS",
    "PATCHING_ONLY_KINDS",
    "DB_CASCADE_KINDS",
    "SEMANTIC_ANOMALY_MARKERS",
    "MatchKind",
    "EventMatch",
    "event_entities",
    "entity_matches",
    "classify",
    "delivered_alerts",
    "sort_predictions",
    "match_event",
    "match_benign_event",
    "flags_semantic_anomaly",
    "is_upstream_fault",
    "maintenance_windows",
    "alerts_in_maintenance_windows",
    "entity_domain",
    "window_bins",
]

#: Kinds treated as "simulated patching/maintenance windows" for AC-11. R4
#: enumerates "OS patching windows, maintenance events, planned deployments" as
#: one family of scheduled changes, so both scheduled-change kinds are counted.
#: The narrower patching-only figure is additionally reported so the choice of
#: window family cannot be mistaken for a convenience.
MAINTENANCE_KINDS: frozenset[str] = frozenset({"patching_window", "planned_deploy"})

#: The strict reading of "patching window": OS patching only.
PATCHING_ONLY_KINDS: frozenset[str] = frozenset({"patching_window"})

#: Ground-truth kinds that are "database-contention-driven application
#: slowness" for AC-10 / RD-05.
DB_CASCADE_KINDS: frozenset[str] = frozenset({"db_lock_contention"})

#: Contract-C2 markers by which a detector declares "this was flagged by the
#: semantic outlier detector" (AC-15). Matched case-insensitively as substrings
#: against ``signals[].name`` and ``root_cause.evidence[].kind``. The harness
#: cannot look inside the detector (R5), so the marker must travel on the wire.
SEMANTIC_ANOMALY_MARKERS: tuple[str, ...] = (
    "semantic",
    "embedding",
    "novel_log",
    "novel_signature",
    "log_outlier",
)


class MatchKind:
    """How a prediction relates to a ground-truth event."""

    #: Entity matched and the prediction preceded onset within the horizon.
    PREDICTIVE = "predictive"
    #: Entity matched and the prediction landed in ``[onset, end]``.
    REACTIVE = "reactive"
    #: Entity matched but the prediction fired earlier than the horizon.
    PRE_HORIZON = "pre_horizon"
    #: No relationship.
    NONE = "none"


@dataclass(frozen=True, slots=True)
class EventMatch:
    """The predictions matched to one ground-truth event.

    Attributes:
        event: The ground-truth event.
        predictive: Alerts with strictly positive lead inside the horizon,
            ordered by ``emitted_at`` then ``prediction_id``.
        reactive: Alerts in ``[onset, end]``, same ordering.
        pre_horizon: Alerts on a matching entity that fired earlier than the
            horizon. Reported, never credited (RD-02).
    """

    event: GroundTruthEvent
    predictive: tuple[Prediction, ...]
    reactive: tuple[Prediction, ...]
    pre_horizon: tuple[Prediction, ...]

    @property
    def recalled(self) -> bool:
        """True if the event has at least one strictly-positive-lead alert."""
        return bool(self.predictive)

    @property
    def alerted(self) -> bool:
        """True if the system alerted on the event at all (RD-03 aside).

        AC-09's denominator is "incidents the system alerts on", which includes
        reactive detections — a reactive alert is still an alert, and excluding
        it would shrink the denominator in the system's favour.
        """
        return bool(self.predictive) or bool(self.reactive)

    @property
    def reactive_only(self) -> bool:
        """True if the only detections were at or after onset (RD-03)."""
        return not self.predictive and bool(self.reactive)

    @property
    def first_alert(self) -> Prediction | None:
        """The earliest alert, predictive preferred over reactive.

        This is the prediction whose attribution is scored and whose
        ``emitted_at`` defines the lead time — "first prediction" in AC-07.
        """
        if self.predictive:
            return self.predictive[0]
        if self.reactive:
            return self.reactive[0]
        return None

    @property
    def lead_time_s(self) -> float | None:
        """Lead time in seconds, or ``None`` if the event was not recalled."""
        if not self.predictive:
            return None
        return (self.event.onset - self.predictive[0].emitted_at).total_seconds()


def event_entities(event: GroundTruthEvent) -> tuple[str, ...]:
    """Every entity an alert may name to be considered "about" ``event``.

    The root-cause entity plus the affected set, de-duplicated and sorted for
    determinism.
    """
    names = {event.root_cause.entity, *event.affected_entities}
    return tuple(sorted(names))


def entity_matches(prediction: Prediction, event: GroundTruthEvent) -> bool:
    """True if ``prediction`` names an entity belonging to ``event`` (RD-02)."""
    return prediction.entity in event_entities(event)


def classify(
    prediction: Prediction,
    event: GroundTruthEvent,
    *,
    horizon_s: int = MAX_PREDICTION_HORIZON_S,
) -> str:
    """Classify one ``(prediction, event)`` pair. Returns a :class:`MatchKind`.

    Args:
        prediction: A contract-C2 prediction.
        event: A contract-C1 ground-truth event.
        horizon_s: Maximum prediction horizon (RD-02 fixes 7200).
    """
    if not entity_matches(prediction, event):
        return MatchKind.NONE
    emitted = prediction.emitted_at
    if emitted < event.onset:
        lead = (event.onset - emitted).total_seconds()
        if lead <= horizon_s:
            return MatchKind.PREDICTIVE
        return MatchKind.PRE_HORIZON
    if emitted <= event.end:
        return MatchKind.REACTIVE
    return MatchKind.NONE


def sort_predictions(predictions: Iterable[Prediction]) -> tuple[Prediction, ...]:
    """Deterministic ordering: ``emitted_at`` then ``prediction_id``.

    Two predictions emitted in the same second must still order identically on
    every run or AC-03's bit-identical requirement fails for reasons that have
    nothing to do with the model.
    """
    return tuple(
        sorted(predictions, key=lambda item: (item.emitted_at, item.prediction_id))
    )


def delivered_alerts(predictions: Iterable[Prediction]) -> tuple[Prediction, ...]:
    """The predictions an operator would actually receive (A-03).

    Contract C2 requires suppression to mark rather than delete, so the
    suppressed ones are present on the wire and are filtered out here. Returned
    in deterministic order.
    """
    return sort_predictions(item for item in predictions if not item.suppressed)


def match_event(
    event: GroundTruthEvent,
    alerts: Sequence[Prediction],
    *,
    horizon_s: int = MAX_PREDICTION_HORIZON_S,
) -> EventMatch:
    """Partition ``alerts`` against one genuine ``event`` (RD-02, RD-03)."""
    predictive: list[Prediction] = []
    reactive: list[Prediction] = []
    pre_horizon: list[Prediction] = []
    for alert in alerts:
        kind = classify(alert, event, horizon_s=horizon_s)
        if kind == MatchKind.PREDICTIVE:
            predictive.append(alert)
        elif kind == MatchKind.REACTIVE:
            reactive.append(alert)
        elif kind == MatchKind.PRE_HORIZON:
            pre_horizon.append(alert)
    return EventMatch(
        event=event,
        predictive=sort_predictions(predictive),
        reactive=sort_predictions(reactive),
        pre_horizon=sort_predictions(pre_horizon),
    )


def match_benign_event(
    event: GroundTruthEvent, alerts: Sequence[Prediction]
) -> tuple[Prediction, ...]:
    """Alerts that a benign ``event`` raised (RD-01).

    A benign event "raised an alert" when a delivered alert names one of its
    entities and was emitted inside the event's own window ``[onset, end]``.
    Counting is per event in :mod:`harness.scoring`; this returns the alerts so
    the raw list is auditable.
    """
    entities = set(event_entities(event))
    matched = [
        alert
        for alert in alerts
        if alert.entity in entities and event.onset <= alert.emitted_at <= event.end
    ]
    return sort_predictions(matched)


def flags_semantic_anomaly(prediction: Prediction) -> bool:
    """True if the prediction declares a semantic/embedding-driven detection.

    AC-15 asks whether "the system flags it as anomalous". The harness may not
    read detector internals (R5), so the declaration must arrive over contract
    C2: a signal name or an evidence kind containing one of
    :data:`SEMANTIC_ANOMALY_MARKERS`.
    """
    haystacks = [signal.name.lower() for signal in prediction.signals]
    haystacks.extend(item.kind.lower() for item in prediction.root_cause.evidence)
    return any(
        marker in haystack
        for haystack in haystacks
        for marker in SEMANTIC_ANOMALY_MARKERS
    )


def is_upstream_fault(event: GroundTruthEvent) -> bool:
    """True if ``event`` is an upstream fault with >= 2 downstream entities.

    AC-13 is about "a single injected upstream fault" whose blast radius covers
    several downstream entities. Operationally that is a genuine event whose
    affected set (excluding the root cause itself) has at least two members.
    """
    if not event.is_genuine:
        return False
    downstream = set(event.affected_entities) - {event.root_cause.entity}
    return len(downstream) >= 2


def maintenance_windows(
    labels: LabelFile, kinds: Iterable[str] = MAINTENANCE_KINDS
) -> tuple[GroundTruthEvent, ...]:
    """Benign events that are patching/maintenance windows (AC-11)."""
    wanted = frozenset(kinds)
    return tuple(event for event in labels.events if event.kind in wanted)


def alerts_in_maintenance_windows(
    alerts: Sequence[Prediction], windows: Sequence[GroundTruthEvent]
) -> tuple[Prediction, ...]:
    """Alerts attributable to a maintenance window (AC-11 numerator).

    "During a patching/maintenance window" is read as *both* inside the window's
    time span *and* about an entity the window touches. A coincidental alert on
    an unrelated entity elsewhere in the estate is not noise the window caused,
    and counting it would flatter the reduction ratio on both sides unevenly.
    """
    selected: dict[str, Prediction] = {}
    for window in windows:
        for alert in match_benign_event(window, alerts):
            selected[alert.prediction_id] = alert
    return sort_predictions(selected.values())


def entity_domain(kind: str) -> str | None:
    """Map a C1 entity kind to the root-cause domain it belongs to.

    Used by the static-threshold baseline, which has no topology and therefore
    attributes a breach to the domain of the entity that breached.
    """
    mapping: Mapping[str, str] = {
        "app_server": "application",
        "node": "application",
        "database": "database",
        "switch": "network",
    }
    return mapping.get(kind)


def window_bins(
    start: _dt.datetime, end: _dt.datetime, width_s: int
) -> tuple[tuple[_dt.datetime, _dt.datetime], ...]:
    """Tile ``[start, end)`` into half-open bins of ``width_s`` seconds.

    The final bin is truncated at ``end`` rather than extended past it, so the
    ``fpr_strict`` denominator never counts time that was not evaluated.
    """
    if width_s <= 0:
        raise ValueError("width_s must be positive")
    bins: list[tuple[_dt.datetime, _dt.datetime]] = []
    cursor = start
    step = _dt.timedelta(seconds=width_s)
    while cursor < end:
        bins.append((cursor, min(cursor + step, end)))
        cursor += step
    return tuple(bins)
