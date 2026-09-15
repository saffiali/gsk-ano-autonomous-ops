"""Raw scoring evidence and its pooling rule.

The harness never averages metrics across seeds. It collects **raw counts and
raw per-incident samples** per seed, pools them by summation/concatenation, and
computes every metric from the pooled evidence. That is exactly what RD-06's
"pooled (micro) aggregate — sum numerators and denominators across all seeds"
requires, and it makes the pooled block structurally incapable of disagreeing
with the per-seed blocks it was built from.

Every field is either

* an ``int`` count (pooled by addition), or
* a ``tuple`` sample (pooled by concatenation, then sorted where order is not
  already meaningful).

:func:`pool` derives the merge from the dataclass fields, so adding a metric
cannot leave a stale hand-written aggregation behind.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterable, Sequence
from dataclasses import dataclass

__all__ = [
    "FaultCollapse",
    "DomainCall",
    "SeedEvidence",
    "pool",
]


@dataclass(frozen=True, slots=True)
class FaultCollapse:
    """Collapse behaviour observed for one injected upstream fault (AC-13).

    Attributes:
        event_id: The ground-truth event id.
        downstream_entities: How many distinct downstream entities the fault
            affected (>= 2 for a fault to qualify).
        pre_collapse_alerts: Operator-deliverable notifications that would have
            been raised for the fault had nothing been collapsed — the matched
            delivered alerts plus every prediction they absorbed via
            ``collapsed_from`` (A-03: post-suppression, pre-collapse).
        incidents: Distinct ``incident_id`` values actually emitted for it.
    """

    event_id: str
    downstream_entities: int
    pre_collapse_alerts: int
    incidents: int

    @property
    def ratio(self) -> float | None:
        """``pre_collapse_alerts / incidents``, or ``None`` if nothing fired."""
        if self.incidents == 0:
            return None
        return self.pre_collapse_alerts / self.incidents


@dataclass(frozen=True, slots=True)
class DomainCall:
    """One row of the root-cause confusion matrix (AC-09 evidence).

    Attributes:
        event_id: The alerted incident.
        truth_domain: Ground-truth root-cause domain.
        predicted_domain: The domain the system attributed.
        truth_entity: Ground-truth root-cause entity.
        predicted_entity: The entity the system attributed.
    """

    event_id: str
    truth_domain: str
    predicted_domain: str
    truth_entity: str
    predicted_entity: str


@dataclass(frozen=True, slots=True)
class SeedEvidence:
    """Everything measured for one seed, in raw numerator/denominator form.

    Field naming is ``<numerator>`` / ``<denominator>`` pairs so the metric
    layer never has to guess what divides what.
    """

    # -- recall (AC-05, AC-08, RD-03) -------------------------------------
    genuine_total: int = 0
    genuine_recalled: int = 0
    genuine_total_unresponsiveness: int = 0
    genuine_recalled_unresponsiveness: int = 0
    genuine_total_cross_domain: int = 0
    genuine_recalled_cross_domain: int = 0
    #: Genuine incidents whose earliest match had non-positive lead (RD-03).
    reactive_detections: int = 0

    # -- suppression A/B (AC-12, RD-07) ------------------------------------
    recall_on_numerator: int = 0
    recall_on_denominator: int = 0
    recall_off_numerator: int = 0
    recall_off_denominator: int = 0

    # -- false positives (AC-06, RD-01) ------------------------------------
    benign_total: int = 0
    benign_alerted: int = 0
    benign_alerted_unresponsiveness: int = 0
    benign_alerted_cross_domain: int = 0
    #: RD-01 as amended: every evaluation window in which no prediction is
    #: legitimate, i.e. no genuine onset for that entity in the next 120 min.
    strict_windows_total: int = 0
    strict_windows_with_alert: int = 0
    #: Same, but additionally excluding windows inside an active genuine
    #: incident. Reported as a non-ratified transparency figure.
    strict_windows_total_excl_active: int = 0
    strict_windows_with_alert_excl_active: int = 0
    #: Alerts matched to no ground-truth incident at all (RD-01).
    unmatched_alert_count: int = 0
    #: Alerts raised during pure-normal-operation events (A-02).
    normal_operation_alert_count: int = 0

    # -- lead time (AC-07, RD-02) ------------------------------------------
    lead_times_s: tuple[float, ...] = ()
    lead_times_unresponsiveness_s: tuple[float, ...] = ()
    lead_times_cross_domain_s: tuple[float, ...] = ()
    #: A-09 / A-10: share of leads inside the narrative requirement bands.
    leads_in_r2_band: int = 0
    leads_in_r3_band: int = 0

    # -- attribution (AC-09, AC-10, A-08) ----------------------------------
    alerted_incidents: int = 0
    domain_correct: int = 0
    entity_correct: int = 0
    #: A-08: the same numerator over ALL genuine incidents (missed = wrong).
    domain_correct_over_all_genuine: int = 0
    db_cascade_total: int = 0
    db_cascade_attributed_database: int = 0
    domain_calls: tuple[DomainCall, ...] = ()

    # -- alert volume and the static baseline (AC-11, RD-04) ---------------
    system_alert_volume_total: int = 0
    baseline_alert_volume_total: int = 0
    system_alert_volume_patching: int = 0
    baseline_alert_volume_patching: int = 0
    system_alert_volume_patching_only: int = 0
    baseline_alert_volume_patching_only: int = 0
    baseline_recall_numerator: int = 0
    baseline_recall_denominator: int = 0
    #: How many baseline rules saw at least one matching telemetry sample.
    #: Zero means the baseline was never wired to the data and any alert-volume
    #: reduction measured against it is meaningless (RD-04 anti-strawman).
    baseline_rules_with_coverage: int = 0
    baseline_rules_total: int = 0

    # -- collapse (AC-13) ---------------------------------------------------
    collapses: tuple[FaultCollapse, ...] = ()

    # -- semantic novelty (AC-15) ------------------------------------------
    novel_signature_total: int = 0
    novel_signature_flagged: int = 0
    #: True only if the adapter supplied a mechanical proof that the novel
    #: signature is absent from the training window (AC-15 sub-obligation b).
    novel_absence_proofs: tuple[str, ...] = ()

    # -- bookkeeping --------------------------------------------------------
    scenario_count: int = 0
    prediction_count: int = 0

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        """Sanity-check internal consistency.

        These are impossible-by-construction conditions; if one trips, the
        scoring code has a bug and the run must stop rather than publish a
        number that cannot be true.
        """
        pairs = (
            ("genuine_recalled", "genuine_total"),
            (
                "genuine_recalled_unresponsiveness",
                "genuine_total_unresponsiveness",
            ),
            ("genuine_recalled_cross_domain", "genuine_total_cross_domain"),
            ("benign_alerted", "benign_total"),
            ("benign_alerted_unresponsiveness", "benign_total"),
            ("benign_alerted_cross_domain", "benign_total"),
            ("strict_windows_with_alert", "strict_windows_total"),
            (
                "strict_windows_with_alert_excl_active",
                "strict_windows_total_excl_active",
            ),
            ("domain_correct", "alerted_incidents"),
            ("entity_correct", "alerted_incidents"),
            ("domain_correct_over_all_genuine", "genuine_total"),
            ("db_cascade_attributed_database", "db_cascade_total"),
            ("novel_signature_flagged", "novel_signature_total"),
            ("recall_on_numerator", "recall_on_denominator"),
            ("recall_off_numerator", "recall_off_denominator"),
            ("baseline_recall_numerator", "baseline_recall_denominator"),
            ("baseline_rules_with_coverage", "baseline_rules_total"),
            ("system_alert_volume_patching", "system_alert_volume_total"),
            ("baseline_alert_volume_patching", "baseline_alert_volume_total"),
        )
        for numerator_name, denominator_name in pairs:
            numerator = getattr(self, numerator_name)
            denominator = getattr(self, denominator_name)
            if numerator > denominator:
                raise ValueError(
                    f"evidence is inconsistent: {numerator_name}={numerator} "
                    f"exceeds {denominator_name}={denominator}"
                )
        for name in dataclasses.fields(self):
            value = getattr(self, name.name)
            if isinstance(value, int) and not isinstance(value, bool) and value < 0:
                raise ValueError(f"{name.name} must be non-negative, got {value}")
        if len(self.lead_times_s) != self.genuine_recalled:
            raise ValueError(
                "one lead time per recalled incident is required: "
                f"{len(self.lead_times_s)} lead times vs "
                f"{self.genuine_recalled} recalled incidents"
            )


def pool(evidences: Sequence[SeedEvidence]) -> SeedEvidence:
    """Merge per-seed evidence into the pooled (micro) aggregate (RD-06).

    ``int`` fields are summed; ``tuple`` fields are concatenated in seed order.
    Nothing is averaged here — averaging happens once, in the metric layer, over
    the pooled sample.

    Args:
        evidences: Per-seed evidence, in the declared seed order.

    Returns:
        A single :class:`SeedEvidence` representing every seed at once.
    """
    if not evidences:
        return SeedEvidence()
    merged: dict[str, object] = {}
    for spec in dataclasses.fields(SeedEvidence):
        values = [getattr(item, spec.name) for item in evidences]
        first = values[0]
        if isinstance(first, tuple):
            combined: list[object] = []
            for value in values:
                combined.extend(value)
            merged[spec.name] = tuple(combined)
        elif isinstance(first, int) and not isinstance(first, bool):
            merged[spec.name] = sum(values)
        else:  # pragma: no cover - guard against a future non-poolable field
            raise TypeError(
                f"field {spec.name!r} of type {type(first).__name__} has no "
                "defined pooling rule; add one rather than dropping the field"
            )
    return SeedEvidence(**merged)  # type: ignore[arg-type]


def _ratio(numerator: int, denominator: int) -> float | None:
    """``numerator / denominator``, or ``None`` when the denominator is zero."""
    if denominator == 0:
        return None
    return numerator / denominator


def evidence_field_names() -> tuple[str, ...]:
    """Names of every evidence field, for reporting and tests."""
    return tuple(spec.name for spec in dataclasses.fields(SeedEvidence))


def qualifying_collapses(evidence: SeedEvidence) -> tuple[FaultCollapse, ...]:
    """Upstream faults the system actually alerted on (AC-13 denominator).

    A fault that produced no alert exhibits no collapse behaviour: its ratio is
    ``0/0``. It is excluded from the *ratio* — and AC-13's verdict separately
    requires at least one qualifying fault to exist, so exclusion cannot be used
    to pass the criterion by detecting nothing.
    """
    return tuple(item for item in evidence.collapses if item.incidents > 0)


def iter_int_fields(evidence: SeedEvidence) -> Iterable[tuple[str, int]]:
    """Yield ``(name, value)`` for every integer evidence field."""
    for spec in dataclasses.fields(SeedEvidence):
        value = getattr(evidence, spec.name)
        if isinstance(value, int) and not isinstance(value, bool):
            yield spec.name, value
