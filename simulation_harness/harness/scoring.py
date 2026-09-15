"""Scoring: turn one seed's ground truth and predictions into raw evidence.

This module is where every ratified definition is actually applied. It reads
contract C1 (ground truth) and contract C2 (predictions) and produces a
:class:`harness.evidence.SeedEvidence`. It computes **no ratios** — that happens
in :mod:`harness.metrics`, over the pooled evidence — so the pooled aggregate is
guaranteed to be a genuine micro aggregate (RD-06).

Definition map (what is implemented where in :func:`score_seed`):

===================================== ==========================================
Ratified definition                   Implementation
===================================== ==========================================
RD-01 ``fpr``                         ``benign_alerted / benign_total``, per
                                      event, benign events only
RD-01 amended ``fpr_strict``          per-entity evaluation windows with no
                                      genuine onset in the following 120 min
RD-01 ``unmatched_alert_count``       alerts matching no genuine incident,
                                      including pre-horizon firings
RD-02 lead time and matching          :mod:`harness.matching`
RD-03 recall                          strictly positive lead only;
                                      ``reactive_detections`` reported apart
RD-04 static baseline                 :mod:`harness.baseline`; its alerts are
                                      scored here with the identical rules
RD-05 DB attribution                  denominator = ALL injected DB cascades
RD-07 suppression A/B                 two prediction streams, one variable
===================================== ==========================================
"""

from __future__ import annotations

import bisect
import datetime as _dt
from collections.abc import Iterable, Sequence

from ano.contracts.labels import GroundTruthEvent, LabelFile
from ano.contracts.prediction import Prediction
from ano.contracts.results import MAX_PREDICTION_HORIZON_S

from harness.evidence import DomainCall, FaultCollapse, SeedEvidence
from harness.matching import (
    DB_CASCADE_KINDS,
    MAINTENANCE_KINDS,
    PATCHING_ONLY_KINDS,
    MatchKind,
    alerts_in_maintenance_windows,
    classify,
    delivered_alerts,
    event_entities,
    flags_semantic_anomaly,
    is_upstream_fault,
    match_benign_event,
    match_event,
    maintenance_windows,
    window_bins,
)

__all__ = [
    "STRICT_WINDOW_S",
    "R2_LEAD_BAND_S",
    "R3_LEAD_BAND_S",
    "score_seed",
    "score_recall_only",
]

#: Width of one evaluation window for ``fpr_strict`` (RD-01 as amended). The
#: amended definition speaks of "every evaluation window" without fixing a
#: width, so the harness declares one and prints it in ``parameters``. Ten
#: minutes is the granularity at which an operator would perceive a distinct
#: page; a much finer grid would inflate the denominator and flatter the
#: system, a much coarser one would hide repeat firing.
STRICT_WINDOW_S = 600

#: A-09: R2 asks for 15-30 minutes of warning. Reported, not gated.
R2_LEAD_BAND_S = (900, 1800)

#: A-10: R3 asks for 30-60 minutes of warning. Reported, not gated.
R3_LEAD_BAND_S = (1800, 3600)


def _recall_counts(
    events: Sequence[GroundTruthEvent],
    alerts: Sequence[Prediction],
    *,
    horizon_s: int,
) -> tuple[int, int]:
    """``(recalled, total)`` over ``events`` under RD-03's positive-lead rule."""
    recalled = 0
    for event in events:
        if match_event(event, alerts, horizon_s=horizon_s).recalled:
            recalled += 1
    return recalled, len(events)


def _entity_onsets(
    labels: LabelFile, genuine: Sequence[GroundTruthEvent]
) -> dict[str, list[_dt.datetime]]:
    """Sorted genuine onsets per entity, for the ``fpr_strict`` window sweep."""
    onsets: dict[str, list[_dt.datetime]] = {
        entity.entity_id: [] for entity in labels.entities
    }
    for event in genuine:
        for name in event_entities(event):
            onsets.setdefault(name, []).append(event.onset)
    for values in onsets.values():
        values.sort()
    return onsets


def _entity_active_spans(
    labels: LabelFile, genuine: Sequence[GroundTruthEvent]
) -> dict[str, list[tuple[_dt.datetime, _dt.datetime]]]:
    """``[onset, end]`` spans per entity, for the excl-active variant."""
    spans: dict[str, list[tuple[_dt.datetime, _dt.datetime]]] = {
        entity.entity_id: [] for entity in labels.entities
    }
    for event in genuine:
        for name in event_entities(event):
            spans.setdefault(name, []).append((event.onset, event.end))
    for values in spans.values():
        values.sort()
    return spans


def _has_onset_in(
    onsets: Sequence[_dt.datetime], after: _dt.datetime, until: _dt.datetime
) -> bool:
    """True if any onset lies in the half-open-left interval ``(after, until]``."""
    index = bisect.bisect_right(onsets, after)
    return index < len(onsets) and onsets[index] <= until


def _alerts_by_entity(
    alerts: Sequence[Prediction],
) -> dict[str, list[_dt.datetime]]:
    """Sorted emission times per alerting entity."""
    buckets: dict[str, list[_dt.datetime]] = {}
    for alert in alerts:
        buckets.setdefault(alert.entity, []).append(alert.emitted_at)
    for values in buckets.values():
        values.sort()
    return buckets


def _has_alert_in(
    emissions: Sequence[_dt.datetime], start: _dt.datetime, end: _dt.datetime
) -> bool:
    """True if any emission lies in the half-open window ``[start, end)``."""
    index = bisect.bisect_left(emissions, start)
    return index < len(emissions) and emissions[index] < end


def _strict_window_counts(
    labels: LabelFile,
    genuine: Sequence[GroundTruthEvent],
    alerts: Sequence[Prediction],
    *,
    horizon_s: int,
    window_s: int,
) -> tuple[int, int, int, int]:
    """Count illegitimate evaluation windows and those that contained an alert.

    Implements RD-01 as amended: a window is one in which *no prediction is
    legitimate*, i.e. no genuine incident onset for that entity falls in the
    following ``horizon_s`` seconds. Windows are per entity, because an alert is
    always about one entity.

    Returns:
        ``(total, with_alert, total_excluding_active, with_alert_excluding_active)``
        where the "excluding active" pair additionally drops windows that
        overlap an in-progress genuine incident. That pair is a non-ratified
        transparency figure; the ratified ``fpr_strict`` uses the first pair,
        which is the stricter reading.
    """
    onsets = _entity_onsets(labels, genuine)
    spans = _entity_active_spans(labels, genuine)
    emissions = _alerts_by_entity(alerts)
    bins = window_bins(labels.window.start, labels.window.end, window_s)
    horizon = _dt.timedelta(seconds=horizon_s)

    total = 0
    with_alert = 0
    total_excl = 0
    with_alert_excl = 0
    for entity in labels.entities:
        name = entity.entity_id
        entity_onsets = onsets.get(name, [])
        entity_spans = spans.get(name, [])
        entity_emissions = emissions.get(name, [])
        for start, end in bins:
            if _has_onset_in(entity_onsets, start, end + horizon):
                continue  # a prediction here could legitimately anticipate it
            total += 1
            alerted = _has_alert_in(entity_emissions, start, end)
            if alerted:
                with_alert += 1
            active = any(
                span_start <= start < span_end or span_start < end <= span_end
                for span_start, span_end in entity_spans
            )
            if not active:
                total_excl += 1
                if alerted:
                    with_alert_excl += 1
    return total, with_alert, total_excl, with_alert_excl


def _in_band(value: float, band: tuple[int, int]) -> bool:
    """True if ``value`` lies inside the inclusive ``band``."""
    return band[0] <= value <= band[1]


def score_recall_only(
    labels: LabelFile,
    predictions: Sequence[Prediction],
    *,
    horizon_s: int = MAX_PREDICTION_HORIZON_S,
) -> tuple[int, int]:
    """``(recalled, total)`` for a prediction stream — used for the A/B runs.

    AC-12 compares recall with suppression on against recall with suppression
    off. Both sides must use the identical recall definition, so both call this.
    """
    alerts = delivered_alerts(predictions)
    return _recall_counts(labels.genuine_events(), alerts, horizon_s=horizon_s)


def score_seed(
    labels: LabelFile,
    predictions: Sequence[Prediction],
    *,
    baseline_predictions: Sequence[Prediction] = (),
    predictions_suppression_off: Sequence[Prediction] | None = None,
    baseline_rules_total: int = 0,
    baseline_rules_with_coverage: int = 0,
    novel_absence_proofs: Iterable[str] = (),
    scenario_count: int = 1,
    horizon_s: int = MAX_PREDICTION_HORIZON_S,
    strict_window_s: int = STRICT_WINDOW_S,
) -> SeedEvidence:
    """Score one seed and return its raw evidence.

    Args:
        labels: Contract-C1 ground truth for this seed. **Only the harness may
            read this** (requirement R5).
        predictions: Contract-C2 predictions from the system under test, with
            suppression enabled. Suppressed records must be present and marked,
            not deleted.
        baseline_predictions: Contract-C2 predictions from the static-threshold
            baseline over the identical replay (RD-04).
        predictions_suppression_off: The same system re-run with suppression
            disabled, for AC-12. ``None`` leaves the A/B numbers undefined,
            which fails AC-12 rather than silently passing it.
        baseline_rules_total: Size of the baseline's fixed rule table.
        baseline_rules_with_coverage: How many of those rules saw at least one
            matching telemetry sample. Zero means the baseline never saw the
            data, and no alert-volume reduction measured against it is
            meaningful.
        novel_absence_proofs: Identifiers of injected novel-signature failures
            for which the generator supplied a mechanical proof that the
            signature is absent from the training window (AC-15 sub-obligation).
        scenario_count: Number of scenarios replayed for this seed.
        horizon_s: RD-02 maximum prediction horizon.
        strict_window_s: Width of one ``fpr_strict`` evaluation window.

    Returns:
        The seed's :class:`SeedEvidence`.
    """
    alerts = delivered_alerts(predictions)
    baseline_alerts = delivered_alerts(baseline_predictions)
    genuine = labels.genuine_events()
    benign = labels.benign_events()
    normal_events = tuple(
        event for event in labels.events if event.kind == "normal"
    )

    matches = {
        event.event_id: match_event(event, alerts, horizon_s=horizon_s)
        for event in genuine
    }

    # -- recall, reactive detections, lead time (RD-02, RD-03) -------------
    genuine_recalled = 0
    reactive_detections = 0
    lead_times: list[float] = []
    lead_times_unresponsiveness: list[float] = []
    lead_times_cross_domain: list[float] = []
    recalled_unresponsiveness = 0
    recalled_cross_domain = 0
    total_unresponsiveness = 0
    total_cross_domain = 0
    leads_in_r2_band = 0
    leads_in_r3_band = 0

    for event in genuine:
        match = matches[event.event_id]
        if event.track == "unresponsiveness":
            total_unresponsiveness += 1
        elif event.track == "cross_domain":
            total_cross_domain += 1
        if match.recalled:
            genuine_recalled += 1
            lead = match.lead_time_s
            assert lead is not None  # recalled implies a predictive match
            lead_times.append(lead)
            if event.track == "unresponsiveness":
                recalled_unresponsiveness += 1
                lead_times_unresponsiveness.append(lead)
                if _in_band(lead, R2_LEAD_BAND_S):
                    leads_in_r2_band += 1
            elif event.track == "cross_domain":
                recalled_cross_domain += 1
                lead_times_cross_domain.append(lead)
                if _in_band(lead, R3_LEAD_BAND_S):
                    leads_in_r3_band += 1
        elif match.reactive_only:
            reactive_detections += 1

    # -- false positives, per benign event (RD-01) --------------------------
    benign_alerted = 0
    benign_alerted_unresponsiveness = 0
    benign_alerted_cross_domain = 0
    for event in benign:
        raised = match_benign_event(event, alerts)
        if raised:
            benign_alerted += 1
        if any(item.track == "unresponsiveness" for item in raised):
            benign_alerted_unresponsiveness += 1
        if any(item.track == "cross_domain" for item in raised):
            benign_alerted_cross_domain += 1

    normal_operation_alert_count = sum(
        len(match_benign_event(event, alerts)) for event in normal_events
    )

    strict_total, strict_alerted, strict_total_excl, strict_alerted_excl = (
        _strict_window_counts(
            labels,
            genuine,
            alerts,
            horizon_s=horizon_s,
            window_s=strict_window_s,
        )
    )

    # -- unmatched alerts (RD-01; pre-horizon firings are unmatched, RD-02) --
    unmatched = 0
    for alert in alerts:
        if not any(
            classify(alert, event, horizon_s=horizon_s)
            in (MatchKind.PREDICTIVE, MatchKind.REACTIVE)
            for event in genuine
        ):
            unmatched += 1

    # -- attribution (AC-09, AC-10, RD-05, A-08) ----------------------------
    alerted_incidents = 0
    domain_correct = 0
    entity_correct = 0
    domain_calls: list[DomainCall] = []
    db_cascade_total = 0
    db_cascade_attributed_database = 0
    for event in genuine:
        match = matches[event.event_id]
        is_db_cascade = event.kind in DB_CASCADE_KINDS
        if is_db_cascade:
            db_cascade_total += 1
        first = match.first_alert
        if first is None:
            continue  # RD-05: an unalerted DB cascade counts as not attributed
        alerted_incidents += 1
        predicted_domain = first.root_cause.domain
        predicted_entity = first.root_cause.entity
        if predicted_domain == event.root_cause.domain:
            domain_correct += 1
        if predicted_entity == event.root_cause.entity:
            entity_correct += 1
        domain_calls.append(
            DomainCall(
                event_id=event.event_id,
                truth_domain=event.root_cause.domain,
                predicted_domain=predicted_domain,
                truth_entity=event.root_cause.entity,
                predicted_entity=predicted_entity,
            )
        )
        if is_db_cascade and predicted_domain == "database":
            db_cascade_attributed_database += 1

    # -- alert volume and the static baseline (AC-11, RD-04) ----------------
    windows = maintenance_windows(labels, MAINTENANCE_KINDS)
    windows_patching_only = maintenance_windows(labels, PATCHING_ONLY_KINDS)
    system_patching = len(alerts_in_maintenance_windows(alerts, windows))
    baseline_patching = len(alerts_in_maintenance_windows(baseline_alerts, windows))
    system_patching_only = len(
        alerts_in_maintenance_windows(alerts, windows_patching_only)
    )
    baseline_patching_only = len(
        alerts_in_maintenance_windows(baseline_alerts, windows_patching_only)
    )
    baseline_recalled, baseline_total = _recall_counts(
        genuine, baseline_alerts, horizon_s=horizon_s
    )

    # -- collapse (AC-13, A-03) --------------------------------------------
    collapses: list[FaultCollapse] = []
    for event in genuine:
        if not is_upstream_fault(event):
            continue
        match = matches[event.event_id]
        matched = match.predictive + match.reactive
        pre_collapse_ids: set[str] = set()
        incident_ids: set[str] = set()
        for alert in matched:
            pre_collapse_ids.add(alert.prediction_id)
            pre_collapse_ids.update(alert.collapsed_from)
            incident_ids.add(alert.incident_id)
        collapses.append(
            FaultCollapse(
                event_id=event.event_id,
                downstream_entities=len(
                    set(event.affected_entities) - {event.root_cause.entity}
                ),
                pre_collapse_alerts=len(pre_collapse_ids),
                incidents=len(incident_ids),
            )
        )

    # -- semantic novelty (AC-15) -------------------------------------------
    novel_total = 0
    novel_flagged = 0
    for event in genuine:
        if not event.novel_log_signature:
            continue
        novel_total += 1
        match = matches[event.event_id]
        if any(
            flags_semantic_anomaly(alert)
            for alert in match.predictive + match.reactive
        ):
            novel_flagged += 1

    # -- suppression A/B (AC-12, RD-07) -------------------------------------
    recall_on_numerator = genuine_recalled
    recall_on_denominator = len(genuine)
    if predictions_suppression_off is None:
        recall_off_numerator = 0
        recall_off_denominator = 0
    else:
        recall_off_numerator, recall_off_denominator = score_recall_only(
            labels, predictions_suppression_off, horizon_s=horizon_s
        )

    return SeedEvidence(
        genuine_total=len(genuine),
        genuine_recalled=genuine_recalled,
        genuine_total_unresponsiveness=total_unresponsiveness,
        genuine_recalled_unresponsiveness=recalled_unresponsiveness,
        genuine_total_cross_domain=total_cross_domain,
        genuine_recalled_cross_domain=recalled_cross_domain,
        reactive_detections=reactive_detections,
        recall_on_numerator=recall_on_numerator,
        recall_on_denominator=recall_on_denominator,
        recall_off_numerator=recall_off_numerator,
        recall_off_denominator=recall_off_denominator,
        benign_total=len(benign),
        benign_alerted=benign_alerted,
        benign_alerted_unresponsiveness=benign_alerted_unresponsiveness,
        benign_alerted_cross_domain=benign_alerted_cross_domain,
        strict_windows_total=strict_total,
        strict_windows_with_alert=strict_alerted,
        strict_windows_total_excl_active=strict_total_excl,
        strict_windows_with_alert_excl_active=strict_alerted_excl,
        unmatched_alert_count=unmatched,
        normal_operation_alert_count=normal_operation_alert_count,
        lead_times_s=tuple(lead_times),
        lead_times_unresponsiveness_s=tuple(lead_times_unresponsiveness),
        lead_times_cross_domain_s=tuple(lead_times_cross_domain),
        leads_in_r2_band=leads_in_r2_band,
        leads_in_r3_band=leads_in_r3_band,
        alerted_incidents=alerted_incidents,
        domain_correct=domain_correct,
        entity_correct=entity_correct,
        domain_correct_over_all_genuine=domain_correct,
        db_cascade_total=db_cascade_total,
        db_cascade_attributed_database=db_cascade_attributed_database,
        domain_calls=tuple(domain_calls),
        system_alert_volume_total=len(alerts),
        baseline_alert_volume_total=len(baseline_alerts),
        system_alert_volume_patching=system_patching,
        baseline_alert_volume_patching=baseline_patching,
        system_alert_volume_patching_only=system_patching_only,
        baseline_alert_volume_patching_only=baseline_patching_only,
        baseline_recall_numerator=baseline_recalled,
        baseline_recall_denominator=baseline_total,
        baseline_rules_with_coverage=baseline_rules_with_coverage,
        baseline_rules_total=baseline_rules_total,
        collapses=tuple(collapses),
        novel_signature_total=novel_total,
        novel_signature_flagged=novel_flagged,
        novel_absence_proofs=tuple(sorted(novel_absence_proofs)),
        scenario_count=scenario_count,
        prediction_count=len(predictions),
    )
