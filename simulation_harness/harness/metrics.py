"""Metric construction: pooled evidence in, contract-C5 :class:`MetricSet` out.

Every ratified metric is built with :func:`ano.contracts.results.metric`, which
injects the ratified definition text verbatim (rule R-PRINT) and refuses a
home-made one. Non-ratified metrics — the transparency figures required by
AMBIGUITIES A-02, A-08, A-09, A-10 and by FEATURE_INVENTORY row 109
(root-cause **entity** accuracy) — carry their own definition strings, because
R-PRINT applies to every reported number, not only the ratified 29.

Undefined is not zero. When a denominator is empty the metric's ``value`` is
``None`` and a ``note`` says why; contract C5 enforces the note. This matters
because "no DB cascade was injected in this seed" and "every DB cascade was
misattributed" are both ``0/0``-adjacent situations that a naive ``0.0`` would
render indistinguishable.
"""

from __future__ import annotations

from ano.contracts.results import MetricSet, MetricValue, metric

from harness import stats
from harness.evidence import SeedEvidence
from harness.scoring import R2_LEAD_BAND_S, R3_LEAD_BAND_S

__all__ = ["EXTRA_METRIC_DEFINITIONS", "build_metric_set", "confusion_matrix"]

#: Definitions for the harness's own (non-ratified) metrics. Rule R-PRINT
#: requires a definition for every reported number.
EXTRA_METRIC_DEFINITIONS: dict[str, str] = {
    "entity_accuracy": (
        "root-cause ENTITY accuracy = (alerted incidents whose attributed "
        "root_cause.entity equals the ground-truth root_cause.entity) / "
        "(incidents the system alerted on). Same denominator as "
        "domain_accuracy. Required by FEATURE_INVENTORY row 109; not itself an "
        "acceptance gate."
    ),
    "domain_accuracy_over_all_genuine": (
        "= (alerted incidents whose attributed root-cause domain equals the "
        "ground-truth domain) / (ALL genuine injected incidents) — domain "
        "accuracy with the denominator widened so a missed incident counts as "
        "incorrect. AMBIGUITIES A-08: reported because AC-09's alerted-only "
        "denominator is gameable by alerting on fewer, easier incidents. "
        "Reported, not gated."
    ),
    "fpr_strict_excluding_active_incidents": (
        "count of 600-second (entity, window) bins holding at least one "
        "delivered alert that is not legitimised by a genuine onset for that "
        "entity within the 7200-second prediction horizon / count of all such "
        "bins, with every bin that overlaps an in-progress genuine incident "
        "removed from both numerator and denominator. The ratified fpr_strict "
        "is the stricter figure and is the one reported against RD-01; this "
        "variant is published so the reader can see how much of fpr_strict is "
        "post-onset (reactive) firing."
    ),
    "normal_operation_alert_count": (
        "raw count of delivered alerts whose entity and timestamp fall inside "
        "a ground-truth event of kind 'normal' (pure normal operation). "
        "AMBIGUITIES A-02: reported so alerts during periods with nothing "
        "injected can never go unreported."
    ),
    "lead_time_share_r2_band": (
        "share of recalled unresponsiveness incidents whose lead time falls "
        f"inside the R2 narrative band [{R2_LEAD_BAND_S[0]}, "
        f"{R2_LEAD_BAND_S[1]}] seconds (15-30 minutes). AMBIGUITIES A-09: "
        "reported so R2's band is evidenced; the gate remains AC-07's >= 15 "
        "minute median."
    ),
    "lead_time_share_r3_band": (
        "share of recalled cross-domain incidents whose lead time falls inside "
        f"the R3 narrative band [{R3_LEAD_BAND_S[0]}, {R3_LEAD_BAND_S[1]}] "
        "seconds (30-60 minutes). AMBIGUITIES A-10: reported so R3's band is "
        "evidenced; the gate remains AC-07's >= 15 minute median."
    ),
    "alert_volume_system_patching_only": (
        "count of operator-deliverable system alerts whose entity and "
        "timestamp fall inside an OS patching window only (ground-truth kind "
        "'patching_window'), excluding planned deploys. Published so the "
        "choice of maintenance-window family behind AC-11 is visible and "
        "cannot be mistaken for a convenience."
    ),
    "alert_volume_baseline_patching_only": (
        "count of static-threshold baseline alerts whose entity and timestamp "
        "fall inside an OS patching window only (ground-truth kind "
        "'patching_window'), excluding planned deploys."
    ),
    "patching_alert_reduction_patching_only": (
        "= 1 - (alert_volume_system_patching_only / "
        "alert_volume_baseline_patching_only): the AC-11 reduction measured "
        "over OS patching windows alone."
    ),
    "collapse_pre_collapse_alerts_total": (
        "total operator-deliverable notifications that would have been raised "
        "across all injected upstream faults with >= 2 affected downstream "
        "entities, counted post-suppression and pre-collapse (AMBIGUITIES "
        "A-03). The raw numerator behind collapse_ratio."
    ),
    "collapse_incidents_total": (
        "total distinct incident_id values emitted across all injected "
        "upstream faults with >= 2 affected downstream entities. The raw "
        "denominator behind collapse_ratio."
    ),
    "upstream_faults_qualifying": (
        "count of injected genuine incidents with >= 2 affected downstream "
        "entities (excluding the root-cause entity itself) — the population "
        "AC-13 is measured over."
    ),
    "upstream_faults_alerted": (
        "count of qualifying upstream faults for which the system emitted at "
        "least one incident. AC-13 cannot be demonstrated at all if this is "
        "zero, so the verdict fails rather than passing vacuously."
    ),
    "upstream_faults_with_multiple_incidents": (
        "count of qualifying upstream faults for which the system emitted more "
        "than one distinct incident_id. AC-13 requires one incident per "
        "upstream fault, so this must be zero."
    ),
    "incidents_per_alerted_upstream_fault": (
        "= (total distinct incident_id values emitted for qualifying upstream "
        "faults that raised at least one alert) / (count of those alerted "
        "qualifying faults). Companion to incidents_per_upstream_fault, whose "
        "denominator is every injected qualifying fault including those the "
        "system missed."
    ),
    "baseline_rule_signal_coverage": (
        "= (static-threshold baseline rules that saw >= 1 matching telemetry "
        "sample) / (rules in the fixed baseline rule table). A value below 1.0 "
        "means part of the published threshold table never saw data; a value "
        "of 0 means the baseline was never wired to the telemetry and no "
        "alert-volume reduction measured against it is meaningful (RD-04)."
    ),
    "novel_signature_absence_proofs": (
        "count of injected novel-signature failures for which the generator "
        "supplied a mechanical proof that the signature does not occur in the "
        "training window. AC-15 requires that proof in addition to the "
        "detection; prose is not sufficient."
    ),
    "reactive_detection_rate": (
        "= reactive_detections / (all genuine injected incidents): the share "
        "of incidents the system saw only at or after onset. RD-03 keeps these "
        "out of recall; this makes the size of the reactive tail visible."
    ),
}

_EMPTY_DENOMINATOR_NOTE = (
    "undefined for this block: the denominator is empty ({detail}). An empty "
    "denominator is not a measured zero."
)


def _ratio_metric(
    name: str,
    numerator: int,
    denominator: int,
    *,
    detail: str,
    unit: str = "ratio",
    extra_note: str | None = None,
) -> MetricValue:
    """A ratio metric, or an explicitly-undefined one when the denominator is 0."""
    definition = EXTRA_METRIC_DEFINITIONS.get(name)
    if denominator == 0:
        return metric(
            name,
            None,
            numerator=numerator,
            denominator=denominator,
            unit=unit,
            note=_EMPTY_DENOMINATOR_NOTE.format(detail=detail),
            definition=definition,
        )
    return metric(
        name,
        numerator / denominator,
        numerator=numerator,
        denominator=denominator,
        unit=unit,
        note=extra_note,
        definition=definition,
    )


def _count_metric(name: str, value: int, *, unit: str = "count") -> MetricValue:
    """A raw count metric."""
    return metric(
        name,
        float(value),
        unit=unit,
        definition=EXTRA_METRIC_DEFINITIONS.get(name),
    )


def _sample_metric(
    name: str,
    sample: tuple[float, ...],
    statistic: str,
    *,
    detail: str,
) -> MetricValue:
    """A percentile metric over a per-incident sample, or undefined if empty."""
    value = stats.median(sample) if statistic == "median" else stats.p10(sample)
    definition = EXTRA_METRIC_DEFINITIONS.get(name)
    if value is None:
        return metric(
            name,
            None,
            denominator=0,
            unit="seconds",
            note=_EMPTY_DENOMINATOR_NOTE.format(detail=detail),
            definition=definition,
        )
    return metric(
        name,
        value,
        denominator=len(sample),
        unit="seconds",
        definition=definition,
    )


def confusion_matrix(evidence: SeedEvidence) -> dict[str, dict[str, int]]:
    """3x3 root-cause domain confusion matrix over alerted incidents (AC-09).

    Returns:
        ``matrix[truth_domain][predicted_domain] -> count``, with all three
        domains present in both axes so an all-zero row is visible rather than
        missing.
    """
    domains = ("application", "database", "network")
    matrix: dict[str, dict[str, int]] = {
        truth: {predicted: 0 for predicted in domains} for truth in domains
    }
    for call in evidence.domain_calls:
        row = matrix.setdefault(
            call.truth_domain, {predicted: 0 for predicted in domains}
        )
        row[call.predicted_domain] = row.get(call.predicted_domain, 0) + 1
    return matrix


def build_metric_set(evidence: SeedEvidence) -> MetricSet:
    """Compute every reported metric from pooled or per-seed evidence.

    The same function builds the pooled block and each per-seed block, so a
    per-seed number and the pooled number can never be computed by different
    code paths.
    """
    metrics: list[MetricValue] = []
    add = metrics.append

    # -- recall (AC-05, AC-08, RD-03) --------------------------------------
    add(
        _ratio_metric(
            "recall",
            evidence.genuine_recalled,
            evidence.genuine_total,
            detail="no genuine incidents were injected",
        )
    )
    add(
        _ratio_metric(
            "recall_unresponsiveness",
            evidence.genuine_recalled_unresponsiveness,
            evidence.genuine_total_unresponsiveness,
            detail="no genuine incidents were injected on the "
            "unresponsiveness track",
        )
    )
    add(
        _ratio_metric(
            "recall_cross_domain",
            evidence.genuine_recalled_cross_domain,
            evidence.genuine_total_cross_domain,
            detail="no genuine incidents were injected on the cross_domain "
            "track",
        )
    )
    add(_count_metric("reactive_detections", evidence.reactive_detections))
    add(
        _ratio_metric(
            "reactive_detection_rate",
            evidence.reactive_detections,
            evidence.genuine_total,
            detail="no genuine incidents were injected",
        )
    )

    # -- false positives (AC-06, RD-01) -------------------------------------
    add(
        _ratio_metric(
            "fpr",
            evidence.benign_alerted,
            evidence.benign_total,
            detail="no benign events were injected",
        )
    )
    add(
        _ratio_metric(
            "fpr_unresponsiveness",
            evidence.benign_alerted_unresponsiveness,
            evidence.benign_total,
            detail="no benign events were injected",
        )
    )
    add(
        _ratio_metric(
            "fpr_cross_domain",
            evidence.benign_alerted_cross_domain,
            evidence.benign_total,
            detail="no benign events were injected",
        )
    )
    add(
        _ratio_metric(
            "fpr_strict",
            evidence.strict_windows_with_alert,
            evidence.strict_windows_total,
            detail="every evaluation window had a genuine onset within the "
            "prediction horizon",
        )
    )
    add(
        _ratio_metric(
            "fpr_strict_excluding_active_incidents",
            evidence.strict_windows_with_alert_excl_active,
            evidence.strict_windows_total_excl_active,
            detail="no quiet evaluation windows remained after removing "
            "in-progress incidents",
        )
    )
    add(_count_metric("unmatched_alert_count", evidence.unmatched_alert_count))
    add(
        _count_metric(
            "normal_operation_alert_count", evidence.normal_operation_alert_count
        )
    )

    # -- lead time (AC-07, RD-02) -------------------------------------------
    add(
        _sample_metric(
            "lead_time_median_s",
            evidence.lead_times_s,
            "median",
            detail="no genuine incident was recalled with positive lead",
        )
    )
    add(
        _sample_metric(
            "lead_time_p10_s",
            evidence.lead_times_s,
            "p10",
            detail="no genuine incident was recalled with positive lead",
        )
    )
    add(
        _sample_metric(
            "lead_time_median_unresponsiveness_s",
            evidence.lead_times_unresponsiveness_s,
            "median",
            detail="no unresponsiveness incident was recalled with positive "
            "lead",
        )
    )
    add(
        _sample_metric(
            "lead_time_p10_unresponsiveness_s",
            evidence.lead_times_unresponsiveness_s,
            "p10",
            detail="no unresponsiveness incident was recalled with positive "
            "lead",
        )
    )
    add(
        _sample_metric(
            "lead_time_median_cross_domain_s",
            evidence.lead_times_cross_domain_s,
            "median",
            detail="no cross_domain incident was recalled with positive lead",
        )
    )
    add(
        _sample_metric(
            "lead_time_p10_cross_domain_s",
            evidence.lead_times_cross_domain_s,
            "p10",
            detail="no cross_domain incident was recalled with positive lead",
        )
    )
    add(
        _ratio_metric(
            "lead_time_share_r2_band",
            evidence.leads_in_r2_band,
            len(evidence.lead_times_unresponsiveness_s),
            detail="no unresponsiveness incident was recalled with positive "
            "lead",
        )
    )
    add(
        _ratio_metric(
            "lead_time_share_r3_band",
            evidence.leads_in_r3_band,
            len(evidence.lead_times_cross_domain_s),
            detail="no cross_domain incident was recalled with positive lead",
        )
    )

    # -- attribution (AC-09, AC-10, RD-05, A-08) ----------------------------
    add(
        _ratio_metric(
            "domain_accuracy",
            evidence.domain_correct,
            evidence.alerted_incidents,
            detail="the system alerted on no genuine incident",
        )
    )
    add(
        _ratio_metric(
            "entity_accuracy",
            evidence.entity_correct,
            evidence.alerted_incidents,
            detail="the system alerted on no genuine incident",
        )
    )
    add(
        _ratio_metric(
            "domain_accuracy_over_all_genuine",
            evidence.domain_correct_over_all_genuine,
            evidence.genuine_total,
            detail="no genuine incidents were injected",
        )
    )
    add(
        _ratio_metric(
            "db_cascade_attribution_accuracy",
            evidence.db_cascade_attributed_database,
            evidence.db_cascade_total,
            detail="no database-contention cascade was injected",
        )
    )

    # -- alert volume and the static baseline (AC-11, RD-04) ----------------
    add(
        _count_metric(
            "alert_volume_system_patching", evidence.system_alert_volume_patching
        )
    )
    add(
        _count_metric(
            "alert_volume_baseline_patching",
            evidence.baseline_alert_volume_patching,
        )
    )
    add(
        _reduction_metric(
            "patching_alert_reduction",
            evidence.system_alert_volume_patching,
            evidence.baseline_alert_volume_patching,
            detail="the static-threshold baseline raised no alert during any "
            "patching/maintenance window, so there is no volume to reduce",
        )
    )
    add(
        _count_metric(
            "alert_volume_system_patching_only",
            evidence.system_alert_volume_patching_only,
        )
    )
    add(
        _count_metric(
            "alert_volume_baseline_patching_only",
            evidence.baseline_alert_volume_patching_only,
        )
    )
    add(
        _reduction_metric(
            "patching_alert_reduction_patching_only",
            evidence.system_alert_volume_patching_only,
            evidence.baseline_alert_volume_patching_only,
            detail="the static-threshold baseline raised no alert during any "
            "OS patching window",
        )
    )
    add(
        _count_metric(
            "system_alert_volume_total", evidence.system_alert_volume_total
        )
    )
    add(
        _count_metric(
            "baseline_alert_volume_total", evidence.baseline_alert_volume_total
        )
    )
    add(
        _ratio_metric(
            "baseline_recall",
            evidence.baseline_recall_numerator,
            evidence.baseline_recall_denominator,
            detail="no genuine incidents were injected",
        )
    )
    add(
        _ratio_metric(
            "baseline_rule_signal_coverage",
            evidence.baseline_rules_with_coverage,
            evidence.baseline_rules_total,
            detail="the baseline rule table is empty",
        )
    )

    # -- suppression A/B (AC-12, RD-07) -------------------------------------
    recall_on = _ratio_metric(
        "recall_suppression_on",
        evidence.recall_on_numerator,
        evidence.recall_on_denominator,
        detail="no genuine incidents were injected",
    )
    recall_off = _ratio_metric(
        "recall_suppression_off",
        evidence.recall_off_numerator,
        evidence.recall_off_denominator,
        detail="no suppression-disabled run was supplied, so AC-12's "
        "comparison cannot be made",
    )
    add(recall_on)
    add(recall_off)
    if recall_on.value is None or recall_off.value is None:
        add(
            metric(
                "suppression_recall_delta_pp",
                None,
                unit="percentage_points",
                note="undefined: one side of the suppression A/B comparison is "
                "undefined, so the delta cannot be computed.",
            )
        )
    else:
        add(
            metric(
                "suppression_recall_delta_pp",
                (recall_off.value - recall_on.value) * 100.0,
                unit="percentage_points",
            )
        )

    # -- collapse (AC-13) ----------------------------------------------------
    metrics.extend(_collapse_metrics(evidence))

    # -- semantic novelty (AC-15) -------------------------------------------
    add(
        _ratio_metric(
            "novel_signature_flagged",
            evidence.novel_signature_flagged,
            evidence.novel_signature_total,
            detail="no failure with a novel log signature was injected",
        )
    )
    add(
        _count_metric(
            "novel_signature_absence_proofs", len(evidence.novel_absence_proofs)
        )
    )

    return MetricSet(tuple(metrics))


def _reduction_metric(
    name: str, system_volume: int, baseline_volume: int, *, detail: str
) -> MetricValue:
    """``1 - system/baseline`` alert-volume reduction, or undefined."""
    definition = EXTRA_METRIC_DEFINITIONS.get(name)
    if baseline_volume == 0:
        return metric(
            name,
            None,
            numerator=system_volume,
            denominator=baseline_volume,
            unit="ratio",
            note=_EMPTY_DENOMINATOR_NOTE.format(detail=detail),
            definition=definition,
        )
    return metric(
        name,
        1.0 - (system_volume / baseline_volume),
        numerator=system_volume,
        denominator=baseline_volume,
        unit="ratio",
        definition=definition,
    )


def _collapse_metrics(evidence: SeedEvidence) -> list[MetricValue]:
    """The AC-13 collapse family, including the raw counts A-03 demands."""
    qualifying = evidence.collapses
    alerted = tuple(item for item in qualifying if item.incidents > 0)
    ratios = [item.ratio for item in alerted]
    ratio_values = [value for value in ratios if value is not None]
    multiple = tuple(item for item in qualifying if item.incidents > 1)

    excluded = len(qualifying) - len(alerted)
    note = None
    if excluded:
        note = (
            f"{excluded} of {len(qualifying)} injected upstream faults raised "
            "no alert; their ratio is 0/0 and undefined, so they are excluded "
            "from this average. They are NOT excluded from "
            "incidents_per_upstream_fault, whose denominator is every injected "
            "qualifying fault, nor from recall."
        )

    # collapse_ratio is a ratified metric: contract C5 injects its definition
    # verbatim and rejects any restatement, so the definition stays None on
    # both branches below.
    definition_ratio = None
    collapse_value = stats.mean(ratio_values)
    if collapse_value is None:
        collapse = metric(
            "collapse_ratio",
            None,
            denominator=len(alerted),
            unit="ratio",
            note=(
                "undefined for this block: no injected upstream fault with >= 2 "
                "affected downstream entities raised an alert, so there is no "
                "collapse behaviour to measure. This is not a pass."
            ),
            definition=definition_ratio,
        )
    else:
        collapse = metric(
            "collapse_ratio",
            collapse_value,
            numerator=sum(item.pre_collapse_alerts for item in alerted),
            denominator=sum(item.incidents for item in alerted),
            unit="ratio",
            note=note,
            definition=definition_ratio,
        )

    per_fault_all = stats.mean([float(item.incidents) for item in qualifying])
    if per_fault_all is None:
        incidents_per_fault = metric(
            "incidents_per_upstream_fault",
            None,
            denominator=0,
            unit="count",
            note=(
                "undefined for this block: no injected upstream fault had >= 2 "
                "affected downstream entities."
            ),
        )
    else:
        incidents_per_fault = metric(
            "incidents_per_upstream_fault",
            per_fault_all,
            numerator=sum(item.incidents for item in qualifying),
            denominator=len(qualifying),
            unit="count",
        )

    return [
        collapse,
        incidents_per_fault,
        _ratio_metric(
            "incidents_per_alerted_upstream_fault",
            sum(item.incidents for item in alerted),
            len(alerted),
            detail="no qualifying upstream fault raised an alert",
            unit="count",
        ),
        _count_metric(
            "collapse_pre_collapse_alerts_total",
            sum(item.pre_collapse_alerts for item in qualifying),
        ),
        _count_metric(
            "collapse_incidents_total",
            sum(item.incidents for item in qualifying),
        ),
        _count_metric("upstream_faults_qualifying", len(qualifying)),
        _count_metric("upstream_faults_alerted", len(alerted)),
        _count_metric("upstream_faults_with_multiple_incidents", len(multiple)),
    ]
