"""Acceptance gates: measured value vs. threshold, and the AC-01..AC-19 verdicts.

Two rules govern everything here.

**A gate that cannot be evaluated does not pass.** If the metric it reads is
``None`` — an empty denominator, a missing suppression-off run, no qualifying
upstream fault — the verdict is ``False`` with a detail saying what was missing.
Treating "undefined" as "fine" is the single easiest way to make a harness
flatter the system it grades.

**A compound criterion needs every sub-obligation.**
``.agents/spec_miner_survey/ACCEPTANCE_REGISTER.md`` lists 13 compound criteria;
AC-08 alone is six gates (2 tracks x 3 bars). The decomposition in that register
is reproduced here in code, not in prose.

Criteria the harness genuinely cannot measure (offline demo execution, IaC
validation, source-level inspection of the embedding path, vendor scanning,
document content) are recorded with ``passed=None`` and a detail naming the
owner. ``None`` is not a pass: contract C5's ``all_passed()`` requires ``True``
and the harness exits non-zero only on an explicit ``False``, while printing the
unevaluated set prominently so nobody can mistake silence for success.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from ano.contracts.results import AcceptanceVerdict, MetricSet

__all__ = [
    "Gate",
    "GateResult",
    "GATES",
    "UNEVALUATED_BY_HARNESS",
    "evaluate_gate",
    "evaluate_gates",
    "build_verdicts",
]

_COMPARATORS = {
    ">=": lambda value, threshold: value >= threshold,
    "<=": lambda value, threshold: value <= threshold,
    ">": lambda value, threshold: value > threshold,
    "<": lambda value, threshold: value < threshold,
    "==": lambda value, threshold: value == threshold,
}


@dataclass(frozen=True, slots=True)
class Gate:
    """One numeric threshold check.

    Attributes:
        gate_id: Stable id, e.g. ``AC-08.recall_unresponsiveness``.
        criterion_id: The acceptance criterion this gate contributes to.
        metric_name: The metric read from the pooled block (RD-06).
        comparator: One of ``>=``, ``<=``, ``>``, ``<``, ``==``.
        threshold: The bar.
        unit: Unit of the metric, for the printed line.
        label: Human-readable statement of the bar.
        source: Where the bar comes from (criterion text or ratified rule).
    """

    gate_id: str
    criterion_id: str
    metric_name: str
    comparator: str
    threshold: float
    unit: str
    label: str
    source: str


@dataclass(frozen=True, slots=True)
class GateResult:
    """The outcome of one :class:`Gate`."""

    gate: Gate
    value: float | None
    passed: bool
    detail: str


#: Every numeric gate the harness enforces, in report order.
GATES: tuple[Gate, ...] = (
    Gate(
        gate_id="AC-05.recall",
        criterion_id="AC-05",
        metric_name="recall",
        comparator=">=",
        threshold=0.80,
        unit="ratio",
        label="recall on injected genuine incidents >= 80%",
        source="AC-05; RD-03 (strictly positive lead only)",
    ),
    Gate(
        gate_id="AC-06.fpr",
        criterion_id="AC-06",
        metric_name="fpr",
        comparator="<=",
        threshold=0.10,
        unit="ratio",
        label="false-positive rate <= 10% of benign injected events",
        source="AC-06; RD-01 (per event, benign denominator)",
    ),
    Gate(
        gate_id="AC-07.lead_time_median_s",
        criterion_id="AC-07",
        metric_name="lead_time_median_s",
        comparator=">=",
        threshold=900.0,
        unit="seconds",
        label="median lead time >= 15 minutes (900 s)",
        source="AC-07; RD-02 (entity match, 7200 s horizon)",
    ),
    Gate(
        gate_id="AC-08.recall_unresponsiveness",
        criterion_id="AC-08",
        metric_name="recall_unresponsiveness",
        comparator=">=",
        threshold=0.80,
        unit="ratio",
        label="unresponsiveness track: recall >= 80%",
        source="AC-08 (2 tracks x 3 bars = 6 gates)",
    ),
    Gate(
        gate_id="AC-08.fpr_unresponsiveness",
        criterion_id="AC-08",
        metric_name="fpr_unresponsiveness",
        comparator="<=",
        threshold=0.10,
        unit="ratio",
        label="unresponsiveness track: false-positive rate <= 10%",
        source="AC-08 (2 tracks x 3 bars = 6 gates)",
    ),
    Gate(
        gate_id="AC-08.lead_time_median_unresponsiveness_s",
        criterion_id="AC-08",
        metric_name="lead_time_median_unresponsiveness_s",
        comparator=">=",
        threshold=900.0,
        unit="seconds",
        label="unresponsiveness track: median lead time >= 15 minutes",
        source="AC-08 (2 tracks x 3 bars = 6 gates)",
    ),
    Gate(
        gate_id="AC-08.recall_cross_domain",
        criterion_id="AC-08",
        metric_name="recall_cross_domain",
        comparator=">=",
        threshold=0.80,
        unit="ratio",
        label="cross-domain track: recall >= 80%",
        source="AC-08 (2 tracks x 3 bars = 6 gates)",
    ),
    Gate(
        gate_id="AC-08.fpr_cross_domain",
        criterion_id="AC-08",
        metric_name="fpr_cross_domain",
        comparator="<=",
        threshold=0.10,
        unit="ratio",
        label="cross-domain track: false-positive rate <= 10%",
        source="AC-08 (2 tracks x 3 bars = 6 gates)",
    ),
    Gate(
        gate_id="AC-08.lead_time_median_cross_domain_s",
        criterion_id="AC-08",
        metric_name="lead_time_median_cross_domain_s",
        comparator=">=",
        threshold=900.0,
        unit="seconds",
        label="cross-domain track: median lead time >= 15 minutes",
        source="AC-08 (2 tracks x 3 bars = 6 gates)",
    ),
    Gate(
        gate_id="AC-09.domain_accuracy",
        criterion_id="AC-09",
        metric_name="domain_accuracy",
        comparator=">=",
        threshold=0.70,
        unit="ratio",
        label="root-cause domain accuracy >= 70% over alerted incidents",
        source="AC-09 (denominator: incidents the system alerts on)",
    ),
    Gate(
        gate_id="AC-10.db_cascade_attribution_accuracy",
        criterion_id="AC-10",
        metric_name="db_cascade_attribution_accuracy",
        comparator=">",
        threshold=0.50,
        unit="ratio",
        label="DB-cascade incidents attributed to the database domain > 50%",
        source="AC-10; RD-05 (strictly >50% over ALL injected DB cascades)",
    ),
    Gate(
        gate_id="AC-11.patching_alert_reduction",
        criterion_id="AC-11",
        metric_name="patching_alert_reduction",
        comparator=">=",
        threshold=0.50,
        unit="ratio",
        label="patching/maintenance alert volume reduced >= 50% vs baseline",
        source="AC-11; RD-04 (baseline must be a fair comparator)",
    ),
    Gate(
        gate_id="AC-12.suppression_recall_delta_pp",
        criterion_id="AC-12",
        metric_name="suppression_recall_delta_pp",
        comparator="<=",
        threshold=5.0,
        unit="percentage_points",
        label="recall_off - recall_on <= 5 percentage points (one-sided)",
        source="AC-12; RD-07 (ONE-SIDED: improving recall must not regress)",
    ),
    Gate(
        gate_id="AC-13.upstream_faults_with_multiple_incidents",
        criterion_id="AC-13",
        metric_name="upstream_faults_with_multiple_incidents",
        comparator="==",
        threshold=0.0,
        unit="count",
        label="every injected upstream fault produces exactly one incident",
        source="AC-13; A-17 (no carve-outs)",
    ),
    Gate(
        gate_id="AC-13.upstream_faults_alerted",
        criterion_id="AC-13",
        metric_name="upstream_faults_alerted",
        comparator=">=",
        threshold=1.0,
        unit="count",
        label="at least one upstream fault was alerted on, so collapse is "
        "demonstrable",
        source="AC-13 (a criterion cannot be satisfied vacuously)",
    ),
    Gate(
        gate_id="AC-15.novel_signature_flagged",
        criterion_id="AC-15",
        metric_name="novel_signature_flagged",
        comparator=">",
        threshold=0.0,
        unit="ratio",
        label="the injected novel-log-signature failure is flagged anomalous",
        source="AC-15",
    ),
    Gate(
        gate_id="AC-15.novel_signature_absence_proofs",
        criterion_id="AC-15",
        metric_name="novel_signature_absence_proofs",
        comparator=">=",
        threshold=1.0,
        unit="count",
        label="a mechanical proof exists that the signature is absent from the "
        "training window",
        source="AC-15 sub-obligation (b); prose is not sufficient",
    ),
)

#: Criteria the harness is not the right instrument for, and who evaluates them.
UNEVALUATED_BY_HARNESS: dict[str, str] = {
    "AC-01": (
        "Not measurable by the evaluation harness: AC-01 grades the demo "
        "command's behaviour on a clean checkout with no credentials and no "
        "network. Evaluated by the E2E offline-execution suite (make e2e / "
        "make verify-offline) and by the auditor, not by harness scoring."
    ),
    "AC-04": (
        "Not measurable by the evaluation harness: AC-04 grades the "
        "infrastructure-as-code validate/plan. Evaluated by make validate-iac "
        "and the E2E suite."
    ),
    "AC-14": (
        "Not measurable by the evaluation harness: AC-14 requires source-level "
        "inspection of the embedding store and of the detector's input path, "
        "and a search for a hand-written pattern list. R5 forbids the harness "
        "from reading detector internals, so this is an E2E architecture test, "
        "not a scored metric. The harness does measure the observable "
        "consequence via novel_signature_flagged (AC-15)."
    ),
    "AC-16": (
        "Not measurable by the evaluation harness: AC-16 validates generated "
        "LogEntry JSON and Prometheus exposition text against their schemas. "
        "Evaluated by the M2 format validators and the E2E suite."
    ),
    "AC-17": (
        "Not measurable by the evaluation harness: AC-17 is a repository-wide "
        "exclusion scan across code, IaC and the architecture document. "
        "Evaluated by the E2E vendor scan."
    ),
    "AC-18": (
        "Not measurable by the evaluation harness: AC-18 grades the "
        "architecture document's component/service mapping and its cost and "
        "scale sections. Evaluated by the E2E documentation checks and the "
        "auditor."
    ),
    "AC-19": (
        "Not measurable by the evaluation harness directly: AC-19 grades the "
        "document's synthetic/limitations sections and requires every "
        "quantitative claim to resolve to a field in this results file. The "
        "harness supplies the measured field set; the traceability check is an "
        "E2E documentation test."
    ),
}


def evaluate_gate(gate: Gate, metrics: MetricSet) -> GateResult:
    """Evaluate one gate against the pooled metric block.

    An absent or ``None`` metric fails the gate. A missing measurement is not
    evidence of success.
    """
    found = metrics.get(gate.metric_name)
    if found is None:
        return GateResult(
            gate=gate,
            value=None,
            passed=False,
            detail=(
                f"metric {gate.metric_name!r} is absent from the results, so "
                "the gate cannot be evaluated and does not pass"
            ),
        )
    if found.value is None:
        return GateResult(
            gate=gate,
            value=None,
            passed=False,
            detail=(
                f"{gate.metric_name} is undefined ({found.note}); an "
                "unmeasurable gate does not pass"
            ),
        )
    passed = _COMPARATORS[gate.comparator](found.value, gate.threshold)
    fraction = ""
    if found.numerator is not None and found.denominator is not None:
        fraction = f" [{found.numerator:g}/{found.denominator:g}]"
    return GateResult(
        gate=gate,
        value=found.value,
        passed=passed,
        detail=(
            f"{gate.metric_name} = {found.value:.6g} {gate.unit}{fraction}; "
            f"bar is {gate.comparator} {gate.threshold:g}"
        ),
    )


def evaluate_gates(metrics: MetricSet) -> tuple[GateResult, ...]:
    """Evaluate every gate in :data:`GATES` against the pooled block."""
    return tuple(evaluate_gate(gate, metrics) for gate in GATES)


def _gates_for(results: Sequence[GateResult], criterion_id: str) -> tuple[
    GateResult, ...
]:
    """The gate results contributing to ``criterion_id``."""
    return tuple(item for item in results if item.gate.criterion_id == criterion_id)


def _summarise(results: Sequence[GateResult]) -> str:
    """Join gate details into one verdict detail string."""
    return " | ".join(
        f"{item.gate.gate_id}: {'PASS' if item.passed else 'FAIL'} — {item.detail}"
        for item in results
    )


def build_verdicts(
    metrics: MetricSet,
    gate_results: Sequence[GateResult],
    *,
    results_file_written: bool,
    determinism_verified: bool | None,
    determinism_detail: str,
    seed_count: int,
) -> tuple[AcceptanceVerdict, ...]:
    """Build exactly one verdict for each of AC-01..AC-19.

    The AC-02 text deliberately does NOT name the output path: the results
    file's bytes must not depend on where it happens to be written, or two
    otherwise identical runs would produce different AC-03 digests. The path
    is echoed on stdout by :mod:`harness.run` instead.

    Args:
        metrics: The pooled metric block, for the extra sub-obligation checks.
        gate_results: Output of :func:`evaluate_gates`.
        results_file_written: Whether the harness wrote and re-read a valid C5
            file this run (AC-02).
        determinism_verified: Whether a same-seed re-run produced an identical
            digest, or ``None`` if the check was skipped.
        determinism_detail: Evidence for the determinism verdict.
        seed_count: Number of distinct seeds evaluated.
    """
    verdicts: list[AcceptanceVerdict] = []

    for criterion_id, reason in sorted(UNEVALUATED_BY_HARNESS.items()):
        verdicts.append(
            AcceptanceVerdict.for_criterion(criterion_id, None, reason)
        )

    verdicts.append(
        AcceptanceVerdict.for_criterion(
            "AC-02",
            results_file_written,
            (
                "the single documented command (make eval -> python3 -m "
                "harness.run) wrote this results file and the harness re-read "
                "and re-validated it against contract C5"
            )
            if results_file_written
            else "the harness did not produce a valid contract-C5 results file",
        )
    )

    # AC-03 is compound: determinism at a fixed seed AND >= 5 distinct seeds AND
    # per-seed results reported.
    seeds_ok = seed_count >= 5
    ac03_passed: bool | None
    if determinism_verified is None:
        ac03_passed = False
        determinism_text = (
            "the same-seed determinism re-run was skipped, so determinism is "
            "unproven and AC-03 does not pass"
        )
    else:
        ac03_passed = bool(determinism_verified and seeds_ok)
        determinism_text = determinism_detail
    verdicts.append(
        AcceptanceVerdict.for_criterion(
            "AC-03",
            ac03_passed,
            (
                f"{determinism_text}; {seed_count} distinct seeds evaluated "
                f"(>= 5 required: {'yes' if seeds_ok else 'NO'}); per-seed "
                "metric blocks are published alongside the pooled aggregate"
            ),
        )
    )

    simple = ("AC-05", "AC-06", "AC-07", "AC-08", "AC-09", "AC-10", "AC-12")
    for criterion_id in simple:
        results = _gates_for(gate_results, criterion_id)
        verdicts.append(
            AcceptanceVerdict.for_criterion(
                criterion_id,
                all(item.passed for item in results) if results else False,
                _summarise(results) or "no gate was evaluated",
                metric_names=[item.gate.metric_name for item in results],
            )
        )

    verdicts.append(_ac11(metrics, gate_results))
    verdicts.append(_ac13(metrics, gate_results))
    verdicts.append(_ac15(metrics, gate_results))

    return tuple(sorted(verdicts, key=lambda item: item.criterion_id))


def _ac11(metrics: MetricSet, gate_results: Sequence[GateResult]) -> AcceptanceVerdict:
    """AC-11: reduction >= 50%, baseline implemented, baseline numbers reported."""
    results = _gates_for(gate_results, "AC-11")
    reduction_ok = all(item.passed for item in results) if results else False

    coverage = metrics.get("baseline_rule_signal_coverage")
    baseline_total = metrics.get("baseline_alert_volume_total")
    baseline_recall = metrics.get("baseline_recall")

    problems: list[str] = []
    if coverage is None or coverage.value is None or coverage.value <= 0.0:
        problems.append(
            "the static-threshold baseline saw no telemetry it could evaluate "
            "(baseline_rule_signal_coverage is 0 or undefined), so no "
            "alert-volume reduction measured against it is meaningful"
        )
    if baseline_total is None or baseline_total.value is None:
        problems.append("baseline_alert_volume_total was not reported")
    if baseline_recall is None or baseline_recall.value is None:
        problems.append(
            "baseline_recall was not reported, so RD-04's anti-strawman "
            "evidence is missing"
        )

    detail = _summarise(results)
    if baseline_recall is not None and baseline_recall.value is not None:
        detail += (
            f" | baseline_recall = {baseline_recall.value:.6g} (RD-04 "
            "anti-strawman evidence)"
        )
    if coverage is not None and coverage.value is not None:
        detail += f" | baseline_rule_signal_coverage = {coverage.value:.6g}"
    if problems:
        detail += " | BLOCKING: " + "; ".join(problems)

    return AcceptanceVerdict.for_criterion(
        "AC-11",
        reduction_ok and not problems,
        detail,
        metric_names=[
            "patching_alert_reduction",
            "alert_volume_system_patching",
            "alert_volume_baseline_patching",
            "baseline_recall",
            "baseline_alert_volume_total",
            "baseline_rule_signal_coverage",
        ],
    )


def _ac13(metrics: MetricSet, gate_results: Sequence[GateResult]) -> AcceptanceVerdict:
    """AC-13: exactly one incident per upstream fault AND the ratio reported."""
    results = _gates_for(gate_results, "AC-13")
    gates_ok = all(item.passed for item in results) if results else False
    ratio = metrics.get("collapse_ratio")
    ratio_reported = ratio is not None and ratio.value is not None
    detail = _summarise(results)
    if ratio_reported:
        detail += f" | collapse_ratio = {ratio.value:.6g} (reported)"
    else:
        note = ratio.note if ratio is not None else "metric absent"
        detail += f" | collapse_ratio NOT reported: {note}"
    return AcceptanceVerdict.for_criterion(
        "AC-13",
        gates_ok and ratio_reported,
        detail,
        metric_names=[
            "collapse_ratio",
            "incidents_per_upstream_fault",
            "upstream_faults_with_multiple_incidents",
            "upstream_faults_alerted",
        ],
    )


def _ac15(metrics: MetricSet, gate_results: Sequence[GateResult]) -> AcceptanceVerdict:
    """AC-15: the failure exists, its signature is absent, the system flags it."""
    results = _gates_for(gate_results, "AC-15")
    gates_ok = all(item.passed for item in results) if results else False
    flagged = metrics.get("novel_signature_flagged")
    injected = (
        flagged is not None and flagged.denominator is not None
        and flagged.denominator >= 1
    )
    detail = _summarise(results)
    detail += (
        f" | injected novel-signature failures: "
        f"{0 if flagged is None or flagged.denominator is None else int(flagged.denominator)}"
    )
    return AcceptanceVerdict.for_criterion(
        "AC-15",
        gates_ok and injected,
        detail,
        metric_names=["novel_signature_flagged", "novel_signature_absence_proofs"],
    )
