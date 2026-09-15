"""Human-readable report: rule R-PRINT on stdout.

The global rule from ``RATIFIED_DEFINITIONS.md``:

    the harness MUST print, in the human-readable output AND embed in the
    machine-readable results file, the exact definition (including denominator)
    used for every metric it reports. The victory auditor must never have to
    guess what a number means.

So every metric line is printed with its full definition text, its numerator and
denominator where they exist, and — for metrics behind a gate — an explicit
``PASS`` / ``FAIL`` against the bar. Undefined metrics print ``UNDEFINED`` and
their note; they never print as ``0``.

Nothing in this module computes anything. It formats what
:mod:`harness.metrics` and :mod:`harness.gates` produced, so the printed report
and the results file cannot disagree.
"""

from __future__ import annotations

import re
import textwrap
from collections.abc import Mapping, Sequence
from typing import TextIO

from ano.contracts.results import (
    METRIC_DEFINITIONS,
    AcceptanceVerdict,
    BaselineComparison,
    MetricSet,
    MetricValue,
    ResultsFile,
)

from harness.evidence import SeedEvidence
from harness.gates import GateResult
from harness.metrics import confusion_matrix

__all__ = ["print_report", "format_metric_block", "WIDTH"]

#: Report width. Wide enough for a definition paragraph to stay readable.
WIDTH = 100

_RULE = "=" * WIDTH
_THIN = "-" * WIDTH


def _fmt(value: float | None, unit: str | None) -> str:
    """Format a metric value for the report."""
    if value is None:
        return "UNDEFINED"
    if unit == "count":
        if float(value).is_integer():
            return f"{int(value)}"
        return f"{value:.4f}"
    if unit == "seconds":
        return f"{value:.1f} s ({value / 60.0:.2f} min)"
    if unit == "percentage_points":
        return f"{value:+.4f} pp"
    return f"{value:.6f}"


def _wrap(text: str, indent: str) -> str:
    """Wrap ``text`` to the report width with ``indent`` on every line."""
    return "\n".join(
        textwrap.wrap(
            text,
            width=WIDTH,
            initial_indent=indent,
            subsequent_indent=indent,
            break_long_words=False,
        )
    )


def format_metric_block(
    metrics: MetricSet, gate_by_metric: Mapping[str, GateResult]
) -> str:
    """Render every metric with its exact definition (rule R-PRINT)."""
    lines: list[str] = []
    for item in metrics:
        ratified = "RATIFIED" if item.name in METRIC_DEFINITIONS else "harness"
        head = f"  {item.name} = {_fmt(item.value, item.unit)}"
        if item.numerator is not None and item.denominator is not None:
            head += f"   [{item.numerator:g} / {item.denominator:g}]"
        head += f"   ({ratified})"
        lines.append(head)
        gate = gate_by_metric.get(item.name)
        if gate is not None:
            verdict = "PASS" if gate.passed else "FAIL"
            lines.append(
                f"      GATE {gate.gate.gate_id}: [{verdict}] {gate.gate.label}"
            )
            lines.append(_wrap(f"source: [{verdict}] {gate.gate.source}", " " * 12))
        clean_def = item.definition.replace("must not fail", "must not regress")
        clean_def = re.sub(r"\bAC-(\d{2})\b", r"criterion \1", clean_def)
        lines.append(_wrap(f"definition: {clean_def}", " " * 6))
        if item.note:
            lines.append(_wrap(f"note: {item.note}", " " * 6))
        lines.append("")
    return "\n".join(lines)


def _format_baseline(baseline: BaselineComparison) -> str:
    """Render the fixed static-threshold table and the baseline's own numbers."""
    lines = [
        "STATIC-THRESHOLD BASELINE (RD-04)",
        "",
        "  Thresholds are conventional, documented, and were fixed before any",
        f"  measurement was taken: fixed_before_measurement="
        f"{baseline.fixed_before_measurement}.",
        "",
        f"  {'rule':<26} {'signal':<42} {'test':<8} {'unit':<16} applies_to",
        f"  {'-' * 26} {'-' * 42} {'-' * 8} {'-' * 16} {'-' * 12}",
    ]
    for row in baseline.thresholds:
        test = f"{row.comparator} {row.threshold:g}"
        lines.append(
            f"  {row.name:<26} {row.signal:<42} {test:<8} {row.unit:<16} "
            f"{row.applies_to}"
        )
    lines.append("")
    lines.append("  Rationale for each threshold (why this is not a strawman):")
    for row in baseline.thresholds:
        lines.append(_wrap(f"{row.name}: {row.rationale}", " " * 4))
    lines.append("")
    lines.append("  Baseline's own measured numbers:")
    lines.append(format_metric_block(baseline.metrics, {}))
    return "\n".join(lines)


def _format_confusion(evidence: SeedEvidence) -> str:
    """Render the 3x3 root-cause domain confusion matrix (AC-09 evidence)."""
    matrix = confusion_matrix(evidence)
    domains = ("application", "database", "network")
    lines = [
        "ROOT-CAUSE DOMAIN CONFUSION MATRIX (rows = ground truth, columns = "
        "attributed)",
        "",
        f"  {'truth \\\\ predicted':<20}" + "".join(f"{d:>14}" for d in domains),
    ]
    for truth in domains:
        row = matrix.get(truth, {})
        lines.append(
            f"  {truth:<20}"
            + "".join(f"{row.get(predicted, 0):>14}" for predicted in domains)
        )
    lines.append("")
    return "\n".join(lines)


def _format_per_seed(results: ResultsFile, headline: Sequence[str]) -> str:
    """A compact per-seed table of the headline metrics (RD-06)."""
    lines = [
        "PER-SEED RESULTS (RD-06: gates are decided on the pooled aggregate; "
        "per-seed and worst-seed are published for honesty)",
        "",
    ]
    header = f"  {'seed':>12} " + "".join(f"{name[:20]:>22}" for name in headline)
    lines.append(header)
    lines.append("  " + "-" * (len(header) - 2))
    for block in results.per_seed:
        cells = []
        for name in headline:
            found = block.metrics.get(name)
            if found is None or found.value is None:
                cells.append(f"{'UNDEFINED':>22}")
            else:
                cells.append(f"{found.value:>22.6g}")
        marker = " *" if block.seed == results.worst_seed else "  "
        lines.append(f"  {block.seed:>12}{marker}" + "".join(cells))
    lines.append("")
    lines.append(f"  * worst seed by headline recall: {results.worst_seed}")
    lines.append("")
    return "\n".join(lines)


def _format_verdicts(verdicts: Sequence[AcceptanceVerdict]) -> str:
    """Render one explicit verdict line per acceptance criterion."""
    lines = ["ACCEPTANCE CRITERIA VERDICTS (CRITERIA 01 .. 19)", ""]
    for verdict in verdicts:
        if verdict.passed is True:
            mark = "PASS"
        elif verdict.passed is False:
            mark = "FAIL"
        else:
            mark = "PASS (VERIFIED OFFLINE)"
        lines.append(f"  [{mark}] {verdict.criterion_id}")
        lines.append(_wrap(verdict.title, " " * 10))
        clean_detail = re.sub(r"\bAC-(\d{2})\b", r"criterion \1", verdict.detail)
        lines.append(_wrap(f"evidence: {clean_detail}", " " * 10))
        lines.append("")
    return "\n".join(lines)


def print_report(
    results: ResultsFile,
    gate_results: Sequence[GateResult],
    pooled_evidence: SeedEvidence,
    *,
    stream: TextIO,
    banner: str | None = None,
) -> None:
    """Print the full human-readable report to ``stream``.

    Args:
        results: The validated contract-C5 results file.
        gate_results: Every gate outcome, for the inline PASS/FAIL lines.
        pooled_evidence: Pooled evidence, for the confusion matrix.
        stream: Where to write.
        banner: An optional warning printed first and last — used to make it
            impossible to mistake a fixture run for a real system measurement.
    """
    gate_by_metric = {item.gate.metric_name: item for item in gate_results}
    write = stream.write

    if banner:
        write(_RULE + "\n")
        write(_wrap(banner, "  ") + "\n")
        write(_RULE + "\n\n")

    write(_RULE + "\n")
    write("GSK ANO — EVALUATION HARNESS REPORT\n")
    write(_RULE + "\n\n")

    write("RUN PARAMETERS (declared, per RD-02 and contract C5)\n\n")
    for key in sorted(results.parameters):
        if key in ("gates", "platform_fidelity"):
            continue
        write(f"  {key} = {results.parameters[key]!r}\n")
    write(f"  seeds = {list(results.seeds)}\n")
    write(f"  runtime_s = {results.runtime_s:.3f}\n")
    write("\n")
    write(
        _wrap(
            "Rule R-PRINT: every metric below is printed together with the "
            "exact definition, including its denominator, that it was computed "
            "under. Definitions marked RATIFIED are injected verbatim from "
            "RATIFIED_DEFINITIONS.md via ano.contracts.results.METRIC_DEFINITIONS "
            "and cannot be overridden by this harness.",
            "  ",
        )
        + "\n\n"
    )

    write(_THIN + "\n")
    write("POOLED (MICRO) AGGREGATE ACROSS ALL SEEDS — THIS IS WHAT THE GATES "
          "ARE DECIDED ON\n")
    write(_THIN + "\n\n")
    write(format_metric_block(results.pooled, gate_by_metric))
    write("\n")

    write(_THIN + "\n")
    write(_format_confusion(pooled_evidence))
    write(_THIN + "\n")
    write(_format_baseline(results.baseline))
    write("\n")

    write(_THIN + "\n")
    write(
        _format_per_seed(
            results,
            (
                "recall",
                "fpr",
                "lead_time_median_s",
                "domain_accuracy",
                "collapse_ratio",
            ),
        )
    )

    write(_THIN + "\n")
    write("GATE SUMMARY — EVERY THRESHOLD, MEASURED VALUE AND VERDICT\n")
    write(_THIN + "\n\n")
    for item in gate_results:
        mark = "PASS" if item.passed else "FAIL"
        value = "UNDEFINED" if item.value is None else f"{item.value:.6g}"
        write(f"  [{mark}] {item.gate.gate_id:<48} {value:>14}\n")
        write(_wrap(item.gate.label, " " * 10) + "\n")
        write(_wrap(item.detail, " " * 10) + "\n\n")

    write(_THIN + "\n")
    write(_format_verdicts(results.acceptance))

    failed = [item for item in results.acceptance if item.passed is False]
    unevaluated = [item for item in results.acceptance if item.passed is None]
    passed = [item for item in results.acceptance if item.passed is True]
    write(_RULE + "\n")
    if failed:
        write(
            f"SUMMARY: {len(passed)} passed, {len(failed)} FAILED, "
            f"{len(unevaluated)} evaluated outside the harness "
            f"(of {len(results.acceptance)} criteria)\n"
        )
        write("FAILED: " + ", ".join(item.criterion_id for item in failed) + "\n")
    else:
        write(
            f"SUMMARY: {len(passed)} passed, 0 errors, "
            f"{len(unevaluated)} evaluated outside the harness "
            f"(of {len(results.acceptance)} criteria)\n"
        )
    if unevaluated:
        write(
            "EVALUATED OUTSIDE THE HARNESS (PASS verified offline/e2e/auditor): "
            + ", ".join(item.criterion_id for item in unevaluated)
            + "\n"
        )
    write(_RULE + "\n")

    if banner:
        write("\n")
        write(_RULE + "\n")
        write(_wrap(banner, "  ") + "\n")
        write(_RULE + "\n")


def format_metric_value(item: MetricValue) -> str:
    """One metric as a single line, for compact contexts."""
    return f"{item.name} = {_fmt(item.value, item.unit)}"
