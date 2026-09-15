"""Contract C5 — the machine-readable harness results file.

Direction of travel: ``harness`` **writes** it; the auditor, the demo surface
and the architecture document **read** it.

``PROJECT.md`` requires the results file to contain

* per-seed **and** pooled blocks,
* every metric in ``RATIFIED_DEFINITIONS.md``,
* **the definition string for each metric** (global rule R-PRINT),
* the static-threshold baseline comparison block,
* an explicit pass/fail per acceptance criterion ``AC-01``..``AC-19``.

This module makes all five machine-checkable.

R-PRINT is enforced, not requested
----------------------------------
Every :class:`MetricValue` carries a non-empty ``definition``. For the metrics
named in :data:`METRIC_DEFINITIONS` the definition must be **exactly** the
ratified text — the ratified definitions "may not be renegotiated by a Worker,
sub-orchestrator or reviewer to make a threshold easier to hit", so a results
file that reports ``recall`` under a home-made definition fails validation
rather than quietly redefining the metric. Use :func:`metric` to construct
ratified metrics and the correct string is filled in for you.

Undefined metrics
-----------------
``MetricValue.value`` may be ``None``, meaning "not defined for this block" —
for example a per-seed DB-cascade accuracy when that seed injected no DB
cascade. ``None`` requires an explanatory ``note`` so an empty denominator can
never be confused with a measured zero. This matters: a zero and an undefined
value aggregate very differently, and hiding the difference is exactly how a
pooled number gets quietly inflated.

Determinism
-----------
Acceptance criterion AC-03 requires identical scores from identical seeds.
``generated_at`` and ``runtime_s`` are wall-clock and therefore excluded from
:meth:`ResultsFile.canonical_payload`, which is what
:meth:`ResultsFile.determinism_digest` hashes. Compare digests, not files.
"""

from __future__ import annotations

import datetime as _dt
import os
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from ano.contracts.common import (
    ValidationError,
    dumps_canonical,
    dumps_pretty,
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
from ano.contracts.determinism import stable_hash_hex

__all__ = [
    "SCHEMA_VERSION",
    "MAX_PREDICTION_HORIZON_S",
    "METRIC_DEFINITIONS",
    "REQUIRED_METRICS",
    "REQUIRED_PARAMETERS",
    "ACCEPTANCE_CRITERIA",
    "MetricValue",
    "MetricSet",
    "SeedBlock",
    "BaselineThreshold",
    "BaselineComparison",
    "AcceptanceVerdict",
    "ResultsFile",
    "metric",
    "parse_results_file",
    "read_results_file",
    "write_results_file",
]

#: Bumped when the C5 shape changes. Consumers should reject a major mismatch.
SCHEMA_VERSION = "1.0"

#: Ratified definition RD-02: the maximum prediction horizon, declared and
#: printed. A firing earlier than this earns no lead-time credit.
MAX_PREDICTION_HORIZON_S = 7200

#: The exact, ratified definition of every metric the harness must report.
#: Sources: ``RATIFIED_DEFINITIONS.md`` (RD-01..RD-07) and
#: ``ACCEPTANCE_REGISTER.md``. Keys are metric names; values are the definition
#: string that must appear verbatim in the results file (rule R-PRINT).
METRIC_DEFINITIONS: Mapping[str, str] = {
    # --- recall (AC-05, RD-03) ------------------------------------------
    "recall": (
        "recall = (genuine injected incidents with >=1 matched prediction at "
        "strictly positive lead time) / (all genuine injected incidents). "
        "RD-03: post-onset detections are excluded from the numerator and "
        "reported separately as reactive_detections."
    ),
    "recall_unresponsiveness": (
        "recall restricted to ground-truth events on the 'unresponsiveness' "
        "track; same numerator and denominator rule as recall (AC-08)."
    ),
    "recall_cross_domain": (
        "recall restricted to ground-truth events on the 'cross_domain' track; "
        "same numerator and denominator rule as recall (AC-08)."
    ),
    "reactive_detections": (
        "count of genuine injected incidents whose earliest matched prediction "
        "has non-positive lead time (i.e. was emitted at or after true onset). "
        "RD-03: reported, excluded from the recall numerator."
    ),
    # --- false positives (AC-06, RD-01, RD-02) --------------------------
    "fpr": (
        "false-positive rate = (# benign injected events that raised >=1 alert) "
        "/ (# all benign injected events), counted per event, not per alert. "
        "RD-01: the denominator is ALL benign injected events (patching "
        "windows, planned deploys, traffic surges), whether or not the system "
        "alerted; genuine incidents are NOT in the denominator."
    ),
    "fpr_unresponsiveness": (
        "fpr computed over alerts on the 'unresponsiveness' track only; same "
        "per-event numerator and benign-event denominator as fpr (AC-08)."
    ),
    "fpr_cross_domain": (
        "fpr computed over alerts on the 'cross_domain' track only; same "
        "per-event numerator and benign-event denominator as fpr (AC-08)."
    ),
    "fpr_strict": (
        "fpr_strict = (# evaluation windows in which no prediction is "
        "legitimate that contained >=1 alert) / (# evaluation windows in which "
        "no prediction is legitimate), where a window is illegitimate if no "
        "genuine incident onset falls in the following 120 minutes. RD-01 as "
        "amended 13:13Z: this covers pure-normal windows AND windows on "
        "genuine-incident entities that sit outside the prediction horizon. "
        "Reported, not gated."
    ),
    "unmatched_alert_count": (
        "raw count of alerts matched to no ground-truth incident. RD-01: "
        "reported, not gated; makes continuous firing visible on the face of "
        "the results file without distorting AC-06."
    ),
    # --- lead time (AC-07, RD-02) ---------------------------------------
    "lead_time_median_s": (
        "median over recalled genuine incidents of (true_onset - emitted_at of "
        "the first matched prediction), in seconds. RD-02: a prediction matches "
        "only if the entity matches and it falls inside the 120-minute "
        "association window preceding true onset; earlier firings earn no "
        "lead-time credit."
    ),
    "lead_time_p10_s": (
        "10th percentile of the same per-incident lead-time distribution as "
        "lead_time_median_s, in seconds (required by R5)."
    ),
    "lead_time_median_unresponsiveness_s": (
        "lead_time_median_s restricted to the 'unresponsiveness' track (AC-08)."
    ),
    "lead_time_p10_unresponsiveness_s": (
        "lead_time_p10_s restricted to the 'unresponsiveness' track (AC-08)."
    ),
    "lead_time_median_cross_domain_s": (
        "lead_time_median_s restricted to the 'cross_domain' track (AC-08)."
    ),
    "lead_time_p10_cross_domain_s": (
        "lead_time_p10_s restricted to the 'cross_domain' track (AC-08)."
    ),
    # --- attribution (AC-09, AC-10, RD-05) ------------------------------
    "domain_accuracy": (
        "domain accuracy = (alerted incidents whose attributed root-cause "
        "domain equals the ground-truth domain) / (incidents the system alerted "
        "on). AC-09 restricts the denominator to alerted incidents."
    ),
    "db_cascade_attribution_accuracy": (
        "= (DB-cascade incidents attributed to the 'database' domain) / (ALL "
        "injected DB-cascade incidents). RD-05: the denominator is all injected "
        "DB-cascade incidents, not only alerted ones, and 'majority' means "
        "strictly > 50%."
    ),
    # --- noise suppression (AC-11..AC-13, RD-04, RD-07) ------------------
    "alert_volume_system_patching": (
        "count of alerts the ANO system raised during simulated "
        "patching/maintenance windows, over the identical replay used for the "
        "baseline."
    ),
    "alert_volume_baseline_patching": (
        "count of alerts the static-threshold baseline raised during the same "
        "simulated patching/maintenance windows, over the identical replay."
    ),
    "patching_alert_reduction": (
        "= 1 - (alert_volume_system_patching / alert_volume_baseline_patching). "
        "AC-11 requires >= 0.50 and requires both raw volumes to be reported "
        "alongside the ratio."
    ),
    "recall_suppression_on": (
        "recall (same definition as the recall metric) measured on a run with "
        "suppression enabled."
    ),
    "recall_suppression_off": (
        "recall (same definition as the recall metric) measured on an "
        "otherwise identical run with suppression disabled; same seeds and "
        "scenarios, suppression is the only variable."
    ),
    "suppression_recall_delta_pp": (
        "= (recall_suppression_off - recall_suppression_on) * 100, in "
        "percentage points. RD-07: the bound is ONE-SIDED, delta <= 5pp; "
        "suppression that improves recall (negative delta) must not regress."
    ),
    "collapse_ratio": (
        "= (downstream alerts that would have fired for a single injected "
        "upstream fault) / (incidents actually emitted for it), averaged over "
        "injected upstream faults with >= 2 affected downstream entities. "
        "A ratio of 1.0 means no collapse occurred."
    ),
    "incidents_per_upstream_fault": (
        "mean number of distinct incident_id values emitted per injected "
        "upstream fault with >= 2 affected downstream entities. AC-13 requires "
        "one incident, not one alert per affected downstream entity."
    ),
    # --- semantic log analysis (AC-15) ----------------------------------
    "novel_signature_flagged": (
        "1.0 if every injected failure labelled novel_log_signature=true was "
        "flagged anomalous by the semantic outlier detector, else the fraction "
        "that were. Denominator is the count of injected novel-signature "
        "failures."
    ),
    # --- static-threshold baseline (RD-04) ------------------------------
    "baseline_recall": (
        "recall of the static-threshold baseline, computed with the identical "
        "recall definition. RD-04: reported as evidence the baseline is a real "
        "comparator and not a strawman that alerts on nothing or everything."
    ),
    "baseline_alert_volume_total": (
        "total count of alerts raised by the static-threshold baseline over the "
        "whole replay (all windows, not just patching windows)."
    ),
    "system_alert_volume_total": (
        "total count of alerts raised by the ANO system over the whole replay "
        "(all windows, not just patching windows)."
    ),
}

#: Every metric that MUST be present in the pooled block and in each per-seed
#: block. This is exactly the key set of :data:`METRIC_DEFINITIONS`.
REQUIRED_METRICS: frozenset[str] = frozenset(METRIC_DEFINITIONS)

#: Declared run parameters that MUST appear in the results file.
#: ``max_prediction_horizon_s`` is required to be declared and printed by RD-02.
REQUIRED_PARAMETERS: frozenset[str] = frozenset(
    {"max_prediction_horizon_s", "suppression_enabled", "scenario_set"}
)

#: The 19 acceptance criteria, verbatim from ``.agents/ORIGINAL_REQUEST.md``.
#: Every one needs an explicit pass/fail verdict in the results file.
ACCEPTANCE_CRITERIA: Mapping[str, str] = {
    "AC-01": (
        "A single documented command runs the full demo offline, with no cloud "
        "credentials and no network access, on a clean checkout."
    ),
    "AC-02": (
        "A single documented command runs the evaluation harness and writes a "
        "machine-readable results file."
    ),
    "AC-03": (
        "Re-running the harness with the same seed produces identical scores; "
        "the harness reports results over at least 5 distinct seeds."
    ),
    "AC-04": "The infrastructure-as-code validates/plans successfully offline.",
    "AC-05": "Recall on injected genuine incidents >= 80%.",
    "AC-06": (
        "False-positive rate <= 10%, where benign injected events (patching "
        "windows, planned deploys, traffic surges) that raise an alert count as "
        "false positives."
    ),
    "AC-07": (
        "Median lead time between first prediction and true incident onset >= "
        "15 minutes."
    ),
    "AC-08": (
        "Unresponsiveness predictions and cross-domain outage forecasts are "
        "each scored separately and each meet the recall, false-positive and "
        "lead-time bars."
    ),
    "AC-09": (
        "Root-cause domain accuracy (application / database / network) >= 70% "
        "over incidents the system alerts on."
    ),
    "AC-10": (
        "Database-contention-driven application slowness is correctly "
        "attributed to the database domain, not the application, in the "
        "majority of such injected incidents - reported as its own number."
    ),
    "AC-11": (
        "Alert volume during simulated patching/maintenance windows is reduced "
        "by >= 50% versus the static-threshold baseline implemented for "
        "comparison, with the baseline's numbers reported alongside."
    ),
    "AC-12": (
        "Suppression does not cost recall: recall with suppression enabled "
        "stays within 5 percentage points of recall with suppression disabled, "
        "and both numbers are reported."
    ),
    "AC-13": (
        "A single injected upstream fault produces one incident, not one alert "
        "per affected downstream entity; the collapse ratio is reported."
    ),
    "AC-14": (
        "Log messages are represented as vector embeddings and outlier "
        "detection operates on those embeddings, not on a hand-written pattern "
        "list."
    ),
    "AC-15": (
        "The harness includes at least one injected failure whose log signature "
        "does not appear anywhere in the training window, and the system flags "
        "it as anomalous."
    ),
    "AC-16": (
        "Synthetic Cloud Logging output validates against the real LogEntry "
        "schema; synthetic metrics validate against Prometheus exposition/GMP "
        "sample format."
    ),
    "AC-17": (
        "No third-party observability SaaS appears in the code, the IaC or the "
        "architecture document."
    ),
    "AC-18": (
        "The architecture document maps every component to a named Google Cloud "
        "service and states cost and scale characteristics at GSK estate scale."
    ),
    "AC-19": (
        "The document states what is synthetic, what would change against real "
        "GSK telemetry, and the known limitations - no claimed result that the "
        "harness does not measure."
    ),
}


@dataclass(frozen=True, slots=True)
class MetricValue:
    """One reported number, with the definition it was computed under.

    Attributes:
        name: Metric name. If it appears in :data:`METRIC_DEFINITIONS` the
            ``definition`` must match the ratified text exactly.
        value: The measured value, or ``None`` when undefined for this block
            (empty denominator). ``None`` requires a ``note``.
        definition: The exact definition string, including the denominator
            (rule R-PRINT).
        numerator: Optional numerator, for auditability.
        denominator: Optional denominator, for auditability.
        unit: Optional unit, e.g. ``"seconds"``, ``"count"``, ``"ratio"``,
            ``"percentage_points"``.
        note: Free text; required when ``value`` is ``None``.
    """

    name: str
    value: float | None
    definition: str
    numerator: float | None = None
    denominator: float | None = None
    unit: str | None = None
    note: str | None = None

    WIRE_KEYS = (
        "name",
        "value",
        "definition",
        "numerator",
        "denominator",
        "unit",
        "note",
    )

    def __post_init__(self) -> None:
        self.validate()

    def validate(self, path: str = "") -> None:
        """Raise :class:`ValidationError` if this metric record is malformed."""
        if not isinstance(self.name, str) or not self.name:
            raise ValidationError(
                "name must be a non-empty string", join_path(path, "name")
            )
        if not isinstance(self.definition, str) or not self.definition.strip():
            raise ValidationError(
                "definition must be a non-empty string (rule R-PRINT: every "
                "reported metric must state the definition, including the "
                "denominator, it was computed under)",
                join_path(path, "definition"),
            )
        ratified = METRIC_DEFINITIONS.get(self.name)
        if ratified is not None and self.definition != ratified:
            raise ValidationError(
                f"definition for ratified metric {self.name!r} does not match "
                "RATIFIED_DEFINITIONS.md. The ratified definitions may not be "
                "renegotiated; use ano.contracts.results.metric() to construct "
                "this metric.",
                join_path(path, "definition"),
            )
        if self.value is None:
            if not self.note:
                raise ValidationError(
                    "a metric with a null value must carry a note explaining why "
                    "it is undefined (an empty denominator is not a measured "
                    "zero)",
                    join_path(path, "note"),
                )
        else:
            if isinstance(self.value, bool) or not isinstance(
                self.value, (int, float)
            ):
                raise ValidationError(
                    "value must be a number or null", join_path(path, "value")
                )
            if self.value != self.value or self.value in (
                float("inf"),
                float("-inf"),
            ):
                raise ValidationError(
                    "value must be finite", join_path(path, "value")
                )
        for attr in ("numerator", "denominator"):
            number = getattr(self, attr)
            if number is None:
                continue
            if isinstance(number, bool) or not isinstance(number, (int, float)):
                raise ValidationError(
                    f"{attr} must be a number or null", join_path(path, attr)
                )
        for attr in ("unit", "note"):
            text = getattr(self, attr)
            if text is not None and not isinstance(text, str):
                raise ValidationError(
                    f"{attr} must be a string or null", join_path(path, attr)
                )

    def to_dict(self) -> dict[str, Any]:
        """Serialise to the C5 wire form."""
        return {
            "name": self.name,
            "value": None if self.value is None else float(self.value),
            "definition": self.definition,
            "numerator": None if self.numerator is None else float(self.numerator),
            "denominator": (
                None if self.denominator is None else float(self.denominator)
            ),
            "unit": self.unit,
            "note": self.note,
        }

    @classmethod
    def from_dict(cls, obj: Any, path: str = "") -> "MetricValue":
        """Parse and strictly validate one metric record."""
        mapping = require_mapping(obj, path)
        reject_unknown_keys(mapping, cls.WIRE_KEYS, path)
        value = mapping["value"] if "value" in mapping else None
        if "value" not in mapping:
            raise ValidationError("missing required key 'value'", path)
        parsed_value = (
            None if value is None else take_float(mapping, "value", path)
        )
        numerator = (
            None
            if mapping.get("numerator") is None
            else take_float(mapping, "numerator", path)
        )
        denominator = (
            None
            if mapping.get("denominator") is None
            else take_float(mapping, "denominator", path)
        )
        return cls(
            name=take_str(mapping, "name", path),
            value=parsed_value,
            definition=take_str(mapping, "definition", path),
            numerator=numerator,
            denominator=denominator,
            unit=take_optional_str(mapping, "unit", path)
            if "unit" in mapping
            else None,
            note=take_optional_str(mapping, "note", path) if "note" in mapping else None,
        )


def metric(
    name: str,
    value: float | None,
    *,
    numerator: float | None = None,
    denominator: float | None = None,
    unit: str | None = None,
    note: str | None = None,
    definition: str | None = None,
) -> MetricValue:
    """Construct a :class:`MetricValue`, filling in the ratified definition.

    For any ``name`` in :data:`METRIC_DEFINITIONS` the definition is looked up,
    so the harness cannot accidentally (or conveniently) report a ratified
    metric under a different definition. For a metric of the harness's own
    invention, pass ``definition`` explicitly — R-PRINT applies to every metric,
    not only the ratified ones.

    Raises:
        ValidationError: if ``name`` is unknown and no ``definition`` is given,
            or if ``definition`` is supplied for a ratified metric and differs.
    """
    ratified = METRIC_DEFINITIONS.get(name)
    if ratified is None:
        if not definition:
            raise ValidationError(
                f"{name!r} is not a ratified metric, so an explicit definition "
                "is required (rule R-PRINT)",
                name,
            )
        resolved = definition
    else:
        if definition is not None and definition != ratified:
            raise ValidationError(
                f"{name!r} is a ratified metric; its definition may not be "
                "overridden",
                name,
            )
        resolved = ratified
    return MetricValue(
        name=name,
        value=value,
        definition=resolved,
        numerator=numerator,
        denominator=denominator,
        unit=unit,
        note=note,
    )


@dataclass(frozen=True, slots=True)
class MetricSet:
    """An ordered, name-unique collection of :class:`MetricValue`."""

    metrics: tuple[MetricValue, ...]

    def __post_init__(self) -> None:
        self.validate()

    def validate(self, path: str = "", *, require: Iterable[str] = ()) -> None:
        """Validate each metric, uniqueness of names, and required presence.

        Args:
            path: Validation path prefix.
            require: Metric names that must be present.
        """
        seen: set[str] = set()
        for index, item in enumerate(self.metrics):
            item_path = join_path(path, index)
            item.validate(item_path)
            if item.name in seen:
                raise ValidationError(f"duplicate metric {item.name!r}", item_path)
            seen.add(item.name)
        missing = sorted(set(require) - seen)
        if missing:
            raise ValidationError(
                "missing required metric(s): " + ", ".join(missing), path
            )

    def __contains__(self, name: object) -> bool:
        return any(item.name == name for item in self.metrics)

    def __len__(self) -> int:
        return len(self.metrics)

    def __iter__(self):
        return iter(self.metrics)

    def get(self, name: str) -> MetricValue | None:
        """Return the metric named ``name``, or ``None``."""
        for item in self.metrics:
            if item.name == name:
                return item
        return None

    def value(self, name: str) -> float | None:
        """Return the value of metric ``name``.

        Raises:
            KeyError: if the metric is absent.
        """
        found = self.get(name)
        if found is None:
            raise KeyError(name)
        return found.value

    def names(self) -> tuple[str, ...]:
        """Metric names in order."""
        return tuple(item.name for item in self.metrics)

    def to_list(self) -> list[dict[str, Any]]:
        """Serialise to the C5 wire form (a JSON array)."""
        return [item.to_dict() for item in self.metrics]

    @classmethod
    def from_list(cls, obj: Any, path: str = "") -> "MetricSet":
        """Parse and strictly validate a metric array."""
        if isinstance(obj, (str, bytes)) or not isinstance(obj, Sequence):
            raise ValidationError(
                f"expected an array of metrics, got {type(obj).__name__}", path
            )
        return cls(
            tuple(
                MetricValue.from_dict(item, join_path(path, index))
                for index, item in enumerate(obj)
            )
        )


@dataclass(frozen=True, slots=True)
class SeedBlock:
    """Results for one seed.

    Attributes:
        seed: The seed these numbers were produced from.
        metrics: Every required metric for this seed (RD-06 requires per-seed
            numbers to be published alongside the pooled aggregate).
        scenario_count: Number of scenarios replayed for this seed.
        prediction_count: Number of C2 predictions the system emitted.
    """

    seed: int
    metrics: MetricSet
    scenario_count: int
    prediction_count: int

    WIRE_KEYS = ("seed", "metrics", "scenario_count", "prediction_count")

    def __post_init__(self) -> None:
        self.validate()

    def validate(self, path: str = "") -> None:
        """Raise :class:`ValidationError` if this seed block is malformed."""
        if isinstance(self.seed, bool) or not isinstance(self.seed, int):
            raise ValidationError("seed must be an integer", join_path(path, "seed"))
        for attr in ("scenario_count", "prediction_count"):
            number = getattr(self, attr)
            if isinstance(number, bool) or not isinstance(number, int) or number < 0:
                raise ValidationError(
                    f"{attr} must be a non-negative integer", join_path(path, attr)
                )
        self.metrics.validate(join_path(path, "metrics"), require=REQUIRED_METRICS)

    def to_dict(self) -> dict[str, Any]:
        """Serialise to the C5 wire form."""
        return {
            "seed": self.seed,
            "metrics": self.metrics.to_list(),
            "scenario_count": self.scenario_count,
            "prediction_count": self.prediction_count,
        }

    @classmethod
    def from_dict(cls, obj: Any, path: str = "") -> "SeedBlock":
        """Parse and strictly validate one seed block."""
        mapping = require_mapping(obj, path)
        reject_unknown_keys(mapping, cls.WIRE_KEYS, path)
        return cls(
            seed=take_int(mapping, "seed", path),
            metrics=MetricSet.from_list(
                take_list(mapping, "metrics", path), join_path(path, "metrics")
            ),
            scenario_count=take_int(mapping, "scenario_count", path, minimum=0),
            prediction_count=take_int(mapping, "prediction_count", path, minimum=0),
        )


@dataclass(frozen=True, slots=True)
class BaselineThreshold:
    """One rule of the static-threshold comparator.

    RD-04 requires the baseline's thresholds to be "conventional and documented,
    published as a table in both the architecture doc and the results file, and
    fixed before measurement". This is that table's row type.

    Attributes:
        name: Rule name, e.g. ``"cpu_saturation"``.
        signal: The signal it watches, e.g. ``"node_cpu_utilisation"``.
        comparator: One of ``">"``, ``">="``, ``"<"``, ``"<="``.
        threshold: The fixed threshold value.
        unit: Unit of ``threshold``.
        applies_to: Entity kind the rule applies to, or ``"*"`` for all.
        rationale: Why this is the conventional value (so a reviewer can judge
            whether the comparator is fair rather than a strawman).
    """

    name: str
    signal: str
    comparator: str
    threshold: float
    unit: str
    applies_to: str
    rationale: str

    WIRE_KEYS = (
        "name",
        "signal",
        "comparator",
        "threshold",
        "unit",
        "applies_to",
        "rationale",
    )
    COMPARATORS = (">", ">=", "<", "<=")

    def __post_init__(self) -> None:
        self.validate()

    def validate(self, path: str = "") -> None:
        """Raise :class:`ValidationError` if this threshold row is malformed."""
        for attr in ("name", "signal", "unit", "applies_to", "rationale"):
            value = getattr(self, attr)
            if not isinstance(value, str) or not value:
                raise ValidationError(
                    f"{attr} must be a non-empty string", join_path(path, attr)
                )
        if self.comparator not in self.COMPARATORS:
            raise ValidationError(
                f"comparator must be one of {self.COMPARATORS}, got "
                f"{self.comparator!r}",
                join_path(path, "comparator"),
            )
        if isinstance(self.threshold, bool) or not isinstance(
            self.threshold, (int, float)
        ):
            raise ValidationError(
                "threshold must be a number", join_path(path, "threshold")
            )

    def to_dict(self) -> dict[str, Any]:
        """Serialise to the C5 wire form."""
        return {
            "name": self.name,
            "signal": self.signal,
            "comparator": self.comparator,
            "threshold": float(self.threshold),
            "unit": self.unit,
            "applies_to": self.applies_to,
            "rationale": self.rationale,
        }

    @classmethod
    def from_dict(cls, obj: Any, path: str = "") -> "BaselineThreshold":
        """Parse and strictly validate one threshold row."""
        mapping = require_mapping(obj, path)
        reject_unknown_keys(mapping, cls.WIRE_KEYS, path)
        return cls(
            name=take_str(mapping, "name", path),
            signal=take_str(mapping, "signal", path),
            comparator=take_str(mapping, "comparator", path),
            threshold=take_float(mapping, "threshold", path),
            unit=take_str(mapping, "unit", path),
            applies_to=take_str(mapping, "applies_to", path),
            rationale=take_str(mapping, "rationale", path),
        )


@dataclass(frozen=True, slots=True)
class BaselineComparison:
    """The static-threshold baseline block required by AC-11 and RD-04.

    Attributes:
        thresholds: The fixed, documented rule table. Must be non-empty — a
            baseline with no rules is not a comparator.
        metrics: Baseline metrics, which must include ``baseline_recall``
            (RD-04's anti-strawman requirement) and both patching-window alert
            volumes (AC-11's "baseline numbers reported alongside").
        fixed_before_measurement: Must be ``True``; RD-04 requires the
            thresholds to be fixed before measurement. Recording it in the file
            makes the claim explicit rather than implied.
    """

    thresholds: tuple[BaselineThreshold, ...]
    metrics: MetricSet
    fixed_before_measurement: bool = True

    WIRE_KEYS = ("thresholds", "metrics", "fixed_before_measurement")

    #: Metrics the baseline block must carry.
    REQUIRED = frozenset(
        {
            "baseline_recall",
            "baseline_alert_volume_total",
            "alert_volume_baseline_patching",
        }
    )

    def __post_init__(self) -> None:
        self.validate()

    def validate(self, path: str = "") -> None:
        """Raise :class:`ValidationError` if the baseline block is malformed."""
        if not isinstance(self.thresholds, tuple) or not self.thresholds:
            raise ValidationError(
                "the static-threshold baseline must publish at least one "
                "threshold rule (RD-04: conventional, documented and fixed "
                "before measurement)",
                join_path(path, "thresholds"),
            )
        for index, row in enumerate(self.thresholds):
            row.validate(join_path(join_path(path, "thresholds"), index))
        if self.fixed_before_measurement is not True:
            raise ValidationError(
                "RD-04 requires the baseline thresholds to be fixed before "
                "measurement",
                join_path(path, "fixed_before_measurement"),
            )
        self.metrics.validate(join_path(path, "metrics"), require=self.REQUIRED)

    def to_dict(self) -> dict[str, Any]:
        """Serialise to the C5 wire form."""
        return {
            "thresholds": [row.to_dict() for row in self.thresholds],
            "metrics": self.metrics.to_list(),
            "fixed_before_measurement": self.fixed_before_measurement,
        }

    @classmethod
    def from_dict(cls, obj: Any, path: str = "") -> "BaselineComparison":
        """Parse and strictly validate the baseline block."""
        mapping = require_mapping(obj, path)
        reject_unknown_keys(mapping, cls.WIRE_KEYS, path)
        thresholds_raw = take_list(mapping, "thresholds", path)
        thresholds = tuple(
            BaselineThreshold.from_dict(
                item, join_path(join_path(path, "thresholds"), index)
            )
            for index, item in enumerate(thresholds_raw)
        )
        return cls(
            thresholds=thresholds,
            metrics=MetricSet.from_list(
                take_list(mapping, "metrics", path), join_path(path, "metrics")
            ),
            fixed_before_measurement=take_bool(
                mapping, "fixed_before_measurement", path
            ),
        )


@dataclass(frozen=True, slots=True)
class AcceptanceVerdict:
    """Explicit pass/fail for one acceptance criterion.

    Attributes:
        criterion_id: ``AC-01``..``AC-19``.
        title: The criterion text (from :data:`ACCEPTANCE_CRITERIA`).
        passed: The verdict, or ``None`` for "not evaluated by the harness"
            (e.g. the documentation criteria), which requires a ``detail``
            saying who does evaluate it. ``None`` is not a pass.
        detail: Evidence: the numbers compared and the threshold applied.
        metric_names: Names of the metrics the verdict was computed from.
    """

    criterion_id: str
    title: str
    passed: bool | None
    detail: str
    metric_names: tuple[str, ...] = ()

    WIRE_KEYS = ("criterion_id", "title", "passed", "detail", "metric_names")

    def __post_init__(self) -> None:
        self.validate()

    def validate(self, path: str = "") -> None:
        """Raise :class:`ValidationError` if this verdict is malformed."""
        if self.criterion_id not in ACCEPTANCE_CRITERIA:
            raise ValidationError(
                f"{self.criterion_id!r} is not a known acceptance criterion id "
                "(AC-01..AC-19)",
                join_path(path, "criterion_id"),
            )
        expected_title = ACCEPTANCE_CRITERIA[self.criterion_id]
        if self.title != expected_title:
            raise ValidationError(
                f"title for {self.criterion_id} does not match the criterion "
                "text in ORIGINAL_REQUEST.md",
                join_path(path, "title"),
            )
        if self.passed is not None and not isinstance(self.passed, bool):
            raise ValidationError(
                "passed must be true, false or null", join_path(path, "passed")
            )
        if not isinstance(self.detail, str) or not self.detail:
            raise ValidationError(
                "detail must be a non-empty string stating the evidence for the "
                "verdict",
                join_path(path, "detail"),
            )
        if not isinstance(self.metric_names, tuple):
            raise ValidationError(
                "metric_names must be a tuple of metric names",
                join_path(path, "metric_names"),
            )
        for index, name in enumerate(self.metric_names):
            if not isinstance(name, str) or not name:
                raise ValidationError(
                    "must be a non-empty string",
                    join_path(join_path(path, "metric_names"), index),
                )

    def to_dict(self) -> dict[str, Any]:
        """Serialise to the C5 wire form."""
        return {
            "criterion_id": self.criterion_id,
            "title": self.title,
            "passed": self.passed,
            "detail": self.detail,
            "metric_names": list(self.metric_names),
        }

    @classmethod
    def from_dict(cls, obj: Any, path: str = "") -> "AcceptanceVerdict":
        """Parse and strictly validate one verdict."""
        mapping = require_mapping(obj, path)
        reject_unknown_keys(mapping, cls.WIRE_KEYS, path)
        if "passed" not in mapping:
            raise ValidationError("missing required key 'passed'", path)
        raw_passed = mapping["passed"]
        if raw_passed is not None and not isinstance(raw_passed, bool):
            raise ValidationError(
                "passed must be true, false or null", join_path(path, "passed")
            )
        names_raw = take_list(mapping, "metric_names", path)
        names: list[str] = []
        for index, item in enumerate(names_raw):
            if not isinstance(item, str) or not item:
                raise ValidationError(
                    "must be a non-empty string",
                    join_path(join_path(path, "metric_names"), index),
                )
            names.append(item)
        return cls(
            criterion_id=take_str(mapping, "criterion_id", path),
            title=take_str(mapping, "title", path),
            passed=raw_passed,
            detail=take_str(mapping, "detail", path),
            metric_names=tuple(names),
        )

    @classmethod
    def for_criterion(
        cls,
        criterion_id: str,
        passed: bool | None,
        detail: str,
        metric_names: Iterable[str] = (),
    ) -> "AcceptanceVerdict":
        """Build a verdict, taking the title from :data:`ACCEPTANCE_CRITERIA`."""
        if criterion_id not in ACCEPTANCE_CRITERIA:
            raise ValidationError(
                f"{criterion_id!r} is not a known acceptance criterion id",
                "criterion_id",
            )
        return cls(
            criterion_id=criterion_id,
            title=ACCEPTANCE_CRITERIA[criterion_id],
            passed=passed,
            detail=detail,
            metric_names=tuple(metric_names),
        )


@dataclass(frozen=True, slots=True)
class ResultsFile:
    """A complete C5 harness results file.

    Attributes:
        schema_version: :data:`SCHEMA_VERSION`.
        generated_at: Wall-clock time of the run. **Excluded** from the
            determinism comparison (D2).
        runtime_s: Wall-clock duration. Also excluded.
        seeds: The seeds evaluated. At least 5 distinct seeds (AC-03).
        per_seed: One block per seed, in the same order as ``seeds``.
        pooled: The pooled (micro) aggregate across all seeds. RD-06: pass/fail
            is decided on this block.
        worst_seed: The seed with the worst headline recall. RD-06 requires the
            worst-seed number to be published for honesty.
        baseline: The static-threshold comparator block.
        acceptance: Exactly one verdict for each of AC-01..AC-19.
        parameters: Declared run parameters; must include
            :data:`REQUIRED_PARAMETERS`.
    """

    schema_version: str
    generated_at: _dt.datetime
    runtime_s: float
    seeds: tuple[int, ...]
    per_seed: tuple[SeedBlock, ...]
    pooled: MetricSet
    worst_seed: int
    baseline: BaselineComparison
    acceptance: tuple[AcceptanceVerdict, ...]
    parameters: Mapping[str, Any]

    WIRE_KEYS = (
        "schema_version",
        "generated_at",
        "runtime_s",
        "seeds",
        "per_seed",
        "pooled",
        "worst_seed",
        "baseline",
        "acceptance",
        "parameters",
    )

    #: Fields excluded from :meth:`canonical_payload` because they are
    #: wall-clock and would defeat the byte-identical comparison AC-03 needs.
    NON_DETERMINISTIC_FIELDS = ("generated_at", "runtime_s")

    def __post_init__(self) -> None:
        self.validate()

    def validate(self, path: str = "") -> None:
        """Raise :class:`ValidationError` if the results file is malformed."""
        if self.schema_version != SCHEMA_VERSION:
            raise ValidationError(
                f"unsupported schema_version {self.schema_version!r}; this build "
                f"speaks {SCHEMA_VERSION!r}",
                join_path(path, "schema_version"),
            )
        format_rfc3339(self.generated_at, join_path(path, "generated_at"))
        if isinstance(self.runtime_s, bool) or not isinstance(
            self.runtime_s, (int, float)
        ):
            raise ValidationError(
                "runtime_s must be a number", join_path(path, "runtime_s")
            )
        if self.runtime_s < 0:
            raise ValidationError(
                "runtime_s must be non-negative", join_path(path, "runtime_s")
            )
        if len(set(self.seeds)) != len(self.seeds):
            raise ValidationError(
                "seeds must be distinct", join_path(path, "seeds")
            )
        if len(self.seeds) < 5:
            raise ValidationError(
                f"AC-03 requires results over at least 5 distinct seeds, got "
                f"{len(self.seeds)}",
                join_path(path, "seeds"),
            )
        if tuple(block.seed for block in self.per_seed) != tuple(self.seeds):
            raise ValidationError(
                "per_seed blocks must appear in the same order as, and cover "
                "exactly, the declared seeds",
                join_path(path, "per_seed"),
            )
        for index, block in enumerate(self.per_seed):
            block.validate(join_path(join_path(path, "per_seed"), index))
        self.pooled.validate(join_path(path, "pooled"), require=REQUIRED_METRICS)
        if self.worst_seed not in self.seeds:
            raise ValidationError(
                f"worst_seed {self.worst_seed} is not one of the evaluated seeds",
                join_path(path, "worst_seed"),
            )
        self.baseline.validate(join_path(path, "baseline"))

        seen_ids: set[str] = set()
        for index, verdict in enumerate(self.acceptance):
            verdict.validate(join_path(join_path(path, "acceptance"), index))
            if verdict.criterion_id in seen_ids:
                raise ValidationError(
                    f"duplicate verdict for {verdict.criterion_id}",
                    join_path(join_path(path, "acceptance"), index),
                )
            seen_ids.add(verdict.criterion_id)
        missing = sorted(set(ACCEPTANCE_CRITERIA) - seen_ids)
        if missing:
            raise ValidationError(
                "missing an explicit pass/fail verdict for: " + ", ".join(missing),
                join_path(path, "acceptance"),
            )

        parameters = require_mapping(self.parameters, join_path(path, "parameters"))
        missing_params = sorted(REQUIRED_PARAMETERS - set(parameters))
        if missing_params:
            raise ValidationError(
                "missing required declared parameter(s): "
                + ", ".join(missing_params),
                join_path(path, "parameters"),
            )
        horizon = parameters["max_prediction_horizon_s"]
        if horizon != MAX_PREDICTION_HORIZON_S:
            raise ValidationError(
                f"RD-02 fixes the maximum prediction horizon at "
                f"{MAX_PREDICTION_HORIZON_S}s; got {horizon!r}",
                join_path(join_path(path, "parameters"), "max_prediction_horizon_s"),
            )

    # -- convenience -------------------------------------------------------
    def verdict(self, criterion_id: str) -> AcceptanceVerdict:
        """Return the verdict for ``criterion_id``.

        Raises:
            KeyError: if absent (validation makes this impossible for a valid
                file).
        """
        for item in self.acceptance:
            if item.criterion_id == criterion_id:
                return item
        raise KeyError(criterion_id)

    def all_passed(self) -> bool:
        """True only if every criterion has an explicit ``passed is True``."""
        return all(item.passed is True for item in self.acceptance)

    def seed_block(self, seed: int) -> SeedBlock:
        """Return the block for ``seed``."""
        for block in self.per_seed:
            if block.seed == seed:
                return block
        raise KeyError(seed)

    def to_dict(self) -> dict[str, Any]:
        """Serialise the whole file to the C5 wire form."""
        return {
            "schema_version": self.schema_version,
            "generated_at": format_rfc3339(self.generated_at),
            "runtime_s": float(self.runtime_s),
            "seeds": list(self.seeds),
            "per_seed": [block.to_dict() for block in self.per_seed],
            "pooled": self.pooled.to_list(),
            "worst_seed": self.worst_seed,
            "baseline": self.baseline.to_dict(),
            "acceptance": [item.to_dict() for item in self.acceptance],
            "parameters": dict(self.parameters),
        }

    def canonical_payload(self) -> dict[str, Any]:
        """The wire form with wall-clock fields removed.

        This is what AC-03's "identical scores" is checked over. Comparing whole
        files would fail on the timestamp alone and prove nothing.
        """
        payload = self.to_dict()
        for field_name in self.NON_DETERMINISTIC_FIELDS:
            payload.pop(field_name, None)
        return payload

    def determinism_digest(self) -> str:
        """Stable blake2b digest of :meth:`canonical_payload`.

        Two runs of the same seeds agree if and only if these digests agree.
        """
        return stable_hash_hex(
            dumps_canonical(self.canonical_payload()), "results", "determinism"
        )

    def to_json(self, indent: int = 2) -> str:
        """Serialise to deterministic, human-readable JSON text."""
        return dumps_pretty(self.to_dict(), indent=indent)

    @classmethod
    def from_dict(cls, obj: Any, path: str = "") -> "ResultsFile":
        """Parse and strictly validate a whole results file from a dict."""
        mapping = require_mapping(obj, path)
        reject_unknown_keys(mapping, cls.WIRE_KEYS, path)
        seeds_raw = take_list(mapping, "seeds", path)
        seeds: list[int] = []
        for index, item in enumerate(seeds_raw):
            item_path = join_path(join_path(path, "seeds"), index)
            if isinstance(item, bool) or not isinstance(item, int):
                raise ValidationError("seed must be an integer", item_path)
            seeds.append(item)
        per_seed_raw = take_list(mapping, "per_seed", path)
        per_seed = tuple(
            SeedBlock.from_dict(item, join_path(join_path(path, "per_seed"), index))
            for index, item in enumerate(per_seed_raw)
        )
        acceptance_raw = take_list(mapping, "acceptance", path)
        acceptance = tuple(
            AcceptanceVerdict.from_dict(
                item, join_path(join_path(path, "acceptance"), index)
            )
            for index, item in enumerate(acceptance_raw)
        )
        return cls(
            schema_version=take_str(mapping, "schema_version", path),
            generated_at=take_timestamp(mapping, "generated_at", path),
            runtime_s=take_float(mapping, "runtime_s", path, minimum=0.0),
            seeds=tuple(seeds),
            per_seed=per_seed,
            pooled=MetricSet.from_list(
                take_list(mapping, "pooled", path), join_path(path, "pooled")
            ),
            worst_seed=take_int(mapping, "worst_seed", path),
            baseline=BaselineComparison.from_dict(
                take_mapping(mapping, "baseline", path), join_path(path, "baseline")
            ),
            acceptance=acceptance,
            parameters=dict(take_mapping(mapping, "parameters", path)),
        )


def parse_results_file(text: str) -> ResultsFile:
    """Parse C5 JSON text into a validated :class:`ResultsFile`."""
    return ResultsFile.from_dict(loads(text))


def read_results_file(path: str | os.PathLike[str]) -> ResultsFile:
    """Read and validate a C5 results file from disk."""
    with open(path, "r", encoding="utf-8") as handle:
        return parse_results_file(handle.read())


def write_results_file(
    path: str | os.PathLike[str], results: ResultsFile, indent: int = 2
) -> None:
    """Validate and write a C5 results file, creating parent directories."""
    results.validate()
    parent = os.path.dirname(os.fspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(results.to_json(indent=indent))
