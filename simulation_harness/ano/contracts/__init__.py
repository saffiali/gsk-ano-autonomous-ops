"""``ano.contracts`` — the shared vocabulary every milestone codes against.

This package is a **published API**. Three milestones are written in parallel
against it, so its shapes are stable and its validators are strict: a malformed
record fails loudly at the boundary instead of producing a plausible-looking
wrong number three stages later.

What is in here
---------------

===========================  =============================================
:mod:`ano.contracts.labels`  **C1** ground-truth label file
                             (``scenariogen`` -> ``harness`` ONLY)
:mod:`ano.contracts.prediction`
                             **C2** detector output / Prediction records
                             (detectors -> ``harness`` and ``ano.demo``)
:mod:`ano.contracts.results` **C5** harness results file
                             (``harness`` -> auditor)
:mod:`ano.contracts.determinism`
                             seeded RNG factory + stable hashing
:mod:`ano.contracts.common`  errors, RFC 3339, canonical JSON, validators
===========================  =============================================

Conventions that hold across all three contracts
------------------------------------------------
* **Timestamps** are timezone-aware UTC ``datetime`` in Python and canonical
  RFC 3339 strings on the wire (``2026-09-13T12:00:00Z``). Naive datetimes are
  rejected.
* **Unknown keys are rejected.** A key the contract does not define is a
  malformed record, not a forward-compatible extension.
* **Records are frozen dataclasses** and validate themselves on construction,
  so an invalid object cannot exist. Every type also offers ``to_dict()`` /
  ``from_dict()`` / ``validate()``.
* **Errors** are always :class:`ano.contracts.common.ValidationError`, carrying
  a path such as ``events[3].root_cause.domain``.
* **Serialisation is canonical** (sorted keys, fixed timestamp rendering), so
  identical data produces identical bytes. Determinism is an acceptance
  criterion, not a nicety.

Quick start::

    from ano.contracts import Prediction, Signal, Attribution, rng

    generator = rng(1234, "my_detector")          # never global random.*
    prediction = Prediction.from_parts(
        prediction_id="pred-0001",
        emitted_at=decision_time,
        track="unresponsiveness",
        entity="tomcat-07",
        time_to_failure_s=1200,
        confidence=0.82,
        signals=[Signal("thread_pool_utilisation", 0.97, 0.41, 0.6)],
        root_cause=Attribution("application", "tomcat-07"),
        recommended_remediation="drain and recycle the Tomcat worker pool",
    )
"""

from ano.contracts.common import (
    UTC,
    ValidationError,
    dumps_canonical,
    dumps_pretty,
    format_rfc3339,
    parse_rfc3339,
)
from ano.contracts.determinism import (
    derive_seed,
    hash_stream,
    rng,
    stable_bucket,
    stable_digest,
    stable_hash_hex,
    stable_hash_int,
    stable_id,
    stable_sign,
    stable_unit_float,
)
from ano.contracts.labels import (
    BENIGN_KINDS,
    DOMAINS,
    ENTITY_KINDS,
    EVENT_KINDS,
    GENUINE_KINDS,
    NORMAL_KINDS,
    TRACKS,
    Entity,
    GroundTruthEvent,
    LabelFile,
    RootCause,
    Window,
    parse_label_file,
    write_label_file,
)
from ano.contracts.prediction import (
    PREDICTION_TRACKS,
    Attribution,
    Evidence,
    Prediction,
    Signal,
    parse_prediction,
    parse_predictions_jsonl,
    predictions_to_jsonl,
    read_predictions,
    write_predictions,
)
from ano.contracts.results import (
    ACCEPTANCE_CRITERIA,
    MAX_PREDICTION_HORIZON_S,
    METRIC_DEFINITIONS,
    REQUIRED_METRICS,
    SCHEMA_VERSION,
    AcceptanceVerdict,
    BaselineComparison,
    BaselineThreshold,
    MetricSet,
    MetricValue,
    ResultsFile,
    SeedBlock,
    metric,
    parse_results_file,
    read_results_file,
    write_results_file,
)

__all__ = [
    # common
    "UTC",
    "ValidationError",
    "parse_rfc3339",
    "format_rfc3339",
    "dumps_canonical",
    "dumps_pretty",
    # determinism
    "rng",
    "derive_seed",
    "stable_digest",
    "stable_hash_hex",
    "stable_hash_int",
    "stable_unit_float",
    "stable_bucket",
    "stable_sign",
    "stable_id",
    "hash_stream",
    # C1
    "ENTITY_KINDS",
    "DOMAINS",
    "TRACKS",
    "EVENT_KINDS",
    "GENUINE_KINDS",
    "BENIGN_KINDS",
    "NORMAL_KINDS",
    "Entity",
    "RootCause",
    "Window",
    "GroundTruthEvent",
    "LabelFile",
    "parse_label_file",
    "write_label_file",
    # C2
    "PREDICTION_TRACKS",
    "Signal",
    "Evidence",
    "Attribution",
    "Prediction",
    "parse_prediction",
    "parse_predictions_jsonl",
    "predictions_to_jsonl",
    "read_predictions",
    "write_predictions",
    # C5
    "SCHEMA_VERSION",
    "MAX_PREDICTION_HORIZON_S",
    "METRIC_DEFINITIONS",
    "REQUIRED_METRICS",
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
