"""Evaluation harness for the GSK ANO reference implementation (milestone M5).

This package is the **only** component permitted to read ground-truth labels
(requirement R5). It replays generated scenarios through the system, scores the
system's contract-C2 predictions against the contract-C1 ground truth, runs a
static-threshold baseline over the identical replay for comparison, and writes
the contract-C5 machine-readable results file.

Every metric is computed exactly as specified in
``.agents/teamwork_preview_orchestrator/RATIFIED_DEFINITIONS.md`` (RD-01..RD-10).
The ratified definition strings themselves live in
:data:`ano.contracts.results.METRIC_DEFINITIONS` and are injected verbatim, so a
metric cannot be reported under a home-made definition.

Separation invariants this package upholds:

* it imports ``ano.contracts`` only — never ``ano.detect``, ``ano.correlate`` or
  ``ano.semantic`` internals. Detector output is consumed through contract C2.
* nothing under ``ano/`` may import ``harness``.

Entry point::

    python3 -m harness.run --seeds 5 --out artifacts/results.json
"""

__all__ = []
