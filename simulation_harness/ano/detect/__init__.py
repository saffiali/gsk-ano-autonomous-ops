"""M4 — Detection, dynamic baselines, change-awareness, suppression, collapse.

This package owns requirements **R2** (unresponsiveness prediction) and **R4**
(dynamic baselines, change-awareness, noise suppression, alert collapse).
Cross-domain attribution (**R3**) lives in the sibling package
:mod:`ano.correlate`, which this package drives.

Separation rule (requirement R5), enforced by
``tests/m4_detect/test_separation.py``: nothing under ``ano/`` may import
``harness`` or ``scenariogen``, and nothing may read a ground-truth label file.
The detector sees telemetry and two *operational* inputs — a topology (a CMDB
extract) and a change calendar — and nothing else.

Layering, bottom up::

    robust.py        hand-rolled robust statistics (median/MAD, Theil-Sen, ...)
    features.py      raw exporter samples  ->  per-entity derived feature series
    baselines.py     seasonal per-entity/hour/day-of-week learned baselines
    signals.py       the signal catalogue: how a feature becomes a severity
    unresponsiveness.py   R2 detector over host + application signal families
    change.py        change calendar, and change inference from telemetry
    suppress.py      marks (never deletes) predictions explained by a change
    collapse.py      one upstream fault -> one incident_id
    remediation.py   the operator-facing action text
    engine.py        wires it together; the entry point the pipeline calls

Everything is Python 3.13 standard library only.
"""

from __future__ import annotations

__all__ = [
    "robust",
    "features",
    "baselines",
    "signals",
    "unresponsiveness",
    "change",
    "suppress",
    "collapse",
    "remediation",
    "engine",
]
