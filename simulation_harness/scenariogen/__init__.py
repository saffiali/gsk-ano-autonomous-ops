"""Deterministic scenario generator with ground truth (requirement R5).

``scenariogen`` is the **verification substrate**: it produces the telemetry the
rest of the system is measured against, and the C1 label file those
measurements are scored with. If the shapes here are wrong, every downstream
number is worthless.

Separation rule (R5)
--------------------
This package MUST NOT import ``ano.detect``, ``ano.correlate`` or
``ano.semantic``, and contains no knowledge of any detector's internals or
thresholds. It depends only on ``ano.contracts`` (shared vocabulary) and
``ano.telemetry`` (the wire formats). That is checked mechanically by
``tests/m2_generator/test_separation.py`` and by the E2E ``INV-SEP``
architecture invariant.

The generator is not tuned to make detectors look good. It models phenomena and
lets the detectors earn their score.

Modules
-------
``estate``       entities, topology and identity
``timeline``     the scrape grid: fixed 48-hour window, variable history depth
``seasonality``  per-entity hour-of-day, day-of-week, trend and AR(1) noise
``scenarios``    what happens to whom and when, plus the operations calendar
``simulate``     physical state per tick; metrics are consequences, not labels
``emit_metrics`` state -> per-target Prometheus exposition documents
``emit_logs``    state -> Cloud Logging entries
``generate``     the CLI, ``python3 -m scenariogen.generate --seed N --out DIR``
"""
