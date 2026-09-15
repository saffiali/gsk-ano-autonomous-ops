"""GSK Autonomous Network Operations (ANO) reference implementation.

Top-level package for the production-path code. Everything under ``ano`` is
written against the **Python 3.13 standard library only** — see
``.agents/teamwork_preview_orchestrator/DECISIONS.md`` (D1) for why.

Sub-packages (write ownership per ``PROJECT.md``):

======================  ==========================================================
``ano.config``          Single configuration object; selects local vs cloud
                        backends by configuration only (requirement R6).
``ano.contracts``       The shared vocabulary: C1 ground-truth labels, C2
                        predictions, C5 harness results, plus the determinism
                        utilities every stochastic component must use.
``ano.gcp``             Google Cloud service adapters with offline local
                        implementations (Cloud Logging, Managed Prometheus,
                        Pub/Sub, Dataflow, BigQuery, Cloud Monitoring).
``ano.telemetry``       LogEntry / Prometheus exposition models (milestone M2).
``ano.ingest``          Pipeline stages (milestone M3).
``ano.semantic``        Log embeddings + semantic outlier detection (M3).
``ano.detect``          Unresponsiveness, baselines, suppression, collapse (M4).
``ano.correlate``       Topology + cross-domain attribution (M4).
``ano.demo``            Operator-facing surface (M6).
``ano.serve``           Cloud Run / Cloud Functions entrypoints (M6).
======================  ==========================================================

Architectural invariant (requirement R5, enforced by the E2E track): nothing
under ``ano`` may import ``harness`` or read a ground-truth label file. The
ground truth is visible to the evaluation harness only.
"""

__all__ = ["__version__"]

#: Version of the reference implementation. Bumped by the project orchestrator.
__version__ = "1.0.0"
