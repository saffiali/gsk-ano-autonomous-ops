"""R3 — cross-domain correlation and root-cause attribution.

Two modules:

``topology``
    The maintained estate graph: which Apache/Tomcat servers query which
    database instances, and which switch ports their traffic traverses. A
    durable, queryable, dumpable model (feature #50) rather than joins invented
    at query time.

``attribution``
    Uses that graph to decide *which domain and which entity* is responsible
    for an observed degradation, and to forecast the resulting outage.

``discovery``
    A fallback that infers topology from telemetry when no CMDB extract is
    available. Secondary by ruling IR-04, and it records that it was used.

The topology and the change calendar are **operations artefacts** — a CMDB
extract and a change schedule — not ground truth. Reading them at inference
time is what a real system does and does not violate requirement R5. Nothing
here may import ``harness``, ``scenariogen`` or ``ano.contracts.labels``.
"""

from __future__ import annotations

__all__ = ["topology", "attribution", "discovery"]
