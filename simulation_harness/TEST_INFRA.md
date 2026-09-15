# TEST_INFRA.md — the GSK ANO opaque-box E2E suite

This document describes the end-to-end test infrastructure: what it believes,
how it is sized, how every one of the 172 inventoried features is traced to a
test, and how to run it.

It is written by the E2E track, which works **independently of the
implementation track**. Every test here is derived from
`.agents/ORIGINAL_REQUEST.md` (R1-R7 and the 19 acceptance criteria) and from
the frozen contracts in `PROJECT.md` (C1, C2, C5, C6) — never from how the
implementation happens to be built.

---

## 1. Test philosophy

**Opaque box.** The suite drives the system only through documented entry
points: `make demo`, `make eval`, the other frozen C6 targets, and the
artifacts those commands leave behind. It never imports `ano`, `scenariogen`
or `harness`. This is enforced mechanically by
`e2e/meta/test_suite_hygiene.py::test_suite_never_imports_implementation_modules`,
not merely intended.

**Requirements, not implementation.** The authoritative source for every
expected value is the user's request and the ratified definitions derived from
it. Where a metric could be computed several defensible ways, the suite uses
`RATIFIED_DEFINITIONS.md` (RD-01 .. RD-10), because those are the definitions
the acceptance criteria were ratified against.

**A missing implementation is a clean failure, never a skip.** Helpers raise
`MissingImplementation`, which subclasses `AssertionError`, so `unittest`
records a one-line FAIL — `not yet implemented: `make eval` exited 1` — rather
than a confusing traceback or, worse, a green skip that hides the gap. The
suite is designed to be run before, during and after implementation, and to
report honestly at every point.

**Progressive testability.** A test's verification mechanism must never require
a feature more complex than the feature under test. Tier 1 verdicts come from
exit codes, file existence and JSON fields.

> **Recorded decision — C33 (operator surface).** R7's surface is the most
> complex artifact in the project, so Tier 1 verifies the *data behind* the
> surface — the C2 fields present in the emitted prediction records, plus the
> exit code — and never parses rendered HTML or ANSI output. Scraping a
> rendered UI to decide whether a timeline entry exists would make a Tier 1
> verdict depend on a Tier 3 feature.

**Anti-vacuity is a first-class concern.** A check that cannot fail is not
evidence. Wherever the suite asserts an absence, it also proves the detector
of that absence works:

| Absence asserted | Anti-vacuity guard |
|---|---|
| no vendor named anywhere | planted vendor strings must be detected, and capacity language must *not* be |
| no `ano/**` frame reads ground truth | the audit hook must have observed >= 1 process, and *something* must have read the labels during `make eval` |
| the demo works offline | `unshare -rn` must be shown to actually block a TCP connect |
| histogram conformance | at least one histogram family must exist |
| determinism under a fixed seed | two *different* seeds must produce different corpora |
| suppression reduces noise | unsuppressed predictions must still exist |
| format corpora conform | the corpora must meet minimum sizes (20 log entries, 50 samples) |

**Integrity.** No test asserts nothing; no test is rigged to whatever the
implementation currently does. `e2e/meta/test_suite_hygiene.py` fails the suite
if any test method contains no assertion, if any test is unconditionally
skipped, or if a method name is shadowed within a class.

---

## 2. Sizing: 172 features, 35 consolidated capabilities

RD-10 is binding:

> the inventory's 172 fine-grained features would force ~1,900 tests under the
> raw tier formula — disproportionate. Group them into a consolidated
> user-observable capability set (expect ~30-40 capabilities) and size tiers
> against THAT. Every one of the 172 must remain traceable to >= 1 test, or be
> explicitly recorded as covered-by-inspection WITH A REASON. Grouping cuts
> duplication, never coverage.

The suite defines **35 consolidated capabilities**, `C01` .. `C35`, in
`e2e/_support/capabilities.py`. Each is phrased as a question a user could ask
of the delivered system. Tier 1 carries **at least 5 tests per capability**.

Three properties are enforced by `e2e/meta/test_traceability.py` rather than
claimed here:

1. the capability count stays inside RD-10's 30-40 band;
2. every one of features 1..172 maps to at least one capability, or carries a
   written covered-by-inspection reason (**the inspection-only set is
   currently empty — all 172 are covered by executing tests**);
3. every capability has a Tier 1 class bound to it, and that class meets the
   >= 5 quota.

| Id | Capability | Question it answers | Features | Tier 1 module |
|---|---|---|---|---|
| `C01` | Demo entry point runs end to end | Does one documented command replay a scenario to completion? | 3 | `tier1/test_t1_platform.py` |
| `C02` | Evaluation entry point and results artifact | Does one documented command score the system and write a machine-readable file? | 3 | `tier1/test_t1_platform.py` |
| `C03` | Offline, credential-free operation | Does everything run with the network off and no cloud credentials? | 3 | `tier1/test_t1_platform.py` |
| `C04` | Repository surface, documented commands, integrity posture | Can a stranger find the commands and reproduce from a clean checkout? | 4 | `tier1/test_t1_platform.py` |
| `C05` | Stdlib-only dependency posture | Does the deliverable import anything that is not Python 3.13 stdlib? | derived: DECISIONS.md D1 (pip does not exist here) plus AC-01 / AMBIGUITIES.md A-13: the graded run must succeed with the network down, so any non-stdlib import is unobtainable. Not a FEATURE_INVENTORY.md row. | `tier1/test_t1_constraints.py` |
| `C06` | No third-party observability vendor | Is any third-party observability SaaS named in code, IaC or docs? | 4 | `tier1/test_t1_constraints.py` |
| `C07` | GCP service mapping and configuration-only local/cloud switch | Is every production component a named managed GCP service, swapped by config alone? | 9 | `tier1/test_t1_constraints.py` |
| `C08` | Infrastructure-as-code validates offline | Does the IaC exist and validate with no network and no credentials? | 3 | `tier1/test_t1_constraints.py` |
| `C09` | Cloud Logging LogEntry structural fidelity | Do emitted log entries validate against the real LogEntry schema? | 9 | `tier1/test_t1_logentry.py` |
| `C10` | LogEntry field-level fidelity traps | Are the quirks right: lowerCamelCase, int64-as-string, resource label sets, no errorGroups? | 4 | `tier1/test_t1_logentry.py` |
| `C11` | Prometheus exposition format fidelity | Do emitted metrics parse as genuine text exposition 0.0.4? | 3 | `tier1/test_t1_prometheus.py` |
| `C12` | Exporter realism and GMP label policy | Do node/container/Apache-Tomcat/database/switch metrics look like real exporters? | 5 | `tier1/test_t1_prometheus.py` |
| `C13` | Deterministic seeded scenario generation | Does one seed always produce one corpus? | 2 | `tier1/test_t1_scenariogen.py` |
| `C14` | Scenario catalogue completeness | Are all the mandated genuine, benign and normal scenario kinds injectable? | 11 | `tier1/test_t1_scenariogen.py` |
| `C15` | Ground-truth label contract and separability | Is ground truth complete, machine-readable and withheld from the detectors? | 7 | `tier1/test_t1_scenariogen.py` |
| `C16` | Unified ingestion into the analytical store | Do logs and metrics reach one queryable store through one pipeline? | 3 | `tier1/test_t1_pipeline.py` |
| `C17` | Log embeddings as the detection substrate | Are log messages vectors, and does outlier detection consume the vectors? | 2 | `tier1/test_t1_pipeline.py` |
| `C18` | Pattern-free semantic novelty detection | Is a never-before-seen log signature flagged without a pattern list? | 6 | `tier1/test_t1_pipeline.py` |
| `C19` | Unresponsiveness prediction record contract | Does each prediction carry timestamp, time-to-failure, confidence and signals? | 8 | `tier1/test_t1_detection.py` |
| `C20` | Unresponsiveness signal families | Are thread starvation, CPU-without-throughput, I/O wait, disk latency and FD exhaustion modelled? | 6 | `tier1/test_t1_detection.py` |
| `C21` | Topology model | Is there a durable topology of app servers, databases and switch ports? | 6 | `tier1/test_t1_detection.py` |
| `C22` | Cross-domain root-cause attribution and evidence | Is the responsible domain and entity named, with the correlation evidence? | 11 | `tier1/test_t1_detection.py` |
| `C23` | Dynamic baselines and seasonality | Are static thresholds replaced by 3-month per-entity hour/day-of-week baselines? | 5 | `tier1/test_t1_noise.py` |
| `C24` | Change-aware suppression | Are alerts explained by patching, maintenance or deploys suppressed, with a reason? | 5 | `tier1/test_t1_noise.py` |
| `C25` | Downstream alert collapse | Does one upstream fault yield one actionable incident? | 2 | `tier1/test_t1_noise.py` |
| `C26` | Results file structure | Per-seed and pooled blocks, >=5 seeds, every metric with its printed definition? | 7 | `tier1/test_t1_harness.py` |
| `C27` | Core predictive metrics | Recall, the false-positive-rate family, median and p10 lead time? | 4 | `tier1/test_t1_harness.py` |
| `C28` | Attribution metrics | Domain accuracy, entity accuracy and the dedicated database-cascade number? | 3 | `tier1/test_t1_harness.py` |
| `C29` | Static-threshold comparator and noise reduction | Is there a real baseline, with its own numbers and the window alert-volume reduction? | 5 | `tier1/test_t1_harness_metrics.py` |
| `C30` | Suppression metrics and collapse ratio | Recall with and without suppression, the signed delta, and the collapse ratio? | 4 | `tier1/test_t1_harness_metrics.py` |
| `C31` | Two-track prediction and separate scoring | Are unresponsiveness and cross-domain forecasts scored independently on all three bars? | 4 | `tier1/test_t1_harness_metrics.py` |
| `C32` | Acceptance gates and printed verdicts | Does the harness print an explicit pass/fail per acceptance criterion? | 11 | `tier1/test_t1_harness_metrics.py` |
| `C33` | Operator surface content | Timeline, lead time, root cause, suppressed alerts and remediation for a replayed scenario? | 8 | `tier1/test_t1_surface.py` |
| `C34` | Architecture document completeness | Component design, data flow, modelling rationale, service map, cost and scale? | 7 | `tier1/test_t1_surface.py` |
| `C35` | Claim honesty and measurement traceability | Is every number in the write-up measured by the harness, with limitations stated? | 9 | `tier1/test_t1_surface.py` |

---

## 3. Traceability matrix — all 172 features

Generated from `e2e/_support/capabilities.py`, which is the same registry the
meta tier validates. `Mode` records how the feature is observed: `EXEC` by
running the system, `ARTIFACT` by inspecting what it emitted, `STATIC` by
analysing the delivered repository.

| # | Feature | Capabilities | Mode | Verified by |
|---|---|---|---|---|
| 1 | Managed-GCP-only production path | `C07` | STATIC | `tier1/test_t1_constraints.py` |
| 2 | Permitted service palette honoured | `C07` | STATIC | `tier1/test_t1_constraints.py` |
| 3 | Cloud Logging is the log-plane service | `C07` | STATIC | `tier1/test_t1_constraints.py` |
| 4 | Managed Prometheus is the metric-plane service | `C07` | STATIC | `tier1/test_t1_constraints.py` |
| 5 | Analytical store is a managed GCP analytical service | `C07` | STATIC | `tier1/test_t1_constraints.py` |
| 6 | Modelling runs on BigQuery ML and/or Vertex AI | `C07` | STATIC | `tier1/test_t1_constraints.py` |
| 7 | No third-party observability SaaS in the code | `C06` | STATIC | `tier1/test_t1_constraints.py`<br>`invariants/test_no_vendor.py` |
| 8 | No third-party observability SaaS in the IaC | `C06`, `C08` | STATIC | `tier1/test_t1_constraints.py`<br>`invariants/test_no_vendor.py` |
| 9 | No third-party observability SaaS in the architecture doc | `C06`, `C34` | STATIC | `tier1/test_t1_constraints.py`<br>`invariants/test_no_vendor.py`<br>`tier1/test_t1_surface.py` |
| 10 | Automated vendor-exclusion check | `C06` | EXEC | `tier1/test_t1_constraints.py`<br>`invariants/test_no_vendor.py` |
| 11 | All telemetry is synthetic | `C03`, `C35` | STATIC | `tier1/test_t1_platform.py`<br>`invariants/test_offline.py`<br>`tier1/test_t1_surface.py` |
| 12 | Log entries use genuine LogEntry JSON structure | `C09` | ARTIFACT | `tier1/test_t1_logentry.py`<br>`invariants/test_format_fidelity.py` |
| 13 | jsonPayload present and structured | `C09` | ARTIFACT | `tier1/test_t1_logentry.py`<br>`invariants/test_format_fidelity.py` |
| 14 | resource.labels present and realistic | `C09`, `C10` | ARTIFACT | `tier1/test_t1_logentry.py`<br>`invariants/test_format_fidelity.py` |
| 15 | severity present and valid | `C09`, `C10` | ARTIFACT | `tier1/test_t1_logentry.py`<br>`invariants/test_format_fidelity.py` |
| 16 | timestamp present and RFC3339 | `C09`, `C10` | ARTIFACT | `tier1/test_t1_logentry.py`<br>`invariants/test_format_fidelity.py` |
| 17 | insertId present and unique | `C09`, `C10` | ARTIFACT | `tier1/test_t1_logentry.py`<br>`invariants/test_format_fidelity.py` |
| 18 | logName / monitored-resource completeness | `C09` | ARTIFACT | `tier1/test_t1_logentry.py`<br>`invariants/test_format_fidelity.py` |
| 19 | Metrics use genuine Prometheus / GMP sample format | `C11` | ARTIFACT | `tier1/test_t1_prometheus.py`<br>`invariants/test_format_fidelity.py` |
| 20 | Realistic node exporter label sets | `C12` | ARTIFACT | `tier1/test_t1_prometheus.py` |
| 21 | Realistic container exporter label sets | `C12` | ARTIFACT | `tier1/test_t1_prometheus.py` |
| 22 | Realistic Apache/Tomcat exporter label sets | `C12` | ARTIFACT | `tier1/test_t1_prometheus.py` |
| 23 | Realistic database exporter label sets | `C12` | ARTIFACT | `tier1/test_t1_prometheus.py` |
| 24 | Realistic network-switch exporter label sets | `C12` | ARTIFACT | `tier1/test_t1_prometheus.py` |
| 25 | Machine-checkable platform-fidelity validator | `C09`, `C11` | EXEC | `tier1/test_t1_logentry.py`<br>`invariants/test_format_fidelity.py`<br>`tier1/test_t1_prometheus.py` |
| 26 | Single unified ingestion pipeline | `C16` | ARTIFACT | `tier1/test_t1_pipeline.py` |
| 27 | Log ingestion into the analytical store | `C16` | ARTIFACT | `tier1/test_t1_pipeline.py` |
| 28 | Metric ingestion into the analytical store | `C16` | ARTIFACT | `tier1/test_t1_pipeline.py` |
| 29 | Log message to vector embedding | `C17` | ARTIFACT | `tier1/test_t1_pipeline.py` |
| 30 | Outlier detection operates on the embeddings | `C17` | ARTIFACT | `tier1/test_t1_pipeline.py` |
| 31 | No pre-written regex rule list | `C18` | STATIC | `tier1/test_t1_pipeline.py` |
| 32 | No fixed error dictionary | `C18` | STATIC | `tier1/test_t1_pipeline.py` |
| 33 | Novel failure-signature surfacing | `C18` | ARTIFACT | `tier1/test_t1_pipeline.py` |
| 34 | Embedding training window bounded and declared | `C18` | ARTIFACT | `tier1/test_t1_pipeline.py` |
| 35 | Proof the novel signature is absent from training | `C18` | ARTIFACT | `tier1/test_t1_pipeline.py` |
| 36 | Predict unresponsiveness 15-30 minutes ahead | `C19`, `C31` | ARTIFACT | `tier1/test_t1_detection.py`<br>`tier1/test_t1_harness_metrics.py` |
| 37 | Covers server unresponsiveness | `C19` | ARTIFACT | `tier1/test_t1_detection.py` |
| 38 | Covers application unresponsiveness | `C19` | ARTIFACT | `tier1/test_t1_detection.py` |
| 39 | Prediction carries a timestamp | `C19` | ARTIFACT | `tier1/test_t1_detection.py` |
| 40 | Prediction carries a predicted time-to-failure | `C19` | ARTIFACT | `tier1/test_t1_detection.py` |
| 41 | Prediction carries a confidence | `C19` | ARTIFACT | `tier1/test_t1_detection.py` |
| 42 | Prediction carries the specific driving signals | `C19` | ARTIFACT | `tier1/test_t1_detection.py` |
| 43 | Signal family: OS thread starvation | `C20` | ARTIFACT | `tier1/test_t1_detection.py` |
| 44 | Signal family: CPU saturation without throughput | `C20` | ARTIFACT | `tier1/test_t1_detection.py` |
| 45 | Signal family: rising I/O wait | `C20` | ARTIFACT | `tier1/test_t1_detection.py` |
| 46 | Signal family: disk latency | `C20` | ARTIFACT | `tier1/test_t1_detection.py` |
| 47 | Signal family: socket / file-descriptor exhaustion | `C20` | ARTIFACT | `tier1/test_t1_detection.py` |
| 48 | Signal set is open, not closed | `C20` | ARTIFACT | `tier1/test_t1_detection.py` |
| 49 | Preemptive actionability | `C19`, `C33` | ARTIFACT | `tier1/test_t1_detection.py`<br>`tier1/test_t1_surface.py` |
| 50 | Persistent topology model | `C21` | ARTIFACT | `tier1/test_t1_detection.py` |
| 51 | Topology edge: app server to database instance | `C21` | ARTIFACT | `tier1/test_t1_detection.py` |
| 52 | Topology edge: app server to switch port | `C21` | ARTIFACT | `tier1/test_t1_detection.py` |
| 53 | Entity class: Apache/Tomcat application servers | `C21` | ARTIFACT | `tier1/test_t1_detection.py` |
| 54 | Entity class: database instances | `C21` | ARTIFACT | `tier1/test_t1_detection.py` |
| 55 | Entity class: network switch ports | `C21` | ARTIFACT | `tier1/test_t1_detection.py` |
| 56 | Domain attribution using topology | `C22` | ARTIFACT | `tier1/test_t1_detection.py` |
| 57 | Discriminate DB slowness: lock waits | `C22` | ARTIFACT | `tier1/test_t1_detection.py` |
| 58 | Discriminate DB slowness: long-running SQL | `C22` | ARTIFACT | `tier1/test_t1_detection.py` |
| 59 | Discriminate DB slowness: session-pool spikes | `C22` | ARTIFACT | `tier1/test_t1_detection.py` |
| 60 | Discriminate network slowness: OSPF flaps | `C22` | ARTIFACT | `tier1/test_t1_detection.py` |
| 61 | Discriminate network slowness: packet loss | `C22` | ARTIFACT | `tier1/test_t1_detection.py` |
| 62 | Discriminate network slowness: port errors | `C22` | ARTIFACT | `tier1/test_t1_detection.py` |
| 63 | Discriminate genuine front-end problems | `C22` | ARTIFACT | `tier1/test_t1_detection.py` |
| 64 | Forecast cross-domain outage 30-60 minutes ahead | `C31` | ARTIFACT | `tier1/test_t1_harness_metrics.py` |
| 65 | Output names the responsible domain | `C22` | ARTIFACT | `tier1/test_t1_detection.py` |
| 66 | Output names the specific responsible entity | `C22` | ARTIFACT | `tier1/test_t1_detection.py` |
| 67 | Output includes the correlation evidence | `C22` | ARTIFACT | `tier1/test_t1_detection.py` |
| 68 | Rolling learned baselines replace static thresholds | `C23` | ARTIFACT | `tier1/test_t1_noise.py` |
| 69 | Three-month rolling baseline window | `C23` | ARTIFACT | `tier1/test_t1_noise.py` |
| 70 | Per-entity baselines | `C23` | ARTIFACT | `tier1/test_t1_noise.py` |
| 71 | Per-hour-of-day seasonality | `C23` | ARTIFACT | `tier1/test_t1_noise.py` |
| 72 | Per-day-of-week seasonality | `C23` | ARTIFACT | `tier1/test_t1_noise.py` |
| 73 | Suppression of alerts explained by OS patching windows | `C24` | ARTIFACT | `tier1/test_t1_noise.py` |
| 74 | Suppression of alerts explained by maintenance events | `C24` | ARTIFACT | `tier1/test_t1_noise.py` |
| 75 | Suppression of alerts explained by planned deployments | `C24` | ARTIFACT | `tier1/test_t1_noise.py` |
| 76 | Change/maintenance schedule as an explicit input | `C24` | ARTIFACT | `tier1/test_t1_noise.py` |
| 77 | Suppression decisions are recorded, not silent | `C24` | ARTIFACT | `tier1/test_t1_noise.py` |
| 78 | Downstream alert collapse into one incident | `C25` | ARTIFACT | `tier1/test_t1_noise.py` |
| 79 | Collapsed incident remains actionable | `C25` | ARTIFACT | `tier1/test_t1_noise.py` |
| 80 | Deterministic, seeded scenario generator | `C13` | EXEC | `tier1/test_t1_scenariogen.py` |
| 81 | Generator emits the described synthetic telemetry | `C13`, `C09`, `C11` | ARTIFACT | `tier1/test_t1_scenariogen.py`<br>`tier1/test_t1_logentry.py`<br>`invariants/test_format_fidelity.py`<br>`tier1/test_t1_prometheus.py` |
| 82 | Ground truth: true onset time | `C15` | ARTIFACT | `tier1/test_t1_scenariogen.py`<br>`invariants/test_anti_gaming.py`<br>`invariants/test_separation.py` |
| 83 | Ground truth: true root-cause domain | `C15` | ARTIFACT | `tier1/test_t1_scenariogen.py`<br>`invariants/test_anti_gaming.py`<br>`invariants/test_separation.py` |
| 84 | Ground truth: true root-cause entity | `C15` | ARTIFACT | `tier1/test_t1_scenariogen.py`<br>`invariants/test_anti_gaming.py`<br>`invariants/test_separation.py` |
| 85 | Ground truth: genuine vs benign flag | `C15` | ARTIFACT | `tier1/test_t1_scenariogen.py`<br>`invariants/test_anti_gaming.py`<br>`invariants/test_separation.py` |
| 86 | Ground-truth labels machine-readable and separable | `C15` | ARTIFACT | `tier1/test_t1_scenariogen.py`<br>`invariants/test_anti_gaming.py`<br>`invariants/test_separation.py` |
| 87 | Scenario: thread-starvation hang | `C14` | ARTIFACT | `tier1/test_t1_scenariogen.py` |
| 88 | Scenario: memory-leak slow death | `C14` | ARTIFACT | `tier1/test_t1_scenariogen.py` |
| 89 | Scenario: socket exhaustion | `C14` | ARTIFACT | `tier1/test_t1_scenariogen.py` |
| 90 | Scenario: database lock contention cascade | `C14` | ARTIFACT | `tier1/test_t1_scenariogen.py` |
| 91 | Scenario: OSPF flap / packet-loss episode | `C14` | ARTIFACT | `tier1/test_t1_scenariogen.py` |
| 92 | Scenario: noisy-but-benign patching window | `C14` | ARTIFACT | `tier1/test_t1_scenariogen.py` |
| 93 | Scenario: planned deploy (benign) | `C14` | ARTIFACT | `tier1/test_t1_scenariogen.py` |
| 94 | Scenario: traffic surge (benign) | `C14` | ARTIFACT | `tier1/test_t1_scenariogen.py` |
| 95 | Scenario: pure normal operation | `C14` | ARTIFACT | `tier1/test_t1_scenariogen.py` |
| 96 | Scenario: never-before-seen log signature | `C14`, `C18` | ARTIFACT | `tier1/test_t1_scenariogen.py`<br>`tier1/test_t1_pipeline.py` |
| 97 | Scenario list is a minimum, not a maximum | `C14` | ARTIFACT | `tier1/test_t1_scenariogen.py` |
| 98 | Benign events are genuinely noisy | `C29` | ARTIFACT | `tier1/test_t1_harness_metrics.py` |
| 99 | Generator blind to detector internals | `C15` | STATIC | `tier1/test_t1_scenariogen.py`<br>`invariants/test_anti_gaming.py`<br>`invariants/test_separation.py` |
| 100 | Detectors blind to ground truth at inference | `C15` | EXEC | `tier1/test_t1_scenariogen.py`<br>`invariants/test_anti_gaming.py`<br>`invariants/test_separation.py` |
| 101 | Harness replays scenarios through the full system | `C26` | ARTIFACT | `tier1/test_t1_harness.py`<br>`invariants/test_anti_gaming.py`<br>`invariants/test_determinism.py` |
| 102 | Harness scores output against ground truth | `C26` | ARTIFACT | `tier1/test_t1_harness.py`<br>`invariants/test_anti_gaming.py`<br>`invariants/test_determinism.py` |
| 103 | Machine-readable results file | `C02`, `C26` | ARTIFACT | `tier1/test_t1_platform.py`<br>`tier1/test_t1_harness.py`<br>`invariants/test_anti_gaming.py`<br>`invariants/test_determinism.py` |
| 104 | Metric: recall on injected genuine incidents | `C27` | ARTIFACT | `tier1/test_t1_harness.py` |
| 105 | Metric: false-positive rate | `C27` | ARTIFACT | `tier1/test_t1_harness.py` |
| 106 | Metric: median lead time | `C27` | ARTIFACT | `tier1/test_t1_harness.py` |
| 107 | Metric: p10 lead time | `C27` | ARTIFACT | `tier1/test_t1_harness.py` |
| 108 | Metric: root-cause domain accuracy | `C28` | ARTIFACT | `tier1/test_t1_harness.py` |
| 109 | Metric: root-cause entity accuracy | `C28` | ARTIFACT | `tier1/test_t1_harness.py` |
| 110 | Metric: alert volume vs static-threshold baseline | `C29` | ARTIFACT | `tier1/test_t1_harness_metrics.py` |
| 111 | A static-threshold baseline is actually implemented | `C29` | ARTIFACT | `tier1/test_t1_harness_metrics.py` |
| 112 | Baseline's numbers reported alongside | `C29` | ARTIFACT | `tier1/test_t1_harness_metrics.py` |
| 113 | Metric: database-attribution-specific number | `C28` | ARTIFACT | `tier1/test_t1_harness.py` |
| 114 | Metric: recall with suppression enabled | `C30` | ARTIFACT | `tier1/test_t1_harness_metrics.py` |
| 115 | Metric: recall with suppression disabled | `C30` | ARTIFACT | `tier1/test_t1_harness_metrics.py` |
| 116 | Metric: suppression recall delta | `C30` | ARTIFACT | `tier1/test_t1_harness_metrics.py` |
| 117 | Metric: collapse ratio | `C30` | ARTIFACT | `tier1/test_t1_harness_metrics.py` |
| 118 | Metric: alert-volume reduction in patching windows | `C29` | ARTIFACT | `tier1/test_t1_harness_metrics.py` |
| 119 | Per-seed results | `C26` | ARTIFACT | `tier1/test_t1_harness.py`<br>`invariants/test_anti_gaming.py`<br>`invariants/test_determinism.py` |
| 120 | Aggregate results across seeds | `C26` | ARTIFACT | `tier1/test_t1_harness.py`<br>`invariants/test_anti_gaming.py`<br>`invariants/test_determinism.py` |
| 121 | At least 5 distinct seeds | `C26` | ARTIFACT | `tier1/test_t1_harness.py`<br>`invariants/test_anti_gaming.py`<br>`invariants/test_determinism.py` |
| 122 | Bit-identical scores on re-run with the same seed | `C26` | EXEC | `tier1/test_t1_harness.py`<br>`invariants/test_anti_gaming.py`<br>`invariants/test_determinism.py` |
| 123 | Separate scoring: unresponsiveness predictions | `C31` | ARTIFACT | `tier1/test_t1_harness_metrics.py` |
| 124 | Separate scoring: cross-domain outage forecasts | `C31` | ARTIFACT | `tier1/test_t1_harness_metrics.py` |
| 125 | Single command runs the harness | `C02` | EXEC | `tier1/test_t1_platform.py` |
| 126 | Harness prints pass/fail against thresholds | `C32` | EXEC | `tier1/test_t1_harness_metrics.py`<br>`invariants/test_anti_gaming.py` |
| 127 | Gate: recall >= 80% | `C32` | ARTIFACT | `tier1/test_t1_harness_metrics.py`<br>`invariants/test_anti_gaming.py` |
| 128 | Gate: FPR <= 10% | `C32` | ARTIFACT | `tier1/test_t1_harness_metrics.py`<br>`invariants/test_anti_gaming.py` |
| 129 | Gate: median lead time >= 15 minutes | `C32` | ARTIFACT | `tier1/test_t1_harness_metrics.py`<br>`invariants/test_anti_gaming.py` |
| 130 | Gate: per-track recall/FPR/lead-time bars | `C32` | ARTIFACT | `tier1/test_t1_harness_metrics.py`<br>`invariants/test_anti_gaming.py` |
| 131 | Gate: domain accuracy >= 70% | `C32` | ARTIFACT | `tier1/test_t1_harness_metrics.py`<br>`invariants/test_anti_gaming.py` |
| 132 | Gate: DB attribution majority | `C32` | ARTIFACT | `tier1/test_t1_harness_metrics.py`<br>`invariants/test_anti_gaming.py` |
| 133 | Gate: patching-window alert volume reduced >= 50% | `C32` | ARTIFACT | `tier1/test_t1_harness_metrics.py`<br>`invariants/test_anti_gaming.py` |
| 134 | Gate: suppression recall delta <= 5 pp | `C32` | ARTIFACT | `tier1/test_t1_harness_metrics.py`<br>`invariants/test_anti_gaming.py` |
| 135 | Gate: one incident per upstream fault | `C32` | ARTIFACT | `tier1/test_t1_harness_metrics.py`<br>`invariants/test_anti_gaming.py` |
| 136 | Gate: novel signature flagged anomalous | `C32` | ARTIFACT | `tier1/test_t1_harness_metrics.py`<br>`invariants/test_anti_gaming.py` |
| 137 | Full end-to-end run on a developer machine | `C01` | EXEC | `tier1/test_t1_platform.py` |
| 138 | No cloud credentials required | `C03` | EXEC | `tier1/test_t1_platform.py`<br>`invariants/test_offline.py` |
| 139 | No network access required | `C03` | EXEC | `tier1/test_t1_platform.py`<br>`invariants/test_offline.py` |
| 140 | Emulators / local substitutes for managed services | `C07` | STATIC | `tier1/test_t1_constraints.py` |
| 141 | Works on a clean checkout | `C04` | EXEC | `tier1/test_t1_platform.py` |
| 142 | One documented command reproduces the full demo | `C01` | EXEC | `tier1/test_t1_platform.py` |
| 143 | One documented command reproduces the full evaluation | `C02` | EXEC | `tier1/test_t1_platform.py` |
| 144 | Commands are documented | `C04` | STATIC | `tier1/test_t1_platform.py` |
| 145 | Same application and pipeline code deployable to real GCP | `C07` | STATIC | `tier1/test_t1_constraints.py` |
| 146 | Local/cloud switch is configuration only | `C07` | STATIC | `tier1/test_t1_constraints.py` |
| 147 | Deployment expressed as infrastructure-as-code | `C08` | STATIC | `tier1/test_t1_constraints.py` |
| 148 | IaC validates/plans cleanly offline | `C08` | EXEC | `tier1/test_t1_constraints.py` |
| 149 | Demo surface for a replayed scenario | `C33` | EXEC | `tier1/test_t1_surface.py` |
| 150 | Demo shows predicted incidents on a timeline | `C33` | ARTIFACT | `tier1/test_t1_surface.py` |
| 151 | Demo shows lead time per prediction | `C33` | ARTIFACT | `tier1/test_t1_surface.py` |
| 152 | Demo shows the attributed root cause | `C33` | ARTIFACT | `tier1/test_t1_surface.py` |
| 153 | Demo shows suppressed / collapsed alerts | `C33` | ARTIFACT | `tier1/test_t1_surface.py` |
| 154 | Demo shows recommended remediation | `C33` | ARTIFACT | `tier1/test_t1_surface.py` |
| 155 | Remediation recommendations are derived, not decorative | `C33` | ARTIFACT | `tier1/test_t1_surface.py` |
| 156 | Architecture doc: component design | `C34` | STATIC | `tier1/test_t1_surface.py` |
| 157 | Architecture doc: data flow | `C34` | STATIC | `tier1/test_t1_surface.py` |
| 158 | Architecture doc: modelling approach and rationale | `C34` | STATIC | `tier1/test_t1_surface.py` |
| 159 | Architecture doc: GCP service mapping for every component | `C34` | STATIC | `tier1/test_t1_surface.py` |
| 160 | Architecture doc: cost characteristics at estate scale | `C34` | STATIC | `tier1/test_t1_surface.py` |
| 161 | Architecture doc: scale characteristics at estate scale | `C34` | STATIC | `tier1/test_t1_surface.py` |
| 162 | Architecture doc: honest statement of limitations | `C35` | STATIC | `tier1/test_t1_surface.py` |
| 163 | Architecture doc: what is synthetic | `C35` | STATIC | `tier1/test_t1_surface.py` |
| 164 | Architecture doc: what would change with real telemetry | `C35` | STATIC | `tier1/test_t1_surface.py` |
| 165 | Architecture doc: no claimed result the harness does not measure | `C35` | STATIC | `tier1/test_t1_surface.py` |
| 166 | Business-case framing: MTTR 40-70% marked as a target | `C35` | STATIC | `tier1/test_t1_surface.py` |
| 167 | Business-case framing: ~50% change-related outages | `C35` | STATIC | `tier1/test_t1_surface.py` |
| 168 | Business-case framing: 80-90k toil hours | `C35` | STATIC | `tier1/test_t1_surface.py` |
| 169 | Deliverable is credible enough to run live | `C01` | EXEC | `tier1/test_t1_platform.py` |
| 170 | Claims are backed by measured numbers | `C35` | STATIC | `tier1/test_t1_surface.py` |
| 171 | Work lives in the specified working directory | `C04` | STATIC | `tier1/test_t1_platform.py` |
| 172 | Integrity mode: development | `C04` | STATIC | `tier1/test_t1_platform.py` |

---

## 4. Architecture of the suite

```
e2e/
├── run.py             the runner: `python3 -m e2e.run`
├── _support/          shared library (never contains tests)
├── fixtures/          known-conformant corpora, used by the selftests
├── selftest/          tests OF the validators (mutation-scored)
├── meta/              tests OF the suite (traceability, hygiene)
├── invariants/        the seven cross-cutting invariants
└── tier1/             happy path, >= 5 tests per capability
```

### 4.1 Tiers

| Tier | Question it answers | Status |
|---|---|---|
| `meta` | is the suite itself honest and complete? | live |
| `selftest` | do the suite's own validators actually detect defects? | live |
| `invariants` | are the cross-cutting, acceptance-relevant invariants held? | live |
| `1` | does each capability work on its happy path? | live |
| `2` | edge cases and boundaries | planned |
| `3` | error handling and adversarial input | planned |
| `4` | quality gates against the acceptance thresholds | planned |

Tiers 2-4 are reported as `PLANNED` by the runner rather than silently omitted.

### 4.2 Support library

| Module | Responsibility |
|---|---|
| `paths.py` | repository locations, source enumeration, scan exclusions |
| `sut.py` | running documented commands; `MissingImplementation`; the session cache |
| `artifacts.py` | discovering emitted artifacts **by shape, not filename** |
| `case.py` | `E2ECase`: capability binding plus clean-failure helpers |
| `capabilities.py` | the 35 capabilities, the 172-feature registry, the 7 invariants |
| `logentry_validator.py` | independent LogEntry validator, rules L1..L18 |
| `prom_validator.py` | independent Prometheus validator, rules P01..P20 |
| `importgraph.py` | pure-`ast` import graph: direct, transitive, third-party |
| `vendors.py` | vendor patterns and the deliverable-surface scan |
| `audit_hook.py`, `audit_run.py`, `audit_launcher.py` | runtime file/network auditing via `sys.addaudithook` |

### 4.3 The independent validators

The suite does **not** call the implementation's validators. It carries its own,
written from `LOGENTRY_SPEC.md` and `PROMETHEUS_SPEC.md`, reusing the *approach*
in `VALIDATION_STRATEGY.md` rather than any code. Each is mutation-tested in the
`selftest` tier: a deliberately corrupted copy of a conformant fixture must be
rejected, and the conformant fixture must be accepted.

| Validator | Rules | Mutations planted | Caught |
|---|---|---|---|
| `logentry_validator` | L1 .. L18 | 28 | 28 |
| `prom_validator` | P01 .. P20 | 29 | 29 |

Named traps covered: JSON lowerCamelCase; int64 as a **quoted string** but
int32 as a **number**; `resource.labels` fixed per `resource.type`
(`gce_instance` is exactly `project_id`/`instance_id`/`zone`); `errorGroups`
never emitted; label-value escapes limited to `\\`, `\n` and `\"` (no `\t`);
histograms carrying `le="+Inf"` with `+Inf` equal to `_count`; exporters never
emitting the GMP-attached labels `project_id`/`location`/`cluster`/
`namespace`/`job`/`instance`.

### 4.4 The runtime audit hook

Static analysis cannot see an `open()` of a path assembled at runtime, and a
runtime hook cannot see a code path the demo never exercises. The suite uses
both. `audit_run.audited_make(target)` generates a `sitecustomize.py`,
places it on `PYTHONPATH`, and runs `make <target>` so that **every** Python
process in that run installs an audit hook recording file opens and network
calls with the module that caused them. This is how the R5 separation
invariant is checked at runtime without knowing anything about the
implementation's internal call signatures.

### 4.5 Session cache

`sut.SESSION` caches `make demo` and `make eval` per process, so ~200 Tier 1
tests do not re-run the system 200 times. Tests that must observe a *fresh*
run — determinism, anti-gaming — call `sut.run_make` directly and bypass the
cache. Set `E2E_NO_CACHE=1` to disable caching everywhere.

---

## 5. The seven invariants

These are acceptance-relevant and cross-cutting; the E2E track is the only
track positioned to check them.

| Id | Invariant | How it is proved |
|---|---|---|
| `INV-SEP` | R5 separation | static import graph (direct + transitive) **and** a runtime audit of a real `make demo` / `make eval` |
| `INV-STDLIB` | Python 3.13 stdlib only | an allow-list built independently from `sys.stdlib_module_names`, cross-checked against `tools/check_imports.py` but never merely delegating to it |
| `INV-VENDOR` | no third-party observability vendor | pattern scan of code, IaC, docs and runtime output, with discrimination self-tests |
| `INV-DET` | determinism | same seed twice, byte-identical, including under a varied `PYTHONHASHSEED` |
| `INV-OFFLINE` | offline operation | `make demo`, `make eval` and IaC validation under `unshare -rn` |
| `INV-FORMAT` | format fidelity | the independent LogEntry and Prometheus validators |
| `INV-GAMING` | anti-gaming | no hardcoded expected outputs, no ground-truth leakage into detector code paths, gates recomputable from published definitions |

`INV-GAMING` is only legitimate because of rule **R-PRINT**: the harness must
print the exact definition, including the denominator, of every metric. That
is what makes a published gate independently recomputable.

---

## 6. Coverage thresholds

| Threshold | Value | Enforced by |
|---|---|---|
| consolidated capabilities | 30 <= n <= 40 (currently 35) | `meta/test_traceability.py` |
| Tier 1 tests per capability | >= 5 | `meta/test_traceability.py` |
| features traced to a test | 172 of 172 | `meta/test_traceability.py` |
| features covered by inspection | 0 (set is empty) | `meta/test_traceability.py` |
| invariants declared | exactly the 7 above | `meta/test_traceability.py` |
| assertion-free tests | 0 | `meta/test_suite_hygiene.py` |
| unconditionally skipped tests | 0 | `meta/test_suite_hygiene.py` |
| LogEntry validator mutation score | 28/28 | `selftest/test_logentry_validator.py` |
| Prometheus validator mutation score | 29/29 | `selftest/test_prom_validator.py` |
| minimum log corpus for a fidelity verdict | 20 entries | `tier1/test_t1_logentry.py` |
| minimum metric corpus for a fidelity verdict | 50 samples | `tier1/test_t1_prometheus.py` |

---

## 7. E2E-declared artifact expectations

The frozen contracts C1/C2/C5/C6 fix the *shapes* the system exchanges, but
not where a run leaves them on disk. Because an opaque-box suite can only read
what it can find, the E2E track declares these expectations **on top of** the
contracts. They are deliberately shape-based and overridable, so the
implementation track keeps freedom over naming and layout.

| Artifact | How the suite finds it | Override |
|---|---|---|
| C5 results | `artifacts/results.json` | `E2E_RESULTS_PATH` |
| ground-truth labels | any JSON under `artifacts/` whose name contains `labels`, `ground_truth` or `truth` | — |
| C2 predictions | any JSON/JSONL object carrying `prediction_id`, `predicted_onset` or `incident_id` | — |
| LogEntry corpus | files under `artifacts/` with suffix `.jsonl`, `.ndjson` or `.logs` | — |
| Prometheus corpus | files under `artifacts/` with suffix `.prom`, `.metrics` or `.openmetrics` | — |

Other environment overrides: `E2E_PROJECT_ROOT`, `E2E_ARTIFACTS_DIR`,
`E2E_TIMEOUT_S`, `E2E_NO_CACHE`.

**What this asks of the implementation track:** `make demo` and `make eval`
must leave machine-readable artifacts under `artifacts/` — the C2 prediction
records, the emitted LogEntry corpus, the emitted Prometheus exposition, and
the ground-truth labels. A run that prints everything to the terminal and
persists nothing cannot be graded by an opaque-box suite, and would also leave
the acceptance claims unauditable by a human reviewer.

---

## 8. Running the suite

```bash
make e2e                        # the C6 entry point
python3 -m e2e.run              # equivalent
python3 -m e2e.run --list       # capabilities, invariants and tiers
python3 -m e2e.run --tier meta --tier selftest
python3 -m e2e.run --tier invariants --verbose
python3 -m e2e.run --tier 1 --failfast
python3 -m e2e.run --json-report report.json
```

The runner prints one summary row per tier and exits non-zero if any test
fails, any module fails to load, or nothing at all was found to run. By
default a failure is rendered as its assertion message rather than a
traceback; pass `--traceback` for the full stack.

Requirements: Python 3.13, `make`, and `unshare -rn` for the offline
invariant. No third-party packages; `pip` is not required and is not present.
