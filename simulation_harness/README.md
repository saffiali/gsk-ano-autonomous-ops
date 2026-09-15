# GSK Autonomous Network Operations (ANO) — reference implementation

A runnable reference implementation of predictive, self-healing IT/hosting/
network operations on Google Cloud. It learns from Cloud Logging entries and
Google Managed Prometheus metrics to predict server and application
unresponsiveness before it happens, attribute cross-domain degradation to the
responsible domain and entity, suppress alerts explained by scheduled change,
and collapse alert storms into single incidents.

Everything runs **offline on a developer machine**, with **no cloud credentials
and no network access**, using local substitutes for each managed service. The
same application code deploys to a real Google Cloud project by **changing
configuration only**.

---

## The two documented commands

```bash
make demo      # replay a scenario and render the operator surface
make eval      # run the evaluation harness and write a machine-readable results file
```

`make demo` replays a generated scenario end to end and shows the predicted
incidents on a timeline with their lead time, the attributed root cause, the
suppressed and collapsed alerts, and the recommended remediation.

```bash
make demo SCENARIO=db_contention_cascade SEED=1234
```

`make eval` replays scenarios across at least five distinct seeds, scores the
system against ground truth and writes `artifacts/results.json`. The results
file contains per-seed and pooled numbers, the exact definition of every metric
it reports, the static-threshold baseline it is compared against, and an
explicit pass/fail verdict for each acceptance criterion.

```bash
make eval SEEDS=5 OUT=artifacts/results.json
```

Both commands write only into `artifacts/`, which is git-ignored. Neither opens
a network socket and neither reads a cloud credential.

---

## Running the tests

```bash
make test                                  # the whole suite
python3 -m unittest discover -s tests      # the same thing, directly
python3 -m unittest discover -s tests -v   # verbose
python3 -m unittest discover -s tests/m1_core -t .   # one milestone's tests
```

Two further checks:

```bash
make check-imports    # every import is stdlib or first-party; no third-party
                      # observability vendor anywhere in code, config or docs
make e2e              # the opaque-box end-to-end suite
```

### Proving the offline claim

```bash
make verify-offline
```

This re-runs the demo, the evaluation and the IaC validation inside
`unshare -rn` — a genuinely empty network namespace with no interfaces and no
DNS — after probing to confirm the namespace really is isolated. The point is
that "works offline" is *demonstrated* rather than asserted: the development
machine has working network access, so a hidden dependency would otherwise go
unnoticed until someone ran the demo somewhere that did not.

---

## Prerequisites — honestly

**Required, and that is the whole list:**

| Requirement | Why |
|---|---|
| **Python 3.13** | The code uses 3.13 syntax and `sys.stdlib_module_names`. No other version is tested. |
| **GNU Make** | Only to provide the documented command surface; every target is a one-line `python3` invocation you can run by hand. |

**Deliberately not required:** no `pip install`, no virtualenv, no
`requirements.txt`, no Docker, no Node, no GPU, no cloud credentials, no
network. The entire system is written against the **Python standard library
only**. This is not minimalism for its own sake — the target environment has no
package manager at all, and an acceptance criterion requires the demo to run on
a clean checkout with no network, which rules out resolving dependencies at run
time.

**Optional, and what you lose without it:**

| Optional | Used for | Without it |
|---|---|---|
| `unshare` (util-linux) | `make verify-offline` | The target fails with a clear message. The demo and evaluation are unaffected. |
| `terraform` CLI | Higher-fidelity IaC checking | `make validate-iac` still runs the repository's own pure-Python HCL validator, which needs nothing. The terraform CLI is an **external prerequisite we do not ship**, and the `hashicorp/google` provider cannot be initialised without network access unless its plugin is pre-vendored. The architecture document states which validation layer actually ran. |

---

## What is real and what is synthetic

There is no access to real GSK data and no live Google Cloud project, so **all
telemetry is synthetic** — but it is shape-faithful: Cloud Logging entries in
genuine `LogEntry` JSON structure (`jsonPayload`, `resource.labels`, `severity`,
`timestamp`, `insertId`), and metrics in genuine Prometheus exposition format
with realistic label sets for node, container, Apache/Tomcat, database and
network-switch exporters.

The managed services are substituted locally, each behind an interface shaped
like the real client:

| Production Google Cloud service | Offline substitute |
|---|---|
| Cloud Logging | JSONL `LogEntry` file + local sink |
| Google Managed Prometheus | exposition-text scrape into a local sample store |
| Pub/Sub | in-process broker (standard-library queues) |
| Dataflow | deterministic single-process stage runner |
| BigQuery | standard-library `sqlite3` |
| BigQuery ML / Vertex AI | pure-Python estimators |
| Cloud Run / Cloud Functions | local process entrypoints, same handlers |
| Cloud Monitoring | local alert store + demo surface |

Selecting between them is configuration, never code:

```bash
python3 -m ano.config --profile local > /tmp/local.json
python3 -m ano.config --profile gcp   > /tmp/gcp.json
diff /tmp/local.json /tmp/gcp.json      # data only, no code
```

`docs/ARCHITECTURE.md` states what would change against real GSK telemetry and
the known limitations.

---

## Determinism

The same seed produces the same output, byte for byte. Two rules make that
true, and both are enforced by tests:

* every hash that affects behaviour is `hashlib.blake2b`, never the builtin
  `hash()` — which is salted per process and would silently change results
  between runs;
* every stochastic component takes an explicit seed and uses its own
  `random.Random` instance; nothing touches the global `random` state.

`make eval` reports a determinism digest computed over the results with the
wall-clock fields removed, so re-running the same seeds is checkable by
comparing one value.

---

## Repository layout

```
ano/config.py        configuration; selects local vs cloud backends
ano/contracts/       shared vocabulary: ground-truth labels, predictions,
                     results file, determinism utilities
ano/gcp/             Google Cloud adapters + offline substitutes
ano/telemetry/       LogEntry and Prometheus models, serialisers, validators
ano/ingest/          pipeline stages
ano/semantic/        log embeddings and semantic outlier detection
ano/detect/          unresponsiveness, baselines, suppression, collapse
ano/correlate/       topology and cross-domain attribution
ano/demo/, ano/serve/ operator surface and Cloud Run/Function entrypoints
scenariogen/         seeded scenario generator and ground-truth labels
harness/             evaluation harness, static-threshold baseline, scoring
iac/                 terraform and the offline HCL validator
docs/ARCHITECTURE.md component design, GCP mapping, cost and scale, limitations
tests/               unit tests (standard-library `unittest`)
e2e/                 opaque-box end-to-end suite
artifacts/           everything generated at run time (git-ignored)
```

## Build status

The milestone table in `PROJECT.md` is the authoritative record of what has
landed. Any command surface target whose milestone has not landed **exits
non-zero with a "not yet implemented" message** — it never reports a false
success, so `make demo` failing today means exactly what it says.
