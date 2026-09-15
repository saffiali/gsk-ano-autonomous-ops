"""The static-threshold alerting baseline (AC-11, RD-04).

AC-11 claims a >= 50% reduction in alert volume during patching/maintenance
windows *versus a static-threshold baseline implemented for comparison*. That
claim is only worth something if the baseline is the kind of alerting a real
operations team would actually configure. RD-04 therefore requires the
thresholds to be

* **conventional** — recognisable industry defaults, not values chosen to make
  the comparison flattering;
* **documented** — published as a table in the results file and the
  architecture document;
* **fixed before measurement** — :data:`DEFAULT_RULES` is a module constant, not
  a function of anything measured; and
* accompanied by the baseline's own **recall**, as evidence it is not a strawman
  that alerts on nothing (or on everything).

Design notes that keep the comparator fair rather than convenient:

* Every rule has a **dwell time** (``for_duration_s``, 5 minutes by default).
  Real alerting rules almost always have a ``for:`` clause; omitting it would
  make the baseline spuriously noisy and the reduction spuriously large.
* A firing rule **re-notifies** on a conventional interval (1 hour) rather than
  on every scrape, for the same reason. Alertmanager's own defaults sit in this
  region.
* A rule **resolves** as soon as the signal falls back below its threshold, so
  a single excursion produces one alert, not a permanent stream.
* The baseline has **no change-awareness and no collapse** — those are exactly
  the capabilities under test, and giving the baseline either would make the
  comparison meaningless in the opposite direction.

The baseline consumes telemetry samples structurally (anything exposing
``name``, ``labels``, ``value`` and ``timestamp``), so it works against
``ano.telemetry.MetricSample`` from contract C7 without importing it and
without any knowledge of detector internals.
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from ano.contracts.common import UTC
from ano.contracts.prediction import Attribution, Evidence, Prediction, Signal
from ano.contracts.results import BaselineThreshold

from harness.matching import entity_domain

__all__ = [
    "DEFAULT_DWELL_S",
    "DEFAULT_REPEAT_INTERVAL_S",
    "ENTITY_LABEL_KEY",
    "BaselineRule",
    "BaselineRunResult",
    "StaticThresholdBaseline",
    "DEFAULT_RULES",
    "threshold_table",
]

#: Conventional ``for:`` clause. Five minutes is the canonical dwell in
#: Prometheus alerting examples and in most enterprise runbooks.
DEFAULT_DWELL_S = 300

#: Conventional re-notification interval while a rule stays firing.
DEFAULT_REPEAT_INTERVAL_S = 3600

#: The label that carries entity identity. INTEGRATION_RULINGS IR-02 makes this
#: an explicit contract rather than a guess: the generator emits per-target
#: scrape documents with no reserved labels, ingest performs the Google Managed
#: Prometheus target attach, and the attached ``instance`` label equals the
#: contract-C1 ``entity_id`` (``job`` names the exporter family). A sample whose
#: ``instance`` does not name a declared entity is dropped rather than guessed
#: at, so a broken attach shows up as missing baseline coverage instead of
#: silently mis-attributed alerts.
ENTITY_LABEL_KEY: str = "instance"

_COMPARATORS = {
    ">": lambda value, threshold: value > threshold,
    ">=": lambda value, threshold: value >= threshold,
    "<": lambda value, threshold: value < threshold,
    "<=": lambda value, threshold: value <= threshold,
}


class SampleLike(Protocol):
    """Structural type of a telemetry sample (contract C7 ``MetricSample``)."""

    name: str
    labels: Mapping[str, str]
    value: float
    timestamp: int | None


@dataclass(frozen=True, slots=True)
class BaselineRule:
    """One conventional static-threshold alerting rule.

    Attributes:
        name: Rule name, used in the published table and in alert ids.
        signal: Metric name the rule watches.
        comparator: One of ``>``, ``>=``, ``<``, ``<=``.
        threshold: The fixed threshold.
        unit: Unit of ``threshold``.
        applies_to: Entity kind the rule applies to, or ``"*"`` for any.
        rationale: Why this is a conventional value — the sentence a reviewer
            uses to decide whether the comparator is fair.
        track: Which scoring track the resulting alert belongs to.
        remediation: The runbook action a conventional rule would carry.
        for_duration_s: Dwell time before the rule fires.
        aliases: Alternative metric names this rule also accepts, so the
            baseline is not silently blind if the generator names a signal
            slightly differently.
    """

    name: str
    signal: str
    comparator: str
    threshold: float
    unit: str
    applies_to: str
    rationale: str
    track: str
    remediation: str
    for_duration_s: int = DEFAULT_DWELL_S
    aliases: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.comparator not in _COMPARATORS:
            raise ValueError(f"unknown comparator {self.comparator!r}")
        if self.track not in ("unresponsiveness", "cross_domain"):
            raise ValueError(f"unknown track {self.track!r}")
        if self.for_duration_s < 0:
            raise ValueError("for_duration_s must be non-negative")

    def signal_names(self) -> tuple[str, ...]:
        """Every metric name this rule will accept."""
        return (self.signal,) + self.aliases

    def breached(self, value: float) -> bool:
        """True if ``value`` violates the threshold."""
        return _COMPARATORS[self.comparator](value, self.threshold)

    def to_threshold_row(self) -> BaselineThreshold:
        """Render as a contract-C5 :class:`BaselineThreshold` table row."""
        return BaselineThreshold(
            name=self.name,
            signal=self.signal,
            comparator=self.comparator,
            threshold=float(self.threshold),
            unit=self.unit,
            applies_to=self.applies_to,
            rationale=(
                f"{self.rationale} Sustained for {self.for_duration_s}s before "
                f"firing; re-notifies every {DEFAULT_REPEAT_INTERVAL_S}s while "
                "the condition holds; resolves when the signal recovers."
            ),
        )


#: The fixed rule table. **Set before any measurement was taken** (RD-04) and
#: never derived from a measured quantity. Thresholds are the conventional
#: values an enterprise operations team would configure: 90% CPU, 90% memory,
#: 20 ms disk service time, 85% of the file-descriptor limit, 90% thread-pool
#: or session-pool utilisation, 5% HTTP 5xx, 1% packet loss, and any sustained
#: interface error or OSPF flap.
DEFAULT_RULES: tuple[BaselineRule, ...] = (
    BaselineRule(
        name="cpu_saturation",
        signal="node_cpu_utilisation_ratio",
        aliases=("node_cpu_utilization_ratio", "node_cpu_usage_ratio"),
        comparator=">=",
        threshold=0.90,
        unit="ratio",
        applies_to="node",
        rationale="90% sustained CPU utilisation is the classic host "
        "saturation page.",
        track="unresponsiveness",
        remediation="Investigate top CPU consumers; consider scaling out or "
        "restarting the saturated process.",
    ),
    BaselineRule(
        name="memory_pressure",
        signal="node_memory_utilisation_ratio",
        aliases=("node_memory_utilization_ratio", "node_memory_usage_ratio"),
        comparator=">=",
        threshold=0.90,
        unit="ratio",
        applies_to="node",
        rationale="90% memory utilisation is the conventional pre-OOM page.",
        track="unresponsiveness",
        remediation="Check for a leaking process; restart or add memory.",
    ),
    BaselineRule(
        name="io_wait",
        signal="node_cpu_iowait_ratio",
        aliases=("node_cpu_iowait_seconds_ratio",),
        comparator=">=",
        threshold=0.20,
        unit="ratio",
        applies_to="node",
        rationale="20% I/O wait is a widely used storage-contention trigger.",
        track="unresponsiveness",
        remediation="Inspect storage latency and the I/O queue depth.",
    ),
    BaselineRule(
        name="disk_latency",
        signal="node_disk_io_time_seconds",
        aliases=("node_disk_latency_seconds",),
        comparator=">=",
        threshold=20 / 1000,
        unit="seconds",
        applies_to="node",
        rationale="20 ms average disk service time is the conventional "
        "spinning/SAN latency alarm.",
        track="unresponsiveness",
        remediation="Check the storage path and array health.",
    ),
    BaselineRule(
        name="fd_exhaustion",
        signal="process_open_fds_ratio",
        aliases=("process_file_descriptors_ratio",),
        comparator=">=",
        threshold=85 / 100,
        unit="ratio",
        applies_to="app_server",
        rationale="85% of the file-descriptor limit is the standard socket/FD "
        "exhaustion warning.",
        track="unresponsiveness",
        remediation="Identify the leaking descriptor source; raise the limit "
        "or restart the service.",
    ),
    BaselineRule(
        name="thread_pool_saturation",
        signal="tomcat_threads_busy_ratio",
        aliases=("jvm_threads_busy_ratio", "apache_workers_busy_ratio"),
        comparator=">=",
        threshold=0.90,
        unit="ratio",
        applies_to="app_server",
        rationale="90% busy worker threads is the standard Tomcat/Apache "
        "thread-starvation alarm.",
        track="unresponsiveness",
        remediation="Increase the pool or relieve the downstream dependency "
        "holding threads.",
    ),
    BaselineRule(
        name="http_error_rate",
        signal="apache_http_5xx_ratio",
        aliases=("http_server_5xx_ratio", "tomcat_http_5xx_ratio"),
        comparator=">=",
        threshold=0.05,
        unit="ratio",
        applies_to="app_server",
        rationale="A 5% 5xx error budget burn is a conventional paging "
        "threshold.",
        track="unresponsiveness",
        remediation="Check the application error log and recent deployments.",
    ),
    BaselineRule(
        name="response_latency",
        signal="apache_request_duration_seconds_p95",
        aliases=(
            "http_request_duration_seconds_p95",
            "tomcat_request_duration_seconds_p95",
        ),
        comparator=">=",
        threshold=2.0,
        unit="seconds",
        applies_to="app_server",
        rationale="A 2 s p95 response time is a common user-facing latency "
        "SLO breach.",
        track="unresponsiveness",
        remediation="Profile the slow path; check database and network "
        "dependencies.",
    ),
    BaselineRule(
        name="db_session_pool",
        signal="db_sessions_active_ratio",
        aliases=("db_connection_pool_utilisation_ratio",),
        comparator=">=",
        threshold=0.90,
        unit="ratio",
        applies_to="database",
        rationale="90% session-pool utilisation is the conventional database "
        "connection-exhaustion alarm.",
        track="cross_domain",
        remediation="Identify session leaks or long transactions; raise the "
        "pool ceiling.",
    ),
    BaselineRule(
        name="db_lock_waits",
        signal="db_lock_wait_seconds",
        aliases=("db_lock_wait_time_seconds",),
        comparator=">=",
        threshold=5.0,
        unit="seconds",
        applies_to="database",
        rationale="A 5 s lock wait is the usual blocking-session trigger.",
        track="cross_domain",
        remediation="Find and clear the blocking session.",
    ),
    BaselineRule(
        name="db_long_queries",
        signal="db_longest_query_seconds",
        aliases=("db_longest_running_query_seconds",),
        comparator=">=",
        threshold=60.0,
        unit="seconds",
        applies_to="database",
        rationale="A 60 s long-running statement is a standard OLTP "
        "long-query alarm.",
        track="cross_domain",
        remediation="Review the execution plan; kill or tune the statement.",
    ),
    BaselineRule(
        name="switch_port_errors",
        signal="switch_port_errors_per_second",
        aliases=("ifInErrors_per_second", "switch_interface_errors_per_second"),
        comparator=">=",
        threshold=1.0,
        unit="errors/second",
        applies_to="switch",
        rationale="Any sustained interface error rate is conventionally "
        "alarmed on a production switch port.",
        track="cross_domain",
        remediation="Check the optic, cable and port counters.",
    ),
    BaselineRule(
        name="packet_loss",
        signal="switch_packet_loss_ratio",
        aliases=("network_packet_loss_ratio",),
        comparator=">=",
        threshold=1 / 100,
        unit="ratio",
        applies_to="switch",
        rationale="1% packet loss is the standard network-degradation "
        "threshold.",
        track="cross_domain",
        remediation="Trace the path; check for congestion or a failing link.",
    ),
    BaselineRule(
        name="ospf_flaps",
        signal="switch_ospf_state_changes_per_minute",
        aliases=("ospf_neighbor_state_changes_per_minute",),
        comparator=">=",
        threshold=1.0,
        unit="changes/minute",
        applies_to="switch",
        rationale="Any OSPF adjacency change inside a minute is conventionally "
        "alarmed as routing instability.",
        track="cross_domain",
        remediation="Check the neighbour, the link and the OSPF timers.",
    ),
)


@dataclass(frozen=True, slots=True)
class BaselineRunResult:
    """What the baseline produced over one replay.

    Attributes:
        predictions: Contract-C2 alerts, in deterministic order.
        rules_total: Size of the rule table.
        rules_with_coverage: Rules that saw at least one matching sample on at
            least one in-scope entity. If this is zero the baseline never saw
            the telemetry, and AC-11's reduction is not measurable.
        fires_by_rule: Per-rule alert counts, for the report.
        samples_seen: Total samples consumed.
        samples_unresolved: Samples whose entity could not be resolved against
            the declared entity set — a wiring diagnostic, reported not hidden.
    """

    predictions: tuple[Prediction, ...]
    rules_total: int
    rules_with_coverage: int
    fires_by_rule: Mapping[str, int]
    samples_seen: int
    samples_unresolved: int



class StaticThresholdBaseline:
    """A conventional static-threshold alerting engine.

    Args:
        entity_kinds: ``entity_id -> entity kind`` for every declared entity,
            from the ground-truth label file's entity list. The baseline uses it
            only to resolve which rules apply and which domain an alert belongs
            to; it reads no incident labels.
        rules: The fixed rule table. Defaults to :data:`DEFAULT_RULES`.
        repeat_interval_s: Re-notification interval while a rule stays firing.
        entity_label_key: Label carrying entity identity (IR-02: ``instance``).
    """

    def __init__(
        self,
        entity_kinds: Mapping[str, str],
        *,
        rules: Sequence[BaselineRule] = DEFAULT_RULES,
        repeat_interval_s: int = DEFAULT_REPEAT_INTERVAL_S,
        entity_label_key: str = ENTITY_LABEL_KEY,
    ) -> None:
        self._entity_kinds = dict(entity_kinds)
        self._rules = tuple(rules)
        self._repeat_interval_s = int(repeat_interval_s)
        self._entity_label_key = str(entity_label_key)
        self._by_signal: dict[str, list[BaselineRule]] = {}
        for rule in self._rules:
            for name in rule.signal_names():
                self._by_signal.setdefault(name, []).append(rule)

    @property
    def rules(self) -> tuple[BaselineRule, ...]:
        """The fixed rule table."""
        return self._rules

    def threshold_table(self) -> tuple[BaselineThreshold, ...]:
        """The published contract-C5 threshold table (RD-04)."""
        return tuple(rule.to_threshold_row() for rule in self._rules)

    def _resolve_entity(self, labels: Mapping[str, str]) -> str | None:
        """Which declared entity a sample belongs to, or ``None``.

        IR-02: identity is the attached ``instance`` label and nothing else.
        There is deliberately no fallback chain — an unresolvable sample is
        dropped and shows up as reduced rule-signal coverage, which is visible,
        rather than being attributed to a guessed entity, which is not.
        """
        candidate = labels.get(self._entity_label_key)
        if candidate and candidate in self._entity_kinds:
            return candidate
        return None

    def _applies(self, rule: BaselineRule, entity: str) -> bool:
        """True if ``rule`` is in scope for ``entity``."""
        if rule.applies_to == "*":
            return True
        kind = self._entity_kinds.get(entity)
        if kind is None:
            return False
        if kind == rule.applies_to:
            return True
        # A node-level rule is also conventionally applied to the host running
        # an application server; treat app_server as a node for host rules.
        return rule.applies_to == "node" and kind == "app_server"

    def run(self, samples: Iterable[Any]) -> BaselineRunResult:
        """Replay ``samples`` through the rule table and emit C2 alerts.

        Samples are grouped by ``(entity, rule)`` and evaluated in timestamp
        order. A rule fires once its condition has held continuously for
        ``for_duration_s``, re-notifies every ``repeat_interval_s`` while it
        holds, and resolves on the first recovering sample.

        Args:
            samples: Telemetry samples exposing ``name``, ``labels``, ``value``
                and ``timestamp`` (epoch seconds).

        Returns:
            A :class:`BaselineRunResult`.
        """
        series: dict[tuple[str, str], list[tuple[int, float]]] = {}
        covered: set[str] = set()
        seen = 0
        unresolved = 0
        for sample in samples:
            seen += 1
            rules = self._by_signal.get(sample.name)
            if not rules:
                continue
            entity = self._resolve_entity(sample.labels)
            if entity is None:
                unresolved += 1
                continue
            timestamp = sample.timestamp
            if timestamp is None:
                unresolved += 1
                continue
            for rule in rules:
                if not self._applies(rule, entity):
                    continue
                covered.add(rule.name)
                series.setdefault((entity, rule.name), []).append(
                    (int(timestamp), float(sample.value))
                )

        rules_by_name = {rule.name: rule for rule in self._rules}
        predictions: list[Prediction] = []
        fires: dict[str, int] = {rule.name: 0 for rule in self._rules}
        for key in sorted(series):
            entity, rule_name = key
            rule = rules_by_name[rule_name]
            points = sorted(series[key])
            for moment in self._fire_times(points, rule):
                predictions.append(
                    self._make_prediction(entity, rule, moment[0], moment[1])
                )
                fires[rule_name] += 1

        predictions.sort(key=lambda item: (item.emitted_at, item.prediction_id))
        return BaselineRunResult(
            predictions=tuple(predictions),
            rules_total=len(self._rules),
            rules_with_coverage=len(covered),
            fires_by_rule=fires,
            samples_seen=seen,
            samples_unresolved=unresolved,
        )

    def _fire_times(
        self, points: Sequence[tuple[int, float]], rule: BaselineRule
    ) -> tuple[tuple[int, float], ...]:
        """When ``rule`` would notify, given one entity's ordered samples."""
        fired: list[tuple[int, float]] = []
        breach_start: int | None = None
        last_fired: int | None = None
        for timestamp, value in points:
            if not rule.breached(value):
                breach_start = None
                last_fired = None
                continue
            if breach_start is None:
                breach_start = timestamp
            if timestamp - breach_start < rule.for_duration_s:
                continue
            if last_fired is None:
                fired.append((timestamp, value))
                last_fired = timestamp
            elif timestamp - last_fired >= self._repeat_interval_s:
                fired.append((timestamp, value))
                last_fired = timestamp
        return tuple(fired)

    def _make_prediction(
        self, entity: str, rule: BaselineRule, timestamp: int, value: float
    ) -> Prediction:
        """Render one baseline firing as a contract-C2 prediction."""
        emitted_at = _dt.datetime.fromtimestamp(timestamp, tz=UTC)
        kind = self._entity_kinds.get(entity, "node")
        domain = entity_domain(kind) or "application"
        prediction_id = f"baseline-{rule.name}-{entity}-{timestamp}"
        return Prediction.from_parts(
            prediction_id=prediction_id,
            emitted_at=emitted_at,
            track=rule.track,
            entity=entity,
            # A static threshold detects the breach in front of it; it does not
            # forecast. Reporting a fabricated time-to-failure would hand the
            # baseline lead time it has not earned.
            time_to_failure_s=0,
            confidence=1.0,
            signals=(
                Signal(
                    name=rule.signal,
                    value=float(value),
                    baseline=float(rule.threshold),
                    contribution=1.0,
                ),
            ),
            root_cause=Attribution(
                domain=domain,
                entity=entity,
                evidence=(
                    Evidence(
                        kind="static_threshold",
                        detail=(
                            f"{rule.signal} {rule.comparator} {rule.threshold} "
                            f"{rule.unit} sustained for {rule.for_duration_s}s"
                        ),
                    ),
                ),
            ),
            recommended_remediation=rule.remediation,
            incident_id=prediction_id,
        )


def threshold_table(
    rules: Sequence[BaselineRule] = DEFAULT_RULES,
) -> tuple[BaselineThreshold, ...]:
    """The published threshold table for ``rules`` (RD-04)."""
    return tuple(rule.to_threshold_row() for rule in rules)
