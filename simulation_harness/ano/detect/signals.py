"""Turning baselined features into scored signals and time-to-failure estimates.

Shared by the R2 unresponsiveness detector and the R3 attribution layer, which
both need the same three things from a feature: *how abnormal is it*, *which
signal family does it belong to*, and *when does its trajectory hit the wall*.

Severity is purely relative
---------------------------
Feature #68 requires static thresholds to be removed from the detection path,
so severity is a function of the robust z-score against the learned seasonal
baseline and of nothing else. There is no "alert above 90%" anywhere in this
file. A host that normally runs at 95% CPU at 09:00 on a Tuesday is not
abnormal at 95% CPU at 09:00 on a Tuesday, and a host that normally idles is
abnormal at 40%.

The z cut-points :data:`Z_ONSET` and :data:`Z_SEVERE` are statistical, not
operational: they say "three robust sigmas is where I start believing this is
not noise, nine is where I am certain", which is a property of the estimator
rather than of any particular metric. They are shared by every feature, and
that is deliberate — a per-feature cut-point table would be static thresholds
wearing a disguise.

Time-to-failure is extrapolated, not assumed
--------------------------------------------
Requirement R2 demands a *predicted time-to-failure*, and a constant would be a
fiction. :func:`time_to_limit` fits a Theil-Sen slope over a trailing window and
projects it to the feature's failure level:

* ``unit``-ceiling features have a physical limit — descriptors against the
  kernel maximum, sessions against ``max_connections``, worker threads against
  the pool. Projecting to 1.0 is a genuine time-to-exhaustion.
* ``learned``-ceiling features have no universal limit, so the ceiling is the
  entity's own learned extreme: "as bad as this entity has ever been".
* ``down``-directional features project toward a *floor* — a collapse in
  throughput per busy thread is the failure, not a rise.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from ano.detect.baselines import BaselineEstimate
from ano.detect.features import FeatureSpec, feature_spec
from ano.detect.robust import clamp, logistic, theil_sen_slope

__all__ = [
    "Z_ONSET",
    "Z_SEVERE",
    "TREND_WINDOW_S",
    "MIN_TREND_POINTS",
    "FAMILY_FOR_FEATURE",
    "FAMILIES",
    "SYMPTOM_FEATURES",
    "HARD_FAULT_FEATURES",
    "SignalReading",
    "severity_from_z",
    "time_to_limit",
    "read_signal",
    "family_severities",
    "aggregate_confidence",
    "contributions",
]

#: Robust z at which a deviation starts counting as signal rather than noise.
#: Three sigmas of a *median/MAD* scale is a considerably stronger statement
#: than three sigmas of a mean/σ scale, because the scale itself is not
#: inflated by the excursion being measured.
Z_ONSET = 1.6

#: Robust z at which a deviation is treated as maximally severe. Above this the
#: severity saturates, so a 40-sigma reading cannot swamp the aggregate and
#: manufacture certainty from one flapping counter.
Z_SEVERE = 9.0

#: Trailing window the trend estimator fits over. Thirty minutes is long enough
#: to see a ramp through scrape noise and short enough that the slope still
#: describes the present rather than the last hour's average.
TREND_WINDOW_S = 1800.0

#: Fewer points than this in the trend window and no slope is reported. Below
#: about six samples Theil-Sen's robustness advantage disappears.
MIN_TREND_POINTS = 6

#: Signal families. Requirement R2 names five; feature #48 records that the
#: list is a minimum rather than a closed set, so the extras are declared here
#: alongside them. Grouping matters because the aggregate must reward
#: *corroboration across independent evidence* rather than repetition of one
#: observation under several names — file-descriptor utilisation and socket
#: pressure are two views of one phenomenon and must count once.
FAMILY_FOR_FEATURE: Mapping[str, str] = {
    # R2 named family: OS thread starvation
    "app.thread_busy_ratio": "service_health",
    "app.apache_busy_ratio": "service_health",
    "app.jvm_blocked_ratio": "thread_starvation",
    "app.jvm_waiting_ratio": "thread_starvation",
    "app.jvm_deadlocked": "thread_starvation",
    "app.scoreboard_wait_ratio": "thread_starvation",
    "app.work_per_busy_thread": "thread_starvation",
    # R2 named family: CPU saturation without corresponding throughput
    "app.work_per_cpu_second": "cpu_without_throughput",
    "host.work_per_cpu_second": "cpu_without_throughput",
    "host.cpu_busy_ratio": "kernel_pressure",
    "host.runq_per_cpu": "kernel_pressure",
    "app.jvm_gc_fraction": "cpu_without_throughput",
    # R2 named family: rising I/O wait
    "host.cpu_iowait_ratio": "io_wait",
    "host.procs_blocked": "io_wait",
    # R2 named family: disk latency
    "host.disk_latency_seconds": "disk_latency",
    "host.disk_busy_ratio": "disk_latency",
    # R2 named family: socket / file-descriptor exhaustion
    "host.fd_utilisation": "descriptor_exhaustion",
    "host.socket_pressure": "descriptor_exhaustion",
    "host.tcp_orphan_ratio": "descriptor_exhaustion",
    "host.tcp_timewait_ratio": "descriptor_exhaustion",
    # Declared extras (feature #48)
    "host.memory_used_ratio": "memory_pressure",
    "host.major_fault_rate": "memory_pressure",
    "app.jvm_heap_ratio": "memory_pressure",
    "host.cpu_system_ratio": "kernel_pressure",
    "host.context_switch_rate": "kernel_pressure",
    "app.error_ratio": "service_health",
    "app.mean_latency_seconds": "service_health",
    "app.request_rate": "service_health",
    "app.session_reject_rate": "service_health",
    # Database evidence (R3)
    "db.lock_wait_rate": "db_contention",
    "db.lock_current_waits": "db_contention",
    "db.lock_time_avg_ms": "db_contention",
    "db.table_lock_contention": "db_contention",
    "db.slow_query_rate": "db_long_running_sql",
    "db.buffer_pool_wait_rate": "db_long_running_sql",
    "db.session_pool_ratio": "db_session_pool",
    "db.threads_running": "db_session_pool",
    "db.aborted_connect_rate": "db_session_pool",
    "db.work_per_running_session": "db_contention",
    "db.query_rate": "db_throughput",
    # Network evidence (R3)
    "net.port_error_rate": "net_port_errors",
    "net.discard_rate": "net_packet_loss",
    "net.loss_ratio": "net_packet_loss",
    "net.port_down": "net_port_errors",
    "net.port_utilisation": "net_congestion",
    "net.ospf_event_rate": "net_ospf_flap",
    "net.ospf_not_full": "net_ospf_flap",
}

#: Every declared family, sorted.
FAMILIES: tuple[str, ...] = tuple(sorted(set(FAMILY_FOR_FEATURE.values())))

#: Features that are *symptoms* of a degradation rather than evidence of its
#: cause. They legitimately predict that an entity is about to become
#: unresponsive (R2), but they must never count as evidence that the entity is
#: itself the root cause (R3) — a Tomcat server whose latency has tripled
#: because its database is blocked looks exactly like one that is broken, and
#: attributing on the symptom is precisely the naive-correlation failure the
#: milestone exists to avoid.
SYMPTOM_FEATURES: frozenset[str] = frozenset(
    {
        "app.mean_latency_seconds",
        "app.error_ratio",
        "app.request_rate",
        "app.thread_busy_ratio",
        "app.apache_busy_ratio",
        "app.jvm_waiting_ratio",
        "app.work_per_busy_thread",
        "app.session_reject_rate",
        "app.scoreboard_wait_ratio",
    }
)

#: Faults that no scheduled change can plausibly explain. A patching window
#: accounts for a CPU spike and a restart; it does not account for a deadlocked
#: JVM or a link that has gone down. Consulted by the suppression layer so
#: change-awareness never mutes a genuine hard failure that merely happens to
#: coincide with maintenance (ruling IR-03a).
HARD_FAULT_FEATURES: frozenset[str] = frozenset(
    {
        "app.jvm_deadlocked",
        "net.port_down",
        "net.ospf_not_full",
    }
)


@dataclass(frozen=True, slots=True)
class SignalReading:
    """One feature, scored against its baseline at one instant.

    Attributes:
        feature: Feature name.
        title: Operator-facing name.
        unit: Unit of ``value`` and ``baseline``.
        family: Signal family this feature belongs to.
        value: The observation.
        baseline: The learned seasonal centre it was scored against.
        sigma: The learned scale.
        z: Robust z-score. Signed in the feature's natural direction.
        severity: Pathological-ness in ``[0, 1]``, already oriented so that
            higher always means worse regardless of the feature's direction.
        direction: ``"up"`` or ``"down"``.
        time_to_limit_s: Extrapolated seconds until this feature reaches its
            failure level, or ``None`` when the trajectory does not point
            there.
        slope_per_s: The fitted trend, in feature units per second.
        ready: Whether the baseline had enough history to be believed.
        hour: Hour of day the reading was taken at.
        weekday: Day of week the reading was taken at.
    """

    feature: str
    title: str
    unit: str
    family: str
    value: float
    baseline: float
    sigma: float
    z: float
    severity: float
    direction: str
    time_to_limit_s: float | None
    slope_per_s: float
    ready: bool
    hour: int
    weekday: int

    def is_symptom(self) -> bool:
        """Whether this feature is a symptom rather than causal evidence."""
        return self.feature in SYMPTOM_FEATURES

    def is_hard_fault(self) -> bool:
        """Whether this feature indicates a fault no change window explains."""
        return self.feature in HARD_FAULT_FEATURES and self.severity > 0.0

    def describe(self) -> str:
        """A one-line operator-facing rendering."""
        return (
            f"{self.title} = {self.value:.4g} {self.unit} "
            f"(baseline {self.baseline:.4g}, {self.z:+.1f} robust sigma)"
        )


def severity_from_z(z: float, direction: str) -> float:
    """Map a signed robust z-score onto a ``[0, 1]`` severity.

    Args:
        z: The robust z-score, signed in the feature's natural direction.
        direction: ``"up"`` if a rise is pathological, ``"down"`` if a fall is.

    Returns:
        Severity in ``[0, 1]``: zero below :data:`Z_ONSET`, rising linearly to
        one at :data:`Z_SEVERE` and saturating there.

    Raises:
        ValueError: If ``direction`` is neither ``"up"`` nor ``"down"``.
    """
    if direction == "up":
        oriented = z
    elif direction == "down":
        oriented = -z
    else:
        raise ValueError(f"unknown direction {direction!r}")
    if not math.isfinite(oriented):
        return 1.0 if oriented > 0 else 0.0
    return clamp((oriented - Z_ONSET) / (Z_SEVERE - Z_ONSET), 0.0, 1.0)


def _failure_level(
    spec: FeatureSpec, estimate: BaselineEstimate
) -> float | None:
    """The value at which a feature's trajectory constitutes failure.

    Args:
        spec: The feature's specification.
        estimate: Its current baseline estimate.

    Returns:
        The failure level, or ``None`` when the feature has none.
    """
    if spec.direction == "down":
        # A collapse is the failure. The floor is a fraction of what this
        # entity normally achieves, so it is learned rather than chosen: losing
        # nine tenths of your normal throughput per thread is a failure at any
        # absolute scale.
        if estimate.center <= 0.0:
            return None
        return 0.1 * estimate.center
    if spec.ceiling == "unit":
        return 1.0
    if spec.ceiling == "learned":
        if estimate.ceiling is not None:
            # The learned extreme, pushed out by a margin so a feature already
            # sitting at its historical worst still reports a finite horizon
            # rather than zero.
            return max(estimate.ceiling, estimate.center + 6.0 * estimate.sigma)
        if estimate.sigma > 0.0:
            return estimate.center + 6.0 * estimate.sigma
        return None
    return None


def time_to_limit(
    spec: FeatureSpec,
    estimate: BaselineEstimate,
    times: Sequence[float],
    values: Sequence[float],
) -> tuple[float | None, float]:
    """Extrapolate a feature's trajectory to its failure level.

    Args:
        spec: The feature's specification.
        estimate: Its current baseline estimate, which supplies the learned
            failure level for unbounded features.
        times: Trailing-window observation times, epoch seconds.
        values: Matching values.

    Returns:
        ``(seconds_to_failure, slope_per_second)``. The first element is
        ``None`` when there is no usable trend, when the trend points away from
        failure, or when the feature has no failure level.
    """
    if len(times) < MIN_TREND_POINTS or len(times) != len(values):
        return None, 0.0
    slope = theil_sen_slope(times, values)
    if slope == 0.0 or not math.isfinite(slope):
        return None, slope

    level = _failure_level(spec, estimate)
    if level is None:
        return None, slope

    current = values[-1]
    if spec.direction == "up":
        if slope <= 0.0 or current >= level:
            return None, slope
        return (level - current) / slope, slope
    if slope >= 0.0 or current <= level:
        return None, slope
    return (current - level) / (-slope), slope


def read_signal(
    feature: str,
    estimate: BaselineEstimate,
    value: float,
    *,
    times: Sequence[float] = (),
    values: Sequence[float] = (),
) -> SignalReading:
    """Score one feature at one instant.

    Args:
        feature: Feature name.
        estimate: Its baseline estimate, produced *before* this observation was
            learned from.
        value: The observation.
        times: Trailing-window times for the trend fit. Optional.
        values: Matching trailing-window values.

    Returns:
        The scored reading. ``severity`` is zero whenever the baseline is not
        ready, so an unwarmed model abstains rather than guessing.

    Raises:
        KeyError: If ``feature`` is not in the catalogue.
    """
    spec = feature_spec(feature)
    severity = severity_from_z(estimate.z, spec.direction) if estimate.ready else 0.0
    if severity > 0.0 and times and values:
        horizon, slope = time_to_limit(spec, estimate, times, values)
    else:
        horizon, slope = None, 0.0
    return SignalReading(
        feature=feature,
        title=spec.title,
        unit=spec.unit,
        family=FAMILY_FOR_FEATURE.get(feature, "other"),
        value=value,
        baseline=estimate.center,
        sigma=estimate.sigma,
        z=estimate.z,
        severity=severity,
        direction=spec.direction,
        time_to_limit_s=horizon,
        slope_per_s=slope,
        ready=estimate.ready,
        hour=estimate.hour,
        weekday=estimate.weekday,
    )


def family_severities(
    readings: Sequence[SignalReading],
    *,
    exclude: frozenset[str] = frozenset(),
) -> dict[str, float]:
    """Collapse readings to one severity per family.

    The maximum within a family, not the sum: four views of one descriptor leak
    are one piece of evidence, and summing them would let a single phenomenon
    masquerade as a corroborated case.

    Args:
        readings: Scored readings.
        exclude: Feature names to leave out entirely.

    Returns:
        Family -> severity, omitting families with no positive severity.
    """
    out: dict[str, float] = {}
    for reading in readings:
        if reading.feature in exclude or reading.severity <= 0.0:
            continue
        current = out.get(reading.family, 0.0)
        if reading.severity > current:
            out[reading.family] = reading.severity
    return out


def aggregate_confidence(
    severities: Mapping[str, float],
    weights: Mapping[str, float],
    bias: float,
) -> float:
    """Combine family severities into a confidence.

    A log-odds sum passed through a logistic. Chosen over a maximum or a mean
    for two reasons that matter to the acceptance criteria:

    * **Corroboration is rewarded.** With a bias more negative than any single
      weight, no lone family can clear a sensible decision threshold on its
      own. That is the main defence against the false-positive bar, because
      isolated excursions are what benign events look like.
    * **Contributions decompose.** Each family's share of the positive log-odds
      mass is a well-defined number, which is exactly what contract C2's
      ``Signal.contribution`` field has to carry and what an operator needs in
      order to act rather than reboot.

    The weights are declared expert priors, not fitted coefficients. They
    cannot be fitted: requirement R5 forbids the detector from seeing a label,
    so there is no training signal to fit against, and inventing one would be
    the integrity violation this project is most alert to. They are documented,
    uniform across scenario kinds, and never adjusted per entity.

    Args:
        severities: Family -> severity in ``[0, 1]``.
        weights: Family -> log-odds weight.
        bias: Base log-odds, negative, setting the no-evidence prior.

    Returns:
        Confidence in ``(0, 1)``.
    """
    logit = bias
    for family, severity in severities.items():
        logit += weights.get(family, 0.0) * severity
    return logistic(logit)


def contributions(
    readings: Sequence[SignalReading],
    severities: Mapping[str, float],
    weights: Mapping[str, float],
) -> dict[str, float]:
    """Per-feature share of the evidence, summing to at most 1.

    A family's log-odds mass is attributed to the single reading that defined
    it — the one whose severity the family took as its maximum — so the
    contributions an operator sees name the specific measurement that drove the
    prediction rather than spreading credit over its correlates.

    Args:
        readings: Scored readings.
        severities: Family -> severity, from :func:`family_severities`.
        weights: Family -> log-odds weight.

    Returns:
        Feature name -> contribution in ``[0, 1]``. Empty when there is no
        positive evidence.
    """
    driver: dict[str, SignalReading] = {}
    for reading in readings:
        if reading.severity <= 0.0:
            continue
        if reading.family not in severities:
            continue
        best = driver.get(reading.family)
        if best is None or reading.severity > best.severity or (
            reading.severity == best.severity and reading.feature < best.feature
        ):
            driver[reading.family] = reading

    mass = {
        family: weights.get(family, 0.0) * severity
        for family, severity in severities.items()
        if weights.get(family, 0.0) > 0.0 and severity > 0.0
    }
    total = sum(mass.values())
    if total <= 0.0:
        return {}
    return {
        driver[family].feature: value / total
        for family, value in mass.items()
        if family in driver
    }
