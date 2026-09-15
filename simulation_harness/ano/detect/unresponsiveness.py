"""R2 — predicting server and application unresponsiveness before it happens.

What it has to do
-----------------
Requirement R2: predict impending server/application unresponsiveness 15-30
minutes ahead, from OS thread starvation, CPU saturation *without corresponding
throughput*, rising I/O wait, disk latency, and socket/file-descriptor
exhaustion. Every prediction carries a timestamp, a predicted time-to-failure,
a confidence, and the specific signals that drove it — "enough for an operator
to act preemptively rather than reboot afterwards".

How it works
------------
One causal pass per entity over the evaluation window. At each scrape:

1. **Score, then learn.** Every computable feature is scored against the
   seasonal baseline *as it stands*, and only afterwards folded into it. The
   model can never be asked about a point it has already seen.
2. **Group into families.** Severities collapse to one value per signal family
   (:mod:`ano.detect.signals`), so four views of one descriptor leak count once.
3. **Aggregate with corroboration required.** A log-odds sum with a bias more
   negative than any single weight, so one family alone cannot clear the
   decision threshold. This is the main defence against the false-positive bar.
4. **Require dwell.** The confidence must hold above the threshold for several
   consecutive scrapes. A single noisy scrape is not a prediction.
5. **Extrapolate a horizon.** Time-to-failure comes from the fastest trajectory
   among the contributing signals, or from the ramp of the hazard itself.

Why not one big anomaly score
-----------------------------
The temptation is to z-score everything and alert on the maximum. That fails
the discriminations this project is graded on. A benign traffic surge maximises
CPU, thread occupancy and request latency simultaneously — a maximum-anomaly
detector alerts, and AC-06 counts it as a false positive. The divergence
features (:data:`~ano.detect.features.WORK_PER_CPU`,
:data:`~ano.detect.features.WORK_PER_BUSY_THREAD`) are what separate the two
cases: under a surge the work rises with the utilisation and efficiency is
unchanged, so those features do not deviate and the corroboration requirement
is never met.

Not firing continuously
-----------------------
RD-02 penalises a detector that simply fires all the time: an alert earlier
than the 120-minute horizon earns no credit and lands in ``fpr_strict`` and
``unmatched_alert_count``. Two mechanisms keep volume honest, and neither looks
at ground truth:

* **Dwell** — a prediction needs sustained evidence, so noise does not fire.
* **Refractory with escalation** — after firing for an entity the detector goes
  quiet for a cooldown, breaking it only if confidence rises materially. An
  operator does not want the same page every thirty seconds either, so this is
  what a real system does regardless of how it is scored.

What this module deliberately does **not** do
---------------------------------------------
It does not decide *who is to blame*. It reports that an entity is heading for
trouble and what evidence says so. :mod:`ano.correlate.attribution` decides
whether the cause is the entity itself, a database behind it, or a switch port
underneath it — and that decision, not this one, sets the contract-C2 track.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field

from ano.contracts.determinism import stable_id
from ano.detect.baselines import SeasonalBaselineStore
from ano.detect.candidate import Candidate
from ano.detect.features import (
    FeatureFrame,
    feature_spec,
    features_for_kind,
)
from ano.detect.robust import clamp, theil_sen_slope
from ano.detect.signals import (
    TREND_WINDOW_S,
    SignalReading,
    aggregate_confidence,
    contributions,
    family_severities,
    read_signal,
    severity_from_z,
)

__all__ = [
    "MAX_HORIZON_S",
    "MIN_HORIZON_S",
    "FAMILY_WEIGHTS",
    "DEFAULT_BIAS",
    "DEFAULT_FIRE_THRESHOLD",
    "DEFAULT_DWELL_STEPS",
    "DEFAULT_REFRACTORY_S",
    "DEFAULT_ESCALATION_DELTA",
    "DetectorConfig",
    "EntityState",
    "UnresponsivenessDetector",
    "detect_unresponsiveness",
]

#: The declared maximum prediction horizon (RD-02), in seconds. A forecast
#: further out than this earns no lead-time credit, so there is no reason to
#: claim one.
MAX_HORIZON_S = 7200

#: Shortest horizon the detector will claim. Below five minutes a "prediction"
#: is a description of something already happening.
MIN_HORIZON_S = 300

#: Log-odds weight per signal family. **Declared expert priors, not fitted
#: coefficients.**
#:
#: They cannot be fitted. Requirement R5 forbids the detector from reading a
#: ground-truth label, so there is no supervision signal to fit against, and
#: manufacturing one would be precisely the integrity violation this project
#: guards against. They are therefore set from operational reasoning, applied
#: uniformly to every entity and every scenario kind, and never tuned per case:
#:
#: * The three families with a **hard physical ceiling** — descriptor
#:   exhaustion, thread starvation, memory pressure — carry the most weight.
#:   They have an unambiguous failure state, so a sustained approach to it is
#:   strong evidence rather than an interpretation.
#: * **CPU without throughput** carries comparable weight because, unlike raw
#:   utilisation, it has no benign explanation: work per unit of resource does
#:   not collapse when a host is merely busy.
#: * **Storage** families carry less. Rising I/O wait and disk latency
#:   genuinely precede hangs, but they also fluctuate with legitimate batch
#:   activity, so they are better corroborators than instigators.
#: * **Service health** carries least. Latency and error ratio are *symptoms*;
#:   they confirm a developing problem but must never be able to raise an alert
#:   by themselves, or every downstream victim of an upstream fault becomes its
#:   own incident.
FAMILY_WEIGHTS: Mapping[str, float] = {
    "descriptor_exhaustion": 3.5,
    "thread_starvation": 2.6,
    "cpu_without_throughput": 2.5,
    "memory_pressure": 3.5,
    "io_wait": 1.8,
    "disk_latency": 1.7,
    "kernel_pressure": 1.2,
    "service_health": 1.1,
    # Database and network families participate so that a cause entity in
    # another domain can raise its own prediction, which is what lets the
    # cross-domain track name the responsible entity directly.
    "db_contention": 2.6,
    "db_session_pool": 2.2,
    "db_long_running_sql": 2.0,
    "db_throughput": 1.1,
    "net_port_errors": 2.6,
    "net_packet_loss": 2.5,
    "net_ospf_flap": 2.5,
    "net_congestion": 1.4,
}

#: Base log-odds with no evidence at all. More negative than the largest single
#: weight, which is the whole point: no lone family can clear the decision
#: threshold, so a prediction always rests on corroborated evidence.
DEFAULT_BIAS = -3.3

#: Confidence at which a sustained candidate becomes a prediction.
DEFAULT_FIRE_THRESHOLD = 0.48

#: Consecutive scrapes the threshold must hold before firing.
DEFAULT_DWELL_STEPS = 2

#: Quiet period after firing for an entity, in seconds.
DEFAULT_REFRACTORY_S = 1800.0

#: Confidence increase that breaks the refractory period early. An escalating
#: situation should re-page; a steady one should not.
DEFAULT_ESCALATION_DELTA = 0.20

#: Confidence fraction of the fire threshold at which a degradation is
#: considered to have *begun*, used for the persistence-based horizon fallback
#: and for the temporal-precedence evidence attribution relies on.
ONSET_FRACTION = 0.5


@dataclass(frozen=True, slots=True)
class DetectorConfig:
    """Tunable decision parameters.

    Every field is a property of the *decision procedure*, not a threshold on
    any telemetry signal — feature #68 removes the latter from the detection
    path entirely. Exposed so the pipeline can vary them from configuration
    without editing code, and so tests can drive the detector deterministically.

    Attributes:
        fire_threshold: Confidence at which a sustained candidate fires.
        dwell_steps: Consecutive scrapes the threshold must hold.
        refractory_s: Quiet period after firing for an entity.
        escalation_delta: Confidence rise that breaks the refractory early.
        bias: Base log-odds with no evidence.
        weights: Family -> log-odds weight.
        trend_window_s: Trailing window the trajectory estimator fits over.
        max_horizon_s: Longest time-to-failure the detector will claim.
        min_horizon_s: Shortest time-to-failure the detector will claim.
        max_signals: Driving signals attached to each prediction.
    """

    fire_threshold: float = DEFAULT_FIRE_THRESHOLD
    dwell_steps: int = DEFAULT_DWELL_STEPS
    refractory_s: float = DEFAULT_REFRACTORY_S
    escalation_delta: float = DEFAULT_ESCALATION_DELTA
    bias: float = DEFAULT_BIAS
    weights: Mapping[str, float] = field(default_factory=lambda: dict(FAMILY_WEIGHTS))
    trend_window_s: float = TREND_WINDOW_S
    max_horizon_s: int = MAX_HORIZON_S
    min_horizon_s: int = MIN_HORIZON_S
    max_signals: int = 8

    def __post_init__(self) -> None:
        if not 0.0 < self.fire_threshold < 1.0:
            raise ValueError("fire_threshold must lie in (0, 1)")
        if self.dwell_steps < 1:
            raise ValueError("dwell_steps must be at least 1")
        if self.min_horizon_s < 1 or self.max_horizon_s < self.min_horizon_s:
            raise ValueError("horizon bounds are inconsistent")


@dataclass
class EntityState:
    """Per-entity detector state carried across scrapes.

    Attributes:
        streak: Consecutive scrapes at or above the fire threshold.
        onset_at: When the current degradation episode first showed evidence,
            or ``None`` between episodes. Exposed to attribution as the
            entity's temporal-precedence marker.
        last_fired_at: When this entity last produced a prediction.
        last_fired_confidence: The confidence it fired at.
        history: Trailing ``(time, confidence)`` pairs, for the hazard ramp.
    """

    streak: int = 0
    onset_at: float | None = None
    last_fired_at: float | None = None
    last_fired_confidence: float = 0.0
    history: list[tuple[float, float]] = field(default_factory=list)

    def record(self, moment: float, confidence: float, window_s: float) -> None:
        """Append a confidence observation and drop points outside the window."""
        self.history.append((moment, confidence))
        cutoff = moment - window_s
        while self.history and self.history[0][0] < cutoff:
            self.history.pop(0)


@dataclass(frozen=True, slots=True)
class _Step:
    """One scored instant for one entity, before any firing decision."""

    entity: str
    moment: float
    readings: tuple[SignalReading, ...]
    severities: Mapping[str, float]
    confidence: float


@dataclass(frozen=True, slots=True)
class StepSummary:
    """A compact record of one entity at one scrape, kept for attribution.

    Attribution has to ask "what was the database doing at the moment this
    application server fired?" — and the database will usually not have fired
    itself, so the answer cannot be recovered from the candidate stream. It
    also cannot be recovered by re-scoring, because the baseline has moved on
    since. Hence a summary of every step is retained.

    Only *causal* evidence is kept, never symptoms: an application's latency is
    excluded by construction, so it can never be mistaken for proof that the
    application is the cause. That exclusion is the single most important line
    of defence against the naive-correlation failure mode.

    Attributes:
        entity: Entity id.
        moment: Epoch seconds.
        confidence: The aggregate hazard at this instant.
        causal_severity: Strongest severity among non-symptom signals — how
            much evidence exists that this entity is *itself* faulty.
        causal_families: Family -> severity, non-symptom signals only, omitting
            zeroes.
        top_causal_feature: The non-symptom feature carrying
            ``causal_severity``, or ``None``.
        top_causal_detail: An operator-readable rendering of that feature.
        horizon_s: The time-to-failure this entity's own trajectory implies, or
            ``None`` when it was not computed.
        hard_fault: Whether a fault no change window can explain is present.
    """

    entity: str
    moment: float
    confidence: float
    causal_severity: float
    causal_families: Mapping[str, float]
    top_causal_feature: str | None
    top_causal_detail: str
    horizon_s: int | None
    hard_fault: bool


class UnresponsivenessDetector:
    """The R2 detector.

    Args:
        baselines: The seasonal baseline store. Shared with the rest of the
            pipeline so every layer scores against the same learned normal.
        config: Decision parameters.
    """

    __slots__ = (
        "_baselines",
        "_config",
        "_state",
        "_onsets",
        "_severity_trace",
        "_summaries",
    )

    def __init__(
        self,
        baselines: SeasonalBaselineStore,
        config: DetectorConfig | None = None,
    ) -> None:
        self._baselines = baselines
        self._config = config or DetectorConfig()
        self._state: dict[str, EntityState] = {}
        self._onsets: dict[str, float] = {}
        self._severity_trace: dict[str, list[tuple[float, float]]] = {}
        self._summaries: dict[str, list[StepSummary]] = {}

    @property
    def config(self) -> DetectorConfig:
        """The decision parameters in force."""
        return self._config

    def summaries(self, entity: str) -> tuple[StepSummary, ...]:
        """Every scored step for ``entity``, chronologically.

        The evidence record attribution reads. See :class:`StepSummary`.

        Args:
            entity: Entity id.

        Returns:
            Chronological summaries, empty for an entity that was never run.
        """
        return tuple(self._summaries.get(entity, ()))

    def all_summaries(self) -> dict[str, tuple[StepSummary, ...]]:
        """Per-step summaries for every entity run so far."""
        return {
            entity: tuple(steps) for entity, steps in sorted(self._summaries.items())
        }

    def state_for(self, entity: str) -> EntityState:
        """Per-entity state, created on first use."""
        found = self._state.get(entity)
        if found is None:
            found = EntityState()
            self._state[entity] = found
        return found

    def distress_onset(self, entity: str) -> float | None:
        """When this entity's current degradation episode first showed evidence.

        Attribution uses this for temporal precedence: a cause that started
        deteriorating before its dependents is a better explanation than one
        that started afterwards.

        Args:
            entity: Entity id.

        Returns:
            Epoch seconds, or ``None`` if the entity has no open episode.
        """
        return self._onsets.get(entity)

    def severity_trace(self, entity: str) -> tuple[tuple[float, float], ...]:
        """This entity's ``(time, confidence)`` history over the whole window.

        Attribution correlates these traces between a symptom and its candidate
        causes, which is the "correlation evidence" requirement R3 asks for.

        Args:
            entity: Entity id.

        Returns:
            Chronological pairs.
        """
        return tuple(self._severity_trace.get(entity, ()))

    # -- learning ---------------------------------------------------------

    def learn(self, frame: FeatureFrame, entity: str) -> int:
        """Fold an entity's whole frame into the baselines without scoring it.

        Used for the corpus's ``history`` phase: three months of unlabelled
        telemetry that exists to teach the model what normal looks like,
        including its hour-of-day and day-of-week shape.

        Args:
            frame: The derived feature frame.
            entity: Entity to learn from.

        Returns:
            How many observations were folded in.
        """
        times = frame.times.get(entity, ())
        learned = 0
        for name in frame.feature_names(entity):
            series = frame.series(entity, name)
            for moment, value in zip(times, series):
                if value is not None and math.isfinite(value):
                    self._baselines.observe(entity, name, moment, value)
                    learned += 1
        return learned

    # -- scoring ----------------------------------------------------------

    def _trailing(
        self,
        times: Sequence[float],
        values: Sequence[float | None],
        index: int,
    ) -> tuple[list[float], list[float]]:
        """Known points inside the trend window ending at ``index``."""
        cutoff = times[index] - self._config.trend_window_s
        out_times: list[float] = []
        out_values: list[float] = []
        for i in range(index, -1, -1):
            if times[i] < cutoff:
                break
            value = values[i]
            if value is not None and math.isfinite(value):
                out_times.append(times[i])
                out_values.append(value)
        out_times.reverse()
        out_values.reverse()
        return out_times, out_values

    def score_step(
        self, frame: FeatureFrame, entity: str, index: int
    ) -> _Step:
        """Score one entity at one scrape, **without** learning from it.

        Args:
            frame: The derived feature frame.
            entity: Entity to score.
            index: Position in that entity's timeline.

        Returns:
            The scored step.
        """
        times = frame.times.get(entity, ())
        moment = times[index]
        kind = frame.kind(entity)
        readings: list[SignalReading] = []

        for spec in features_for_kind(kind):
            series = frame.values.get((entity, spec.name))
            if series is None:
                continue
            value = series[index]
            if value is None or not math.isfinite(value):
                continue
            estimate = self._baselines.score(
                entity,
                spec.name,
                moment,
                value,
                absolute_floor=spec.absolute_floor,
                relative_floor=spec.relative_floor,
            )
            sev = severity_from_z(estimate.z, spec.direction) if estimate.ready else 0.0
            if sev > 0.0:
                trail_times, trail_values = self._trailing(times, series, index)
            else:
                trail_times, trail_values = (), ()
            readings.append(
                read_signal(
                    spec.name,
                    estimate,
                    value,
                    times=trail_times,
                    values=trail_values,
                )
            )

        severities = family_severities(readings)
        confidence = aggregate_confidence(
            severities, self._config.weights, self._config.bias
        )
        return _Step(
            entity=entity,
            moment=moment,
            readings=tuple(readings),
            severities=severities,
            confidence=confidence,
        )

    def observe_step(self, frame: FeatureFrame, entity: str, index: int) -> None:
        """Fold one scrape into the baselines. Call **after** scoring it."""
        times = frame.times.get(entity, ())
        moment = times[index]
        for name in frame.feature_names(entity):
            value = frame.at(entity, name, index)
            if value is not None and math.isfinite(value):
                self._baselines.observe(entity, name, moment, value)

    # -- horizon ----------------------------------------------------------

    def _horizon(self, step: _Step, state: EntityState) -> int:
        """Predicted seconds to failure.

        Three estimators, in preference order, and the soonest wins because the
        first wall you hit is the one that stops you:

        1. **Signal trajectories.** The soonest extrapolated arrival at a
           failure level among the contributing signals. Physically meaningful
           wherever a hard limit exists — descriptors, session pools, thread
           pools.
        2. **Hazard ramp.** Fit the confidence itself over the trailing window
           and project it to certainty. Works when no individual signal has a
           ceiling but the situation is visibly accelerating.
        3. **Persistence.** If neither applies, expect the episode to take
           about as long again to complete as it has already run. Crude, and
           labelled as such, but bounded and never zero — an honest fallback
           beats an invented constant.

        Args:
            step: The scored instant.
            state: The entity's carried state.

        Returns:
            Seconds, clamped into the configured horizon bounds.
        """
        estimates: list[float] = []

        for reading in step.readings:
            if reading.severity <= 0.0 or reading.time_to_limit_s is None:
                continue
            if reading.time_to_limit_s > 0.0 and math.isfinite(reading.time_to_limit_s):
                estimates.append(reading.time_to_limit_s)

        if len(state.history) >= 4:
            times = [moment for moment, _ in state.history]
            values = [value for _, value in state.history]
            slope = theil_sen_slope(times, values)
            if slope > 0.0:
                remaining = (1.0 - step.confidence) / slope
                if remaining > 0.0 and math.isfinite(remaining):
                    estimates.append(remaining)

        if not estimates and state.onset_at is not None:
            elapsed = step.moment - state.onset_at
            if elapsed > 0.0:
                estimates.append(elapsed)

        if not estimates:
            # Nothing to extrapolate from: claim the shortest horizon the
            # detector is willing to stand behind rather than a flattering one.
            return self._config.min_horizon_s

        return int(
            round(
                clamp(
                    min(estimates),
                    float(self._config.min_horizon_s),
                    float(self._config.max_horizon_s),
                )
            )
        )

    # -- firing -----------------------------------------------------------

    def _should_fire(self, step: _Step, state: EntityState) -> bool:
        """Whether a sustained candidate should become a prediction."""
        if state.streak < self._config.dwell_steps:
            return False
        if state.last_fired_at is None:
            return True
        quiet_for = step.moment - state.last_fired_at
        if quiet_for >= self._config.refractory_s:
            return True
        # Escalation breaks the refractory: a situation that is materially
        # worse than when we last paged deserves another page.
        return step.confidence >= state.last_fired_confidence + self._config.escalation_delta

    def _build(
        self, step: _Step, state: EntityState, horizon: int | None = None
    ) -> Candidate:
        """Turn a firing step into a candidate.

        Args:
            step: The scored instant.
            state: The entity's carried state.
            horizon: Precomputed time-to-failure. Recomputed when omitted, but
                the caller normally has it already and passing it through keeps
                the prediction and the retained step summary in agreement.

        Returns:
            The candidate.
        """
        shares = contributions(step.readings, step.severities, self._config.weights)
        seconds = self._horizon(step, state) if horizon is None else horizon
        candidate_id = stable_id(
            f"{step.entity}|{step.moment:.3f}|{seconds}",
            "ano",
            "detect",
            "candidate",
            prefix="pred-",
        )
        return Candidate(
            candidate_id=candidate_id,
            entity=step.entity,
            emitted_at=step.moment,
            time_to_failure_s=seconds,
            confidence=step.confidence,
            raw_confidence=step.confidence,
            readings=step.readings,
            contributions=shares,
            family_severities=dict(step.severities),
        )

    def _summarise(
        self, step: _Step, horizon: int | None
    ) -> StepSummary:
        """Build the retained evidence record for one scored step.

        Causal severity deliberately excludes :data:`SYMPTOM_FEATURES`. An
        application server whose latency has tripled because its database is
        blocked shows a large *hazard* and near-zero *causal* evidence, and
        that gap is what lets attribution look past it to the real cause.
        """
        causal = [
            reading
            for reading in step.readings
            if reading.severity > 0.0 and not reading.is_symptom()
        ]
        families: dict[str, float] = {}
        for reading in causal:
            current = families.get(reading.family, 0.0)
            if reading.severity > current:
                families[reading.family] = reading.severity
        top = max(
            causal,
            key=lambda r: (r.severity, r.feature),
            default=None,
        )
        return StepSummary(
            entity=step.entity,
            moment=step.moment,
            confidence=step.confidence,
            causal_severity=top.severity if top is not None else 0.0,
            causal_families=families,
            top_causal_feature=top.feature if top is not None else None,
            top_causal_detail=top.describe() if top is not None else "",
            horizon_s=horizon,
            hard_fault=any(reading.is_hard_fault() for reading in step.readings),
        )

    def run_entity(self, frame: FeatureFrame, entity: str) -> list[Candidate]:
        """Score an entity's whole window causally and return its candidates.

        Args:
            frame: The derived feature frame for the evaluation window.
            entity: Entity to run.

        Returns:
            Candidates in chronological order.
        """
        times = frame.times.get(entity, ())
        state = self.state_for(entity)
        trace = self._severity_trace.setdefault(entity, [])
        summaries = self._summaries.setdefault(entity, [])
        onset_level = self._config.fire_threshold * ONSET_FRACTION
        produced: list[Candidate] = []

        for index in range(len(times)):
            step = self.score_step(frame, entity, index)
            trace.append((step.moment, step.confidence))
            state.record(step.moment, step.confidence, self._config.trend_window_s)

            if step.confidence >= onset_level:
                if state.onset_at is None:
                    state.onset_at = step.moment
                    self._onsets[entity] = step.moment
            else:
                state.onset_at = None
                self._onsets.pop(entity, None)

            if step.confidence >= self._config.fire_threshold:
                state.streak += 1
            else:
                state.streak = 0

            # Only worth extrapolating once there is something to extrapolate.
            horizon = (
                self._horizon(step, state) if step.confidence >= onset_level else None
            )
            summaries.append(self._summarise(step, horizon))

            if step.confidence >= self._config.fire_threshold and self._should_fire(
                step, state
            ):
                candidate = self._build(step, state, horizon)
                produced.append(candidate)
                state.last_fired_at = step.moment
                state.last_fired_confidence = step.confidence

            # Learn only after the decision, so the model can never be asked
            # about a point it has already absorbed.
            self.observe_step(frame, entity, index)

        return produced

    def run(
        self, frame: FeatureFrame, *, entities: Iterable[str] | None = None
    ) -> list[Candidate]:
        """Run every entity in the frame.

        Entities are processed in sorted order and their candidates returned
        sorted by ``(emitted_at, candidate_id)``, so a run is reproducible
        regardless of dictionary iteration order.

        Args:
            frame: The derived feature frame for the evaluation window.
            entities: Restrict to these entities. Defaults to all.

        Returns:
            All candidates, chronologically ordered.
        """
        chosen = sorted(entities) if entities is not None else list(frame.entities())
        produced: list[Candidate] = []
        for entity in chosen:
            produced.extend(self.run_entity(frame, entity))
        produced.sort(key=lambda item: (item.emitted_at, item.candidate_id))
        return produced


def detect_unresponsiveness(
    frame: FeatureFrame,
    baselines: SeasonalBaselineStore,
    *,
    config: DetectorConfig | None = None,
) -> list[Candidate]:
    """Convenience wrapper: run the R2 detector over a whole frame.

    Args:
        frame: The derived feature frame for the evaluation window.
        baselines: Seasonal baselines, ideally already warmed on the corpus's
            history phase.
        config: Decision parameters.

    Returns:
        Candidates, chronologically ordered.
    """
    return UnresponsivenessDetector(baselines, config).run(frame)


_ = feature_spec  # re-exported indirectly through the signal layer
