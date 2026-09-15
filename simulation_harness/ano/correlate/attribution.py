"""R3 — attributing a degradation to the right domain and the right entity.

The problem, stated precisely
-----------------------------
When a database seizes on a lock, every application server that queries it goes
slow within a scrape or two. An operator's console shows three unhappy
application servers and one database whose *latency* metrics look unremarkable
— databases under lock contention are often doing very little work, which is
the whole problem. Correlate naively and you file three application-domain
incidents against the three victims and miss the cause entirely. Requirement R3
exists because that is what most monitoring actually does, and RD-05 grades it:
database-contention-driven application slowness must be attributed to the
**database** domain in a strict majority of *all* such injected incidents, not
merely the ones the system happens to alert on.

How this module gets it right
-----------------------------
Four independent lines of evidence per candidate cause, combined
multiplicatively on the first:

**1. Local causal evidence — necessary, not merely contributory.**
   Only *non-symptom* signals count. An application's latency, error ratio,
   worker occupancy and requests-per-thread are excluded by construction: they
   are what a victim looks like. What remains for an application is genuinely
   intrinsic — heap exhaustion, garbage-collection collapse, blocked monitors,
   descriptor exhaustion, storage stalls. A database offers lock waits, slow
   statements and session-pool pressure; a port offers errors, discards and
   lost adjacency. The score is *multiplied* by this term, so a candidate with
   no evidence of its own cannot win on circumstantial grounds alone. That
   single design choice is what stops the symptomatic application server from
   being blamed.

**2. Fan-out consistency — does the blast radius match?**
   A fault at ``C`` predicts a specific set of unhappy entities: ``C`` itself
   plus everything transitively downstream of it. Compare that prediction with
   who is *actually* unhappy and score it like a classifier: coverage (how much
   of the predicted set is really distressed) and precision (how much of the
   real distress the hypothesis explains), combined as their harmonic mean.
   This is what distinguishes the cases. Blaming one application when three
   sharing a database are unhappy scores precision 1/3. Blaming the database
   explains all four at once.

**3. Temporal precedence — did the cause move first?**
   A cause whose own evidence appeared before its dependents' distress is a
   better explanation than one that appeared afterwards. Measured from first
   crossing, in both directions: arriving *later* than the symptom counts
   against a candidate rather than merely failing to count for it.

**4. Correlation — do they move together?**
   Rank correlation between the candidate's causal-evidence trace and the
   subject's hazard trace over the lookback window. Rank rather than linear,
   because the relationship between lock waits and request latency is
   emphatically monotone and emphatically not linear.

Forecasting the outage (feature #64)
------------------------------------
Once the cause is identified the horizon should describe *the cause's*
trajectory, not the symptom's: the outage arrives when the upstream fault
matures. So a cross-domain attribution adopts the cause entity's own
extrapolated time-to-failure when it has one.

Nothing here reads a label, a scenario kind or an entity-name convention. The
only inputs are telemetry-derived evidence and the CMDB topology.
"""

from __future__ import annotations

import bisect
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field

from ano.correlate.topology import Topology, domain_for_kind
from ano.detect.candidate import Candidate, CausalEvidence
from ano.detect.robust import clamp, spearman
from ano.detect.unresponsiveness import StepSummary

__all__ = [
    "LOOKBACK_S",
    "PRECEDENCE_SCALE_S",
    "SELF_EVIDENCE_FLOOR",
    "EVIDENCE_TOLERANCE_S",
    "AttributionConfig",
    "CauseScore",
    "DistressIndex",
    "Attributor",
    "attribute",
]

#: How far back the correlation and precedence terms look. One hour comfortably
#: covers the ramp of every degradation this system is meant to see ahead of,
#: while staying short enough that last night's unrelated blip is out of scope.
LOOKBACK_S = 3600.0

#: Lead, in seconds, at which temporal precedence saturates. Ten minutes is
#: about the propagation delay from an upstream fault to downstream distress at
#: a 30-second scrape interval; beyond it, earlier is not more informative.
PRECEDENCE_SCALE_S = 600.0

#: Minimum local evidence credited to the subject entity as its own cause.
#:
#: Without a floor, multiplying by local evidence would make self-attribution
#: impossible whenever an application's only visible signals are symptoms — and
#: a genuine front-end problem (feature #63) often looks exactly like that. The
#: floor keeps self-causation permanently in the running as the default
#: explanation, which is correct: absent evidence pointing elsewhere, the thing
#: that is broken is the thing that looks broken.
SELF_EVIDENCE_FLOOR = 0.08

#: Evidence is read over a short trailing window rather than at the exact
#: instant, so that a scrape-interval skew between two exporters cannot hide a
#: cause that is plainly visible one sample earlier.
EVIDENCE_TOLERANCE_S = 300.0


@dataclass(frozen=True, slots=True)
class AttributionConfig:
    """Weights and thresholds for the attribution score.

    Like the detector's family weights these are declared expert priors rather
    than fitted coefficients — requirement R5 leaves nothing to fit against.
    They are uniform across scenario kinds, entities and domains.

    Attributes:
        fanout_weight: Weight on blast-radius agreement.
        precedence_weight: Weight on the cause having moved first.
        correlation_weight: Weight on co-movement.
        distress_level: Hazard at which an entity counts as distressed when
            computing the blast radius.
        lookback_s: Window for correlation and precedence.
        precedence_scale_s: Lead at which precedence saturates.
        self_floor: Minimum local evidence credited to self-causation.
        tolerance_s: Trailing window over which evidence is read.
        min_margin: Margin below which the attribution is reported as a close
            call in its evidence. Does not change the choice, only the honesty
            of how it is described.
    """

    fanout_weight: float = 0.9
    precedence_weight: float = 0.5
    correlation_weight: float = 0.5
    distress_level: float = 0.275
    lookback_s: float = LOOKBACK_S
    precedence_scale_s: float = PRECEDENCE_SCALE_S
    self_floor: float = SELF_EVIDENCE_FLOOR
    tolerance_s: float = EVIDENCE_TOLERANCE_S
    min_margin: float = 0.15


@dataclass(frozen=True, slots=True)
class CauseScore:
    """One candidate cause, scored.

    Attributes:
        entity: The candidate cause.
        domain: Its domain.
        local_evidence: Strongest non-symptom severity on it, in ``[0, 1]``.
        coverage: Fraction of its predicted blast radius actually distressed.
        precision: Fraction of observed distress its hypothesis explains.
        fanout: Harmonic mean of coverage and precision.
        precedence: Signed lead over the subject in ``[-1, 1]``.
        correlation: Rank correlation with the subject's hazard trace.
        score: The combined score.
        is_self: Whether this candidate is the subject itself.
        detail: The specific measurement backing ``local_evidence``.
        horizon_s: The cause's own extrapolated time-to-failure, if any.
    """

    entity: str
    domain: str
    local_evidence: float
    coverage: float
    precision: float
    fanout: float
    precedence: float
    correlation: float
    score: float
    is_self: bool
    detail: str
    horizon_s: int | None


class DistressIndex:
    """Time-indexed access to every entity's scored steps.

    Attribution asks a lot of point-in-time questions — "what was this database
    showing 90 seconds ago?" — across many entities and many candidates, so the
    summaries are sorted once and queried by binary search.

    Args:
        summaries: Entity -> its chronological step summaries.
    """

    __slots__ = ("_summaries", "_times")

    def __init__(self, summaries: Mapping[str, Sequence[StepSummary]]) -> None:
        self._summaries: dict[str, tuple[StepSummary, ...]] = {}
        self._times: dict[str, list[float]] = {}
        for entity, steps in summaries.items():
            ordered = tuple(sorted(steps, key=lambda item: item.moment))
            self._summaries[entity] = ordered
            self._times[entity] = [item.moment for item in ordered]

    def entities(self) -> tuple[str, ...]:
        """Observed entities, sorted."""
        return tuple(sorted(self._summaries))

    def has(self, entity: str) -> bool:
        """Whether any telemetry was observed for ``entity``."""
        return entity in self._summaries

    def _slice(self, entity: str, start: float, end: float) -> tuple[StepSummary, ...]:
        """Summaries with ``start <= moment <= end``."""
        times = self._times.get(entity)
        if not times:
            return ()
        left = bisect.bisect_left(times, start)
        right = bisect.bisect_right(times, end)
        return self._summaries[entity][left:right]

    def at(self, entity: str, moment: float) -> StepSummary | None:
        """The most recent summary at or before ``moment``."""
        times = self._times.get(entity)
        if not times:
            return None
        position = bisect.bisect_right(times, moment) - 1
        if position < 0:
            return None
        return self._summaries[entity][position]

    def confidence_at(self, entity: str, moment: float) -> float:
        """Hazard at or before ``moment``; zero when unobserved."""
        found = self.at(entity, moment)
        return 0.0 if found is None else found.confidence

    def causal_evidence(
        self, entity: str, moment: float, tolerance_s: float
    ) -> tuple[float, str, Mapping[str, float]]:
        """Strongest non-symptom evidence in the trailing tolerance window.

        Args:
            entity: Entity to inspect.
            moment: Upper bound of the window.
            tolerance_s: How far back to look.

        Returns:
            ``(severity, detail, families)``. Zero and empty when the entity
            shows no causal evidence.
        """
        window = self._slice(entity, moment - tolerance_s, moment)
        best: StepSummary | None = None
        for item in window:
            if best is None or item.causal_severity > best.causal_severity:
                best = item
        if best is None or best.causal_severity <= 0.0:
            return 0.0, "", {}
        return best.causal_severity, best.top_causal_detail, best.causal_families

    def horizon_at(self, entity: str, moment: float, tolerance_s: float) -> int | None:
        """The most recent extrapolated horizon in the trailing window."""
        window = self._slice(entity, moment - tolerance_s, moment)
        for item in reversed(window):
            if item.horizon_s is not None:
                return item.horizon_s
        return None

    def distressed(self, moment: float, level: float) -> frozenset[str]:
        """Entities whose hazard is at or above ``level`` at ``moment``."""
        return frozenset(
            entity
            for entity in self._summaries
            if self.confidence_at(entity, moment) >= level
        )

    def first_causal_crossing(
        self, entity: str, moment: float, lookback_s: float
    ) -> float | None:
        """When this entity's causal evidence first appeared in the lookback.

        Args:
            entity: Entity to inspect.
            moment: End of the lookback window.
            lookback_s: Window length.

        Returns:
            Epoch seconds, or ``None`` if no causal evidence appeared.
        """
        for item in self._slice(entity, moment - lookback_s, moment):
            if item.causal_severity > 0.0:
                return item.moment
        return None

    def first_distress_crossing(
        self, entity: str, moment: float, lookback_s: float, level: float
    ) -> float | None:
        """When this entity's hazard first reached ``level`` in the lookback."""
        for item in self._slice(entity, moment - lookback_s, moment):
            if item.confidence >= level:
                return item.moment
        return None

    def causal_trace(
        self, entity: str, moment: float, lookback_s: float
    ) -> tuple[tuple[float, ...], tuple[float, ...]]:
        """``(times, causal severities)`` over the lookback window."""
        window = self._slice(entity, moment - lookback_s, moment)
        return (
            tuple(item.moment for item in window),
            tuple(item.causal_severity for item in window),
        )

    def hazard_trace(
        self, entity: str, moment: float, lookback_s: float
    ) -> tuple[tuple[float, ...], tuple[float, ...]]:
        """``(times, hazard)`` over the lookback window."""
        window = self._slice(entity, moment - lookback_s, moment)
        return (
            tuple(item.moment for item in window),
            tuple(item.confidence for item in window),
        )


def _align_on_time(
    left: tuple[tuple[float, ...], tuple[float, ...]],
    right: tuple[tuple[float, ...], tuple[float, ...]],
) -> tuple[list[float], list[float]]:
    """Pair two traces on their shared timestamps.

    Two exporters scraped on the same nominal interval can still disagree on
    exact timestamps, so correlation is computed only over instants both series
    actually observed rather than over a resampling that would invent values.
    """
    left_times, left_values = left
    right_times, right_values = right
    right_index = {moment: value for moment, value in zip(right_times, right_values)}
    xs: list[float] = []
    ys: list[float] = []
    for moment, value in zip(left_times, left_values):
        other = right_index.get(moment)
        if other is not None:
            xs.append(value)
            ys.append(other)
    return xs, ys


class Attributor:
    """Attributes degradations to a domain and an entity using the topology.

    Args:
        topology: The maintained estate graph.
        index: Time-indexed evidence for every observed entity.
        config: Scoring weights and thresholds.
    """

    __slots__ = ("_topology", "_index", "_config")

    def __init__(
        self,
        topology: Topology,
        index: DistressIndex,
        config: AttributionConfig | None = None,
    ) -> None:
        self._topology = topology
        self._index = index
        self._config = config or AttributionConfig()

    @property
    def config(self) -> AttributionConfig:
        """The scoring parameters in force."""
        return self._config

    def _domain_of(self, entity: str) -> str:
        """Domain of an entity, defaulting to application for unknowns.

        An entity absent from the CMDB is some kind of host as far as we can
        tell, and a host fault is an application-domain incident under contract
        C1's three-way split.
        """
        found = self._topology.domain(entity)
        if found is not None:
            return found
        return domain_for_kind("node")

    def _fanout(
        self, cause: str, distressed: frozenset[str]
    ) -> tuple[float, float, float, tuple[str, ...]]:
        """Score how well a cause's blast radius matches observed distress.

        Args:
            cause: The candidate cause.
            distressed: Entities currently distressed.

        Returns:
            ``(coverage, precision, harmonic_mean, predicted_set)``.
        """
        observed = set(self._index.entities())
        predicted = ({cause} | set(self._topology.downstream_of(cause))) & observed
        if not predicted:
            predicted = {cause}
        explained = predicted & distressed
        coverage = len(explained) / len(predicted)
        precision = len(explained) / len(distressed) if distressed else 0.0
        if coverage + precision <= 0.0:
            harmonic = 0.0
        else:
            harmonic = 2.0 * coverage * precision / (coverage + precision)
        return coverage, precision, harmonic, tuple(sorted(predicted))

    def _precedence(self, cause: str, subject: str, moment: float) -> float:
        """Signed lead of the cause's evidence over the subject's distress.

        Returns:
            ``+1`` when the cause moved a full :attr:`precedence_scale_s`
            earlier, ``-1`` when it moved that much later, ``0`` when there is
            nothing to compare.
        """
        config = self._config
        cause_at = self._index.first_causal_crossing(cause, moment, config.lookback_s)
        subject_at = self._index.first_distress_crossing(
            subject, moment, config.lookback_s, config.distress_level
        )
        if cause_at is None or subject_at is None:
            return 0.0
        lead = subject_at - cause_at
        return clamp(lead / config.precedence_scale_s, -1.0, 1.0)

    def _correlation(self, cause: str, subject: str, moment: float) -> float:
        """Rank correlation of the cause's evidence with the subject's hazard."""
        config = self._config
        xs, ys = _align_on_time(
            self._index.causal_trace(cause, moment, config.lookback_s),
            self._index.hazard_trace(subject, moment, config.lookback_s),
        )
        if len(xs) < 4:
            return 0.0
        value = spearman(xs, ys)
        return 0.0 if not math.isfinite(value) else value

    def score_cause(
        self,
        cause: str,
        subject: str,
        moment: float,
        distressed: frozenset[str],
    ) -> CauseScore:
        """Score one candidate cause for one distressed subject.

        Args:
            cause: The hypothesised responsible entity.
            subject: The entity observed to be degrading.
            moment: Decision time, epoch seconds.
            distressed: Entities currently distressed.

        Returns:
            The scored candidate.
        """
        config = self._config
        is_self = cause == subject
        local, detail, _families = self._index.causal_evidence(
            cause, moment, config.tolerance_s
        )
        if is_self:
            local = max(local, config.self_floor)
            if not detail:
                detail = (
                    "no upstream dependency shows causal evidence; the "
                    "degradation is local to this entity"
                )

        coverage, precision, fanout, _predicted = self._fanout(cause, distressed)

        # Self-causation offers no cross-entity evidence by definition: an
        # entity cannot precede itself, and correlating a trace with itself is
        # not evidence of anything. Crediting either term would hand the
        # subject a permanent advantage over every real upstream cause.
        precedence = 0.0 if is_self else self._precedence(cause, subject, moment)
        correlation = 0.0 if is_self else self._correlation(cause, subject, moment)

        support = (
            1.0
            + config.fanout_weight * fanout
            + config.precedence_weight * precedence
            + config.correlation_weight * max(0.0, correlation)
        )
        score = local * max(0.0, support)

        return CauseScore(
            entity=cause,
            domain=self._domain_of(cause),
            local_evidence=local,
            coverage=coverage,
            precision=precision,
            fanout=fanout,
            precedence=precedence,
            correlation=correlation,
            score=score,
            is_self=is_self,
            detail=detail,
            horizon_s=self._index.horizon_at(cause, moment, config.tolerance_s),
        )

    def rank_causes(self, subject: str, moment: float) -> list[CauseScore]:
        """Score every topologically reachable cause, best first.

        Args:
            subject: The degrading entity.
            moment: Decision time, epoch seconds.

        Returns:
            Scored causes ordered by score descending, then by entity id so
            ties resolve deterministically.
        """
        distressed = self._index.distressed(moment, self._config.distress_level)
        candidates = self._topology.upstream_candidates(subject)
        if subject not in candidates:
            candidates = candidates + (subject,)
        scored = [
            self.score_cause(cause, subject, moment, distressed)
            for cause in candidates
            if cause == subject or self._index.has(cause)
        ]
        scored.sort(key=lambda item: (-item.score, item.entity))
        return scored

    def _evidence_for(
        self, winner: CauseScore, runner_up: CauseScore | None, subject: str
    ) -> list[CausalEvidence]:
        """Build the correlation-evidence block (feature #67)."""
        evidence: list[CausalEvidence] = [
            CausalEvidence(
                kind="local_signal",
                detail=(
                    f"{winner.entity}: {winner.detail}"
                    if winner.detail
                    else f"{winner.entity} shows the strongest causal evidence"
                ),
            )
        ]

        if not winner.is_self:
            path = self._topology.explain_path(subject, winner.entity)
            if path:
                evidence.append(
                    CausalEvidence(
                        kind="topology_path",
                        detail=" -> ".join(edge.describe() for edge in path),
                    )
                )

        affected = self._topology.downstream_of(winner.entity)
        if affected:
            evidence.append(
                CausalEvidence(
                    kind="fan_out",
                    detail=(
                        f"{winner.coverage:.0%} of the {len(affected)} entities "
                        f"downstream of {winner.entity} are degraded, and this "
                        f"hypothesis explains {winner.precision:.0%} of all "
                        "observed distress"
                    ),
                )
            )
        elif winner.is_self:
            evidence.append(
                CausalEvidence(
                    kind="fan_out",
                    detail=(
                        f"this hypothesis explains {winner.precision:.0%} of all "
                        "observed distress; no other entity depends on "
                        f"{winner.entity}"
                    ),
                )
            )

        if winner.precedence > 0.0:
            lead = winner.precedence * self._config.precedence_scale_s
            evidence.append(
                CausalEvidence(
                    kind="temporal_precedence",
                    detail=(
                        f"{winner.entity} began deviating about {lead / 60.0:.0f} "
                        f"minutes before {subject} started degrading"
                    ),
                )
            )

        if winner.correlation > 0.0:
            evidence.append(
                CausalEvidence(
                    kind="correlation",
                    detail=(
                        f"rank correlation {winner.correlation:+.2f} between "
                        f"{winner.entity}'s causal signals and {subject}'s "
                        "degradation over the preceding hour"
                    ),
                )
            )

        if runner_up is not None:
            margin = winner.score - runner_up.score
            kind = (
                "ruled_out"
                if margin >= self._config.min_margin
                else "close_call"
            )
            evidence.append(
                CausalEvidence(
                    kind=kind,
                    detail=(
                        f"next best explanation was {runner_up.entity} "
                        f"({runner_up.domain}) at score {runner_up.score:.2f} "
                        f"versus {winner.score:.2f}"
                    ),
                )
            )

        return evidence

    def attribute_one(self, candidate: Candidate) -> Candidate:
        """Attribute one candidate to a domain and an entity.

        Args:
            candidate: The unattributed candidate.

        Returns:
            A copy carrying ``cause_entity``, ``cause_domain``, the correlation
            evidence, the affected downstream set, and — for a cross-domain
            attribution — the cause's own forecast horizon.
        """
        ranked = self.rank_causes(candidate.entity, candidate.emitted_at)
        if not ranked:
            return candidate.with_attribution(
                cause_entity=candidate.entity,
                cause_domain=self._domain_of(candidate.entity),
                evidence=(
                    *candidate.evidence,
                    CausalEvidence(
                        kind="local_signal",
                        detail="no topology available; attributed to the "
                        "degrading entity itself",
                    ),
                ),
            )

        winner = ranked[0]
        runner_up = ranked[1] if len(ranked) > 1 else None
        margin = winner.score - (runner_up.score if runner_up else 0.0)
        downstream = tuple(
            entity
            for entity in self._topology.downstream_of(winner.entity)
            if self._index.has(entity)
        )

        # A cross-domain outage arrives when the upstream fault matures, so the
        # cause's own trajectory is the better forecast (feature #64). Fall
        # back to the symptom's horizon when the cause has not produced one.
        horizon = None
        if not winner.is_self and winner.horizon_s is not None:
            horizon = max(winner.horizon_s, candidate.time_to_failure_s)

        return candidate.with_attribution(
            cause_entity=winner.entity,
            cause_domain=winner.domain,
            evidence=(*candidate.evidence, *self._evidence_for(winner, runner_up, candidate.entity)),
            downstream=downstream,
            margin=margin,
            time_to_failure_s=horizon,
        )

    def attribute(self, candidates: Iterable[Candidate]) -> list[Candidate]:
        """Attribute every candidate.

        Args:
            candidates: Unattributed candidates.

        Returns:
            Attributed candidates in the input order.
        """
        return [self.attribute_one(item) for item in candidates]


def attribute(
    candidates: Iterable[Candidate],
    topology: Topology,
    summaries: Mapping[str, Sequence[StepSummary]],
    *,
    config: AttributionConfig | None = None,
) -> list[Candidate]:
    """Convenience wrapper: attribute a whole candidate stream.

    Args:
        candidates: Unattributed candidates from the detector.
        topology: The maintained estate graph.
        summaries: Per-entity step summaries from the detector.
        config: Scoring weights and thresholds.

    Returns:
        Attributed candidates.
    """
    return Attributor(topology, DistressIndex(summaries), config).attribute(candidates)


_ = (dataclass, field)  # referenced by the annotations above
