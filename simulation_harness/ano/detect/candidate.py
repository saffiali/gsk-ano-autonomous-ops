"""The internal prediction record, before it becomes a contract-C2 ``Prediction``.

Why an intermediate record exists at all
----------------------------------------
``ano.contracts.prediction.Prediction`` is frozen and validates on
construction, which is exactly right for the wire format and exactly wrong for
a multi-stage pipeline. Attribution has to attach a root cause, collapse has to
attach an incident id and the downstream set, and suppression has to attach a
reason — and each of those stages wants to *add evidence*, not just flip a
flag. Rebuilding a validated wire record four times would either lose evidence
or require the contract to expose mutators it should not have.

So the stages operate on :class:`Candidate`, and exactly one place —
:mod:`ano.detect.engine` — turns the finished candidates into ``Prediction``
objects via ``Prediction.from_parts``. That also keeps the contract's
invariants (non-empty signals, suppression reason present iff suppressed,
``predicted_onset`` consistent with ``time_to_failure_s``) enforced at a single
boundary instead of being re-satisfied in four places.

Suppression marks, it never deletes
-----------------------------------
A suppressed candidate keeps every field it had and gains
``suppressed=True`` plus a reason. Nothing in this pipeline drops a candidate
because it was explained by a change. That is a contract requirement, and it is
also what makes the "suppression must not cost recall" criterion measurable at
all: a deleted prediction is indistinguishable from one that was never made.
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace

from ano.detect.signals import SignalReading

__all__ = [
    "TRACK_UNRESPONSIVENESS",
    "TRACK_CROSS_DOMAIN",
    "TRACK_FOR_DOMAIN",
    "CausalEvidence",
    "Candidate",
    "track_for_domain",
]

#: Contract-C2 track for a fault whose cause is in the application domain.
TRACK_UNRESPONSIVENESS = "unresponsiveness"

#: Contract-C2 track for a fault whose cause is in another domain.
TRACK_CROSS_DOMAIN = "cross_domain"

#: Track selection is a function of the **attributed root-cause domain**, not
#: of which entity happened to look unhappy first.
#:
#: Contract C1's own ``RECOMMENDED_TRACK_FOR_KIND`` partitions the six genuine
#: fault kinds exactly this way — thread starvation, memory leak and socket
#: exhaustion are application-domain and belong to the unresponsiveness track;
#: database lock contention, OSPF flaps and packet loss are database- or
#: network-domain and belong to the cross-domain track. Deriving the track from
#: the domain therefore reproduces the intended partition without the detector
#: ever needing to know what kind of scenario it is looking at, which it must
#: not know.
TRACK_FOR_DOMAIN: Mapping[str, str] = {
    "application": TRACK_UNRESPONSIVENESS,
    "database": TRACK_CROSS_DOMAIN,
    "network": TRACK_CROSS_DOMAIN,
}


def track_for_domain(domain: str) -> str:
    """Contract-C2 track implied by a root-cause domain.

    Args:
        domain: ``"application"``, ``"database"`` or ``"network"``.

    Returns:
        The track name.

    Raises:
        ValueError: If ``domain`` is not one of the three.
    """
    try:
        return TRACK_FOR_DOMAIN[domain]
    except KeyError:
        raise ValueError(f"unknown root-cause domain {domain!r}") from None


@dataclass(frozen=True, slots=True)
class CausalEvidence:
    """One item of correlation evidence backing an attribution (feature #67).

    Attributes:
        kind: Short machine-readable evidence type, e.g.
            ``"topology_path"``, ``"temporal_precedence"``, ``"fan_out"``.
        detail: Human-readable justification an operator can act on.
    """

    kind: str
    detail: str


@dataclass(frozen=True, slots=True)
class Candidate:
    """A prediction in flight through the detection pipeline.

    Attributes:
        candidate_id: Stable, content-derived id. Becomes the C2
            ``prediction_id``.
        entity: The entity the prediction is *about* — the one an operator
            would see degrading. Not necessarily the cause.
        emitted_at: Model decision time, epoch seconds. Lead time is measured
            from here (RD-02), so it must be the moment the evidence crossed
            the decision threshold, never the file-write time.
        time_to_failure_s: Extrapolated seconds until failure.
        confidence: Post-adjustment confidence in ``[0, 1]``.
        raw_confidence: Confidence before any change-awareness adjustment.
            Retained so the suppression decision is auditable.
        readings: Every scored signal at the decision instant.
        contributions: Feature name -> share of the evidence, summing to <= 1.
        family_severities: Family -> severity at the decision instant.
        cause_entity: Attributed responsible entity. Defaults to ``entity``
            until attribution runs.
        cause_domain: Attributed responsible domain.
        evidence: Correlation evidence for the attribution.
        incident_id: Collapse key. ``None`` until collapse runs, at which point
            every candidate sharing one upstream cause gets the same value.
        collapsed_from: Sibling candidate ids folded into this one.
        downstream: Affected downstream entities, for the collapsed incident to
            remain actionable (feature #79).
        suppressed: Whether a scheduled change explains this candidate.
        suppression_reason: Why, non-``None`` iff ``suppressed``.
        remediation: The recommended operator action.
        attribution_margin: How decisively the winning cause beat the runner-up.
            Reported as evidence; a narrow margin is an honest signal that the
            attribution is a judgement call.
    """

    candidate_id: str
    entity: str
    emitted_at: float
    time_to_failure_s: int
    confidence: float
    raw_confidence: float
    readings: tuple[SignalReading, ...]
    contributions: Mapping[str, float] = field(default_factory=dict)
    family_severities: Mapping[str, float] = field(default_factory=dict)
    cause_entity: str | None = None
    cause_domain: str | None = None
    evidence: tuple[CausalEvidence, ...] = ()
    incident_id: str | None = None
    collapsed_from: tuple[str, ...] = ()
    downstream: tuple[str, ...] = ()
    suppressed: bool = False
    suppression_reason: str | None = None
    remediation: str = ""
    attribution_margin: float = 0.0

    @property
    def track(self) -> str:
        """Contract-C2 track, derived from the attributed root-cause domain.

        Falls back to the unresponsiveness track before attribution has run,
        which is the correct default: an unattributed degradation is an
        entity-local one until evidence says otherwise.
        """
        if self.cause_domain is None:
            return TRACK_UNRESPONSIVENESS
        return track_for_domain(self.cause_domain)

    @property
    def responsible_entity(self) -> str:
        """The attributed cause entity, defaulting to the subject entity."""
        return self.cause_entity or self.entity

    def emitted_datetime(self) -> _dt.datetime:
        """``emitted_at`` as a timezone-aware UTC datetime for contract C2."""
        return _dt.datetime.fromtimestamp(self.emitted_at, tz=_dt.UTC)

    def driving_readings(self, limit: int = 8) -> tuple[SignalReading, ...]:
        """The readings that actually drove the decision, strongest first.

        Requirement R2 asks for "the specific signals that drove it —
        enough for an operator to act preemptively". That means the handful
        that carry the evidence, ordered by how much they carry, not all
        forty-seven.

        Args:
            limit: Maximum readings to return.

        Returns:
            Contributing readings ordered by contribution then severity. Falls
            back to the most severe readings if no contribution was recorded,
            so the contract's non-empty-signals invariant can always be met.
        """
        scored = [
            reading
            for reading in self.readings
            if self.contributions.get(reading.feature, 0.0) > 0.0
        ]
        if not scored:
            scored = [r for r in self.readings if r.severity > 0.0]
        if not scored:
            scored = list(self.readings)
        scored.sort(
            key=lambda r: (
                -self.contributions.get(r.feature, 0.0),
                -r.severity,
                r.feature,
            )
        )
        return tuple(scored[:limit])

    def has_hard_fault(self) -> bool:
        """Whether any driving signal is a fault no change window explains."""
        return any(reading.is_hard_fault() for reading in self.readings)

    def with_attribution(
        self,
        *,
        cause_entity: str,
        cause_domain: str,
        evidence: Sequence[CausalEvidence],
        downstream: Sequence[str] = (),
        margin: float = 0.0,
        time_to_failure_s: int | None = None,
    ) -> "Candidate":
        """Return a copy carrying an attribution.

        Args:
            cause_entity: The responsible entity.
            cause_domain: The responsible domain.
            evidence: Correlation evidence justifying the choice.
            downstream: Affected downstream entities.
            margin: How far the winner beat the runner-up.
            time_to_failure_s: Replacement horizon, when the cause's own
                trajectory gives a better forecast than the symptom's.

        Returns:
            A new candidate.
        """
        return replace(
            self,
            cause_entity=cause_entity,
            cause_domain=cause_domain,
            evidence=tuple(evidence),
            downstream=tuple(downstream),
            attribution_margin=margin,
            time_to_failure_s=(
                self.time_to_failure_s
                if time_to_failure_s is None
                else int(time_to_failure_s)
            ),
        )

    def with_incident(
        self, incident_id: str, *, collapsed_from: Sequence[str] = ()
    ) -> "Candidate":
        """Return a copy joined to an incident.

        Args:
            incident_id: The collapse key shared by every candidate arising
                from one upstream fault.
            collapsed_from: Sibling candidate ids folded into this one. The
                candidate's own id is filtered out, because contract C2
                forbids self-reference.

        Returns:
            A new candidate.
        """
        return replace(
            self,
            incident_id=incident_id,
            collapsed_from=tuple(
                sorted({item for item in collapsed_from if item != self.candidate_id})
            ),
        )

    def with_suppression(
        self, reason: str, *, adjusted_confidence: float | None = None
    ) -> "Candidate":
        """Return a copy **marked** suppressed. The candidate is never dropped.

        Args:
            reason: Why the alert is explained by a scheduled change.
            adjusted_confidence: The post-adjustment confidence, if it moved.

        Returns:
            A new candidate with ``suppressed=True``.

        Raises:
            ValueError: If ``reason`` is empty. Contract C2 requires a reason
                whenever ``suppressed`` is set.
        """
        if not reason:
            raise ValueError("a suppressed candidate must carry a reason")
        return replace(
            self,
            suppressed=True,
            suppression_reason=reason,
            confidence=(
                self.confidence if adjusted_confidence is None else adjusted_confidence
            ),
        )

    def with_remediation(self, remediation: str) -> "Candidate":
        """Return a copy carrying the recommended operator action."""
        return replace(self, remediation=remediation)
