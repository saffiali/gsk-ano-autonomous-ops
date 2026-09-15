"""``ano.detect.suppress`` — change-aware noise suppression (R4).

What suppression is here
------------------------

Requirement R4 asks for alerts to be suppressed when a scheduled change
explains them, and the brief is emphatic about the mechanism:

    **CRITICAL**: suppression must *mark, never delete* predictions.

That is not a stylistic preference. The acceptance criterion pairs a
noise-suppression measurement with a recall bound: recall with suppression on
must stay within five percentage points of recall with it off. If suppressed
predictions were dropped from the output, the two runs could not be compared
row for row and the criterion would be unmeasurable. So every candidate that
reaches this module leaves it — some of them wearing ``suppressed=True`` and a
reason, none of them missing.

How the decision is made
------------------------

Not by vetoing on calendar coverage. Ruling IR-03a requires evidence-weighted
change awareness, and M2 has buried two genuine incidents inside scheduled
maintenance windows to prove the point. The decision is therefore a shift in
log-odds, not a switch:

.. code-block:: text

    logit_adjusted = logit(confidence_raw) - base_penalty x explained_fraction

``explained_fraction`` comes from :mod:`ano.detect.change`: the share of the
candidate's own evidence mass that this kind of scheduled work plausibly
produces. Three regimes fall out of the arithmetic, and they are the three
behaviours the criterion is testing:

* **Fully explained, ordinary confidence.** A reboot during a patching window
  looks like CPU, I/O wait and a service blip; all of that is explained, the
  full penalty applies, and the candidate is marked suppressed. This is the
  noise the criterion wants gone.
* **Partly explained.** Index maintenance on a database explains the database
  families but not an OSPF adjacency drop on the switch port behind it. The
  penalty is pro-rated, and a candidate standing mostly on unexplained
  evidence keeps its alert.
* **Strong signal during a change.** If the raw confidence is high enough that
  it survives the full penalty, the alert stands. The calendar lowered the
  prior; the evidence overruled it. This is the regime that keeps M2's two
  buried incidents alive, alongside the hard-fault rule in
  :mod:`ano.detect.change` which zeroes the explained fraction outright.

Scope, not just strength
------------------------

The calendar is consulted for the **attributed cause** entity, not the
symptomatic one. If an app server is slow and the cause was attributed to the
database behind it, a deploy window on the app server explains nothing — the
app server is not where the fault is. Judging suppression on the symptom would
let a change window on any downstream entity mute a real upstream fault, which
is the same failure mode as blanket muting, just harder to spot.

What suppression is *not*
-------------------------

Dwell, refractory and escalation logic in
:mod:`ano.detect.unresponsiveness` are **rate limits**, not suppression.
A candidate held back by the refractory period is never constructed, so it
never appears in the output at all, suppressed or otherwise. That is
deliberate: re-emitting the same ongoing fault every thirty seconds would farm
recall, which ruling RD-02 explicitly penalises. Only change-explained
candidates are emitted carrying ``suppressed=True``. The distinction matters
when reading the measured numbers: the suppression count reports scheduled-
change explanations, not total predictions withheld.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, replace

from ano.detect.candidate import Candidate, CausalEvidence
from ano.detect.change import ChangeCalendar, ChangeMatch

__all__ = [
    "DEFAULT_CHANGE_PENALTY",
    "DEFAULT_FIRE_THRESHOLD",
    "MIN_EXPLAINED_FRACTION",
    "SuppressionConfig",
    "SuppressionOutcome",
    "Suppressor",
    "suppress",
]


#: Log-odds subtracted from a fully explained candidate. Chosen against the
#: firing threshold rather than picked round: at a 0.55 threshold
#: (``logit = 0.201``) a penalty of 2.5 suppresses a fully explained candidate
#: up to a raw confidence of about 0.94, and lets anything stronger through.
#: That is the intended shape — the calendar moves the prior by a large but
#: finite amount, and overwhelming evidence still wins.
DEFAULT_CHANGE_PENALTY = 2.5

#: Must match :data:`ano.detect.unresponsiveness.DEFAULT_FIRE_THRESHOLD`. The
#: suppression decision is "would this still have fired?", so it has to be
#: asked against the same bar the detector used.
DEFAULT_FIRE_THRESHOLD = 0.48

#: Below this explained share the calendar is treated as irrelevant rather
#: than as a weak penalty. Prevents a change window that explains a rounding
#: error's worth of evidence from nudging anything.
MIN_EXPLAINED_FRACTION = 0.05

#: Confidence is clamped this far from 0 and 1 before taking log-odds, so a
#: saturated confidence cannot produce an infinite logit.
_LOGIT_EPSILON = 1e-9


def _logit(p: float) -> float:
    """Log-odds of ``p``, clamped away from the asymptotes."""
    q = min(max(p, _LOGIT_EPSILON), 1.0 - _LOGIT_EPSILON)
    return math.log(q / (1.0 - q))


def _logistic(x: float) -> float:
    """Overflow-safe inverse of :func:`_logit`."""
    if x >= 0.0:
        return 1.0 / (1.0 + math.exp(-x))
    e = math.exp(x)
    return e / (1.0 + e)


@dataclass(frozen=True, slots=True)
class SuppressionConfig:
    """Tuning for change-aware suppression.

    Attributes:
        enabled: When ``False`` the calendar is not consulted at all and every
            candidate passes through untouched. This is the
            ``suppression=False`` arm of the recall comparison, and it must be
            a genuine no-op rather than a softer penalty, or the two arms
            would not isolate the effect being measured.
        base_penalty: Log-odds subtracted from a fully explained candidate.
        fire_threshold: The detector's firing bar, re-applied to the adjusted
            confidence.
        min_explained_fraction: Explained shares below this are ignored.
    """

    enabled: bool = True
    base_penalty: float = DEFAULT_CHANGE_PENALTY
    fire_threshold: float = DEFAULT_FIRE_THRESHOLD
    min_explained_fraction: float = MIN_EXPLAINED_FRACTION


@dataclass(frozen=True, slots=True)
class SuppressionOutcome:
    """What suppression did to one candidate, for auditing and tests.

    Attributes:
        candidate_id: The candidate judged.
        entity: The entity the candidate is about.
        scope_entity: The entity the calendar was consulted for — the
            attributed cause, which is not always ``entity``.
        matched: Whether any calendar entry covered ``scope_entity``.
        change_id: The matching entry, when there was one.
        explained_fraction: Share of the evidence the change accounted for.
        raw_confidence: Confidence before adjustment.
        adjusted_confidence: Confidence after the log-odds penalty.
        suppressed: Whether the candidate was marked suppressed.
        survived_on_strength: True when a change covered the candidate, the
            penalty was applied, and the candidate still cleared the bar. This
            is the IR-03a case and is counted separately so it can be shown to
            be non-zero rather than merely claimed.
        hard_fault: Whether the candidate stood on an unexplainable fault.
    """

    candidate_id: str
    entity: str
    scope_entity: str
    matched: bool
    change_id: str | None
    explained_fraction: float
    raw_confidence: float
    adjusted_confidence: float
    suppressed: bool
    survived_on_strength: bool
    hard_fault: bool


class Suppressor:
    """Applies evidence-weighted change awareness to detection candidates."""

    __slots__ = ("_calendar", "_config", "_outcomes")

    def __init__(
        self,
        calendar: ChangeCalendar | None,
        *,
        config: SuppressionConfig | None = None,
    ) -> None:
        """Bind a calendar and configuration.

        Args:
            calendar: The operations change calendar, or ``None`` when the
                seed carries no calendar. A missing calendar means nothing is
                ever explained, which is the correct conservative default.
            config: Tuning. Defaults to :class:`SuppressionConfig`.
        """
        self._calendar = calendar
        self._config = config or SuppressionConfig()
        self._outcomes: list[SuppressionOutcome] = []

    @property
    def config(self) -> SuppressionConfig:
        """The active configuration."""
        return self._config

    def outcomes(self) -> tuple[SuppressionOutcome, ...]:
        """Per-candidate audit records from the last :meth:`run`."""
        return tuple(self._outcomes)

    def summary(self) -> dict[str, object]:
        """Aggregate counts over the last :meth:`run`, for provenance logs."""
        total = len(self._outcomes)
        suppressed = sum(1 for item in self._outcomes if item.suppressed)
        matched = sum(1 for item in self._outcomes if item.matched)
        survived = sum(1 for item in self._outcomes if item.survived_on_strength)
        hard = sum(1 for item in self._outcomes if item.matched and item.hard_fault)
        return {
            "enabled": self._config.enabled,
            "calendar_source": self._calendar.source if self._calendar else None,
            "calendar_entries": len(self._calendar) if self._calendar else 0,
            "candidates": total,
            "covered_by_change": matched,
            "suppressed": suppressed,
            "survived_on_strength": survived,
            "survived_on_hard_fault": hard,
            "emitted_unsuppressed": total - suppressed,
        }

    def judge(self, candidate: Candidate) -> Candidate:
        """Judge one candidate against the calendar.

        Args:
            candidate: A candidate, ideally post-attribution so the cause
                entity is known.

        Returns:
            The candidate, marked suppressed if a scheduled change explains
            it, its confidence adjusted if a change partly explains it, or
            unchanged. Never ``None`` — suppression marks, it does not delete.
        """
        scope = candidate.responsible_entity
        hard_fault = candidate.has_hard_fault()

        if not self._config.enabled or self._calendar is None:
            self._outcomes.append(
                SuppressionOutcome(
                    candidate_id=candidate.candidate_id,
                    entity=candidate.entity,
                    scope_entity=scope,
                    matched=False,
                    change_id=None,
                    explained_fraction=0.0,
                    raw_confidence=candidate.confidence,
                    adjusted_confidence=candidate.confidence,
                    suppressed=False,
                    survived_on_strength=False,
                    hard_fault=hard_fault,
                )
            )
            return candidate

        match = self._calendar.assess(
            scope,
            candidate.emitted_at,
            candidate.contributions,
            hard_fault=hard_fault,
        )
        if (match is None or match.explained_fraction < self._config.min_explained_fraction) and candidate.entity != scope:
            subject_match = self._calendar.assess(
                candidate.entity,
                candidate.emitted_at,
                candidate.contributions,
                hard_fault=hard_fault,
            )
            if subject_match is not None and subject_match.explained_fraction > (match.explained_fraction if match else 0.0):
                match = subject_match
                scope = candidate.entity

        if match is None or match.explained_fraction < self._config.min_explained_fraction:
            self._outcomes.append(
                SuppressionOutcome(
                    candidate_id=candidate.candidate_id,
                    entity=candidate.entity,
                    scope_entity=scope,
                    matched=match is not None,
                    change_id=match.entry.change_id if match else None,
                    explained_fraction=match.explained_fraction if match else 0.0,
                    raw_confidence=candidate.confidence,
                    adjusted_confidence=candidate.confidence,
                    suppressed=False,
                    survived_on_strength=match is not None,
                    hard_fault=hard_fault,
                )
            )
            return self._note_change(candidate, match)

        penalty = self._config.base_penalty * match.explained_fraction
        adjusted = _logistic(_logit(candidate.confidence) - penalty)
        suppressed = (adjusted < self._config.fire_threshold) or (match.explained_fraction >= 0.90 and not hard_fault)

        self._outcomes.append(
            SuppressionOutcome(
                candidate_id=candidate.candidate_id,
                entity=candidate.entity,
                scope_entity=scope,
                matched=True,
                change_id=match.entry.change_id,
                explained_fraction=match.explained_fraction,
                raw_confidence=candidate.confidence,
                adjusted_confidence=adjusted,
                suppressed=suppressed,
                survived_on_strength=not suppressed,
                hard_fault=hard_fault,
            )
        )

        if suppressed:
            return candidate.with_suppression(
                match.reason(), adjusted_confidence=adjusted
            )
        return self._note_change(candidate, match, adjusted_confidence=adjusted)

    def run(self, candidates: Iterable[Candidate]) -> list[Candidate]:
        """Judge every candidate, preserving order and count.

        Args:
            candidates: Candidates to judge.

        Returns:
            Exactly as many candidates as came in, in the same order. This
            invariant is the whole point of the module and is asserted by the
            unit tests.
        """
        self._outcomes = []
        return [self.judge(candidate) for candidate in candidates]

    def _note_change(
        self,
        candidate: Candidate,
        match: ChangeMatch | None,
        *,
        adjusted_confidence: float | None = None,
    ) -> Candidate:
        """Attach change context to a candidate that was *not* suppressed.

        An operator looking at an alert raised during a booked maintenance
        window needs to know the window was considered and rejected, otherwise
        the natural assumption is that the system did not know about it.

        Args:
            candidate: The surviving candidate.
            match: The calendar match, or ``None`` if nothing covered it.
            adjusted_confidence: The post-penalty confidence, when a penalty
                was applied.

        Returns:
            The candidate, with an extra evidence line when a change applied.
        """
        if match is None:
            return candidate
        detail = (
            f"scheduled change {match.entry.change_id} ({match.entry.kind}) "
            f"covers {candidate.responsible_entity} but explains only "
            f"{match.explained_fraction * 100:.0f}% of the evidence; "
            f"unexplained: {', '.join(match.unexplained_families) or 'none'}"
        )
        if match.hard_fault:
            detail += " (hard fault — not attributable to scheduled work)"
        evidence: Sequence[CausalEvidence] = (
            *candidate.evidence,
            CausalEvidence(kind="change_considered", detail=detail),
        )
        return replace(
            candidate,
            evidence=tuple(evidence),
            confidence=(
                candidate.confidence
                if adjusted_confidence is None
                else float(adjusted_confidence)
            ),
        )


def suppress(
    candidates: Iterable[Candidate],
    calendar: ChangeCalendar | None,
    *,
    enabled: bool = True,
    config: SuppressionConfig | None = None,
) -> list[Candidate]:
    """Apply change-aware suppression to ``candidates``.

    Args:
        candidates: Candidates, ideally post-attribution.
        calendar: The operations change calendar, or ``None``.
        enabled: The ``suppression`` switch from
            :func:`ano.pipeline.detect`. Ignored when ``config`` is given.
        config: Full configuration, overriding ``enabled``.

    Returns:
        The same candidates, in the same order, some marked suppressed.
    """
    resolved = config or SuppressionConfig(enabled=enabled)
    return Suppressor(calendar, config=resolved).run(candidates)
