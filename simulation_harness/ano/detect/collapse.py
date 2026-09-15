"""``ano.detect.collapse`` — one upstream fault becomes one incident (R4).

The problem
-----------

A single upstream fault does not produce a single symptom. When a database
locks up, the three application servers querying it all go slow; when a switch
port starts discarding, everything behind it degrades at once. Requirement R4
asks that these be *collapsed into a single actionable incident* rather than
delivered as a storm of separate alerts, because a storm is how an operator
misses the one line that matters.

The separation of concerns
--------------------------

This module **groups; it does not attribute**. That boundary is deliberate and
worth stating plainly, because an earlier draft of this file violated it and
the bug was subtle:

:mod:`ano.correlate.attribution` has already decided, per candidate, which
entity and domain is responsible. If collapse were also allowed to rewrite
``cause_entity`` — say, by noticing two clusters are topologically related and
forcing the downstream one onto the upstream cause — then a *correct*
attribution could be silently overwritten by a topology-only guess made with
less evidence. Worse, it would inflate the database-attribution rate that
ruling RD-05 measures, by relabelling application-attributed candidates as
database ones after the fact. That would be measuring our own thumb on the
scale. Collapse therefore assigns ``incident_id`` and nothing else; a wrong
attribution stays visibly wrong.

How grouping works
------------------

1. **Partition by attributed cause.** Candidates sharing a ``cause_entity``
   are, by attribution's own verdict, manifestations of one fault. This is the
   step that does the real work: if attribution is right, one database fault
   with three affected app servers has already produced four candidates that
   all name the database, and they fall into one group here.
2. **Split into temporal episodes.** Within a cause, candidates are linked
   into an episode when each falls within :data:`DEFAULT_JOIN_WINDOW_S` of the
   previous one — single-linkage, so a long-running fault stays one incident
   however many times it re-fires, while the same entity failing again next
   week is correctly a second incident.
3. **Merge cascades, conservatively.** Two episodes with *different* causes
   are joined when one cause is strictly downstream of the other in the
   topology **and their time spans genuinely overlap**. Overlap is required
   rather than mere proximity: two distinct faults on topologically related
   entities an hour apart are two incidents, not one. Again — only the
   ``incident_id`` is shared; neither episode's attribution is touched.

What each candidate carries out
-------------------------------

Every member of an incident gets the same ``incident_id``. The
**representative** — the most confident candidate on the cause entity —
additionally carries ``collapsed_from``, listing the sibling candidate ids, and
a ``downstream`` roll-up of the affected entities so the collapsed incident is
still actionable (feature #79). Non-representatives are left with an empty
``collapsed_from``: contract C2 forbids self-reference, and having all N
members each point at the other N-1 is mutually-referential noise that tells an
operator nothing about which line to read first.

Collapse does not delete
------------------------

Every candidate in is a candidate out. Collapse is a *grouping*, not a filter.
The harness counts an alert as a prediction with ``suppressed is False``,
counted after suppression and before collapse, so dropping members here would
not even help the noise numbers — it would only destroy the evidence trail.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import replace

from ano.contracts.determinism import stable_id
from ano.detect.candidate import Candidate, CausalEvidence
from ano.correlate.topology import Topology

__all__ = [
    "DEFAULT_JOIN_WINDOW_S",
    "Incident",
    "collapse_candidates",
    "collapse",
]


#: Maximum gap between consecutive candidates on one cause before they are
#: treated as separate incidents. An hour: long enough that a fault which
#: re-fires after the detector's 30-minute refractory period stays one
#: incident, short enough that yesterday's fault does not absorb today's.
DEFAULT_JOIN_WINDOW_S = 3600.0


class Incident:
    """One collapsed incident: a cause, a time span, and its members.

    Attributes are exposed as read-only properties so the grouping cannot be
    mutated after :func:`collapse_candidates` has reasoned about it.
    """

    __slots__ = ("_cause", "_members")

    def __init__(self, cause: str, members: Sequence[Candidate]) -> None:
        """Build an incident from its cause and members.

        Args:
            cause: The attributed cause entity shared by the members.
            members: The candidates belonging to this incident. Must be
                non-empty.

        Raises:
            ValueError: If ``members`` is empty.
        """
        if not members:
            raise ValueError("an incident must have at least one member")
        self._cause = cause
        self._members = tuple(
            sorted(members, key=lambda item: (item.emitted_at, item.candidate_id))
        )

    @property
    def cause(self) -> str:
        """The attributed cause entity."""
        return self._cause

    @property
    def members(self) -> tuple[Candidate, ...]:
        """Members, ordered by emission time then id."""
        return self._members

    @property
    def start(self) -> float:
        """Earliest member emission time, epoch seconds."""
        return self._members[0].emitted_at

    @property
    def end(self) -> float:
        """Latest member emission time, epoch seconds."""
        return max(item.emitted_at for item in self._members)

    def overlaps(self, other: "Incident", window_s: float = 0.0) -> bool:
        """Whether two incidents' time spans intersect or fall within window_s."""
        return (
            self.start <= (other.end + window_s)
            and other.start <= (self.end + window_s)
        )

    def entities(self) -> tuple[str, ...]:
        """The distinct entities showing symptoms, sorted."""
        return tuple(sorted({item.entity for item in self._members}))

    def incident_id(self) -> str:
        """A stable, content-derived incident id.

        Derived from the cause and the episode's start instant, so the same
        fault in the same run always yields the same id, and two different
        faults cannot collide. Built via
        :func:`ano.contracts.determinism.stable_id` — builtin ``hash()`` on a
        string is forbidden in this codebase because it is salted per process
        and would break reproducibility.
        """
        return stable_id(
            f"{self._cause}|{self.start:.3f}",
            "ano",
            "detect",
            "incident",
            prefix="inc-",
            length=12,
        )

    def representative(self) -> Candidate:
        """The member an operator should read first.

        Prefers a candidate raised *on the cause entity itself* — that is the
        one naming the thing to go and fix — and breaks ties by confidence,
        then by id for determinism.
        """
        return max(
            self._members,
            key=lambda item: (
                item.entity == self._cause,
                item.confidence,
                item.candidate_id,
            ),
        )


def _episodes(
    cause: str, members: Sequence[Candidate], window_s: float
) -> list[Incident]:
    """Split one cause's candidates into single-linkage temporal episodes.

    Args:
        cause: The attributed cause entity.
        members: That cause's candidates, any order.
        window_s: Maximum gap between consecutive candidates in an episode.

    Returns:
        One :class:`Incident` per episode, in time order.
    """
    ordered = sorted(members, key=lambda item: (item.emitted_at, item.candidate_id))
    episodes: list[Incident] = []
    current: list[Candidate] = []
    previous: float | None = None
    for item in ordered:
        if previous is not None and (item.emitted_at - previous) > window_s:
            episodes.append(Incident(cause, current))
            current = []
        current.append(item)
        previous = item.emitted_at
    if current:
        episodes.append(Incident(cause, current))
    return episodes


def _merge_cascades(
    incidents: list[Incident],
    topology: Topology | None,
    window_s: float = DEFAULT_JOIN_WINDOW_S,
) -> list[Incident]:
    """Join episodes whose causes are in an upstream/downstream relationship.

    Episodes within window_s of each other are merged when one cause is strictly
    downstream of the other. The merged incident keeps the **upstream** cause,
    because that is the thing to go and fix.

    Args:
        incidents: Episodes, one cause each.
        topology: The estate graph, or ``None`` to skip cascade merging.
        window_s: Maximum temporal gap between cascaded episodes to merge.

    Returns:
        The merged episode list. Order is deterministic.
    """
    if topology is None or len(incidents) < 2:
        return incidents

    # Union-find over episode indices, joining downstream into upstream.
    parent = list(range(len(incidents)))

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    downstream_cache = {
        index: frozenset(topology.downstream_of(item.cause))
        for index, item in enumerate(incidents)
    }

    for i in range(len(incidents)):
        for j in range(i + 1, len(incidents)):
            left, right = incidents[i], incidents[j]
            if left.cause == right.cause or not left.overlaps(right, window_s=window_s):
                continue
            if right.cause in downstream_cache[i]:
                upstream, downstream = find(i), find(j)
            elif left.cause in downstream_cache[j]:
                upstream, downstream = find(j), find(i)
            else:
                continue
            if upstream != downstream:
                parent[downstream] = upstream

    grouped: dict[int, list[Candidate]] = {}
    for index, incident in enumerate(incidents):
        grouped.setdefault(find(index), []).extend(incident.members)
    return [
        Incident(incidents[root].cause, members)
        for root, members in sorted(
            grouped.items(), key=lambda pair: incidents[pair[0]].start
        )
    ]


def collapse_candidates(
    candidates: Iterable[Candidate],
    *,
    window_s: float = DEFAULT_JOIN_WINDOW_S,
    topology: Topology | None = None,
    topo: Topology | None = None,
) -> list[Candidate]:
    """Collapse candidates arising from one upstream fault into one incident.

    Args:
        candidates: Candidates, post-attribution. Any order.
        window_s: Maximum gap between consecutive candidates on one cause
            before they become separate incidents.
        topology: The estate graph, used to merge overlapping cascades across
            different attributed causes. ``None`` disables cascade merging;
            grouping by cause still happens.
        topo: Alias for topology.

    Returns:
        Exactly as many candidates as came in, ordered by emission time then
        id, each carrying an ``incident_id``. The representative of each
        incident additionally carries ``collapsed_from`` and a ``downstream``
        roll-up.
    """
    resolved_topology = topology if topology is not None else topo
    items = list(candidates)
    if not items:
        return []

    by_cause: dict[str, list[Candidate]] = {}
    for item in items:
        by_cause.setdefault(item.responsible_entity, []).append(item)

    incidents: list[Incident] = []
    for cause in sorted(by_cause):
        incidents.extend(_episodes(cause, by_cause[cause], window_s))

    incidents = _merge_cascades(incidents, resolved_topology, window_s=window_s)

    result: list[Candidate] = []
    for incident in incidents:
        incident_id = incident.incident_id()
        representative = incident.representative()
        affected = incident.entities()
        sibling_ids = [
            member.candidate_id
            for member in incident.members
            if member.candidate_id != representative.candidate_id
        ]
        for member in incident.members:
            if member.candidate_id != representative.candidate_id:
                result.append(member.with_incident(incident_id))
                continue
            joined = member.with_incident(incident_id, collapsed_from=sibling_ids)
            if len(incident.members) > 1:
                detail = (
                    f"{len(incident.members)} predictions across "
                    f"{len(affected)} entities collapsed into one incident "
                    f"caused by {incident.cause}; affected: "
                    f"{', '.join(affected)}"
                )
                joined = replace(
                    joined,
                    downstream=tuple(
                        sorted(set(joined.downstream) | set(affected) - {incident.cause})
                    ),
                    evidence=(
                        *joined.evidence,
                        CausalEvidence(kind="alert_collapse", detail=detail),
                    ),
                )
            result.append(joined)

    result.sort(key=lambda item: (item.emitted_at, item.candidate_id))
    return result


#: Backwards-compatible alias. ``collapse`` reads better at the call site.
collapse = collapse_candidates
