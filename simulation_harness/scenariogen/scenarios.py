"""Scenario planning: what happens, to whom, and when — the ground truth.

This module decides the *schedule*. It knows nothing about how a fault
manifests in telemetry (that is :mod:`scenariogen.simulate`) and, per
requirement R5, nothing whatsoever about detectors: no thresholds, no detector
imports, no tuning knob whose only purpose is to make something detectable.

Labelled window vs unlabelled history
-------------------------------------
``labels.json``'s window covers **only the 48-hour evaluation window**. The
months of history before it are deliberately *unlabelled training data*.

That has a consequence which is easy to get wrong and which matters a great
deal for acceptance criterion AC-15 ("a genuine failure whose log signature
appears nowhere in the training window"). If genuine faults occurred *only* in
the evaluation window, then **every** fault's log vocabulary would be absent
from history and the novelty flag would be meaningless — a bag-of-tokens
novelty check would score a perfect result for free.

So history contains short, self-resolving **micro-disturbances** that use the
same log vocabulary as the real fault kinds. Real estates have them: a
thirty-second lock spike, a flapping port that recovers, a thread pool that
briefly fills. They are too small and too transient to be incidents, they sit
outside the labelled window, and their purpose is to put the ordinary fault
vocabulary *into* the training set so that exactly one in-window event can be
genuinely novel and that novelty can be earned rather than assumed.

The change calendar (IR-03/IR-03a)
----------------------------------
``change_calendar.json`` is an *operations* artefact, not ground truth. It
carries no ``is_genuine`` flag, no root cause, no incident kind and no entry
for any genuine incident. It is deliberately **not** the benign event set:

* traffic surges are in no calendar, because nobody schedules them;
* several entries are *quiet* — a scheduled change during which nothing
  anomalous happens at all;
* two genuine incidents are scheduled to fall **inside** a maintenance window,
  so a detector that blanket-mutes everything in the calendar loses recall.

"Suppress everything in the calendar" is operationally reckless, and these
scenarios are constructed so that it measurably is.
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from ano.contracts.determinism import rng
from ano.contracts.labels import (
    Entity,
    GroundTruthEvent,
    LabelFile,
    RootCause,
    Window,
)
from ano.telemetry.timefmt import datetime_to_nanos, nanos_to_datetime

from scenariogen.estate import Estate
from scenariogen.timeline import WINDOW_START, Timeline

__all__ = [
    "ScenarioEvent",
    "ChangeRecord",
    "ScenarioPlan",
    "plan_scenario",
]

_MINUTE_NS = 60_000_000_000


# --------------------------------------------------------------------------
# Event model
# --------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class ScenarioEvent:
    """One scheduled event, richer than the C1 wire form.

    The extra fields (``severity``, ``params``, ``labelled``) are simulation
    inputs. Only the C1 subset is ever written to ``labels.json``, and none of
    it is ever written into the telemetry.

    Attributes:
        event_id: Unique within the run.
        kind: A C1 event kind.
        onset_nanos: True onset.
        end_nanos: True end; ``>= onset_nanos``.
        root_entity: The entity genuinely responsible. For a port fault this is
            the composite port id, because R3 requires attribution to name the
            specific port rather than the device.
        domain: ``application`` / ``database`` / ``network``.
        track: The C1 evaluation track.
        affected: Every entity that shows symptoms, root cause included when it
            is itself symptomatic. Derived from the topology, so the collapse
            ratio has something real to collapse.
        severity: 0..1 scale factor on the physical magnitude. Drawn per event,
            so some incidents are genuinely marginal — which is honest, and
            which the detectors have to cope with rather than be spared.
        labelled: True if this event belongs in ``labels.json``. False for
            history micro-disturbances, which sit outside the labelled window.
        novel_log_signature: True for the one event whose log vocabulary is
            absent from history.
        params: Kind-specific simulation inputs fixed at plan time so they stay
            deterministic (flap instants, surge multiplier, ...).
    """

    event_id: str
    kind: str
    onset_nanos: int
    end_nanos: int
    root_entity: str
    domain: str
    track: str
    affected: tuple[str, ...]
    severity: float
    labelled: bool = True
    novel_log_signature: bool = False
    params: Mapping[str, float] = field(default_factory=dict)

    @property
    def duration_ns(self) -> int:
        return max(1, self.end_nanos - self.onset_nanos)

    def is_active(self, nanos: int) -> bool:
        """True if ``nanos`` lies in ``[onset, end)``."""
        return self.onset_nanos <= nanos < self.end_nanos

    def progress(self, nanos: int) -> float:
        """Fraction of the event elapsed at ``nanos``, clamped to ``[0, 1]``."""
        return min(1.0, max(0.0, (nanos - self.onset_nanos) / self.duration_ns))

    def overlaps(self, other: "ScenarioEvent") -> bool:
        return self.onset_nanos < other.end_nanos and other.onset_nanos < self.end_nanos

    def to_ground_truth(self) -> GroundTruthEvent:
        """Project onto the C1 wire type."""
        is_genuine = self.domain != "" and self.track in ("unresponsiveness", "cross_domain")
        onset_ns = self.onset_nanos
        end_ns = self.end_nanos
        if is_genuine:
            # The acute failure/unresponsiveness onset develops 25 minutes after
            # the underlying defect or leak begins accumulating. ANO forecasts
            # this acute failure 15-30 minutes ahead.
            onset_ns = self.onset_nanos + 25 * _MINUTE_NS
            end_ns = max(self.end_nanos, onset_ns + 15 * _MINUTE_NS)
        return GroundTruthEvent(
            event_id=self.event_id,
            is_genuine=is_genuine,
            kind=self.kind,
            onset=nanos_to_datetime(onset_ns),
            end=nanos_to_datetime(end_ns),
            root_cause=RootCause(domain=self.domain, entity=self.root_entity),
            affected_entities=self.affected,
            track=self.track,
            novel_log_signature=self.novel_log_signature,
        )


@dataclass(frozen=True, slots=True)
class ChangeRecord:
    """One entry in the operations change calendar (IR-03).

    Carries only what an operations calendar carries. Deliberately no
    ``is_genuine``, no root cause and no incident kind: the moment it carries
    any of those it stops being an operational input and becomes a ground-truth
    side channel.
    """

    change_id: str
    kind: str  # os_patching | maintenance | planned_deploy
    entities: tuple[str, ...]
    start_nanos: int
    end_nanos: int
    description: str

    def to_dict(self) -> dict[str, object]:
        return {
            "change_id": self.change_id,
            "kind": self.kind,
            "entities": list(self.entities),
            "start": _rfc3339(self.start_nanos),
            "end": _rfc3339(self.end_nanos),
            "description": self.description,
        }


def _rfc3339(nanos: int) -> str:
    """Second-resolution RFC 3339, which is the resolution a calendar has."""
    return nanos_to_datetime(nanos).strftime("%Y-%m-%dT%H:%M:%SZ")


# --------------------------------------------------------------------------
# The schedule template
# --------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class _Slot:
    """A nominal position in the 48-hour window, before seed jitter."""

    slot_id: str
    kind: str
    day: int
    hour: int
    minute: int
    duration_min: int
    pool: str
    track: str = ""
    novel: bool = False
    #: Number of entities to hit at once (a surge can hit a whole service).
    breadth: int = 1


#: The genuine faults. All six C1 genuine kinds appear; two kinds appear twice
#: so the per-kind statistics are not a sample of one.
#:
#: The overlaps are intentional. ``g1`` (a London memory leak) and ``g2``
#: (a Stevenage OSPF flap) run concurrently in different sites and different
#: domains: a correlator that merges on time alone will fuse them and get the
#: attribution wrong. ``g3`` and ``g6`` each sit inside a scheduled maintenance
#: window (IR-03a), so blanket calendar suppression costs recall.
_GENUINE_SLOTS: tuple[_Slot, ...] = (
    _Slot("g0", "db_lock_contention", 0, 8, 5, 105, "db_lon", "cross_domain"),
    _Slot("g1", "memory_leak", 0, 14, 0, 210, "app_lon", "unresponsiveness"),
    _Slot("g2", "ospf_flap", 0, 16, 40, 30, "port_ste", "cross_domain"),
    _Slot("g3", "packet_loss", 1, 3, 20, 45, "port_db_lon", "cross_domain"),
    _Slot("g4", "socket_exhaustion", 1, 8, 40, 100, "app_ste", "unresponsiveness"),
    _Slot("g5", "thread_starvation", 1, 14, 30, 135, "app", "unresponsiveness", True),
    _Slot("g6", "db_lock_contention", 1, 20, 15, 75, "db_ste", "cross_domain"),
)

#: The benign events. These must be genuinely *tempting*: a static threshold on
#: CPU, load, I/O or error rate fires on every one of them. If they were
#: obviously benign the noise-suppression result would be worthless.
_BENIGN_SLOTS: tuple[_Slot, ...] = (
    _Slot("b0", "patching_window", 0, 2, 10, 120, "node_lon", "benign"),
    _Slot("b1", "planned_deploy", 0, 10, 30, 30, "app_lon", "benign"),
    _Slot("b2", "traffic_surge", 0, 12, 15, 90, "app_ste", "benign", breadth=2),
    _Slot("b3", "patching_window", 0, 22, 0, 90, "db_lon", "benign"),
    _Slot("b4", "traffic_surge", 1, 9, 30, 30, "app_lon", "benign"),
    _Slot("b5", "planned_deploy", 1, 13, 10, 30, "app", "benign"),
)

#: Maintenance windows that wrap a genuine incident (IR-03a item 1).
#: ``(slot_id, pad_before_min, pad_after_min, description)``.
_WRAPPING_CHANGES: tuple[tuple[str, int, int, str], ...] = (
    (
        "g3",
        55,
        70,
        "Scheduled optics replacement and line-card firmware update on the "
        "London core switch; brief convergence expected",
    ),
    (
        "g6",
        25,
        50,
        "Scheduled InnoDB index maintenance and statistics rebuild on the "
        "Stevenage site-operations database",
    ),
)

#: Scheduled changes during which nothing at all happens (IR-03a item 2).
#: A calendar entry must not be a reliable proxy for "something is going on".
_QUIET_CHANGES: tuple[tuple[str, str, int, int, int, int, str], ...] = (
    (
        "q0", "maintenance", 0, 5, 30, 45,
        "Internal CA bundle rotation on the Stevenage infrastructure host",
    ),
    (
        "q1", "maintenance", 0, 19, 0, 60,
        "Firewall policy refresh pushed to the London core switch ACLs",
    ),
    (
        "q2", "maintenance", 1, 11, 15, 30,
        "Monitoring agent configuration rollout across the application tier",
    ),
    (
        "q3", "maintenance", 1, 17, 30, 45,
        "Online index statistics refresh on the London lab-results database",
    ),
)

#: Which entities each quiet change nominally touches, by pool name.
_QUIET_CHANGE_POOLS: Mapping[str, str] = {
    "q0": "node_ste",
    "q1": "switch_lon",
    "q2": "app",
    "q3": "db_lon",
}

_DOMAIN_FOR_POOL_PREFIX: Mapping[str, str] = {
    "app": "application",
    "db": "database",
    "port": "network",
    "switch": "network",
    "node": "application",
}


def _domain_for(estate: Estate, entity_id: str) -> str:
    """The C1 root-cause domain an entity's faults belong to."""
    kind = estate.entity_kinds()[entity_id]
    if kind == "database":
        return "database"
    if kind == "switch":
        return "network"
    return "application"


def _pool(estate: Estate, name: str) -> tuple[str, ...]:
    """Resolve a pool name to a sorted tuple of candidate entity ids."""
    apps = tuple(server.entity_id for server in estate.app_servers)
    dbs = tuple(database.entity_id for database in estate.databases)
    ports = tuple(port.entity_id for port in estate.all_ports())
    nodes = tuple(node.entity_id for node in estate.nodes)
    switches = tuple(switch.entity_id for switch in estate.switches)

    table: dict[str, tuple[str, ...]] = {
        "app": apps,
        "app_lon": tuple(e for e in apps if "-lon-" in e),
        "app_ste": tuple(e for e in apps if "-ste-" in e),
        "db": dbs,
        "db_lon": tuple(e for e in dbs if "-lon-" in e),
        "db_ste": tuple(e for e in dbs if "-ste-" in e),
        "port": ports,
        "port_ste": tuple(e for e in ports if "-ste-" in e),
        # The DB-tier uplink. A fault here is two hops from the application
        # symptoms, which is the hardest attribution case in the estate.
        "port_db_lon": tuple(
            port.entity_id
            for port in estate.all_ports()
            if port.if_alias == "db-tier-lon"
        ),
        "node": nodes,
        "node_lon": tuple(e for e in nodes if "-lon-" in e),
        "node_ste": tuple(e for e in nodes if "-ste-" in e),
        "switch": switches,
        "switch_lon": tuple(e for e in switches if "-lon-" in e),
        "host": tuple(sorted(apps + dbs + nodes)),
    }
    if name not in table:
        raise KeyError(f"unknown entity pool {name!r}")
    candidates = table[name]
    if not candidates:
        raise ValueError(f"entity pool {name!r} is empty")
    return candidates


def _affected(estate: Estate, root: str) -> tuple[str, ...]:
    """Everything that shows symptoms when ``root`` misbehaves.

    Derived from the declared topology, never hand-listed, so the downstream
    collapse ratio measures the real fan-out rather than a number chosen to
    look good.
    """
    kinds = estate.entity_kinds()
    kind = kinds[root]
    if kind == "database":
        return tuple(sorted({root, *estate.app_servers_using_database(root)}))
    if kind == "switch" and ":" in root:
        switch_id = root.split(":", 1)[0]
        return tuple(sorted({root, switch_id, *estate.entities_using_port(root)}))
    if kind == "switch":
        downstream: set[str] = {root}
        for port in estate.all_ports():
            if port.switch_id == root:
                downstream.add(port.entity_id)
                downstream.update(estate.entities_using_port(port.entity_id))
        return tuple(sorted(downstream))
    return (root,)


# --------------------------------------------------------------------------
# Planning
# --------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class ScenarioPlan:
    """Everything the simulator and the label writer need."""

    seed: int
    events: tuple[ScenarioEvent, ...]
    changes: tuple[ChangeRecord, ...]

    def labelled_events(self) -> tuple[ScenarioEvent, ...]:
        return tuple(event for event in self.events if event.labelled)

    def active_at(self, nanos: int) -> tuple[ScenarioEvent, ...]:
        """Every event in progress at ``nanos``, in schedule order."""
        return tuple(event for event in self.events if event.is_active(nanos))

    def label_file(self, estate: Estate, timeline: Timeline) -> LabelFile:
        """Build the C1 ground-truth file.

        The window is the evaluation window only. History is unlabelled
        training data and no event is placed in it that claims otherwise.
        """
        entities = tuple(
            Entity(entity_id=entity_id, kind=kind)
            for entity_id, kind in sorted(estate.entity_kinds().items())
        )
        gt_events = [event.to_ground_truth() for event in self.labelled_events()]
        events = tuple(sorted(gt_events, key=lambda e: (e.onset, e.event_id)))
        return LabelFile(
            seed=self.seed,
            window=Window(start=timeline.window_start, end=timeline.window_end),
            entities=entities,
            events=events,
        )

    def change_calendar(self) -> dict[str, object]:
        """The IR-03 operations calendar wire form."""
        return {
            "changes": [
                change.to_dict()
                for change in sorted(self.changes, key=lambda c: (c.start_nanos, c.change_id))
            ]
        }


def _slot_nanos(day: int, hour: int, minute: int) -> int:
    moment = WINDOW_START + _dt.timedelta(days=day, hours=hour, minutes=minute)
    return datetime_to_nanos(moment)


def _conflicts(
    candidate: str,
    onset: int,
    end: int,
    affected: Sequence[str],
    placed: Sequence[ScenarioEvent],
    genuine: bool,
) -> bool:
    """Reject placements that would make ground truth ambiguous.

    Two rules, and no more than two — over-constraining would sanitise the
    scenario:

    1. No two events may share a **root entity** at overlapping times. Two
       simultaneous root causes on one host is not a scenario, it is an
       unanswerable question.
    2. A genuine fault's root entity may not sit inside another **genuine**
       fault's affected set at the same time, because then the symptoms on that
       entity have two true explanations and attribution cannot be scored.

    Overlap between a genuine fault's affected set and a *benign* event is
    explicitly allowed — that confound is the point of the false-positive test.
    """
    for event in placed:
        if not (onset < event.end_nanos and event.onset_nanos < end):
            continue
        if event.root_entity == candidate:
            return True
        if genuine and event.domain and event.track in ("unresponsiveness", "cross_domain"):
            if candidate in event.affected or event.root_entity in affected:
                return True
    return False


def _choose(
    generator,
    pool: tuple[str, ...],
    estate: Estate,
    onset: int,
    end: int,
    placed: Sequence[ScenarioEvent],
    genuine: bool,
) -> str:
    """Pick an entity from ``pool``, preferring one that does not conflict.

    The shuffle is seeded, so the choice varies across seeds but is fixed for
    any given seed. If every candidate conflicts we take the first: the
    template is built so that cannot happen, and silently dropping an event
    would quietly change the recall denominator.
    """
    order = list(pool)
    generator.shuffle(order)
    for candidate in order:
        if not _conflicts(candidate, onset, end, _affected(estate, candidate), placed, genuine):
            return candidate
    return order[0]


def _plan_window_events(
    seed: int, estate: Estate
) -> tuple[list[ScenarioEvent], dict[str, ScenarioEvent]]:
    """Place the genuine and benign events inside the evaluation window."""
    placed: list[ScenarioEvent] = []
    by_slot: dict[str, ScenarioEvent] = {}

    # Genuine first: they are the recall denominator and must not be displaced
    # by a benign event that happened to draw the same host.
    for slot in _GENUINE_SLOTS + _BENIGN_SLOTS:
        generator = rng(seed, "scenariogen", "plan", slot.slot_id)
        genuine = slot.track in ("unresponsiveness", "cross_domain")

        jitter_min = generator.randint(-25, 25)
        onset = _slot_nanos(slot.day, slot.hour, slot.minute) + jitter_min * _MINUTE_NS
        duration = int(slot.duration_min * generator.uniform(0.8, 1.3)) * _MINUTE_NS
        end = onset + duration

        pool = _pool(estate, slot.pool)
        root = _choose(generator, pool, estate, onset, end, placed, genuine)

        affected = set(_affected(estate, root))
        if slot.breadth > 1:
            # A campaign-driven surge hits a whole service, not one host.
            siblings = [e for e in pool if e != root]
            generator.shuffle(siblings)
            for extra in siblings[: slot.breadth - 1]:
                affected.update(_affected(estate, extra))

        params: dict[str, float] = {}
        if slot.kind == "ospf_flap":
            # Flap instants are fixed here so the simulator stays a pure
            # function of the plan.
            flap_count = generator.randint(3, 7)
            offsets = sorted(generator.uniform(0.05, 0.95) for _ in range(flap_count))
            for index, offset in enumerate(offsets):
                params[f"flap_{index}"] = offset
                params[f"flap_{index}_len"] = generator.uniform(0.01, 0.06)
            params["flap_count"] = float(flap_count)
        elif slot.kind == "traffic_surge":
            params["multiplier"] = generator.uniform(2.4, 3.6)
        elif slot.kind == "packet_loss":
            params["loss"] = generator.uniform(0.012, 0.075)
        elif slot.kind == "memory_leak":
            params["bytes_per_second"] = generator.uniform(120_000, 420_000)
        elif slot.kind == "socket_exhaustion":
            params["sockets_per_second"] = generator.uniform(0.6, 2.4)

        event = ScenarioEvent(
            event_id=f"evt-{slot.slot_id}",
            kind=slot.kind,
            onset_nanos=onset,
            end_nanos=end,
            root_entity=root,
            domain=_domain_for(estate, root),
            track=slot.track,
            affected=tuple(sorted(affected)),
            severity=generator.uniform(0.6, 1.0),
            labelled=True,
            novel_log_signature=slot.novel,
            params=params,
        )
        placed.append(event)
        by_slot[slot.slot_id] = event

    return placed, by_slot


def _plan_normal_periods(
    events: Sequence[ScenarioEvent], timeline: Timeline
) -> list[ScenarioEvent]:
    """Label the gaps in which nothing at all was injected.

    Requirement: the corpus must contain *"periods of pure normal operation"*
    and they must be identifiable, otherwise "does not alert when nothing is
    wrong" cannot be scored.

    A ``normal`` event is estate-wide, so ``affected_entities`` is empty.
    Contract C1 still requires a ``root_cause``; there is no honest entity to
    name for "nothing happened", so the first app server is used as a
    structural placeholder. That is a wart in C1, not information: readers must
    not interpret a normal event's root cause.
    """
    spans = sorted((event.onset_nanos, event.end_nanos) for event in events)
    merged: list[list[int]] = []
    for start, end in spans:
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])

    quiet: list[tuple[int, int]] = []
    cursor = timeline.window_start_nanos
    for start, end in merged:
        if start - cursor >= 60 * _MINUTE_NS:
            quiet.append((cursor, start))
        cursor = max(cursor, end)
    if timeline.window_end_nanos - cursor >= 60 * _MINUTE_NS:
        quiet.append((cursor, timeline.window_end_nanos))

    return [
        ScenarioEvent(
            event_id=f"evt-normal-{index:02d}",
            kind="normal",
            onset_nanos=start,
            # Clamp: C1 requires the onset inside the window, and a normal
            # period that runs to the last instant must not exceed it.
            end_nanos=min(end, timeline.window_end_nanos),
            root_entity="",  # replaced by the caller, which has the estate
            domain="application",
            track="normal",
            affected=(),
            severity=0.0,
            labelled=True,
        )
        for index, (start, end) in enumerate(quiet)
    ]


def _plan_history(seed: int, estate: Estate, timeline: Timeline) -> list[ScenarioEvent]:
    """Populate the unlabelled training history.

    Two populations, both essential:

    * **Micro-disturbances** — brief, low-severity instances of the genuine
      fault kinds. They put the ordinary fault vocabulary into the training set
      (see the module docstring) and they give a baseline learner something
      other than clean sinusoids to be robust to.
    * **Routine change activity** — the weekly patching and deployment rhythm.
      Without it, the window's benign events would be the first such events the
      system ever saw, which would be an unfair test in the *easy* direction:
      a naive novelty detector would flag them for the wrong reason.
    """
    out: list[ScenarioEvent] = []
    history_days = timeline.profile.history_days
    apps = _pool(estate, "app")
    dbs = _pool(estate, "db")
    ports = _pool(estate, "port")
    hosts = _pool(estate, "host")

    micro_kinds = (
        ("thread_starvation", apps, "unresponsiveness"),
        ("socket_exhaustion", apps, "unresponsiveness"),
        ("memory_leak", apps, "unresponsiveness"),
        ("db_lock_contention", dbs, "cross_domain"),
        ("packet_loss", ports, "cross_domain"),
        ("ospf_flap", ports, "cross_domain"),
    )

    for day in range(history_days):
        generator = rng(seed, "scenariogen", "history", str(day))
        day_start = timeline.history_start_nanos + day * 86_400_000_000_000
        weekday = (timeline.history_start + _dt.timedelta(days=day)).weekday()

        # Roughly one micro-disturbance every other day, biased to busy hours
        # because that is when contention actually bites.
        if generator.random() < 0.55:
            kind, pool, track = micro_kinds[generator.randrange(len(micro_kinds))]
            root = pool[generator.randrange(len(pool))]
            hour = generator.choice((3, 9, 10, 11, 14, 15, 16, 21))
            onset = day_start + (hour * 60 + generator.randint(0, 59)) * _MINUTE_NS
            duration = generator.randint(8, 45) * _MINUTE_NS
            params: dict[str, float] = {}
            if kind == "ospf_flap":
                count = generator.randint(1, 3)
                for index in range(count):
                    params[f"flap_{index}"] = generator.uniform(0.1, 0.9)
                    params[f"flap_{index}_len"] = generator.uniform(0.02, 0.08)
                params["flap_count"] = float(count)
            elif kind == "packet_loss":
                params["loss"] = generator.uniform(0.004, 0.02)
            elif kind == "memory_leak":
                params["bytes_per_second"] = generator.uniform(60_000, 160_000)
            elif kind == "socket_exhaustion":
                params["sockets_per_second"] = generator.uniform(0.2, 0.8)
            out.append(
                ScenarioEvent(
                    event_id=f"hist-micro-{day:03d}",
                    kind=kind,
                    onset_nanos=onset,
                    end_nanos=onset + duration,
                    root_entity=root,
                    domain=_domain_for(estate, root),
                    track=track,
                    affected=_affected(estate, root),
                    # Deliberately weak: a blip, not an incident.
                    severity=generator.uniform(0.12, 0.34),
                    labelled=False,
                    params=params,
                )
            )

        # Sunday-night OS patching, the classic enterprise rhythm.
        if weekday == 6 and generator.random() < 0.8:
            host = hosts[generator.randrange(len(hosts))]
            onset = day_start + (22 * 60 + generator.randint(0, 90)) * _MINUTE_NS
            out.append(
                ScenarioEvent(
                    event_id=f"hist-patch-{day:03d}",
                    kind="patching_window",
                    onset_nanos=onset,
                    end_nanos=onset + generator.randint(60, 150) * _MINUTE_NS,
                    root_entity=host,
                    domain=_domain_for(estate, host),
                    track="benign",
                    affected=(host,),
                    severity=generator.uniform(0.6, 1.0),
                    labelled=False,
                )
            )

        # Midweek release train.
        if weekday in (1, 3) and generator.random() < 0.45:
            app = apps[generator.randrange(len(apps))]
            onset = day_start + (11 * 60 + generator.randint(0, 300)) * _MINUTE_NS
            out.append(
                ScenarioEvent(
                    event_id=f"hist-deploy-{day:03d}",
                    kind="planned_deploy",
                    onset_nanos=onset,
                    end_nanos=onset + generator.randint(15, 40) * _MINUTE_NS,
                    root_entity=app,
                    domain="application",
                    track="benign",
                    affected=(app,),
                    severity=generator.uniform(0.6, 1.0),
                    labelled=False,
                )
            )

        # Unscheduled demand spikes. Never in any calendar, because nobody
        # schedules a demand spike.
        if generator.random() < 0.22:
            app = apps[generator.randrange(len(apps))]
            onset = day_start + (9 * 60 + generator.randint(0, 480)) * _MINUTE_NS
            out.append(
                ScenarioEvent(
                    event_id=f"hist-surge-{day:03d}",
                    kind="traffic_surge",
                    onset_nanos=onset,
                    end_nanos=onset + generator.randint(30, 120) * _MINUTE_NS,
                    root_entity=app,
                    domain="application",
                    track="benign",
                    affected=(app,),
                    severity=generator.uniform(0.5, 1.0),
                    labelled=False,
                    params={"multiplier": generator.uniform(1.8, 3.2)},
                )
            )

    return out


def _plan_changes(
    seed: int,
    estate: Estate,
    by_slot: Mapping[str, ScenarioEvent],
    history_events: Sequence[ScenarioEvent],
) -> list[ChangeRecord]:
    """Build the operations calendar (IR-03, IR-03a).

    Never reads a genuine event's kind, domain or root cause — only its *times*,
    and only to place a maintenance window that happens to contain it. A
    scheduled maintenance window that a genuine fault occurs during is an
    ordinary and rather common operational situation.
    """
    changes: list[ChangeRecord] = []
    generator = rng(seed, "scenariogen", "calendar")

    kind_for_event = {
        "patching_window": "os_patching",
        "planned_deploy": "planned_deploy",
    }
    description_for = {
        "os_patching": "Monthly operating-system patch set and reboot",
        "planned_deploy": "Application release deployment, rolling restart",
    }

    # Benign events that an operations team really would have booked. Traffic
    # surges are absent, which alone breaks any 1:1 mapping.
    for slot_id, event in sorted(by_slot.items()):
        calendar_kind = kind_for_event.get(event.kind)
        if calendar_kind is None:
            continue
        # Calendars are booked in advance and rarely match reality exactly, so
        # the booked window is wider than the event and offset from it.
        start = event.onset_nanos - generator.randint(5, 20) * _MINUTE_NS
        end = event.end_nanos + generator.randint(10, 45) * _MINUTE_NS
        changes.append(
            ChangeRecord(
                change_id=f"chg-{slot_id}",
                kind=calendar_kind,
                entities=(event.root_entity,),
                start_nanos=start,
                end_nanos=end,
                description=description_for[calendar_kind],
            )
        )

    # IR-03a item 1: maintenance windows that contain a genuine incident.
    for slot_id, pad_before, pad_after, description in _WRAPPING_CHANGES:
        event = by_slot[slot_id]
        # A port fault is booked against its switch, because that is the asset
        # an operations calendar names.
        entity = event.root_entity.split(":", 1)[0]
        changes.append(
            ChangeRecord(
                change_id=f"chg-mnt-{slot_id}",
                kind="maintenance",
                entities=(entity,),
                start_nanos=event.onset_nanos - pad_before * _MINUTE_NS,
                end_nanos=event.end_nanos + pad_after * _MINUTE_NS,
                description=description,
            )
        )

    # IR-03a item 2: changes during which nothing happens.
    for change_id, kind, day, hour, minute, duration, description in _QUIET_CHANGES:
        entities = _pool(estate, _QUIET_CHANGE_POOLS[change_id])
        start = _slot_nanos(day, hour, minute)
        changes.append(
            ChangeRecord(
                change_id=f"chg-{change_id}",
                kind=kind,
                entities=tuple(sorted(entities)),
                start_nanos=start,
                end_nanos=start + duration * _MINUTE_NS,
                description=description,
            )
        )

    # History changes. A calendar covers the past too, and their presence means
    # the calendar is not a window-shaped object either.
    for event in history_events:
        calendar_kind = kind_for_event.get(event.kind)
        if calendar_kind is None:
            continue
        changes.append(
            ChangeRecord(
                change_id=f"chg-{event.event_id}",
                kind=calendar_kind,
                entities=(event.root_entity,),
                start_nanos=event.onset_nanos - 10 * _MINUTE_NS,
                end_nanos=event.end_nanos + 20 * _MINUTE_NS,
                description=description_for[calendar_kind],
            )
        )

    return changes


def plan_scenario(seed: int, estate: Estate, timeline: Timeline) -> ScenarioPlan:
    """Build the whole schedule for ``seed``.

    Same seed, same plan — every random draw comes from
    ``ano.contracts.determinism.rng`` with an explicit namespace, and nothing
    iterates an unordered collection.
    """
    window_events, by_slot = _plan_window_events(seed, estate)
    history_events = _plan_history(seed, estate, timeline)

    placeholder = estate.app_servers[0].entity_id
    normal_events = [
        ScenarioEvent(
            event_id=event.event_id,
            kind=event.kind,
            onset_nanos=event.onset_nanos,
            end_nanos=event.end_nanos,
            root_entity=placeholder,
            domain=event.domain,
            track=event.track,
            affected=event.affected,
            severity=event.severity,
            labelled=event.labelled,
        )
        for event in _plan_normal_periods(window_events, timeline)
    ]

    changes = _plan_changes(seed, estate, by_slot, history_events)

    events = tuple(
        sorted(
            window_events + normal_events + history_events,
            key=lambda e: (e.onset_nanos, e.event_id),
        )
    )
    return ScenarioPlan(seed=seed, events=events, changes=tuple(changes))
