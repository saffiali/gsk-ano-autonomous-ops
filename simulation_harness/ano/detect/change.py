"""``ano.detect.change`` — evidence-weighted change awareness (R4).

The operations change calendar (``change_calendar.json``, admitted as an
operational input by ruling IR-03) lists scheduled work: which entities, which
window, which kind. It carries no genuineness flag, no root cause and no
incident kind, so reading it is not a requirement-R5 violation — it is the same
artefact a real on-call engineer has open in another tab.

Why this module is not a mute switch
------------------------------------

The obvious implementation is a veto: if a change window covers the entity,
drop the alert. Ruling IR-03a forbids that, and for a good reason — M2 has
deliberately placed **two genuine incidents inside scheduled maintenance
windows** (a packet-loss fault on a DB-tier uplink port, and lock contention on
a database). A blanket mute loses both, and the noise-suppression acceptance
criterion is paired with a recall bound precisely to catch that trade.

So a calendar entry here **lowers the prior, it never vetoes the evidence**.
Concretely, :class:`ChangeCalendar` answers one question:

    *Of the evidence this candidate is standing on, how much of it would a
    change of this kind plausibly have produced by itself?*

That fraction — :meth:`ChangeCalendar.explained_fraction` — scales a log-odds
penalty. Three consequences fall out of it for free:

* A candidate driven entirely by families a patching window explains (CPU, I/O
  wait, disk latency, service restarts) is penalised in full. That is the
  reboot-looks-like-an-outage case the criterion is about.
* A candidate driven by families the change does not explain keeps its
  confidence. Index maintenance on a database does not explain OSPF adjacency
  loss on a switch port.
* A candidate standing on a **hard fault** — a deadlocked JVM, a down port, an
  OSPF neighbour out of ``full`` — is never explained at all. Scheduled work
  does not deadlock a JVM. These features are listed in
  :data:`ano.detect.signals.HARD_FAULT_FEATURES` and are excluded from the
  explained mass by construction, which is the mechanism that keeps M2's two
  buried incidents alive.

Two further guards, both about scope rather than strength:

* An entry only applies to the entities it names (plus, for estate-wide
  entries, everything). A deploy on the London app tier says nothing about
  Stevenage.
* Suppression is judged against the **attributed cause** entity, not the
  symptomatic one. If the calendar covers an app server but the cause was
  attributed to the database behind it, the change explains nothing.

Grace margins
-------------

Real changes leak outside their booked window: a rolling restart booked for
30 minutes warms caches for another ten, and pre-checks start early. The
calendar is therefore widened by :data:`DEFAULT_PRE_MARGIN_S` before and
:data:`DEFAULT_POST_MARGIN_S` after. The margins are deliberately modest;
widening them is the cheapest way to accidentally build the blanket mute this
module exists to avoid.
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field

from ano.detect.signals import FAMILIES, FAMILY_FOR_FEATURE, HARD_FAULT_FEATURES

__all__ = [
    "DEFAULT_PRE_MARGIN_S",
    "DEFAULT_POST_MARGIN_S",
    "CHANGE_KINDS",
    "EXPLAINED_FAMILIES",
    "ESTATE_WIDE",
    "ChangeEntry",
    "ChangeMatch",
    "ChangeCalendar",
    "explained_families_for",
    "load_change_calendar",
    "parse_change_calendar",
]


#: Seconds of grace before a booked window. Pre-checks and drain steps.
DEFAULT_PRE_MARGIN_S = 300.0

#: Seconds of grace after a booked window. Restart settle, cache warm, the
#: backlog a drained node works off once traffic returns.
DEFAULT_POST_MARGIN_S = 900.0

#: Sentinel entity for a calendar entry that names no entity at all.
ESTATE_WIDE = "*"


#: The change kinds the operations calendar uses. Unknown kinds are tolerated
#: (see :func:`explained_families_for`) but get the conservative default.
CHANGE_KINDS = ("os_patching", "planned_deploy", "maintenance")


#: Which signal families each kind of scheduled work can plausibly produce on
#: its own. Read this as a *causal claim about the work*, not as a list of
#: things to hide.
#:
#: ``os_patching``
#:     A patch run reboots or restarts. It burns CPU on package unpacking,
#:     hammers the disk, pushes I/O wait up, and the service goes away and
#:     comes back. It does not exhaust file descriptors over hours, and it
#:     does not touch the network control plane.
#: ``planned_deploy``
#:     A rolling release restarts application instances: thread pools refill
#:     from cold, the JVM heap climbs through its warm-up, request throughput
#:     dips against steady utilisation while classes load, and health checks
#:     flap. Confined to the application tier.
#: ``maintenance``
#:     The broadest kind — index rebuilds, statistics refreshes, link work.
#:     It covers database contention and session pressure, host resource load,
#:     and deliberate link-down work on a port. It is the kind most likely to
#:     be abused as a mute, so note what it still does *not* explain:
#:     descriptor exhaustion and OSPF flap.
EXPLAINED_FAMILIES: Mapping[str, frozenset[str]] = {
    "os_patching": frozenset(
        {
            "cpu_without_throughput",
            "io_wait",
            "disk_latency",
            "memory_pressure",
            "kernel_pressure",
            "service_health",
            "db_contention",
            "db_long_running_sql",
            "db_session_pool",
            "db_throughput",
        }
    ),
    "planned_deploy": frozenset(
        {
            "thread_starvation",
            "cpu_without_throughput",
            "memory_pressure",
            "service_health",
            "db_throughput",
        }
    ),
    "maintenance": frozenset(
        {
            "cpu_without_throughput",
            "io_wait",
            "disk_latency",
            "memory_pressure",
            "kernel_pressure",
            "service_health",
            "db_contention",
            "db_long_running_sql",
            "db_session_pool",
            "db_throughput",
            "net_congestion",
            "net_port_errors",
        }
    ),
}

#: What an unrecognised change kind is allowed to explain. Deliberately the
#: intersection of the known kinds rather than their union: an unknown kind is
#: an unknown risk, and the failure we care about is suppressing a real fault.
_UNKNOWN_KIND_FAMILIES = frozenset(
    EXPLAINED_FAMILIES["os_patching"]
    & EXPLAINED_FAMILIES["planned_deploy"]
    & EXPLAINED_FAMILIES["maintenance"]
)


def explained_families_for(kind: str) -> frozenset[str]:
    """Return the signal families a change of ``kind`` can plausibly produce.

    Args:
        kind: A calendar entry kind, e.g. ``"os_patching"``.

    Returns:
        The families that kind of work explains. An unrecognised kind gets the
        conservative intersection of the known kinds, never their union.
    """
    return EXPLAINED_FAMILIES.get(kind, _UNKNOWN_KIND_FAMILIES)


#: Family reported for a feature the signal catalogue does not classify. It is
#: deliberately not a member of any kind's explained set, so a feature added to
#: the catalogue without a family cannot silently become suppressible.
UNCLASSIFIED_FAMILY = "unclassified"


def _family_of(feature: str) -> str:
    """Return the signal family a feature belongs to.

    Args:
        feature: A feature name from the detection catalogue.

    Returns:
        The family, or :data:`UNCLASSIFIED_FAMILY` when the feature is not
        classified — which makes its evidence unexplainable rather than
        explainable, the safe direction.
    """
    return FAMILY_FOR_FEATURE.get(feature, UNCLASSIFIED_FAMILY)


def _to_epoch(value: object) -> float:
    """Coerce a calendar timestamp to epoch seconds.

    Accepts an aware :class:`datetime.datetime`, an ISO-8601 string (with a
    trailing ``Z`` permitted), or a number already in epoch seconds.

    Raises:
        ValueError: if the value is a naive datetime, or is not a recognised
            timestamp at all.
    """
    if isinstance(value, _dt.datetime):
        if value.tzinfo is None:
            raise ValueError("change calendar timestamps must be timezone-aware")
        return value.timestamp()
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        parsed = _dt.datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=_dt.timezone.utc)
        return parsed.timestamp()
    raise ValueError(f"unrecognised change calendar timestamp: {value!r}")


@dataclass(frozen=True, slots=True)
class ChangeEntry:
    """One scheduled change, as the operations calendar records it.

    Attributes:
        change_id: The calendar's own identifier.
        kind: One of :data:`CHANGE_KINDS`, or something unrecognised.
        entities: The entities in scope. Empty means estate-wide.
        start: Booked start, epoch seconds.
        end: Booked end, epoch seconds.
        description: Free text from the calendar. Never parsed for meaning —
            it is operator prose, and treating it as a signal would be exactly
            the generator-artefact pattern-matching the brief forbids.
    """

    change_id: str
    kind: str
    entities: frozenset[str]
    start: float
    end: float
    description: str = ""

    def covers_entity(self, entity: str) -> bool:
        """True if ``entity`` is in scope (estate-wide entries cover all)."""
        return not self.entities or entity in self.entities

    def covers_time(
        self,
        moment: float,
        *,
        pre_margin_s: float = DEFAULT_PRE_MARGIN_S,
        post_margin_s: float = DEFAULT_POST_MARGIN_S,
    ) -> bool:
        """True if ``moment`` falls in the window widened by the margins."""
        return (self.start - pre_margin_s) <= moment < (self.end + post_margin_s)

    def explained_families(self) -> frozenset[str]:
        """The signal families this entry can plausibly account for."""
        return explained_families_for(self.kind)


@dataclass(frozen=True, slots=True)
class ChangeMatch:
    """The verdict for one candidate against the calendar.

    Attributes:
        entry: The change entry that matched.
        explained_fraction: Share of the candidate's contribution mass sitting
            in families this change explains, in ``[0, 1]``.
        explained_families: Which families contributed to that share.
        unexplained_families: Which families did not — the evidence that
            survives the change and keeps the candidate alive.
        hard_fault: True if the candidate stands on a hard fault. Forces the
            explained fraction to zero: scheduled work does not deadlock a JVM
            or take an OSPF adjacency out of ``full``.
    """

    entry: ChangeEntry
    explained_fraction: float
    explained_families: tuple[str, ...]
    unexplained_families: tuple[str, ...]
    hard_fault: bool

    def reason(self) -> str:
        """A human-readable suppression reason, safe to put in contract C2."""
        share = f"{self.explained_fraction * 100:.0f}%"
        families = ", ".join(self.explained_families) or "none"
        return (
            f"change {self.entry.change_id} ({self.entry.kind}) covers this "
            f"entity and accounts for {share} of the evidence "
            f"[{families}]"
        )


class ChangeCalendar:
    """The scheduled-change calendar, indexed for per-entity lookup.

    Construct from :func:`parse_change_calendar`, :func:`load_change_calendar`,
    or :meth:`from_change_windows` when reading through M3's
    :class:`ano.ingest.FeatureReader`.
    """

    __slots__ = ("_entries", "_by_entity", "_estate_wide", "_pre", "_post", "_source")

    def __init__(
        self,
        entries: Iterable[ChangeEntry] = (),
        *,
        pre_margin_s: float = DEFAULT_PRE_MARGIN_S,
        post_margin_s: float = DEFAULT_POST_MARGIN_S,
        source: str = "change_calendar.json",
    ) -> None:
        """Index ``entries`` by entity.

        Args:
            entries: The scheduled changes.
            pre_margin_s: Grace before each booked window, in seconds.
            post_margin_s: Grace after each booked window, in seconds.
            source: Where the calendar came from. Recorded so a fallback can
                never silently masquerade as the real artefact (ruling IR-04).
        """
        self._entries = tuple(
            sorted(entries, key=lambda item: (item.start, item.change_id))
        )
        self._pre = float(pre_margin_s)
        self._post = float(post_margin_s)
        self._source = source
        by_entity: dict[str, list[ChangeEntry]] = {}
        estate_wide: list[ChangeEntry] = []
        for entry in self._entries:
            if not entry.entities:
                estate_wide.append(entry)
                continue
            for entity in entry.entities:
                by_entity.setdefault(entity, []).append(entry)
        self._by_entity = {key: tuple(value) for key, value in by_entity.items()}
        self._estate_wide = tuple(estate_wide)

    def __len__(self) -> int:
        """The number of calendar entries."""
        return len(self._entries)

    def __bool__(self) -> bool:
        """True if the calendar holds at least one entry."""
        return bool(self._entries)

    @property
    def source(self) -> str:
        """Where this calendar came from, for provenance logging (IR-04)."""
        return self._source

    def entries(self) -> tuple[ChangeEntry, ...]:
        """All entries, ordered by start time then id."""
        return self._entries

    def entities(self) -> tuple[str, ...]:
        """Entities named by at least one entry, sorted."""
        return tuple(sorted(self._by_entity))

    def active(self, entity: str, moment: float) -> tuple[ChangeEntry, ...]:
        """Entries covering ``entity`` at ``moment``, margins included.

        Args:
            entity: The entity to test.
            moment: Epoch seconds.

        Returns:
            The covering entries, ordered by start time. Empty when the entity
            is not under change.
        """
        found = [
            entry
            for entry in self._by_entity.get(entity, ())
            if entry.covers_time(moment, pre_margin_s=self._pre, post_margin_s=self._post)
        ]
        found.extend(
            entry
            for entry in self._estate_wide
            if entry.covers_time(moment, pre_margin_s=self._pre, post_margin_s=self._post)
        )
        found.sort(key=lambda item: (item.start, item.change_id))
        return tuple(found)

    def explained_fraction(
        self,
        entry: ChangeEntry,
        contributions: Mapping[str, float],
        *,
        hard_fault: bool = False,
    ) -> tuple[float, tuple[str, ...], tuple[str, ...]]:
        """Share of the evidence ``entry`` plausibly accounts for.

        The contribution map is the detector's own decomposition of its
        log-odds: **feature name** to the share of the total that feature
        supplied, summing to one over the evidence that fired
        (:func:`ano.detect.signals.contributions`). Each feature is mapped
        back to its signal family, and the share is counted as explained only
        when that family is one this kind of change plausibly produces. The
        result means exactly what the suppression decision needs it to mean.

        Args:
            entry: The covering calendar entry.
            contributions: Feature name to contribution share.
            hard_fault: True if the candidate stands on a hard fault. Forces
                the result to zero — see the module docstring.

        Returns:
            ``(fraction, explained_families, unexplained_families)``. The
            fraction is clamped into ``[0, 1]``.
        """
        if hard_fault:
            return 0.0, (), tuple(
                sorted({_family_of(feature) for feature in contributions})
            )
        allowed = entry.explained_families()
        total = 0.0
        explained = 0.0
        hit: set[str] = set()
        miss: set[str] = set()
        for feature, share in contributions.items():
            weight = max(0.0, float(share))
            if weight <= 0.0:
                continue
            total += weight
            family = _family_of(feature)
            # A hard fault is structurally unexplainable, independent of the
            # caller-supplied flag: no amount of scheduled work deadlocks a
            # JVM or drops an OSPF adjacency out of ``full``.
            if family in allowed and feature not in HARD_FAULT_FEATURES:
                explained += weight
                hit.add(family)
            else:
                miss.add(family)
        if total <= 0.0:
            return 0.0, (), ()
        fraction = explained / total
        fraction = 0.0 if fraction < 0.0 else (1.0 if fraction > 1.0 else fraction)
        return fraction, tuple(sorted(hit)), tuple(sorted(miss))

    def assess(
        self,
        entity: str,
        moment: float,
        contributions: Mapping[str, float],
        *,
        hard_fault: bool = False,
    ) -> ChangeMatch | None:
        """Assess one candidate against the calendar.

        Args:
            entity: The entity whose alert is being judged. Callers should
                pass the **attributed cause** entity, not the symptomatic one.
            moment: Epoch seconds at which the candidate was raised.
            contributions: Family name to contribution share.
            hard_fault: True if the candidate stands on a hard fault.

        Returns:
            The best-explaining :class:`ChangeMatch`, or ``None`` when no
            entry covers the entity at that moment.
        """
        covering = self.active(entity, moment)
        if not covering:
            return None
        best: ChangeMatch | None = None
        for entry in covering:
            fraction, hit, miss = self.explained_fraction(
                entry, contributions, hard_fault=hard_fault
            )
            match = ChangeMatch(
                entry=entry,
                explained_fraction=fraction,
                explained_families=hit,
                unexplained_families=miss,
                hard_fault=hard_fault,
            )
            if best is None or match.explained_fraction > best.explained_fraction:
                best = match
        return best

    def describe(self) -> dict[str, object]:
        """A small summary, for provenance logging and tests."""
        by_kind: dict[str, int] = {}
        for entry in self._entries:
            by_kind[entry.kind] = by_kind.get(entry.kind, 0) + 1
        return {
            "source": self._source,
            "entries": len(self._entries),
            "by_kind": dict(sorted(by_kind.items())),
            "entities": len(self._by_entity),
            "estate_wide": len(self._estate_wide),
            "pre_margin_s": self._pre,
            "post_margin_s": self._post,
        }

    @classmethod
    def from_change_windows(
        cls,
        windows: Iterable[object],
        *,
        pre_margin_s: float = DEFAULT_PRE_MARGIN_S,
        post_margin_s: float = DEFAULT_POST_MARGIN_S,
        source: str = "FeatureReader.change_windows",
    ) -> "ChangeCalendar":
        """Build from :class:`ano.ingest.ChangeWindow` rows.

        M3's reader fans a multi-entity calendar entry out into one row per
        entity, so rows sharing a ``change_id`` are folded back together here.

        Args:
            windows: ``ChangeWindow`` records from
                :meth:`ano.ingest.FeatureReader.change_windows`.
            pre_margin_s: Grace before each booked window.
            post_margin_s: Grace after each booked window.
            source: Provenance label (ruling IR-04).

        Returns:
            The assembled calendar.
        """
        folded: dict[str, dict[str, object]] = {}
        for window in windows:
            change_id = str(getattr(window, "change_id"))
            span = getattr(window, "window")
            start = _to_epoch(getattr(span, "start"))
            end = _to_epoch(getattr(span, "end"))
            entity = getattr(window, "entity_id", None)
            slot = folded.get(change_id)
            if slot is None:
                slot = {
                    "kind": str(getattr(window, "kind", "")),
                    "entities": set(),
                    "start": start,
                    "end": end,
                    "description": str(getattr(window, "description", "") or ""),
                }
                folded[change_id] = slot
            else:
                slot["start"] = min(float(slot["start"]), start)
                slot["end"] = max(float(slot["end"]), end)
            if entity:
                assert isinstance(slot["entities"], set)
                slot["entities"].add(str(entity))
        entries = [
            ChangeEntry(
                change_id=change_id,
                kind=str(slot["kind"]),
                entities=frozenset(slot["entities"]),  # type: ignore[arg-type]
                start=float(slot["start"]),
                end=float(slot["end"]),
                description=str(slot["description"]),
            )
            for change_id, slot in folded.items()
        ]
        return cls(
            entries,
            pre_margin_s=pre_margin_s,
            post_margin_s=post_margin_s,
            source=source,
        )


def parse_change_calendar(
    payload: object,
    *,
    pre_margin_s: float = DEFAULT_PRE_MARGIN_S,
    post_margin_s: float = DEFAULT_POST_MARGIN_S,
    source: str = "change_calendar.json",
) -> ChangeCalendar:
    """Parse the on-disk ``change_calendar.json`` shape.

    Accepts either ``{"changes": [...]}`` or a bare list of entries. Each entry
    needs ``change_id``, ``kind``, ``start``, ``end``; ``entities`` and
    ``description`` are optional.

    Args:
        payload: The decoded JSON document.
        pre_margin_s: Grace before each booked window.
        post_margin_s: Grace after each booked window.
        source: Provenance label (ruling IR-04).

    Returns:
        The assembled calendar.

    Raises:
        ValueError: if an entry is missing a required field or carries an
            unparseable timestamp.
    """
    if isinstance(payload, Mapping):
        raw: Sequence[object] = tuple(payload.get("changes", ()))  # type: ignore[arg-type]
    elif isinstance(payload, Sequence) and not isinstance(payload, (str, bytes)):
        raw = payload
    else:
        raise ValueError("change calendar must be a mapping or a sequence")

    entries: list[ChangeEntry] = []
    for item in raw:
        if not isinstance(item, Mapping):
            raise ValueError(f"change calendar entry must be a mapping: {item!r}")
        try:
            change_id = str(item["change_id"])
            kind = str(item["kind"])
            start = _to_epoch(item["start"])
            end = _to_epoch(item["end"])
        except KeyError as exc:  # pragma: no cover - defensive
            raise ValueError(f"change calendar entry missing {exc}") from exc
        entities = frozenset(
            str(value) for value in item.get("entities", ()) if str(value)
        )
        entries.append(
            ChangeEntry(
                change_id=change_id,
                kind=kind,
                entities=entities,
                start=start,
                end=end,
                description=str(item.get("description", "")),
            )
        )
    return ChangeCalendar(
        entries,
        pre_margin_s=pre_margin_s,
        post_margin_s=post_margin_s,
        source=source,
    )


def load_change_calendar(
    path: str,
    *,
    pre_margin_s: float = DEFAULT_PRE_MARGIN_S,
    post_margin_s: float = DEFAULT_POST_MARGIN_S,
) -> ChangeCalendar:
    """Read and parse ``change_calendar.json`` from disk.

    Args:
        path: Path to the calendar file.
        pre_margin_s: Grace before each booked window.
        post_margin_s: Grace after each booked window.

    Returns:
        The assembled calendar.
    """
    import json
    import os

    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return parse_change_calendar(
        payload,
        pre_margin_s=pre_margin_s,
        post_margin_s=post_margin_s,
        source=os.path.basename(path),
    )


# Fail loudly at import time if the family vocabulary drifts. A typo here would
# quietly stop a family from ever being explained, which is invisible in
# testing and shows up only as an unexplained noise-suppression regression.
_KNOWN = frozenset(FAMILIES)
for _kind, _families in EXPLAINED_FAMILIES.items():
    _unknown = _families - _KNOWN
    if _unknown:  # pragma: no cover - import-time contract check
        raise AssertionError(
            f"EXPLAINED_FAMILIES[{_kind!r}] names unknown signal families: "
            f"{sorted(_unknown)}"
        )
del _KNOWN, _kind, _families, _unknown

# The hard-fault features must not be reachable through any kind's explained
# families, or the two genuine incidents M2 buried inside maintenance windows
# would be muted. Asserted here rather than trusted.
_HARD_FAULT_GUARD = frozenset(HARD_FAULT_FEATURES)
assert _HARD_FAULT_GUARD, "HARD_FAULT_FEATURES must not be empty"
