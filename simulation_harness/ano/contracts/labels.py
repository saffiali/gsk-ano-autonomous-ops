"""Contract C1 — the ground-truth label file.

Direction of travel: ``scenariogen`` **writes** it, ``harness`` **reads** it.
Nothing else may touch it.

    Requirement R5: "The scenario generator must not be able to see the
    detector's internals, and the detectors must not read the ground-truth
    labels at inference time."

Consequently no module under ``ano.detect``, ``ano.correlate``,
``ano.semantic`` or ``ano.ingest`` may import this module. It lives under
``ano.contracts`` because the *shape* is shared vocabulary, not because the
*data* is shared. For the same reason the on-disk **reader** is not here at
all: it lives in ``harness.ground_truth``, so R5 is a property of the import
graph rather than of a comment. ``write_label_file`` stays, because
``scenariogen`` is the writer and writing is not the risk.

Wire format (from ``PROJECT.md``)::

    { "seed": 1234,
      "window": {"start": "RFC3339", "end": "RFC3339"},
      "entities": [{"entity_id": "...", "kind": "app_server|database|switch|node"}],
      "events": [{
         "event_id": "...", "is_genuine": true,
         "kind": "thread_starvation|memory_leak|socket_exhaustion|
                  db_lock_contention|ospf_flap|packet_loss|patching_window|
                  planned_deploy|traffic_surge|normal",
         "onset": "RFC3339", "end": "RFC3339",
         "root_cause": {"domain": "application|database|network", "entity": "..."},
         "affected_entities": ["..."],
         "track": "unresponsiveness|cross_domain|benign|normal",
         "novel_log_signature": false }] }

Validation is strict: unknown keys are rejected, enums are closed, timestamps
must be RFC 3339, and the cross-field invariants below are enforced.

Cross-field invariants
----------------------
* ``is_genuine`` is **derived from** ``kind`` and must agree with it: it is true
  for the six fault kinds and false for the benign kinds and ``normal``. A
  label file that disagrees is malformed, not merely unusual — the harness
  computes recall and false-positive rate from these two fields and a
  disagreement would silently corrupt both.
* ``track`` must be ``unresponsiveness`` or ``cross_domain`` for genuine faults,
  ``benign`` for benign kinds, ``normal`` for ``normal``.
* ``end >= onset``; ``onset`` lies inside the declared window.
* Every entity referenced by ``root_cause.entity`` or ``affected_entities`` must
  be declared in ``entities``.
* ``event_id`` and ``entity_id`` are unique within the file.
"""

from __future__ import annotations

import datetime as _dt
import os
import pathlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from ano.contracts.common import (
    ValidationError,
    check_enum,
    dumps_pretty,
    format_rfc3339,
    join_path,
    loads,
    reject_unknown_keys,
    require_mapping,
    take_bool,
    take_int,
    take_list,
    take_mapping,
    take_str,
    take_timestamp,
)

__all__ = [
    "ENTITY_KINDS",
    "DOMAINS",
    "TRACKS",
    "GENUINE_KINDS",
    "BENIGN_KINDS",
    "NORMAL_KINDS",
    "EVENT_KINDS",
    "RECOMMENDED_TRACK_FOR_KIND",
    "Entity",
    "RootCause",
    "Window",
    "GroundTruthEvent",
    "LabelFile",
    "parse_label_file",
    "write_label_file",
]

#: Kinds of entity the topology can contain.
ENTITY_KINDS: frozenset[str] = frozenset(
    {"app_server", "database", "switch", "node"}
)

#: Root-cause domains. These are exactly the three the acceptance criteria
#: score domain accuracy over.
DOMAINS: frozenset[str] = frozenset({"application", "database", "network"})

#: Evaluation tracks. The two predictive tracks are scored separately (AC-08).
TRACKS: frozenset[str] = frozenset(
    {"unresponsiveness", "cross_domain", "benign", "normal"}
)

#: Genuine fault kinds — these are the incidents recall is measured over.
GENUINE_KINDS: frozenset[str] = frozenset(
    {
        "thread_starvation",
        "memory_leak",
        "socket_exhaustion",
        "db_lock_contention",
        "ospf_flap",
        "packet_loss",
    }
)

#: Benign injected events. An alert raised for one of these is a false positive
#: (ratified definition RD-01).
BENIGN_KINDS: frozenset[str] = frozenset(
    {"patching_window", "planned_deploy", "traffic_surge"}
)

#: Periods of pure normal operation.
NORMAL_KINDS: frozenset[str] = frozenset({"normal"})

#: All event kinds the generator may emit.
EVENT_KINDS: frozenset[str] = GENUINE_KINDS | BENIGN_KINDS | NORMAL_KINDS

#: The track each kind is normally labelled with. Advisory for generators: the
#: validator only enforces the *set* a kind may use (see module docstring), so a
#: generator may legitimately label, say, a ``memory_leak`` that manifests as a
#: cross-domain cascade on the ``cross_domain`` track.
RECOMMENDED_TRACK_FOR_KIND: Mapping[str, str] = {
    "thread_starvation": "unresponsiveness",
    "memory_leak": "unresponsiveness",
    "socket_exhaustion": "unresponsiveness",
    "db_lock_contention": "cross_domain",
    "ospf_flap": "cross_domain",
    "packet_loss": "cross_domain",
    "patching_window": "benign",
    "planned_deploy": "benign",
    "traffic_surge": "benign",
    "normal": "normal",
}

_PREDICTIVE_TRACKS = frozenset({"unresponsiveness", "cross_domain"})


def _allowed_tracks_for(kind: str) -> frozenset[str]:
    if kind in GENUINE_KINDS:
        return _PREDICTIVE_TRACKS
    if kind in BENIGN_KINDS:
        return frozenset({"benign"})
    return frozenset({"normal"})


@dataclass(frozen=True, slots=True)
class Entity:
    """A monitored entity that scenarios and predictions refer to by id."""

    entity_id: str
    kind: str

    def __post_init__(self) -> None:
        self.validate()

    def validate(self, path: str = "") -> None:
        """Raise :class:`ValidationError` if this record is malformed."""
        if not isinstance(self.entity_id, str) or not self.entity_id:
            raise ValidationError(
                "entity_id must be a non-empty string", join_path(path, "entity_id")
            )
        check_enum(self.kind, ENTITY_KINDS, join_path(path, "kind"))

    def to_dict(self) -> dict[str, Any]:
        """Serialise to the C1 wire form."""
        return {"entity_id": self.entity_id, "kind": self.kind}

    @classmethod
    def from_dict(cls, obj: Any, path: str = "") -> "Entity":
        """Parse and strictly validate one entity record."""
        mapping = require_mapping(obj, path)
        reject_unknown_keys(mapping, ("entity_id", "kind"), path)
        return cls(
            entity_id=take_str(mapping, "entity_id", path),
            kind=check_enum(
                take_str(mapping, "kind", path), ENTITY_KINDS, join_path(path, "kind")
            ),
        )


@dataclass(frozen=True, slots=True)
class RootCause:
    """The true responsible domain and entity for an event."""

    domain: str
    entity: str

    def __post_init__(self) -> None:
        self.validate()

    def validate(self, path: str = "") -> None:
        """Raise :class:`ValidationError` if this record is malformed."""
        check_enum(self.domain, DOMAINS, join_path(path, "domain"))
        if not isinstance(self.entity, str) or not self.entity:
            raise ValidationError(
                "entity must be a non-empty string", join_path(path, "entity")
            )

    def to_dict(self) -> dict[str, Any]:
        """Serialise to the C1 wire form."""
        return {"domain": self.domain, "entity": self.entity}

    @classmethod
    def from_dict(cls, obj: Any, path: str = "") -> "RootCause":
        """Parse and strictly validate one root-cause record."""
        mapping = require_mapping(obj, path)
        reject_unknown_keys(mapping, ("domain", "entity"), path)
        return cls(
            domain=check_enum(
                take_str(mapping, "domain", path), DOMAINS, join_path(path, "domain")
            ),
            entity=take_str(mapping, "entity", path),
        )


@dataclass(frozen=True, slots=True)
class Window:
    """The closed time window a scenario covers."""

    start: _dt.datetime
    end: _dt.datetime

    def __post_init__(self) -> None:
        self.validate()

    def validate(self, path: str = "") -> None:
        """Raise :class:`ValidationError` if the window is malformed."""
        format_rfc3339(self.start, join_path(path, "start"))
        format_rfc3339(self.end, join_path(path, "end"))
        if self.end <= self.start:
            raise ValidationError(
                f"window end ({format_rfc3339(self.end)}) must be after start "
                f"({format_rfc3339(self.start)})",
                path,
            )

    @property
    def duration_s(self) -> float:
        """Window length in seconds."""
        return (self.end - self.start).total_seconds()

    def contains(self, moment: _dt.datetime) -> bool:
        """True if ``moment`` lies inside the window (inclusive)."""
        return self.start <= moment <= self.end

    def to_dict(self) -> dict[str, Any]:
        """Serialise to the C1 wire form."""
        return {
            "start": format_rfc3339(self.start),
            "end": format_rfc3339(self.end),
        }

    @classmethod
    def from_dict(cls, obj: Any, path: str = "") -> "Window":
        """Parse and strictly validate a window record."""
        mapping = require_mapping(obj, path)
        reject_unknown_keys(mapping, ("start", "end"), path)
        return cls(
            start=take_timestamp(mapping, "start", path),
            end=take_timestamp(mapping, "end", path),
        )


@dataclass(frozen=True, slots=True)
class GroundTruthEvent:
    """One injected event with its ground truth.

    Attributes:
        event_id: Unique id within the label file.
        is_genuine: True for a real fault, False for a benign event or normal
            operation. Must agree with ``kind`` (see module docstring).
        kind: One of :data:`EVENT_KINDS`.
        onset: True onset time. Lead time is measured against this.
        end: End of the event; must be >= ``onset``.
        root_cause: True responsible domain and entity.
        affected_entities: Every entity that shows symptoms, including the
            root-cause entity if it is itself symptomatic. This is what makes
            the collapse ratio (AC-13) measurable.
        track: One of :data:`TRACKS`.
        novel_log_signature: True if this event's log signature is absent from
            the training window (AC-15).
    """

    event_id: str
    is_genuine: bool
    kind: str
    onset: _dt.datetime
    end: _dt.datetime
    root_cause: RootCause
    affected_entities: tuple[str, ...]
    track: str
    novel_log_signature: bool = False

    #: Keys allowed on the wire, in contract order.
    WIRE_KEYS = (
        "event_id",
        "is_genuine",
        "kind",
        "onset",
        "end",
        "root_cause",
        "affected_entities",
        "track",
        "novel_log_signature",
    )

    def __post_init__(self) -> None:
        self.validate()

    def validate(self, path: str = "") -> None:
        """Raise :class:`ValidationError` if this event is malformed."""
        if not isinstance(self.event_id, str) or not self.event_id:
            raise ValidationError(
                "event_id must be a non-empty string", join_path(path, "event_id")
            )
        if not isinstance(self.is_genuine, bool):
            raise ValidationError(
                "is_genuine must be a boolean", join_path(path, "is_genuine")
            )
        if not isinstance(self.novel_log_signature, bool):
            raise ValidationError(
                "novel_log_signature must be a boolean",
                join_path(path, "novel_log_signature"),
            )
        check_enum(self.kind, EVENT_KINDS, join_path(path, "kind"))
        check_enum(self.track, TRACKS, join_path(path, "track"))
        format_rfc3339(self.onset, join_path(path, "onset"))
        format_rfc3339(self.end, join_path(path, "end"))
        if self.end < self.onset:
            raise ValidationError(
                f"end ({format_rfc3339(self.end)}) must not precede onset "
                f"({format_rfc3339(self.onset)})",
                path,
            )
        self.root_cause.validate(join_path(path, "root_cause"))
        expected_genuine = self.kind in GENUINE_KINDS
        if self.is_genuine != expected_genuine:
            raise ValidationError(
                f"is_genuine={self.is_genuine} contradicts kind={self.kind!r}, "
                f"which is {'a genuine fault' if expected_genuine else 'not a genuine fault'}",
                path,
            )
        allowed = _allowed_tracks_for(self.kind)
        if self.track not in allowed:
            raise ValidationError(
                f"track={self.track!r} is not valid for kind={self.kind!r}; "
                "allowed: " + ", ".join(repr(t) for t in sorted(allowed)),
                join_path(path, "track"),
            )
        if not isinstance(self.affected_entities, tuple):
            raise ValidationError(
                "affected_entities must be a tuple of entity ids",
                join_path(path, "affected_entities"),
            )
        seen: set[str] = set()
        for index, entity_id in enumerate(self.affected_entities):
            item_path = join_path(join_path(path, "affected_entities"), index)
            if not isinstance(entity_id, str) or not entity_id:
                raise ValidationError("must be a non-empty string", item_path)
            if entity_id in seen:
                raise ValidationError(f"duplicate entity id {entity_id!r}", item_path)
            seen.add(entity_id)

    @property
    def duration_s(self) -> float:
        """Event duration in seconds."""
        return (self.end - self.onset).total_seconds()

    def to_dict(self) -> dict[str, Any]:
        """Serialise to the C1 wire form."""
        return {
            "event_id": self.event_id,
            "is_genuine": self.is_genuine,
            "kind": self.kind,
            "onset": format_rfc3339(self.onset),
            "end": format_rfc3339(self.end),
            "root_cause": self.root_cause.to_dict(),
            "affected_entities": list(self.affected_entities),
            "track": self.track,
            "novel_log_signature": self.novel_log_signature,
        }

    @classmethod
    def from_dict(cls, obj: Any, path: str = "") -> "GroundTruthEvent":
        """Parse and strictly validate one event record."""
        mapping = require_mapping(obj, path)
        reject_unknown_keys(mapping, cls.WIRE_KEYS, path)
        affected_raw = take_list(mapping, "affected_entities", path)
        affected: list[str] = []
        for index, item in enumerate(affected_raw):
            item_path = join_path(join_path(path, "affected_entities"), index)
            if not isinstance(item, str) or not item:
                raise ValidationError("must be a non-empty string", item_path)
            affected.append(item)
        return cls(
            event_id=take_str(mapping, "event_id", path),
            is_genuine=take_bool(mapping, "is_genuine", path),
            kind=check_enum(
                take_str(mapping, "kind", path), EVENT_KINDS, join_path(path, "kind")
            ),
            onset=take_timestamp(mapping, "onset", path),
            end=take_timestamp(mapping, "end", path),
            root_cause=RootCause.from_dict(
                mapping["root_cause"], join_path(path, "root_cause")
            ),
            affected_entities=tuple(affected),
            track=check_enum(
                take_str(mapping, "track", path), TRACKS, join_path(path, "track")
            ),
            novel_log_signature=take_bool(mapping, "novel_log_signature", path),
        )


@dataclass(frozen=True, slots=True)
class LabelFile:
    """A complete C1 ground-truth label file.

    Attributes:
        seed: The seed the scenario was generated from. The harness reports
            per-seed results keyed on this.
        window: The time window the scenario covers.
        entities: Every entity in the scenario topology.
        events: Every injected event, genuine and benign.
    """

    seed: int
    window: Window
    entities: tuple[Entity, ...]
    events: tuple[GroundTruthEvent, ...] = field(default=())

    #: Keys allowed on the wire.
    WIRE_KEYS = ("seed", "window", "entities", "events")

    def __post_init__(self) -> None:
        self.validate()

    def validate(self, path: str = "") -> None:
        """Raise :class:`ValidationError` if the file is malformed.

        In addition to per-record validation this checks the file-level
        invariants: unique ids and referential integrity of entity references.
        """
        if not isinstance(self.seed, int) or isinstance(self.seed, bool):
            raise ValidationError("seed must be an integer", join_path(path, "seed"))
        self.window.validate(join_path(path, "window"))

        declared: set[str] = set()
        for index, entity in enumerate(self.entities):
            entity_path = join_path(join_path(path, "entities"), index)
            entity.validate(entity_path)
            if entity.entity_id in declared:
                raise ValidationError(
                    f"duplicate entity_id {entity.entity_id!r}", entity_path
                )
            declared.add(entity.entity_id)

        seen_events: set[str] = set()
        for index, event in enumerate(self.events):
            event_path = join_path(join_path(path, "events"), index)
            event.validate(event_path)
            if event.event_id in seen_events:
                raise ValidationError(
                    f"duplicate event_id {event.event_id!r}", event_path
                )
            seen_events.add(event.event_id)
            if not self.window.contains(event.onset):
                raise ValidationError(
                    f"onset {format_rfc3339(event.onset)} lies outside the scenario "
                    f"window {format_rfc3339(self.window.start)}.."
                    f"{format_rfc3339(self.window.end)}",
                    join_path(event_path, "onset"),
                )
            if event.root_cause.entity not in declared:
                raise ValidationError(
                    f"root_cause.entity {event.root_cause.entity!r} is not declared "
                    "in entities",
                    join_path(event_path, "root_cause"),
                )
            for pos, entity_id in enumerate(event.affected_entities):
                if entity_id not in declared:
                    raise ValidationError(
                        f"affected entity {entity_id!r} is not declared in entities",
                        join_path(
                            join_path(event_path, "affected_entities"), pos
                        ),
                    )

    # -- convenience accessors used by the harness ------------------------
    def entity_kinds(self) -> dict[str, str]:
        """Map of ``entity_id`` -> ``kind`` for every declared entity."""
        return {entity.entity_id: entity.kind for entity in self.entities}

    def genuine_events(self) -> tuple[GroundTruthEvent, ...]:
        """Every genuine fault (the recall denominator, AC-05)."""
        return tuple(event for event in self.events if event.is_genuine)

    def benign_events(self) -> tuple[GroundTruthEvent, ...]:
        """Every benign injected event (the AC-06 false-positive denominator)."""
        return tuple(event for event in self.events if event.kind in BENIGN_KINDS)

    def events_on_track(self, track: str) -> tuple[GroundTruthEvent, ...]:
        """Every event on ``track`` (AC-08 scores the two tracks separately)."""
        check_enum(track, TRACKS, "track")
        return tuple(event for event in self.events if event.track == track)

    def to_dict(self) -> dict[str, Any]:
        """Serialise the whole file to the C1 wire form."""
        return {
            "seed": self.seed,
            "window": self.window.to_dict(),
            "entities": [entity.to_dict() for entity in self.entities],
            "events": [event.to_dict() for event in self.events],
        }

    def to_json(self, indent: int = 2) -> str:
        """Serialise to deterministic, human-readable JSON text."""
        return dumps_pretty(self.to_dict(), indent=indent)

    @classmethod
    def from_dict(cls, obj: Any, path: str = "") -> "LabelFile":
        """Parse and strictly validate a whole label file from a dict."""
        mapping = require_mapping(obj, path)
        reject_unknown_keys(mapping, cls.WIRE_KEYS, path)
        entities_raw = take_list(mapping, "entities", path)
        events_raw = take_list(mapping, "events", path)
        entities = tuple(
            Entity.from_dict(item, join_path(join_path(path, "entities"), index))
            for index, item in enumerate(entities_raw)
        )
        events = tuple(
            GroundTruthEvent.from_dict(
                item, join_path(join_path(path, "events"), index)
            )
            for index, item in enumerate(events_raw)
        )
        return cls(
            seed=take_int(mapping, "seed", path),
            window=Window.from_dict(
                take_mapping(mapping, "window", path), join_path(path, "window")
            ),
            entities=entities,
            events=events,
        )


def parse_label_file(text: str) -> LabelFile:
    """Parse C1 JSON text into a validated :class:`LabelFile`."""
    return LabelFile.from_dict(loads(text))


# There is deliberately no ``read_label_file`` here. Reading ground truth from
# disk lives in ``harness.ground_truth``, so requirement R5 is enforced by the
# import graph instead of by a docstring asking politely: a detector that wants
# the labels would have to import ``harness``, which is a visible violation.
# ``write_label_file`` stays — scenariogen writes the file, and writing it is
# not the R5 risk.


def write_label_file(
    path: str | os.PathLike[str], label_file: LabelFile, indent: int = 2
) -> None:
    """Validate and write a C1 label file to disk, creating parent directories."""
    label_file.validate()
    target = pathlib.Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(label_file.to_json(indent=indent), encoding="utf-8")
