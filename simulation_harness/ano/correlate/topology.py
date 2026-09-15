"""The maintained estate topology (R3, features #50-#56).

What it models
--------------
Three entity classes and the two dependency relations that connect them:

* ``app_server`` — an Apache/Tomcat application server (feature #53)
* ``database`` — a database instance (feature #54)
* ``switch`` — a switch, and each of its **ports** as a separate entity with
  the composite id ``"<device>:<ifName>"`` (feature #55). Ports are first class
  because R3 requires attribution to name "this port", not merely the device.
* ``node`` — shared infrastructure with no application role

* ``queries``   : ``app_server -> database`` (feature #51)
* ``traverses`` : ``host -> switch port`` (feature #52)

Why the graph has to exist at all
---------------------------------
The failure mode this whole milestone exists to avoid is *blaming the
symptomatic entity*. When a database seizes, every application that queries it
goes slow simultaneously. Naive correlation sees N unhappy application servers
and files N application-domain incidents. The correct answer requires two graph
facts that no amount of single-entity analysis can supply:

1. the *fan-out* — all three apps that share the database are unhappy, and the
   two that do not share it are fine; and
2. the *path* — there is a dependency edge from each unhappy app to that
   database, so the database is a reachable cause rather than a coincidence.

:meth:`Topology.downstream_of` supplies the first and
:meth:`Topology.explain_path` the second. The same two facts drive alert
collapse: entities sharing one upstream cause share one ``incident_id``.

Two-hop cascades
----------------
A port carrying a *database's* uplink is upstream of every application that
queries that database, even though no application traverses the port itself.
:meth:`Topology.downstream_of` follows that second hop, because otherwise a
port fault would be attributed to the database it starves.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field

__all__ = [
    "ENTITY_KINDS",
    "DOMAINS",
    "RELATIONS",
    "DOMAIN_FOR_KIND",
    "TopologyError",
    "Edge",
    "EntityNode",
    "Topology",
    "TopologyBuilder",
    "domain_for_kind",
    "load_topology",
    "parse_topology",
]

#: Entity kinds, matching the closed enumeration contract C1 uses.
ENTITY_KINDS: frozenset[str] = frozenset({"app_server", "database", "switch", "node"})

#: Root-cause domains, matching contract C1.
DOMAINS: frozenset[str] = frozenset({"application", "database", "network"})

#: Dependency relations. ``queries`` and ``traverses`` are the two R3 names;
#: ``contains`` links a switch to its ports so a device fault reaches them.
RELATIONS: frozenset[str] = frozenset({"queries", "traverses", "contains"})

#: Which domain owns each entity kind. A bare node has no database or network
#: role, so an infrastructure host fault is an application-domain incident —
#: contract C1 offers exactly three domains and this is the honest mapping.
DOMAIN_FOR_KIND: Mapping[str, str] = {
    "app_server": "application",
    "database": "database",
    "switch": "network",
    "node": "application",
}


class TopologyError(ValueError):
    """The topology document is malformed or internally inconsistent."""


def domain_for_kind(kind: str) -> str:
    """Domain owning an entity kind.

    Args:
        kind: One of :data:`ENTITY_KINDS`.

    Returns:
        One of :data:`DOMAINS`.

    Raises:
        TopologyError: If ``kind`` is not a known entity kind.
    """
    try:
        return DOMAIN_FOR_KIND[kind]
    except KeyError:
        raise TopologyError(f"unknown entity kind {kind!r}") from None


@dataclass(frozen=True, slots=True, order=True)
class Edge:
    """A directed dependency edge.

    Direction is *dependent -> dependency*: ``app queries database`` means the
    application depends on the database, so a fault travels backwards along the
    arrow.

    Attributes:
        relation: One of :data:`RELATIONS`.
        source: The dependent entity.
        target: The entity depended upon.
    """

    relation: str
    source: str
    target: str

    def __post_init__(self) -> None:
        if self.relation not in RELATIONS:
            raise TopologyError(
                f"unknown relation {self.relation!r}; expected one of "
                f"{sorted(RELATIONS)}"
            )

    def describe(self) -> str:
        """A human-readable rendering for attribution evidence."""
        verb = {
            "queries": "queries",
            "traverses": "traverses",
            "contains": "contains",
        }[self.relation]
        return f"{self.source} {verb} {self.target}"


@dataclass(frozen=True, slots=True)
class EntityNode:
    """One entity in the topology.

    Attributes:
        entity_id: Its id, which must match the telemetry's ``instance`` label
            (or ``"<instance>:<ifName>"`` for a port).
        kind: One of :data:`ENTITY_KINDS`.
        attributes: Free-form CMDB attributes — site, service, pool ceilings.
            Consulted for evidence text and for physical limits; never required.
    """

    entity_id: str
    kind: str
    attributes: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.kind not in ENTITY_KINDS:
            raise TopologyError(
                f"{self.entity_id}: unknown kind {self.kind!r}; expected one of "
                f"{sorted(ENTITY_KINDS)}"
            )

    @property
    def domain(self) -> str:
        """The domain this entity's faults are attributed to."""
        return domain_for_kind(self.kind)

    def is_port(self) -> bool:
        """Whether this entity is a switch *port* rather than a whole device."""
        return self.kind == "switch" and ":" in self.entity_id


def _build_log_indices(
    entities: Mapping[str, "EntityNode"],
) -> tuple[
    dict[tuple[str, tuple[tuple[str, str], ...]], str],
    dict[tuple[str, str, str], str],
]:
    """Index each entity's ``log_resource`` block for the Cloud Logging join.

    Args:
        entities: The estate's entities, keyed by id.

    Returns:
        ``(full_identity_index, unambiguous_label_index)``. Label triples
        claimed by more than one entity are removed from the second index
        rather than resolved arbitrarily — ``project_id`` and ``zone`` are
        shared across the estate and must never resolve anything on their own.
    """
    full: dict[tuple[str, tuple[tuple[str, str], ...]], str] = {}
    label_owners: dict[tuple[str, str, str], set[str]] = {}
    for entity_id, node in entities.items():
        resource = node.attributes.get("log_resource")
        if not isinstance(resource, Mapping):
            continue
        resource_type = str(resource.get("type", ""))
        raw_labels = resource.get("labels")
        labels = (
            {str(k): str(v) for k, v in raw_labels.items()}
            if isinstance(raw_labels, Mapping)
            else {}
        )
        if not resource_type or not labels:
            continue
        full[(resource_type, tuple(sorted(labels.items())))] = entity_id
        for key, value in labels.items():
            label_owners.setdefault((resource_type, key, value), set()).add(entity_id)
    unambiguous = {
        triple: next(iter(owners))
        for triple, owners in label_owners.items()
        if len(owners) == 1
    }
    return full, unambiguous


class Topology:
    """A queryable estate graph.

    Immutable once built. Adjacency is indexed in both directions at
    construction time, so the attribution hot loop does no scanning.

    Args:
        entities: The estate's entities.
        edges: Dependency edges. Every endpoint must be a declared entity.
        source: Where this topology came from, recorded so a run can say
            whether it used the CMDB extract or the discovery fallback
            (ruling IR-04).

    Raises:
        TopologyError: On a duplicate entity id or an edge naming an entity
            that was never declared.
    """

    __slots__ = (
        "_entities",
        "_edges",
        "_out",
        "_in",
        "_source",
        "_downstream_cache",
        "_log_resource_index",
        "_log_label_index",
    )

    def __init__(
        self,
        entities: Iterable[EntityNode],
        edges: Iterable[Edge] = (),
        *,
        source: str = "unknown",
    ) -> None:
        table: dict[str, EntityNode] = {}
        for node in entities:
            if node.entity_id in table:
                raise TopologyError(f"duplicate entity {node.entity_id!r}")
            table[node.entity_id] = node

        edge_set = sorted(set(edges))
        for edge in edge_set:
            for endpoint in (edge.source, edge.target):
                if endpoint not in table:
                    raise TopologyError(
                        f"edge {edge.describe()!r} names undeclared entity "
                        f"{endpoint!r}"
                    )

        out: dict[str, list[Edge]] = {}
        into: dict[str, list[Edge]] = {}
        for edge in edge_set:
            out.setdefault(edge.source, []).append(edge)
            into.setdefault(edge.target, []).append(edge)

        self._entities = table
        self._edges = tuple(edge_set)
        self._out = {k: tuple(v) for k, v in out.items()}
        self._in = {k: tuple(v) for k, v in into.items()}
        self._source = source
        self._downstream_cache: dict[str, tuple[str, ...]] = {}
        self._log_resource_index, self._log_label_index = _build_log_indices(table)

    # -- basic accessors --------------------------------------------------

    def __len__(self) -> int:
        return len(self._entities)

    def __contains__(self, entity_id: object) -> bool:
        return entity_id in self._entities

    @property
    def source(self) -> str:
        """Provenance of this topology, for the run log."""
        return self._source

    def entities(self) -> tuple[EntityNode, ...]:
        """Every entity, sorted by id."""
        return tuple(self._entities[key] for key in sorted(self._entities))

    def edges(self) -> tuple[Edge, ...]:
        """Every edge, sorted."""
        return self._edges

    def node(self, entity_id: str) -> EntityNode | None:
        """The entity, or ``None`` if unknown."""
        return self._entities.get(entity_id)

    def kind(self, entity_id: str) -> str:
        """Entity kind, or ``"unknown"``."""
        node = self._entities.get(entity_id)
        return node.kind if node is not None else "unknown"

    def entity_kinds(self) -> dict[str, str]:
        """``entity_id -> kind`` for the whole estate."""
        return {key: node.kind for key, node in sorted(self._entities.items())}

    def domain(self, entity_id: str) -> str | None:
        """Domain owning ``entity_id``, or ``None`` if unknown."""
        node = self._entities.get(entity_id)
        return None if node is None else node.domain

    def of_kind(self, kind: str) -> tuple[str, ...]:
        """Ids of every entity of ``kind``, sorted."""
        return tuple(
            key for key in sorted(self._entities) if self._entities[key].kind == kind
        )

    # -- Cloud Logging identity join --------------------------------------

    def log_resource_index(self) -> dict[tuple[str, tuple[tuple[str, str], ...]], str]:
        """Full monitored-resource identity to entity id.

        Cloud Logging entries identify a host by its monitored resource — for
        GCE, ``{"type": "gce_instance", "labels": {"instance_id": ...}}`` —
        and that numeric id is not the C1 ``entity_id``. The CMDB is the only
        legitimate place to resolve one to the other, so ``topology.json``
        carries a ``log_resource`` block per entity and this is the index over
        it.

        Deriving the id instead — reproducing however the generator minted it
        — would mean copying ``scenariogen`` logic into the product, which is
        the requirement-R5 separation violation wearing a different hat. Look
        it up or report it unresolved; never compute it.

        Returns:
            ``(resource_type, sorted label items) -> entity_id``.
        """
        return dict(self._log_resource_index)

    def log_label_index(self) -> dict[tuple[str, str, str], str]:
        """Single identifying label to entity id, ambiguity removed.

        Real log entries do not always carry every label the CMDB records, so
        a full-identity match is too strict on its own. This index maps one
        ``(resource_type, label_key, label_value)`` triple to an entity — but
        **only where that triple names exactly one entity**. Triples that
        match more than one entity are dropped rather than resolved
        arbitrarily, so a shared ``project_id`` or ``zone`` can never silently
        attach telemetry to the wrong host.

        Returns:
            ``(resource_type, label_key, label_value) -> entity_id``.
        """
        return dict(self._log_label_index)

    def entity_for_log_resource(
        self, resource_type: str, labels: Mapping[str, str]
    ) -> str | None:
        """Resolve a Cloud Logging monitored resource to a C1 entity id.

        Tries the full identity first, then any single unambiguous label.

        Args:
            resource_type: The monitored resource type, e.g.
                ``"gce_instance"``.
            labels: The resource labels from the log entry.

        Returns:
            The entity id, or ``None`` when the CMDB does not know this
            resource. Callers must **count** the ``None`` results and surface
            them as a coverage number — a broken identity join has to be
            visible as a metric, not as mysteriously poor detection.
        """
        key = (resource_type, tuple(sorted((str(k), str(v)) for k, v in labels.items())))
        found = self._log_resource_index.get(key)
        if found is not None:
            return found
        for label_key, label_value in sorted(labels.items()):
            found = self._log_label_index.get(
                (resource_type, str(label_key), str(label_value))
            )
            if found is not None:
                return found
        return None

    # -- adjacency --------------------------------------------------------

    def dependencies(self, entity_id: str, relation: str | None = None) -> tuple[str, ...]:
        """Entities ``entity_id`` depends on, optionally filtered by relation."""
        return tuple(
            sorted(
                edge.target
                for edge in self._out.get(entity_id, ())
                if relation is None or edge.relation == relation
            )
        )

    def dependents(self, entity_id: str, relation: str | None = None) -> tuple[str, ...]:
        """Entities that depend on ``entity_id``, optionally filtered."""
        return tuple(
            sorted(
                edge.source
                for edge in self._in.get(entity_id, ())
                if relation is None or edge.relation == relation
            )
        )

    def databases_for(self, app_server: str) -> tuple[str, ...]:
        """Databases an application server queries (feature #51)."""
        return self.dependencies(app_server, "queries")

    def apps_for_database(self, database: str) -> tuple[str, ...]:
        """Application servers that query a database — its blast radius."""
        return self.dependents(database, "queries")

    def uplink_ports(self, entity_id: str) -> tuple[str, ...]:
        """Switch ports an entity's traffic traverses (feature #52)."""
        return self.dependencies(entity_id, "traverses")

    def switch_of(self, port_id: str) -> str | None:
        """The device a port belongs to.

        Prefers a declared ``contains`` edge and falls back to the composite id
        convention, so a topology that omits the edge still resolves.
        """
        for edge in self._in.get(port_id, ()):
            if edge.relation == "contains":
                return edge.source
        if ":" in port_id:
            candidate = port_id.split(":", 1)[0]
            if candidate in self._entities:
                return candidate
        return None

    def ports_of(self, switch_id: str) -> tuple[str, ...]:
        """Ports belonging to a switch."""
        return self.dependencies(switch_id, "contains")

    # -- reachability -----------------------------------------------------

    def downstream_of(self, entity_id: str) -> tuple[str, ...]:
        """Every entity degraded if ``entity_id`` fails, excluding itself.

        Traversal runs backwards along dependency edges and is transitive, so
        the two-hop cascade "port carries the database uplink, therefore the
        applications that query that database are affected" is included. That
        second hop is exactly the case a one-hop model attributes to the
        database instead of the port.

        Args:
            entity_id: The hypothesised faulty entity.

        Returns:
            Affected entity ids, sorted. Empty for an unknown entity.
        """
        cached = self._downstream_cache.get(entity_id)
        if cached is not None:
            return cached
        if entity_id not in self._entities:
            return ()
        seen: set[str] = set()
        frontier = [entity_id]
        while frontier:
            current = frontier.pop()
            # Only ``queries`` and ``traverses`` mean "depends on", so only
            # those are followed backwards. ``contains`` points device->port
            # and is followed forwards instead: a device fault reaches its
            # ports, but a port fault does not reach its device or its
            # siblings. Following ``contains`` backwards would make every port
            # incident collapse into its whole switch.
            neighbours = set(self.dependents(current, "queries"))
            neighbours |= set(self.dependents(current, "traverses"))
            neighbours |= set(self.ports_of(current))
            for neighbour in neighbours:
                if neighbour not in seen and neighbour != entity_id:
                    seen.add(neighbour)
                    frontier.append(neighbour)
        result = tuple(sorted(seen))
        self._downstream_cache[entity_id] = result
        return result

    def upstream_candidates(self, entity_id: str) -> tuple[str, ...]:
        """Entities whose failure could explain a degradation of ``entity_id``.

        The candidate set attribution competes over. Includes ``entity_id``
        itself, because "the application really is the problem" (feature #63)
        must be reachable — otherwise the model can never attribute anything to
        the application domain and AC-09 becomes unwinnable by construction.

        Walks dependency edges forwards, and ascends from a port to its device
        because a device fault manifests on its ports. It deliberately does not
        then descend to that device's *other* ports: they carry different
        traffic and are not on this entity's path.

        Args:
            entity_id: The distressed entity.

        Returns:
            Candidate cause ids, sorted, including ``entity_id``.
        """
        if entity_id not in self._entities:
            return (entity_id,)
        found: set[str] = {entity_id}
        frontier = [entity_id]
        while frontier:
            current = frontier.pop()
            neighbours = set(self.dependencies(current, "queries"))
            neighbours |= set(self.dependencies(current, "traverses"))
            node = self._entities.get(current)
            if node is not None and node.is_port():
                switch = self.switch_of(current)
                if switch is not None:
                    neighbours.add(switch)
            for neighbour in neighbours:
                if neighbour not in found:
                    found.add(neighbour)
                    frontier.append(neighbour)
        return tuple(sorted(found))

    def explain_path(self, dependent: str, dependency: str) -> tuple[Edge, ...]:
        """Shortest dependency path from ``dependent`` to ``dependency``.

        The correlation evidence feature #67 requires: the actual chain of
        declared relations that makes the cause reachable from the symptom.
        Breadth-first, so the returned path is the shortest and therefore the
        most plausible explanation.

        Args:
            dependent: The symptomatic entity.
            dependency: The hypothesised cause.

        Returns:
            Edges from ``dependent`` to ``dependency``. Empty when they are the
            same entity or no path exists.
        """
        if dependent == dependency or dependent not in self._entities:
            return ()
        # Predecessor node is stored alongside the edge rather than inferred
        # from it: a ``contains`` edge is walked switch->port when descending
        # into a device and port->switch when ascending out of one, so the edge
        # alone cannot say which way it was used.
        previous: dict[str, tuple[str, Edge]] = {}
        seen = {dependent}
        queue = [dependent]
        while queue:
            current = queue.pop(0)
            steps: list[tuple[str, Edge]] = [
                (edge.target, edge) for edge in self._out.get(current, ())
            ]
            switch = self.switch_of(current)
            if switch is not None and switch != current:
                steps.append((switch, Edge("contains", switch, current)))
            for nxt, edge in steps:
                if nxt in seen:
                    continue
                seen.add(nxt)
                previous[nxt] = (current, edge)
                if nxt == dependency:
                    path: list[Edge] = []
                    cursor = dependency
                    while cursor != dependent:
                        back, edge_back = previous[cursor]
                        path.append(edge_back)
                        cursor = back
                    path.reverse()
                    return tuple(path)
                queue.append(nxt)
        return ()

    def shares_dependency(self, left: str, right: str) -> tuple[str, ...]:
        """Common dependencies of two entities — the fan-in a fault exploits."""
        return tuple(sorted(set(self.dependencies(left)) & set(self.dependencies(right))))

    # -- serialisation ----------------------------------------------------

    def to_dict(self) -> dict[str, object]:
        """A machine-readable dump.

        Feature #50 requires the topology be inspectable, so a run writes this
        beside its predictions and a reviewer can check that the adjacency
        attribution claims to have traversed is really there.
        """
        return {
            "source": self._source,
            "entities": [
                {
                    "entity_id": node.entity_id,
                    "kind": node.kind,
                    **({"attributes": dict(node.attributes)} if node.attributes else {}),
                }
                for node in self.entities()
            ],
            "edges": [[edge.relation, edge.source, edge.target] for edge in self._edges],
        }

    def to_json(self, indent: int | None = 2) -> str:
        """Canonical JSON rendering of :meth:`to_dict`."""
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True)


class TopologyBuilder:
    """Incremental construction of a :class:`Topology`.

    Used by the discovery fallback and by tests. Adding an edge implicitly
    declares its endpoints when their kind can be inferred, so a caller can
    describe a graph without listing every node twice.
    """

    def __init__(self, *, source: str = "built") -> None:
        """Start an empty builder.

        Args:
            source: Provenance string recorded on the finished topology.
        """
        self._entities: dict[str, EntityNode] = {}
        self._edges: set[Edge] = set()
        self._source = source

    def add_entity(
        self, entity_id: str, kind: str, **attributes: object
    ) -> "TopologyBuilder":
        """Declare an entity, replacing any previous declaration.

        Args:
            entity_id: Its id.
            kind: One of :data:`ENTITY_KINDS`.
            **attributes: Free-form CMDB attributes.

        Returns:
            ``self``, for chaining.
        """
        self._entities[entity_id] = EntityNode(entity_id, kind, dict(attributes))
        return self

    def add_edge(self, relation: str, source: str, target: str) -> "TopologyBuilder":
        """Declare a dependency edge.

        Args:
            relation: One of :data:`RELATIONS`.
            source: The dependent entity.
            target: The entity depended upon.

        Returns:
            ``self``, for chaining.
        """
        self._edges.add(Edge(relation, source, target))
        return self

    def queries(self, app_server: str, database: str) -> "TopologyBuilder":
        """Record that ``app_server`` queries ``database`` (feature #51)."""
        return self.add_edge("queries", app_server, database)

    def traverses(self, entity_id: str, port_id: str) -> "TopologyBuilder":
        """Record that ``entity_id``'s traffic crosses ``port_id`` (feature #52)."""
        return self.add_edge("traverses", entity_id, port_id)

    def port(self, switch_id: str, if_name: str, **attributes: object) -> str:
        """Declare a port on a switch and return its composite entity id.

        Args:
            switch_id: The device.
            if_name: The interface name as the exporter reports it.
            **attributes: Free-form port attributes.

        Returns:
            The port's entity id, ``"<switch_id>:<if_name>"``.
        """
        port_id = f"{switch_id}:{if_name}"
        self.add_entity(port_id, "switch", ifName=if_name, **attributes)
        self.add_edge("contains", switch_id, port_id)
        return port_id

    def build(self) -> Topology:
        """Finish, validating that every edge endpoint is declared.

        Returns:
            The immutable topology.

        Raises:
            TopologyError: If an edge names an undeclared entity.
        """
        return Topology(
            self._entities.values(), self._edges, source=self._source
        )


# --------------------------------------------------------------------------
# Loading the CMDB extract
# --------------------------------------------------------------------------


def _as_mapping(obj: object, where: str) -> Mapping[str, object]:
    """Require a JSON object."""
    if not isinstance(obj, Mapping):
        raise TopologyError(f"{where}: expected an object, got {type(obj).__name__}")
    return obj


def _as_sequence(obj: object, where: str) -> Sequence[object]:
    """Require a JSON array."""
    if isinstance(obj, (str, bytes)) or not isinstance(obj, Sequence):
        raise TopologyError(f"{where}: expected an array, got {type(obj).__name__}")
    return obj


def parse_topology(payload: Mapping[str, object], *, source: str = "topology.json") -> Topology:
    """Build a topology from a CMDB document.

    Accepts two shapes, because the generator writes the richer one and the
    discovery fallback and tests write the leaner one:

    * **Generic** — ``{"entities": [{"entity_id", "kind"}],
      "edges": [[relation, source, target]]}``.
    * **Estate** — the generator's dump, which additionally carries
      ``app_servers`` (with ``databases`` and ``uplink_port``), ``databases``
      (with ``uplink_port``), ``switches`` (with nested ``ports``) and
      ``nodes``. Those blocks are read for entity kinds, per-entity attributes
      such as ``fd_limit`` and ``max_connections``, and the port ownership
      edges the generic ``edges`` list does not carry.

    Both shapes may appear in one document; the estate blocks are additive.

    Args:
        payload: The parsed JSON document.
        source: Provenance recorded on the topology.

    Returns:
        The topology.

    Raises:
        TopologyError: If the document is malformed or an edge dangles.
    """
    builder = TopologyBuilder(source=source)

    for raw in _as_sequence(payload.get("entities", []), "entities"):
        item = _as_mapping(raw, "entities[]")
        entity_id = item.get("entity_id")
        kind = item.get("kind")
        if not isinstance(entity_id, str) or not isinstance(kind, str):
            raise TopologyError("entities[]: entity_id and kind must be strings")
        attributes = item.get("attributes")
        # Carry every other key through as an attribute. The estate-shaped
        # blocks below already do this, and the generic block must too:
        # ``log_resource`` arrives as a sibling of ``entity_id``, not nested
        # under ``attributes``, and silently dropping it would break the
        # Cloud Logging identity join.
        extra = {
            key: value
            for key, value in item.items()
            if key not in ("entity_id", "kind", "attributes")
        }
        if attributes:
            extra.update(dict(_as_mapping(attributes, "entities[].attributes")))
        builder.add_entity(entity_id, kind, **extra)

    # Estate-shaped blocks. Declaring entities before edges means the richer
    # per-entity attributes win over a bare generic declaration.
    for raw in _as_sequence(payload.get("switches", []), "switches"):
        switch = _as_mapping(raw, "switches[]")
        switch_id = str(switch["entity_id"])
        builder.add_entity(
            switch_id,
            "switch",
            **{k: v for k, v in switch.items() if k not in ("entity_id", "ports")},
        )
        for raw_port in _as_sequence(switch.get("ports", []), "switches[].ports"):
            port = _as_mapping(raw_port, "switches[].ports[]")
            port_id = str(
                port.get("entity_id") or f"{switch_id}:{port.get('ifName', '')}"
            )
            builder.add_entity(
                port_id,
                "switch",
                **{k: v for k, v in port.items() if k != "entity_id"},
            )
            builder.add_edge("contains", switch_id, port_id)

    for raw in _as_sequence(payload.get("databases", []), "databases"):
        database = _as_mapping(raw, "databases[]")
        database_id = str(database["entity_id"])
        builder.add_entity(
            database_id,
            "database",
            **{k: v for k, v in database.items() if k != "entity_id"},
        )
        uplink = database.get("uplink_port")
        if isinstance(uplink, str) and uplink:
            builder.traverses(database_id, uplink)

    for raw in _as_sequence(payload.get("app_servers", []), "app_servers"):
        server = _as_mapping(raw, "app_servers[]")
        server_id = str(server["entity_id"])
        builder.add_entity(
            server_id,
            "app_server",
            **{k: v for k, v in server.items() if k != "entity_id"},
        )
        for database_id in _as_sequence(
            server.get("databases", []), "app_servers[].databases"
        ):
            builder.queries(server_id, str(database_id))
        uplink = server.get("uplink_port")
        if isinstance(uplink, str) and uplink:
            builder.traverses(server_id, uplink)

    for raw in _as_sequence(payload.get("nodes", []), "nodes"):
        node = _as_mapping(raw, "nodes[]")
        node_id = str(node["entity_id"])
        builder.add_entity(
            node_id, "node", **{k: v for k, v in node.items() if k != "entity_id"}
        )
        uplink = node.get("uplink_port")
        if isinstance(uplink, str) and uplink:
            builder.traverses(node_id, uplink)

    for raw in _as_sequence(payload.get("edges", []), "edges"):
        edge = _as_sequence(raw, "edges[]")
        if len(edge) != 3:
            raise TopologyError(
                f"edges[]: expected [relation, source, target], got {list(edge)!r}"
            )
        builder.add_edge(str(edge[0]), str(edge[1]), str(edge[2]))

    return builder.build()


def load_topology(path: str | os.PathLike[str]) -> Topology:
    """Read and parse a topology document from disk.

    Args:
        path: Path to ``topology.json``.

    Returns:
        The topology, with ``source`` set to the file path.

    Raises:
        TopologyError: If the file is absent or malformed.
    """
    text_path = os.fspath(path)
    if not os.path.exists(text_path):
        raise TopologyError(f"no topology document at {text_path}")
    try:
        with open(text_path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except json.JSONDecodeError as error:
        raise TopologyError(f"{text_path}: invalid JSON: {error}") from error
    return parse_topology(_as_mapping(payload, text_path), source=text_path)
