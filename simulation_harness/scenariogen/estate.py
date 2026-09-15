"""The synthetic GSK estate: entities, topology and their identities.

This module is pure description. It knows nothing about faults, nothing about
time, and — per requirement R5 — nothing whatsoever about detectors.

What it models
--------------
A representative slice of the estate declared in ``RATIFIED_DEFINITIONS.md``
RD-09, small enough to generate and replay in minutes but wired so that R3's
attribution problem is genuinely hard:

* **app servers** — Apache httpd reverse-proxying a Tomcat JVM on one host.
* **database instances** — MySQL hosts.
* **switches**, each with several **ports**. Ports are first-class entities
  because R3 requires attribution to name *"the specific responsible entity
  (this DB instance, this port)"*, not merely a device.
* **nodes** — shared infrastructure (a batch host, a load balancer host).

The topology is what makes downstream collapse measurable: several app servers
depend on the same database, and several entities' traffic traverses the same
switch port. A single upstream fault must therefore visibly degrade multiple
downstream entities — otherwise the collapse ratio (AC-13) has nothing to
collapse.

Entity kinds are constrained by contract C1 to
``{app_server, database, switch, node}``. A switch **port** is declared with
kind ``switch`` and a composite id (``gsk-sw-lon-core-01:Te0/0/0/3``) so it can
be named as a root cause while still satisfying the closed enum.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field

from ano.contracts.determinism import stable_hash_int

__all__ = [
    "Site",
    "Port",
    "Switch",
    "Database",
    "AppServer",
    "Node",
    "Estate",
    "build_estate",
    "PROJECT_ID",
    "ZONE_FOR_SITE",
    "CLUSTER",
    "instance_id_for",
    "gce_log_resource",
    "switch_log_resource",
    "container_log_resource",
]

#: The demo project. Synthetic: no such project exists and none is contacted.
PROJECT_ID = "gsk-ano-demo"

#: Sites and the GCP zone/region their cloud-hosted tier sits in.
ZONE_FOR_SITE: Mapping[str, str] = {
    "uk-london": "europe-west2-a",
    "uk-stevenage": "europe-west2-b",
}


#: Cluster name used by the ``k8s_container`` monitored resource. Declared
#: here rather than in the emitters so the topology dump and the log stream
#: cannot drift apart.
CLUSTER = "gsk-ano-uk"


def _instance_id(name: str) -> str:
    """A stable, plausible numeric Compute Engine instance id.

    Derived with ``stable_hash_int`` rather than builtin ``hash()``: the latter
    is salted per process and would make the generated corpus differ between
    runs at the same seed.
    """
    # 19 digits is the shape real GCE instance ids have. ``stable_hash_int``
    # only accepts byte-aligned widths, so take 64 bits and fold them into the
    # 19-digit range.
    digest = stable_hash_int(name, "estate", "instance-id", bits=64)
    return str(1_000_000_000_000_000_000 + digest % 1_000_000_000_000_000_000)


# -- monitored-resource identities ---------------------------------------
#
# A Cloud Logging entry never carries our ``entity_id``; it carries the
# identity the *platform* knows the machine by. For a VM that is a numeric
# instance id, which is faithful but opaque. These three builders are the
# single definition of that identity, and ``Estate.to_dict`` publishes their
# output into ``topology.json`` so a consumer can perform the join without
# importing this package. Emitter and topology call the same function, so the
# published key is the emitted key by construction rather than by agreement.


def gce_log_resource(entity_id: str, site: str) -> dict[str, object]:
    """The ``gce_instance`` identity a VM-hosted entity logs under."""
    return {
        "type": "gce_instance",
        "labels": {
            "instance_id": _instance_id(entity_id),
            "project_id": PROJECT_ID,
            "zone": ZONE_FOR_SITE[site],
        },
    }


def switch_log_resource(entity_id: str, site: str) -> dict[str, object]:
    """The ``generic_node`` identity a switch logs under.

    A switch is not a VM and has no instance id; ``generic_node`` is the
    descriptor Cloud Logging provides for exactly this case.
    """
    return {
        "type": "generic_node",
        "labels": {
            "location": ZONE_FOR_SITE[site],
            "namespace": site,
            "node_id": entity_id,
            "project_id": PROJECT_ID,
        },
    }


def container_log_resource(site: str, container: str) -> dict[str, object]:
    """The ``k8s_container`` identity a batch workload logs under."""
    return {
        "type": "k8s_container",
        "labels": {
            "cluster_name": CLUSTER,
            "container_name": container,
            "location": ZONE_FOR_SITE[site],
            "namespace_name": "batch",
            "pod_name": f"{container}-0",
            "project_id": PROJECT_ID,
        },
    }


@dataclass(frozen=True, slots=True)
class Site:
    """A physical site."""

    name: str

    @property
    def zone(self) -> str:
        return ZONE_FOR_SITE[self.name]


@dataclass(frozen=True, slots=True)
class Port:
    """One switch interface."""

    switch_id: str
    if_index: int
    if_name: str
    if_descr: str
    if_alias: str
    speed_mbit: int

    @property
    def entity_id(self) -> str:
        return f"{self.switch_id}:{self.if_name}"

    def labels(self) -> dict[str, str]:
        """The snmp_exporter ``if_mib`` label set for this interface."""
        return {
            "ifAlias": self.if_alias,
            "ifDescr": self.if_descr,
            "ifIndex": str(self.if_index),
            "ifName": self.if_name,
        }


@dataclass(frozen=True, slots=True)
class Switch:
    """A network switch and its ports."""

    entity_id: str
    site: str
    ports: tuple[Port, ...]
    ospf_neighbour_ip: str
    ospf_router_id: str

    kind = "switch"

    def port(self, entity_id: str) -> Port:
        for port in self.ports:
            if port.entity_id == entity_id:
                return port
        raise KeyError(entity_id)


@dataclass(frozen=True, slots=True)
class Database:
    """A MySQL instance on a dedicated host."""

    entity_id: str
    site: str
    schema: str
    max_connections: int
    #: The switch port this host's traffic traverses.
    uplink_port: str

    kind = "database"


@dataclass(frozen=True, slots=True)
class AppServer:
    """An Apache + Tomcat application server."""

    entity_id: str
    site: str
    service: str
    #: Databases this server queries, in query-weight order.
    databases: tuple[str, ...]
    #: The switch port this server's traffic traverses.
    uplink_port: str
    thread_pool_size: int
    heap_max_bytes: int
    fd_limit: int
    http_port: str = "8080"
    protocol: str = "http-nio"

    kind = "app_server"


@dataclass(frozen=True, slots=True)
class Node:
    """Shared infrastructure host with no application role."""

    entity_id: str
    site: str
    role: str
    uplink_port: str
    #: Container workloads this node runs, as ``(name, cpu_share)``. Declared
    #: here rather than inside the simulator because each container is a
    #: distinct Cloud Logging identity that ``topology.json`` has to publish;
    #: keeping the roster in one place stops the simulator, the log emitter
    #: and the topology dump disagreeing about which workloads exist.
    containers: tuple[tuple[str, float], ...] = ()

    kind = "node"


@dataclass(frozen=True, slots=True)
class Estate:
    """The whole modelled estate, plus derived adjacency."""

    app_servers: tuple[AppServer, ...]
    databases: tuple[Database, ...]
    switches: tuple[Switch, ...]
    nodes: tuple[Node, ...]

    # -- lookups ---------------------------------------------------------
    def entity_kinds(self) -> dict[str, str]:
        """``entity_id -> kind`` for every declared entity, ports included."""
        kinds: dict[str, str] = {}
        for server in self.app_servers:
            kinds[server.entity_id] = "app_server"
        for database in self.databases:
            kinds[database.entity_id] = "database"
        for switch in self.switches:
            kinds[switch.entity_id] = "switch"
            for port in switch.ports:
                kinds[port.entity_id] = "switch"
        for node in self.nodes:
            kinds[node.entity_id] = "node"
        return kinds

    def hosts(self) -> tuple[object, ...]:
        """Every entity that runs a node_exporter, in a stable order."""
        return self.app_servers + self.databases + self.nodes

    def log_identity(self, entity_id: str) -> dict[str, object] | None:
        """The monitored resource this entity's own log entries carry.

        ``None`` for switch ports: a port has counters but no syslog of its
        own, so nothing in the log stream ever names one. Publishing a
        fabricated identity for it would create a join key that can never be
        exercised, which is worse than an honest absence.
        """
        for server in self.app_servers:
            if server.entity_id == entity_id:
                return gce_log_resource(entity_id, server.site)
        for database in self.databases:
            if database.entity_id == entity_id:
                return gce_log_resource(entity_id, database.site)
        for node in self.nodes:
            if node.entity_id == entity_id:
                return gce_log_resource(entity_id, node.site)
        for switch in self.switches:
            if switch.entity_id == entity_id:
                return switch_log_resource(entity_id, switch.site)
        return None

    def hosted_log_identities(self, entity_id: str) -> list[dict[str, object]]:
        """Extra monitored resources used by workloads running *on* an entity.

        A container logs under its own ``k8s_container`` descriptor, not under
        the host's ``gce_instance`` one, so a host genuinely has more than one
        logging identity. Attribution still lands on the host, which is why
        these are published against it rather than as separate entities.
        """
        for node in self.nodes:
            if node.entity_id == entity_id:
                return [
                    container_log_resource(node.site, name)
                    for name, _share in node.containers
                ]
        return []

    def all_ports(self) -> tuple[Port, ...]:
        return tuple(port for switch in self.switches for port in switch.ports)

    def port(self, entity_id: str) -> Port:
        for switch in self.switches:
            for candidate in switch.ports:
                if candidate.entity_id == entity_id:
                    return candidate
        raise KeyError(entity_id)

    def switch_of_port(self, port_entity_id: str) -> Switch:
        switch_id = port_entity_id.split(":", 1)[0]
        for switch in self.switches:
            if switch.entity_id == switch_id:
                return switch
        raise KeyError(port_entity_id)

    def app_server(self, entity_id: str) -> AppServer:
        for server in self.app_servers:
            if server.entity_id == entity_id:
                return server
        raise KeyError(entity_id)

    def database(self, entity_id: str) -> Database:
        for database in self.databases:
            if database.entity_id == entity_id:
                return database
        raise KeyError(entity_id)

    # -- topology --------------------------------------------------------
    def app_servers_using_database(self, database_id: str) -> tuple[str, ...]:
        """Downstream fan-out of a database fault."""
        return tuple(
            server.entity_id
            for server in self.app_servers
            if database_id in server.databases
        )

    def entities_using_port(self, port_entity_id: str) -> tuple[str, ...]:
        """Downstream fan-out of a switch-port fault.

        Includes app servers whose *database* sits behind the port, because a
        port fault upstream of a database degrades the apps that query it —
        a two-hop cascade the attribution layer has to unpick.
        """
        direct = [
            entity.entity_id
            for entity in self.hosts()
            if getattr(entity, "uplink_port", None) == port_entity_id
        ]
        indirect: list[str] = []
        for database in self.databases:
            if database.uplink_port == port_entity_id:
                indirect.extend(self.app_servers_using_database(database.entity_id))
        # Sorted + de-duplicated: a set's iteration order is stable in CPython
        # but sorting makes determinism explicit rather than incidental.
        return tuple(sorted(set(direct) | set(indirect)))

    def _entity_block(self, entity_id: str, kind: str) -> dict[str, object]:
        """One ``entities[]`` row of the topology dump."""
        block: dict[str, object] = {"entity_id": entity_id, "kind": kind}
        identity = self.log_identity(entity_id)
        if identity is not None:
            block["log_resource"] = identity
        hosted = self.hosted_log_identities(entity_id)
        if hosted:
            block["hosted_log_resources"] = hosted
        return block

    def to_dict(self) -> dict[str, object]:
        """A machine-readable topology dump.

        Written beside the telemetry so the correlation layer has an explicit
        topology to load, and so a reviewer can inspect the adjacency that
        attribution claims are traversing (feature #50).

        Each entity that appears in the log stream also carries the monitored
        resource it logs under (``log_resource``, plus ``hosted_log_resources``
        for container workloads). Cloud Logging identifies a VM by numeric
        instance id and never by our ``entity_id``, while metrics resolve by
        ``instance`` == ``entity_id``; without this block published here the
        two streams describe the same machine under two different keys and
        nothing joins. It carries identity only — no genuineness flag, no root
        cause, no incident kind — so IR-03 is untouched.
        """
        return {
            "project_id": PROJECT_ID,
            "sites": sorted({entity.site for entity in self.hosts()}),
            "entities": [
                self._entity_block(entity_id, kind)
                for entity_id, kind in sorted(self.entity_kinds().items())
            ],
            "app_servers": [
                {
                    "entity_id": server.entity_id,
                    "site": server.site,
                    "service": server.service,
                    "databases": list(server.databases),
                    "uplink_port": server.uplink_port,
                    "thread_pool_size": server.thread_pool_size,
                    "heap_max_bytes": server.heap_max_bytes,
                    "fd_limit": server.fd_limit,
                }
                for server in self.app_servers
            ],
            "databases": [
                {
                    "entity_id": database.entity_id,
                    "site": database.site,
                    "schema": database.schema,
                    "max_connections": database.max_connections,
                    "uplink_port": database.uplink_port,
                }
                for database in self.databases
            ],
            "switches": [
                {
                    "entity_id": switch.entity_id,
                    "site": switch.site,
                    "ospf_neighbour": {
                        "ospfNbrIpAddr": switch.ospf_neighbour_ip,
                        "ospfNbrRtrId": switch.ospf_router_id,
                    },
                    "ports": [
                        {
                            "entity_id": port.entity_id,
                            "ifIndex": port.if_index,
                            "ifName": port.if_name,
                            "ifDescr": port.if_descr,
                            "ifAlias": port.if_alias,
                            "speed_mbit": port.speed_mbit,
                        }
                        for port in switch.ports
                    ],
                }
                for switch in self.switches
            ],
            "nodes": [
                {
                    "entity_id": node.entity_id,
                    "site": node.site,
                    "role": node.role,
                    "uplink_port": node.uplink_port,
                }
                for node in self.nodes
            ],
            "edges": sorted(
                [["queries", server.entity_id, database]
                 for server in self.app_servers
                 for database in server.databases]
                + [["traverses", entity.entity_id, entity.uplink_port]
                   for entity in self.hosts()]
            ),
        }


def _ports(switch_id: str, specs: Sequence[tuple[int, str, str, str, int]]) -> tuple[Port, ...]:
    return tuple(
        Port(switch_id, index, name, descr, alias, speed)
        for index, name, descr, alias, speed in specs
    )


def build_estate() -> Estate:
    """Construct the fixed estate.

    Deliberately **not** seeded: the estate is the same in every scenario, so
    that scores across seeds compare like with like and so that the topology
    file is stable. The seed varies the *events*, not the *estate*.
    """
    core = Switch(
        entity_id="gsk-sw-lon-core-01",
        site="uk-london",
        ospf_neighbour_ip="10.42.255.1",
        ospf_router_id="10.42.0.1",
        ports=_ports(
            "gsk-sw-lon-core-01",
            [
                (501, "Te0/0/0/1", "TenGigE0/0/0/1", "app-tier-lon-a", 10000),
                (502, "Te0/0/0/2", "TenGigE0/0/0/2", "app-tier-lon-b", 10000),
                (503, "Te0/0/0/3", "TenGigE0/0/0/3", "db-tier-lon", 10000),
            ],
        ),
    )
    access = Switch(
        entity_id="gsk-sw-ste-acc-01",
        site="uk-stevenage",
        ospf_neighbour_ip="10.44.255.1",
        ospf_router_id="10.44.0.1",
        ports=_ports(
            "gsk-sw-ste-acc-01",
            [
                (301, "Gi1/0/11", "GigabitEthernet1/0/11", "app-tier-ste", 1000),
                (302, "Gi1/0/12", "GigabitEthernet1/0/12", "infra-tier-ste", 1000),
            ],
        ),
    )

    databases = (
        Database(
            entity_id="gsk-db-lon-my-01",
            site="uk-london",
            schema="clinsupply",
            max_connections=500,
            uplink_port="gsk-sw-lon-core-01:Te0/0/0/3",
        ),
        Database(
            entity_id="gsk-db-lon-my-02",
            site="uk-london",
            schema="labresults",
            max_connections=500,
            uplink_port="gsk-sw-lon-core-01:Te0/0/0/3",
        ),
        Database(
            entity_id="gsk-db-ste-my-01",
            site="uk-stevenage",
            schema="siteops",
            max_connections=300,
            uplink_port="gsk-sw-ste-acc-01:Gi1/0/12",
        ),
    )

    # Fan-in is deliberate: gsk-db-lon-my-01 serves three app servers, so a
    # single database fault degrades three downstream entities and the
    # collapse ratio has something real to measure.
    app_servers = (
        AppServer(
            entity_id="gsk-app-lon-tc-01",
            site="uk-london",
            service="clinical-supply-portal",
            databases=("gsk-db-lon-my-01",),
            uplink_port="gsk-sw-lon-core-01:Te0/0/0/1",
            thread_pool_size=200,
            heap_max_bytes=8 * 1024**3,
            fd_limit=65536,
        ),
        AppServer(
            entity_id="gsk-app-lon-tc-02",
            site="uk-london",
            service="clinical-supply-portal",
            databases=("gsk-db-lon-my-01",),
            uplink_port="gsk-sw-lon-core-01:Te0/0/0/1",
            thread_pool_size=200,
            heap_max_bytes=8 * 1024**3,
            fd_limit=65536,
        ),
        AppServer(
            entity_id="gsk-app-lon-tc-03",
            site="uk-london",
            service="lab-results-gateway",
            databases=("gsk-db-lon-my-02", "gsk-db-lon-my-01"),
            uplink_port="gsk-sw-lon-core-01:Te0/0/0/2",
            thread_pool_size=150,
            heap_max_bytes=6 * 1024**3,
            fd_limit=32768,
        ),
        AppServer(
            entity_id="gsk-app-ste-tc-01",
            site="uk-stevenage",
            service="site-operations",
            databases=("gsk-db-ste-my-01",),
            uplink_port="gsk-sw-ste-acc-01:Gi1/0/11",
            thread_pool_size=120,
            heap_max_bytes=4 * 1024**3,
            fd_limit=16384,
        ),
        AppServer(
            entity_id="gsk-app-ste-tc-02",
            site="uk-stevenage",
            service="site-operations",
            databases=("gsk-db-ste-my-01",),
            uplink_port="gsk-sw-ste-acc-01:Gi1/0/11",
            thread_pool_size=120,
            heap_max_bytes=4 * 1024**3,
            fd_limit=16384,
        ),
    )

    nodes = (
        Node("gsk-node-lon-bat-01", "uk-london", "batch",
             "gsk-sw-lon-core-01:Te0/0/0/2",
             containers=(("etl-worker", 0.62), ("report-renderer", 0.38))),
        Node("gsk-node-ste-inf-01", "uk-stevenage", "infra",
             "gsk-sw-ste-acc-01:Gi1/0/12"),
    )

    return Estate(app_servers, databases, (core, access), nodes)


def instance_id_for(entity_id: str) -> str:
    """Stable numeric Compute Engine instance id for a host entity."""
    return _instance_id(entity_id)
