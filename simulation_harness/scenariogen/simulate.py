"""Physical simulation of the estate, tick by tick.

The rule this module is built around
------------------------------------
**A fault changes a state variable; the metrics are consequences.** Nothing
here says "during a thread starvation, set the blocked-thread gauge high". It
says "an external dependency stalls, so service time rises", and then Little's
law fills the thread pool, the pool backs up, the run queue lengthens, CPU
*stays low* because no work is completing, and the blocked-thread gauge rises
on its own.

That distinction is the whole point. Metrics painted directly onto the series a
detector happens to read would make detection trivially easy and would make
every downstream number meaningless. Requirement R5 and the integrity note in
the original request both call that out explicitly, so the discriminating
signals here are emergent:

* **thread starvation** — service time explodes, threads fill, throughput
  collapses, and **CPU falls**, because CPU tracks *completed* work.
* **memory leak** — the live set grows, so each full GC traces more, so GC
  time per interval climbs superlinearly: **CPU rises while throughput stays
  flat**. That is the exact opposite CPU/throughput relationship to a traffic
  surge, and it is the honest way to make "CPU saturation without
  corresponding throughput" a real discriminator rather than a label.
* **traffic surge** — offered load rises, and so does completed throughput.
  CPU is just as high as in the leak case. Anything keying on CPU alone must
  fire on this, which is precisely what the false-positive criterion tests.
* **socket exhaustion** — descriptors accumulate monotonically against a hard
  limit; nothing is wrong at all until the limit is approached, then accepts
  start failing.
* **database lock contention** — lock waits raise query latency, which is a
  *term inside* the application's service time, so the application symptoms
  are produced by the same arithmetic that produces the healthy ones. The
  cross-domain cascade is not scripted; it falls out.
* **port faults** — loss and reroute latency raise network RTT, which is
  another term in service time, and which also sits upstream of the database,
  so a single port fault degrades two tiers.

Simulation order is **switches → databases → app servers → nodes**, because
coupling runs strictly that way. Offered load is computed for every app server
*before* the switch pass, since demand is exogenous; only *completed* work
depends on the downstream state.

Determinism
-----------
Every random draw comes from ``ano.contracts.determinism.rng`` with an explicit
namespace. Noise generators are created once per entity per channel and
advanced **exactly once per tick, unconditionally**, before any branching — if
a branch could skip a draw, the stream would desynchronise and two runs at the
same seed would diverge. Nothing iterates an unordered collection.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from ano.contracts.determinism import rng

from scenariogen.estate import AppServer, Database, Estate, Node, Port
from scenariogen.scenarios import ScenarioEvent, ScenarioPlan
from scenariogen.seasonality import AutoCorrelatedNoise, SeasonalProfile, build_profile
from scenariogen.timeline import Tick, Timeline

__all__ = [
    "HostObservation",
    "AppObservation",
    "DatabaseObservation",
    "PortObservation",
    "SwitchObservation",
    "NodeObservation",
    "TickObservation",
    "Simulator",
]

# -- workload shape constants ------------------------------------------------
#: Database round trips per HTTP request.
QUERIES_PER_REQUEST = 3.0
#: Application CPU milliseconds per request, excluding GC and I/O waits.
CPU_MS_PER_REQUEST = 10.0
#: Healthy database query service time.
BASE_QUERY_MS = 2.4
#: Healthy switch-port one-way latency contribution.
BASE_RTT_MS = 0.35
#: Request/response sizes, used for interface octet counters.
REQUEST_BYTES = 1400
RESPONSE_BYTES = 9600

#: Nominal requests/second at seasonal shape 1.0, by service.
BASE_RPS = {
    "clinical-supply-portal": 64.0,
    "lab-results-gateway": 38.0,
    "site-operations": 22.0,
}

_CPU_MODES = ("user", "system", "nice", "iowait", "irq", "softirq", "steal", "idle")
_GIB = 1024 ** 3

#: Request-duration histogram boundaries, in seconds. These are Micrometer's
#: defaults for an HTTP server timer, which is what a Spring Boot application
#: actually publishes.
LATENCY_BUCKETS: tuple[float, ...] = (
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
)


def _lognormal_cdf(x: float, median: float, sigma: float) -> float:
    """P(X <= x) for a log-normal with the given median and log-sigma."""
    if x <= 0.0:
        return 0.0
    z = (math.log(x) - math.log(median)) / (sigma * math.sqrt(2.0))
    return 0.5 * (1.0 + math.erf(z))


def _clamp(value: float, low: float, high: float) -> float:
    return low if value < low else (high if value > high else value)


def _sigmoid(x: float) -> float:
    """Logistic curve, used for onsets that are fast but not instantaneous."""
    if x < -40.0:
        return 0.0
    if x > 40.0:
        return 1.0
    return 1.0 / (1.0 + math.exp(-x))


def _trapezoid(progress: float, rise: float = 0.12, fall: float = 0.15) -> float:
    """Ramp up, plateau, ramp down over a normalised event progress."""
    if progress <= 0.0 or progress >= 1.0:
        return 0.0
    if progress < rise:
        return progress / rise
    if progress > 1.0 - fall:
        return (1.0 - progress) / fall
    return 1.0


# ---------------------------------------------------------------------------
# Observation records — what one scrape of one entity would show
# ---------------------------------------------------------------------------
@dataclass(slots=True)
class HostObservation:
    """Operating-system level view of one host."""

    entity_id: str
    up: int
    cores: int
    boot_time_s: float
    cpu_seconds: dict[str, float] = field(default_factory=dict)
    load1: float = 0.0
    load5: float = 0.0
    load15: float = 0.0
    procs_running: int = 0
    procs_blocked: int = 0
    mem_total: int = 0
    mem_free: int = 0
    mem_available: int = 0
    mem_cached: int = 0
    mem_buffers: int = 0
    swap_free: int = 0
    filefd_allocated: int = 0
    filefd_maximum: int = 0
    sockstat: dict[str, float] = field(default_factory=dict)
    disk: dict[str, float] = field(default_factory=dict)
    net: dict[str, float] = field(default_factory=dict)
    vm: dict[str, float] = field(default_factory=dict)
    ctx_switches: float = 0.0
    interrupts: float = 0.0
    fs: dict[str, float] = field(default_factory=dict)


@dataclass(slots=True)
class AppObservation:
    """Apache + Tomcat + JVM view of one application server."""

    entity_id: str
    host: HostObservation
    #: True if the JVM itself answered this scrape. False during a restart.
    jvm_up: int = 1
    offered_rps: float = 0.0
    completed_rps: float = 0.0
    error_rps: float = 0.0
    service_ms: float = 0.0
    busy_threads: int = 0
    pool_size: int = 0
    apache: dict[str, float] = field(default_factory=dict)
    tomcat: dict[str, float] = field(default_factory=dict)
    jvm: dict[str, float] = field(default_factory=dict)
    thread_states: dict[str, int] = field(default_factory=dict)
    gc: dict[str, tuple[float, float]] = field(default_factory=dict)
    #: Request-latency histogram, keyed by ``method|outcome|status|uri``. The
    #: value is ``(cumulative bucket counts, total count, total seconds)``.
    #: A distribution rather than a mean, because a mean hides the tail that
    #: a stall produces.
    latency: dict[str, tuple[tuple[int, ...], int, float]] = field(default_factory=dict)
    #: Fault kinds physically in progress on this entity. Used ONLY to pick log
    #: templates — a real system's logs do reflect what is happening. Never
    #: written into a metric label or a log field.
    conditions: tuple[str, ...] = ()


@dataclass(slots=True)
class DatabaseObservation:
    """MySQL view of one database host."""

    entity_id: str
    host: HostObservation
    db_up: int = 1
    query_rate: float = 0.0
    query_ms: float = 0.0
    active_sessions: int = 0
    mysql: dict[str, float] = field(default_factory=dict)
    #: ``(command, state)`` -> ``(thread count, total seconds of age)``. The
    #: only place session age appears, and therefore the only way a
    #: long-running analytical query is visible at all.
    processlist: dict[tuple[str, str], tuple[float, float]] = field(default_factory=dict)
    conditions: tuple[str, ...] = ()


@dataclass(slots=True)
class PortObservation:
    """SNMP view of one switch interface."""

    entity_id: str
    port: Port
    oper_status: int = 1
    admin_status: int = 1
    ospf_state: int = 8
    counters: dict[str, float] = field(default_factory=dict)
    last_change_s: float = 0.0
    #: Extra one-way latency this port contributes right now, milliseconds.
    rtt_extra_ms: float = 0.0
    loss_fraction: float = 0.0
    conditions: tuple[str, ...] = ()


@dataclass(slots=True)
class SwitchObservation:
    entity_id: str
    ports: tuple[PortObservation, ...]
    scrape_duration_s: float = 0.0
    scrape_pdus: float = 0.0
    request_errors: float = 0.0
    conditions: tuple[str, ...] = ()


@dataclass(slots=True)
class NodeObservation:
    entity_id: str
    host: HostObservation
    #: cAdvisor series, present only on the container host.
    containers: dict[str, dict[str, float]] = field(default_factory=dict)
    conditions: tuple[str, ...] = ()


@dataclass(slots=True)
class TickObservation:
    """One synchronous scrape of the whole estate."""

    tick: Tick
    apps: tuple[AppObservation, ...]
    databases: tuple[DatabaseObservation, ...]
    switches: tuple[SwitchObservation, ...]
    nodes: tuple[NodeObservation, ...]


# ---------------------------------------------------------------------------
# Mutable per-entity state
# ---------------------------------------------------------------------------
@dataclass(slots=True)
class _HostState:
    """Persistent OS state. Counters here are cumulative since boot."""

    entity_id: str
    cores: int
    mem_total: int
    fd_limit: int
    boot_nanos: int
    counters: dict[str, float] = field(default_factory=dict)
    load1: float = 0.2
    load5: float = 0.2
    load15: float = 0.2
    leaked_sockets: float = 0.0
    leaked_fds: float = 0.0
    fs_used_bytes: float = 0.0
    #: Scrape instants at which the host was unreachable.
    down: bool = False

    def add(self, key: str, delta: float) -> None:
        """Accumulate a counter. Deltas are clamped non-negative."""
        if delta < 0.0:
            delta = 0.0
        self.counters[key] = self.counters.get(key, 0.0) + delta

    def get(self, key: str) -> float:
        return self.counters.get(key, 0.0)

    def reboot(self, nanos: int) -> None:
        """Model a real reboot: kernel counters restart from zero.

        This is a genuine phenomenon and it is deliberately *not* smoothed
        away. A naive rate calculation across a reboot produces a huge
        spurious value, which is exactly the sort of thing a change-aware
        system is supposed to handle — and it happens during a **benign**
        patching window, so it is a false-positive trap rather than a gift.
        """
        self.counters.clear()
        self.boot_nanos = nanos
        self.leaked_sockets = 0.0
        self.leaked_fds = 0.0
        self.load1 = self.load5 = self.load15 = 0.1


@dataclass(slots=True)
class _AppState:
    server: AppServer
    host: _HostState
    profile: SeasonalProfile
    #: Retained heap, i.e. what a full GC cannot reclaim.
    heap_retained: float = 0.0
    #: Bytes leaked so far by an in-progress leak.
    leak_bytes: float = 0.0
    #: Young-generation fill fraction, drives the allocation sawtooth.
    young_fill: float = 0.0
    heap_used: float = 0.0
    classes_loaded: float = 0.0
    session_count: float = 0.0
    max_service_ms: float = 0.0


@dataclass(slots=True)
class _DbState:
    database: Database
    host: _HostState
    profile: SeasonalProfile
    buffer_pool_bytes: float = 0.0
    max_used_connections: float = 0.0
    #: Longest single lock wait observed so far, milliseconds. InnoDB reports
    #: a running maximum, not a windowed one, so this only ever grows.
    row_lock_time_max: float = 12.0
    #: Nightly reporting batch, minutes after midnight and length. Every
    #: database in a real estate has one and it is the single most common
    #: cause of a static long-query threshold firing on a perfectly healthy
    #: system.
    batch_start_min: int = 120
    batch_length_min: int = 90


@dataclass(slots=True)
class _PortState:
    port: Port
    counters: dict[str, float] = field(default_factory=dict)
    oper_status: int = 1
    ospf_state: int = 8
    last_change_nanos: int = 0
    #: Throughput seen on the previous tick, used for octet accumulation.
    last_rps: float = 0.0

    def add(self, key: str, delta: float) -> None:
        if delta < 0.0:
            delta = 0.0
        self.counters[key] = self.counters.get(key, 0.0) + delta

    def get(self, key: str) -> float:
        return self.counters.get(key, 0.0)


@dataclass(slots=True)
class _NodeState:
    node: Node
    host: _HostState
    profile: SeasonalProfile


# ---------------------------------------------------------------------------
# Simulator
# ---------------------------------------------------------------------------
class Simulator:
    """Advances the whole estate through a timeline, one tick at a time.

    Args:
        seed: Run seed. Every stochastic decision derives from it.
        estate: The topology.
        timeline: The scrape grid.
        plan: The scenario schedule.
    """

    def __init__(
        self,
        seed: int,
        estate: Estate,
        timeline: Timeline,
        plan: ScenarioPlan,
    ) -> None:
        self.seed = seed
        self.estate = estate
        self.timeline = timeline
        self.plan = plan
        self._noise: dict[tuple[str, str], AutoCorrelatedNoise] = {}
        self._apps: list[_AppState] = []
        self._dbs: list[_DbState] = []
        self._ports: dict[str, _PortState] = {}
        self._nodes: list[_NodeState] = []
        self._build_state()

    # -- construction ----------------------------------------------------
    def _noise_for(self, entity_id: str, channel: str, sigma: float, phi: float = 0.72):
        key = (entity_id, channel)
        generator = self._noise.get(key)
        if generator is None:
            generator = AutoCorrelatedNoise(
                rng(self.seed, "scenariogen", "noise", channel, entity_id), sigma, phi
            )
            self._noise[key] = generator
        return generator

    def _build_state(self) -> None:
        epoch = self.timeline.epoch_nanos
        # Hosts boot some weeks before the corpus starts, so uptime is
        # plausible rather than suspiciously fresh.
        for server in self.estate.app_servers:
            generator = rng(self.seed, "scenariogen", "init", server.entity_id)
            cores = 4
            host = _HostState(
                entity_id=server.entity_id,
                cores=cores,
                mem_total=int(server.heap_max_bytes * 2.0),
                fd_limit=server.fd_limit,
                boot_nanos=epoch - generator.randint(11, 64) * 86_400_000_000_000,
            )
            host.fs_used_bytes = 120 * _GIB * generator.uniform(0.28, 0.52)
            self._apps.append(
                _AppState(
                    server=server,
                    host=host,
                    profile=build_profile(
                        self.seed,
                        server.entity_id,
                        BASE_RPS[server.service],
                        epoch,
                    ),
                    heap_retained=server.heap_max_bytes * generator.uniform(0.17, 0.26),
                    young_fill=generator.random(),
                    classes_loaded=generator.randint(14_000, 22_000),
                    session_count=generator.randint(200, 900),
                )
            )

        for database in self.estate.databases:
            generator = rng(self.seed, "scenariogen", "init", database.entity_id)
            host = _HostState(
                entity_id=database.entity_id,
                cores=8,
                mem_total=64 * _GIB,
                fd_limit=65536,
                boot_nanos=epoch - generator.randint(30, 120) * 86_400_000_000_000,
            )
            host.fs_used_bytes = 900 * _GIB * generator.uniform(0.33, 0.61)
            self._dbs.append(
                _DbState(
                    database=database,
                    host=host,
                    profile=build_profile(
                        self.seed, database.entity_id, 1.0, epoch
                    ),
                    buffer_pool_bytes=48 * _GIB * generator.uniform(0.86, 0.98),
                    # Staggered so the estate's batch windows do not all land
                    # on the same minute, as a real schedule would be.
                    batch_start_min=generator.randint(100, 200),
                    batch_length_min=generator.randint(65, 125),
                )
            )

        for switch in self.estate.switches:
            for port in switch.ports:
                generator = rng(self.seed, "scenariogen", "init", port.entity_id)
                state = _PortState(port=port, last_change_nanos=epoch)
                # Interfaces have been up and carrying traffic for a while.
                state.add("in_octets", generator.uniform(4e11, 9e12))
                state.add("out_octets", generator.uniform(4e11, 9e12))
                state.add("in_pkts", generator.uniform(3e8, 5e9))
                state.add("out_pkts", generator.uniform(3e8, 5e9))
                state.add("ospf_events", float(generator.randint(2, 30)))
                self._ports[port.entity_id] = state

        for node in self.estate.nodes:
            generator = rng(self.seed, "scenariogen", "init", node.entity_id)
            host = _HostState(
                entity_id=node.entity_id,
                cores=4,
                mem_total=32 * _GIB,
                fd_limit=65536,
                boot_nanos=epoch - generator.randint(20, 90) * 86_400_000_000_000,
            )
            host.fs_used_bytes = 400 * _GIB * generator.uniform(0.2, 0.45)
            self._nodes.append(
                _NodeState(
                    node=node,
                    host=host,
                    profile=build_profile(
                        self.seed,
                        node.entity_id,
                        6.0 if node.role == "batch" else 2.0,
                        epoch,
                    ),
                )
            )

    # -- event lookup ----------------------------------------------------
    @staticmethod
    def _events_for(
        active: Sequence[ScenarioEvent], entity_id: str
    ) -> tuple[ScenarioEvent, ...]:
        """Events whose **root cause** is ``entity_id``.

        Only root-cause membership drives physics. Being in an event's
        ``affected`` set is a *consequence*, and consequences are produced by
        the coupling equations, never injected directly. If downstream symptoms
        were injected from the label, attribution would be scoring the
        generator's opinion rather than the detector's inference.
        """
        return tuple(event for event in active if event.root_entity == entity_id)

    # -- the main loop ---------------------------------------------------
    def run(self):
        """Yield one :class:`TickObservation` per scrape instant."""
        for tick in self.timeline.ticks:
            yield self._step(tick)

    def _step(self, tick: Tick) -> TickObservation:
        active = self.plan.active_at(tick.nanos)

        # 1. Exogenous demand. Offered load depends on nothing downstream.
        offered: dict[str, float] = {}
        for app in self._apps:
            noise = self._noise_for(app.server.entity_id, "demand", 0.11)
            value = app.profile.demand_at(tick.moment, tick.nanos) * noise.factor()
            for event in self._events_for(active, app.server.entity_id):
                if event.kind == "traffic_surge":
                    multiplier = float(event.params.get("multiplier", 2.5))
                    value *= 1.0 + (multiplier - 1.0) * _trapezoid(
                        event.progress(tick.nanos), rise=0.10, fall=0.18
                    ) * event.severity
                elif event.kind == "planned_deploy":
                    # Connection draining: the load balancer pulls this node.
                    progress = event.progress(tick.nanos)
                    if progress < 0.45:
                        value *= max(0.05, 1.0 - progress / 0.45)
            offered[app.server.entity_id] = max(0.0, value)

        # 2. Network. Ports carry the previous tick's throughput.
        switches = tuple(self._step_switch(tick, active, index)
                         for index in range(len(self.estate.switches)))
        port_by_id: dict[str, PortObservation] = {
            observation.entity_id: observation
            for switch in switches
            for observation in switch.ports
        }

        # 3. Databases, whose query load comes from offered application load.
        databases = tuple(
            self._step_database(tick, active, state, offered, port_by_id)
            for state in self._dbs
        )
        db_by_id = {observation.entity_id: observation for observation in databases}

        # 4. Application servers.
        apps = tuple(
            self._step_app(tick, active, state, offered, db_by_id, port_by_id)
            for state in self._apps
        )

        # 5. Shared infrastructure nodes.
        nodes = tuple(self._step_node(tick, active, state) for state in self._nodes)

        # 6. Feed this tick's throughput back for the next tick's octets.
        for app in apps:
            port_id = self.estate.app_server(app.entity_id).uplink_port
            self._ports[port_id].last_rps += app.completed_rps
        for database in databases:
            port_id = self.estate.database(database.entity_id).uplink_port
            self._ports[port_id].last_rps += database.query_rate * 0.35

        return TickObservation(
            tick=tick, apps=apps, databases=databases, switches=switches, nodes=nodes
        )

    # -- switches --------------------------------------------------------
    def _step_switch(
        self, tick: Tick, active: Sequence[ScenarioEvent], index: int
    ) -> SwitchObservation:
        switch = self.estate.switches[index]
        observations: list[PortObservation] = []
        conditions: list[str] = []

        for port in switch.ports:
            state = self._ports[port.entity_id]
            noise = self._noise_for(port.entity_id, "traffic", 0.09)
            jitter = self._noise_for(port.entity_id, "jitter", 0.22)
            factor = noise.factor()
            jitter_value = jitter.next()

            rps = state.last_rps
            state.last_rps = 0.0

            loss = 0.0
            rtt_extra = 0.0
            oper = 1
            ospf = 8
            port_conditions: list[str] = []
            flapped = 0

            for event in self._events_for(active, port.entity_id):
                progress = event.progress(tick.nanos)
                if event.kind == "packet_loss":
                    # A dirty optic or a marginal cable: loss ramps, the link
                    # never goes down, and OSPF never notices. Subtle by
                    # construction.
                    target = float(event.params.get("loss", 0.02)) * event.severity
                    loss = max(loss, target * _trapezoid(progress, rise=0.22, fall=0.2))
                    port_conditions.append("packet_loss")
                elif event.kind == "ospf_flap":
                    count = int(event.params.get("flap_count", 0))
                    for flap in range(count):
                        start = float(event.params.get(f"flap_{flap}", 2.0))
                        length = float(event.params.get(f"flap_{flap}_len", 0.02))
                        if start <= progress < start + length:
                            # Adjacency down: the link drops, traffic reroutes
                            # the long way round and RTT jumps.
                            oper = 2
                            ospf = 1
                            loss = max(loss, 0.55 * event.severity)
                            rtt_extra += 9.0 * event.severity
                            flapped = 1
                        elif start + length <= progress < start + length * 2.2:
                            # Re-forming the adjacency: up but not converged.
                            ospf = 3
                            loss = max(loss, 0.12 * event.severity)
                            rtt_extra += 3.0 * event.severity
                            flapped = 1
                    port_conditions.append("ospf_flap")

            if flapped and state.ospf_state != ospf:
                state.add("ospf_events", 1.0)
                state.last_change_nanos = tick.nanos
            if state.oper_status != oper:
                state.last_change_nanos = tick.nanos
            state.oper_status = oper
            state.ospf_state = ospf

            # Queuing delay: utilisation raised to a power, the usual convex
            # shape. Healthy links contribute a fraction of a millisecond.
            offered_bits = rps * (REQUEST_BYTES + RESPONSE_BYTES) * 8.0
            utilisation = _clamp(offered_bits / (port.speed_mbit * 1e6), 0.0, 0.995)
            rtt_extra += BASE_RTT_MS * (utilisation ** 3) * 14.0
            rtt = BASE_RTT_MS + rtt_extra + 0.05 * jitter_value
            # Retransmissions multiply effective latency super-linearly.
            if loss > 0.0:
                rtt *= 1.0 + 11.0 * loss

            packets_in = rps * 2.6 * factor * tick.interval_s
            packets_out = rps * 3.4 * factor * tick.interval_s
            if oper == 2:
                packets_in = packets_out = 0.0
            state.add("in_pkts", packets_in * (1.0 - loss))
            state.add("out_pkts", packets_out)
            state.add("in_octets", packets_in * (1.0 - loss) * REQUEST_BYTES / 2.6)
            state.add("out_octets", packets_out * RESPONSE_BYTES / 3.4)
            state.add("in_discards", packets_in * loss)
            state.add("out_discards", packets_out * loss * 0.35)
            # Errors are rarer than discards and only appear with real damage.
            state.add("in_errors", packets_in * loss * 0.09)
            state.add("out_errors", packets_out * loss * 0.02)

            observations.append(
                PortObservation(
                    entity_id=port.entity_id,
                    port=port,
                    oper_status=oper,
                    admin_status=1,
                    ospf_state=ospf,
                    counters=dict(state.counters),
                    last_change_s=(tick.nanos - state.last_change_nanos) / 1e9,
                    rtt_extra_ms=max(0.0, rtt),
                    loss_fraction=loss,
                    conditions=tuple(port_conditions),
                )
            )
            conditions.extend(port_conditions)

        scrape_noise = self._noise_for(switch.entity_id, "scrape", 0.18)
        return SwitchObservation(
            entity_id=switch.entity_id,
            ports=tuple(observations),
            scrape_duration_s=max(0.02, 0.42 * scrape_noise.factor()),
            scrape_pdus=float(len(switch.ports) * 14 + 23),
            request_errors=0.0,
            conditions=tuple(sorted(set(conditions))),
        )

    # -- databases -------------------------------------------------------
    def _step_database(
        self,
        tick: Tick,
        active: Sequence[ScenarioEvent],
        state: _DbState,
        offered: Mapping[str, float],
        ports: Mapping[str, PortObservation],
    ) -> DatabaseObservation:
        database = state.database
        host = state.host
        # Advance every noise stream unconditionally, before any branching.
        io_noise = self._noise_for(database.entity_id, "io", 0.14)
        cpu_noise = self._noise_for(database.entity_id, "cpu", 0.10)
        lock_noise = self._noise_for(database.entity_id, "lock", 0.30)
        io_factor = io_noise.factor()
        cpu_factor = cpu_noise.factor()
        lock_factor = lock_noise.factor()

        events = self._events_for(active, database.entity_id)
        conditions: list[str] = []

        query_rate = sum(
            offered[server.entity_id] * QUERIES_PER_REQUEST
            for server in self.estate.app_servers
            if database.entity_id in server.databases
        )
        # Replication, reporting and the scheduler all query too.
        query_rate += 4.0

        uplink = ports[database.uplink_port]
        net_ms = uplink.rtt_extra_ms

        lock_wait_prob = 0.0012
        lock_wait_ms = 4.0
        extra_cpu = 0.0
        patching = 0.0
        down = False

        for event in events:
            progress = event.progress(tick.nanos)
            if event.kind == "db_lock_contention":
                # A long-running transaction holds row locks. Waiters queue,
                # each waiter occupies a session, and sessions are finite.
                intensity = _trapezoid(progress, rise=0.14, fall=0.22) * event.severity
                lock_wait_prob = 0.0012 + 0.34 * intensity * max(0.4, lock_factor)
                lock_wait_ms = 4.0 + 1500.0 * intensity
                conditions.append("db_lock_contention")
            elif event.kind == "patching_window":
                patching = _patch_intensity(progress)
                down = _patch_is_rebooting(progress)
                conditions.append("patching_window")

        if down:
            if not host.down:
                host.reboot(tick.nanos)
            host.down = True
            return DatabaseObservation(
                entity_id=database.entity_id,
                host=HostObservation(
                    entity_id=database.entity_id,
                    up=0,
                    cores=host.cores,
                    boot_time_s=host.boot_nanos / 1e9,
                ),
                db_up=0,
                query_rate=0.0,
                query_ms=0.0,
                active_sessions=0,
                conditions=tuple(sorted(set(conditions))),
            )
        host.down = False

        query_ms = BASE_QUERY_MS + net_ms * 0.4 + lock_wait_prob * lock_wait_ms

        # Nightly reporting batch. A handful of analytical statements run for
        # minutes against the same instance. This is ORDINARY OPERATION, not an
        # incident and not labelled as one, but it is exactly what makes a
        # static "longest query > 60s" rule fire on a healthy database. Leaving
        # it out would hand the comparator an artificially quiet baseline.
        minute_of_day = tick.moment.hour * 60 + tick.moment.minute
        batch_elapsed_s = 0.0
        batch_threads = 0
        if state.batch_start_min <= minute_of_day < state.batch_start_min + state.batch_length_min:
            batch_elapsed_s = (minute_of_day - state.batch_start_min) * 60.0
            # Statements are re-issued in stages, so ages saw-tooth rather than
            # growing without bound for the whole window.
            stage = batch_elapsed_s % 1500.0
            batch_elapsed_s = stage
            batch_threads = 2 if stage > 45.0 else 1
            query_ms += 1.6
            query_rate += 3.0

        # Connection pressure: as sessions approach max_connections, the
        # scheduler itself becomes a bottleneck.
        raw_sessions = query_rate * (query_ms / 1000.0) + 12.0
        saturation = _clamp(raw_sessions / database.max_connections, 0.0, 1.2)
        if saturation > 0.7:
            query_ms *= 1.0 + 2.4 * (saturation - 0.7)
            raw_sessions = query_rate * (query_ms / 1000.0) + 12.0
        active_sessions = int(min(database.max_connections, raw_sessions))
        state.max_used_connections = max(state.max_used_connections, active_sessions)

        interval = tick.interval_s
        completed = query_rate * interval
        waiters = query_rate * lock_wait_prob
        current_waits = waiters * (lock_wait_ms / 1000.0)
        threads_running = int(
            _clamp(query_rate * (query_ms / 1000.0) * 0.8 + 1.0, 1.0, host.cores * 6.0)
        )

        # The lock-wait *distribution* has a long right tail: most waiters are
        # served quickly and a few sit until InnoDB's own
        # innodb_lock_wait_timeout (50 s by default) gives up on them. InnoDB
        # reports the maximum as a running high-water mark, so it only grows.
        if waiters > 0.0:
            tail_ms = min(50_000.0, lock_wait_ms * (2.2 + 5.5 * max(0.0, lock_factor - 1.0) * 6.0))
            state.row_lock_time_max = max(state.row_lock_time_max, tail_ms)

        host.add("mysql_queries", completed)
        host.add("mysql_questions", completed * 0.97)
        host.add("mysql_row_lock_waits", waiters * interval)
        host.add("mysql_row_lock_time", waiters * interval * lock_wait_ms)
        host.add("mysql_table_locks_immediate", completed * 0.21)
        host.add("mysql_table_locks_waited", waiters * interval * 0.11)
        # A slow query is one that exceeded the server's long_query_time. The
        # fraction rises naturally as the latency distribution shifts right.
        slow_fraction = _clamp((query_ms - 220.0) / 900.0, 0.0, 0.85)
        host.add("mysql_slow_queries", completed * slow_fraction)
        host.add("mysql_innodb_log_waits", completed * 0.00004 * (1.0 + patching))
        host.add("mysql_buffer_pool_wait_free", completed * 0.000012 * (1.0 + 6.0 * patching))
        # New connections, and the ones that could not be served. Aborted
        # connects climb as the session pool saturates — which is a *symptom*
        # of the latency rise, not an independently injected signal.
        new_connections = query_rate * 0.02 * interval
        host.add("mysql_connections", new_connections)
        host.add("mysql_aborted_connects", new_connections * _clamp(saturation - 0.8, 0.0, 0.2) * 2.0)
        host.add("mysql_aborted_clients", new_connections * 0.0018)
        for command, share in (
            ("select", 0.74), ("insert", 0.11), ("update", 0.09),
            ("delete", 0.02), ("commit", 0.04),
        ):
            host.add(f"mysql_command_{command}", completed * share)

        # CPU: query work plus, during a patch, heavy package installation.
        db_cpu_cores = query_rate * (0.55 / 1000.0) * host.cores * cpu_factor
        db_cpu_cores += extra_cpu
        system_cores = host.cores * (0.03 + 0.52 * patching)
        iowait_cores = host.cores * (0.012 * io_factor + 0.30 * patching)
        self._accumulate_host_cpu(
            host, interval, user_cores=db_cpu_cores, system_cores=system_cores,
            iowait_cores=iowait_cores,
        )

        read_ops = (completed * 0.06 + 4.0) * io_factor + 260.0 * patching * interval
        write_ops = (completed * 0.11 + 6.0) * io_factor + 180.0 * patching * interval
        # Disk service time degrades with queue depth, so a patch storm raises
        # per-operation latency as well as operation count.
        service_s = 0.00042 * (1.0 + 5.5 * patching)
        self._accumulate_host_disk(host, interval, read_ops, write_ops, service_s)

        fd_used = 900 + active_sessions * 2 + int(host.leaked_fds)
        sockets = active_sessions + 40
        self._accumulate_host_net(
            host, interval, rx_bytes=query_rate * 320 * interval,
            tx_bytes=query_rate * 2400 * interval, loss=uplink.loss_fraction,
        )

        run_queue = threads_running / max(1.0, host.cores)
        blocked = int(current_waits * 0.6 + host.cores * patching * 2.4)
        self._advance_load(host, interval, threads_running + blocked)

        observation_host = self._host_observation(
            host, tick, up=1,
            procs_running=max(1, int(run_queue) + 1),
            procs_blocked=blocked,
            fd_used=fd_used,
            tcp_inuse=sockets,
            cached_fraction=0.62,
        )

        mysql = {
            "threads_connected": float(active_sessions),
            "threads_running": float(threads_running),
            "threads_cached": float(max(0, 40 - threads_running)),
            "max_used_connections": state.max_used_connections,
            "max_connections": float(database.max_connections),
            "queries": host.get("mysql_queries"),
            "questions": host.get("mysql_questions"),
            "slow_queries": host.get("mysql_slow_queries"),
            "row_lock_waits": host.get("mysql_row_lock_waits"),
            "row_lock_time": host.get("mysql_row_lock_time"),
            "row_lock_time_avg": (
                host.get("mysql_row_lock_time") / max(1.0, host.get("mysql_row_lock_waits"))
            ),
            "row_lock_time_max": state.row_lock_time_max,
            "row_lock_current_waits": current_waits,
            "table_locks_immediate": host.get("mysql_table_locks_immediate"),
            "table_locks_waited": host.get("mysql_table_locks_waited"),
            "innodb_log_waits": host.get("mysql_innodb_log_waits"),
            "buffer_pool_wait_free": host.get("mysql_buffer_pool_wait_free"),
            "connections": host.get("mysql_connections"),
            "aborted_connects": host.get("mysql_aborted_connects"),
            "aborted_clients": host.get("mysql_aborted_clients"),
            "command_select": host.get("mysql_command_select"),
            "command_insert": host.get("mysql_command_insert"),
            "command_update": host.get("mysql_command_update"),
            "command_delete": host.get("mysql_command_delete"),
            "command_commit": host.get("mysql_command_commit"),
        }

        # SHOW PROCESSLIST, grouped the way the exporter groups it. Sessions
        # blocked on a row lock sit in "Waiting for table metadata lock"; the
        # batch's analytical statements sit in "Sending data" and are the only
        # sessions whose age is measured in minutes.
        sleeping = max(0.0, active_sessions - threads_running - batch_threads)
        processlist: dict[tuple[str, str], tuple[float, float]] = {
            ("Query", "executing"): (
                float(threads_running), threads_running * query_ms / 1000.0
            ),
            ("Sleep", "cleaned up"): (sleeping, sleeping * 42.0),
        }
        if batch_threads:
            processlist[("Query", "Sending data")] = (
                float(batch_threads), batch_threads * batch_elapsed_s
            )
        if current_waits > 0.01:
            processlist[("Query", "Waiting for table metadata lock")] = (
                current_waits, current_waits * lock_wait_ms / 1000.0
            )

        return DatabaseObservation(
            entity_id=database.entity_id,
            host=observation_host,
            db_up=1,
            query_rate=query_rate,
            query_ms=query_ms,
            active_sessions=active_sessions,
            mysql=mysql,
            processlist=processlist,
            conditions=tuple(sorted(set(conditions))),
        )

    # -- application servers ---------------------------------------------
    def _step_app(
        self,
        tick: Tick,
        active: Sequence[ScenarioEvent],
        state: _AppState,
        offered: Mapping[str, float],
        databases: Mapping[str, DatabaseObservation],
        ports: Mapping[str, PortObservation],
    ) -> AppObservation:
        server = state.server
        host = state.host
        interval = tick.interval_s

        cpu_noise = self._noise_for(server.entity_id, "cpu", 0.08)
        io_noise = self._noise_for(server.entity_id, "io", 0.13)
        alloc_noise = self._noise_for(server.entity_id, "alloc", 0.12)
        cpu_factor = cpu_noise.factor()
        io_factor = io_noise.factor()
        alloc_factor = alloc_noise.factor()

        events = self._events_for(active, server.entity_id)
        conditions: list[str] = []

        offered_rps = offered[server.entity_id]
        uplink = ports[server.uplink_port]
        net_ms = uplink.rtt_extra_ms * 2.0  # request and response

        # Database term. Every query this request makes crosses the network to
        # a database whose own latency already includes ITS uplink. This is the
        # only place the cross-domain cascade is expressed, and it is ordinary
        # arithmetic rather than a special case.
        db_ms = 0.0
        db_error_rate = 0.0
        for database_id in server.databases:
            observation = databases[database_id]
            share = 1.0 / len(server.databases)
            if observation.db_up:
                db_ms += observation.query_ms * QUERIES_PER_REQUEST * share
            else:
                db_error_rate += share

        stall_ms = 0.0
        leak_rate = 0.0
        socket_leak_rate = 0.0
        patching = 0.0
        deploy_progress: float | None = None
        host_down = False

        for event in events:
            progress = event.progress(tick.nanos)
            if event.kind == "thread_starvation":
                # A downstream dependency stops answering. Threads block in a
                # socket read until the timeout, which is the classic cause.
                intensity = _sigmoid((progress - 0.10) * 26.0) * (
                    1.0 - _sigmoid((progress - 0.93) * 34.0)
                )
                stall_ms = 30_000.0 * intensity * event.severity
                conditions.append("thread_starvation")
            elif event.kind == "memory_leak":
                leak_rate = float(event.params.get("bytes_per_second", 200_000)) * event.severity
                conditions.append("memory_leak")
            elif event.kind == "socket_exhaustion":
                socket_leak_rate = (
                    float(event.params.get("sockets_per_second", 1.0)) * event.severity
                )
                conditions.append("socket_exhaustion")
            elif event.kind == "patching_window":
                patching = _patch_intensity(progress)
                host_down = _patch_is_rebooting(progress)
                conditions.append("patching_window")
            elif event.kind == "planned_deploy":
                deploy_progress = progress
                conditions.append("planned_deploy")
            elif event.kind == "traffic_surge":
                conditions.append("traffic_surge")

        if host_down:
            if not host.down:
                host.reboot(tick.nanos)
            host.down = True
            return AppObservation(
                entity_id=server.entity_id,
                host=HostObservation(
                    entity_id=server.entity_id, up=0, cores=host.cores,
                    boot_time_s=host.boot_nanos / 1e9,
                ),
                jvm_up=0,
                pool_size=server.thread_pool_size,
                conditions=tuple(sorted(set(conditions))),
            )
        host.down = False

        # Accumulate the leaks. Both are monotonic while the fault runs and
        # both reset only on a restart, which is what makes them slow deaths
        # rather than spikes.
        host.leaked_sockets += socket_leak_rate * interval
        state.leak_bytes += leak_rate * interval

        # -- JVM heap and garbage collection ------------------------------
        live_set = state.heap_retained + state.leak_bytes
        heap_max = float(server.heap_max_bytes)
        headroom = _clamp(1.0 - live_set / heap_max, 0.02, 1.0)

        allocation_bytes = offered_rps * 260_000.0 * interval * alloc_factor
        young_capacity = heap_max * 0.30 * headroom
        young_collections = allocation_bytes / max(young_capacity, heap_max * 0.01)
        young_pause_s = 0.012 + 0.055 * (live_set / heap_max)
        # Full collections are triggered by promotion pressure, so they become
        # far more frequent as headroom shrinks — and each one traces the whole
        # live set, so they also become slower. The product of those two is why
        # a leak ends as CPU saturation with no extra throughput.
        full_rate_per_s = 0.0006 + 0.34 * max(0.0, (live_set / heap_max) - 0.62) ** 2 * 34.0
        full_collections = full_rate_per_s * interval
        full_pause_s = 0.35 + 4.2 * (live_set / heap_max) ** 3

        gc_seconds = young_collections * young_pause_s + full_collections * full_pause_s
        gc_fraction = _clamp(gc_seconds / max(1.0, interval), 0.0, 0.94)

        # -- service time -------------------------------------------------
        # Stop-the-world pauses inflate every request that is in flight.
        gc_penalty_ms = 1000.0 * gc_fraction / max(0.06, 1.0 - gc_fraction) * 0.20
        service_ms = CPU_MS_PER_REQUEST + db_ms + net_ms + stall_ms + gc_penalty_ms

        warmup_multiplier = 1.0
        deploy_errors = 0.0
        jvm_up = 1
        if deploy_progress is not None:
            if 0.25 <= deploy_progress < 0.45:
                # Process stopped. Apache is still up and returns 503.
                jvm_up = 0
            elif deploy_progress >= 0.45:
                if state.heap_retained > 0.0 and deploy_progress < 0.52:
                    # Fresh JVM: heap, class table and counters all reset.
                    state.leak_bytes = 0.0
                    state.heap_retained = heap_max * 0.16
                    state.classes_loaded = 8_000.0
                    host.leaked_sockets = 0.0
                    host.leaked_fds = 0.0
                # Cold JIT and an empty class cache: slow and CPU-hungry.
                warm = _clamp((deploy_progress - 0.45) / 0.45, 0.0, 1.0)
                warmup_multiplier = 1.0 + 5.5 * (1.0 - warm)
                state.classes_loaded = min(
                    state.classes_loaded + 900.0 * interval / 60.0, 24_000.0
                )
            if 0.2 <= deploy_progress < 0.5:
                deploy_errors = 1.0
        service_ms *= warmup_multiplier
        service_ms *= 1.0 + 1.9 * patching  # cold cache and I/O contention

        # -- Little's law and pool saturation ------------------------------
        pool = float(server.thread_pool_size)
        service_s = max(service_ms, 0.5) / 1000.0
        demanded_threads = offered_rps * service_s
        if demanded_threads > pool:
            busy_threads = pool
            completed_rps = pool / service_s
            # The excess queues in the accept backlog and then gets rejected.
            rejected_rps = offered_rps - completed_rps
        else:
            busy_threads = demanded_threads
            completed_rps = offered_rps
            rejected_rps = 0.0

        # -- descriptors and sockets ---------------------------------------
        established = completed_rps * (service_s + 4.0)  # keep-alive dwell
        tcp_inuse = established + host.leaked_sockets + 30.0
        fd_used = 180.0 + busy_threads + established + host.leaked_sockets
        fd_pressure = fd_used / server.fd_limit
        accept_failures = 0.0
        if fd_pressure > 0.94:
            # accept() starts returning EMFILE. Connections are refused before
            # any application code runs, so latency does NOT rise — the failure
            # mode is refusal, not slowness. That asymmetry is the tell.
            accept_failures = offered_rps * _clamp((fd_pressure - 0.94) / 0.06, 0.0, 0.8)
            completed_rps = max(0.0, completed_rps - accept_failures)

        error_rps = (
            rejected_rps * 0.55
            + accept_failures
            + completed_rps * db_error_rate
            + (offered_rps * 0.92 if jvm_up == 0 else 0.0)
            + completed_rps * 0.0009  # background application error rate
            + (offered_rps * 0.35 * deploy_errors if jvm_up else 0.0)
        )
        if jvm_up == 0:
            completed_rps = 0.0
            busy_threads = 0.0

        # -- heap trajectory ------------------------------------------------
        state.young_fill = (state.young_fill + young_collections) % 1.0
        state.heap_used = live_set + heap_max * 0.30 * headroom * state.young_fill
        state.max_service_ms = max(state.max_service_ms, service_ms)
        state.session_count += max(0.0, completed_rps * 0.012 * interval)

        # -- CPU --------------------------------------------------------------
        # Application CPU tracks COMPLETED work. This single line is what makes
        # thread starvation (low CPU) and a traffic surge (high CPU) different
        # phenomena in the data rather than different labels.
        app_cores = completed_rps * (CPU_MS_PER_REQUEST / 1000.0) * cpu_factor
        gc_cores = gc_fraction * min(host.cores, 4.0) * 0.85
        user_cores = _clamp(app_cores + gc_cores, 0.0, host.cores * 0.97)
        system_cores = host.cores * (0.025 + 0.48 * patching) + established * 0.00018
        iowait_cores = host.cores * (0.008 * io_factor + 0.26 * patching)
        self._accumulate_host_cpu(
            host, interval, user_cores, system_cores, iowait_cores
        )

        read_ops = (completed_rps * 0.02 + 2.0) * io_factor + 220.0 * patching * interval
        write_ops = (completed_rps * 0.05 + 3.0) * io_factor + 150.0 * patching * interval
        self._accumulate_host_disk(
            host, interval, read_ops, write_ops, 0.00038 * (1.0 + 5.0 * patching)
        )
        self._accumulate_host_net(
            host, interval,
            rx_bytes=completed_rps * REQUEST_BYTES * interval,
            tx_bytes=completed_rps * RESPONSE_BYTES * interval,
            loss=uplink.loss_fraction,
        )

        # Blocked threads are threads in uninterruptible or socket wait. During
        # a stall they dominate the run queue while CPU sits idle: the signature
        # combination the requirement asks for.
        blocked = int(busy_threads * _clamp(stall_ms / max(service_ms, 1.0), 0.0, 1.0))
        blocked += int(busy_threads * _clamp(db_ms / max(service_ms, 1.0), 0.0, 1.0) * 0.5)
        runnable = max(1, int(user_cores) + 1)
        self._advance_load(host, interval, runnable + blocked + host.cores * patching * 2.0)

        host_observation = self._host_observation(
            host, tick, up=1, procs_running=runnable, procs_blocked=blocked,
            fd_used=int(fd_used), tcp_inuse=tcp_inuse, cached_fraction=0.34,
            heap_bytes=state.heap_used,
        )

        # -- exporter-facing counters ---------------------------------------
        latency = self._accumulate_latency(
            host, interval, completed_rps, error_rps, service_ms
        )

        served = (completed_rps + error_rps) * interval
        host.add("http_requests", served)
        host.add("http_errors", error_rps * interval)
        host.add("http_duration_ms", completed_rps * interval * service_ms)
        host.add("http_bytes_out", completed_rps * interval * RESPONSE_BYTES)
        host.add("http_bytes_in", completed_rps * interval * REQUEST_BYTES)
        host.add("jvm_young_gc_count", young_collections)
        host.add("jvm_young_gc_seconds", young_collections * young_pause_s)
        host.add("jvm_full_gc_count", full_collections)
        host.add("jvm_full_gc_seconds", full_collections * full_pause_s)
        host.add("jvm_threads_started", max(0.0, busy_threads * 0.02 * interval / 60.0))
        host.add("sessions_created", completed_rps * 0.012 * interval)
        host.add("sessions_expired", completed_rps * 0.011 * interval)
        host.add("sessions_rejected", rejected_rps * 0.02 * interval)

        idle_threads = max(0.0, pool - busy_threads)
        apache_busy = _clamp(busy_threads + 4.0, 1.0, 256.0)
        apache = {
            "up": 1.0,
            "accesses": host.get("http_requests"),
            "sent_kilobytes": host.get("http_bytes_out") / 1024.0,
            "duration_ms": host.get("http_duration_ms"),
            "cpu_load": _clamp(user_cores / host.cores * 100.0, 0.0, 100.0),
            "uptime": max(0.0, (tick.nanos - host.boot_nanos) / 1e9),
            "workers_busy": apache_busy,
            "workers_idle": max(0.0, 256.0 - apache_busy),
            # The scoreboard is where a stall is visible without any derived
            # metric: workers pile into "reply" while nothing is written.
            "sb_reply": apache_busy * 0.72,
            "sb_read": apache_busy * 0.14,
            "sb_keepalive": max(0.0, established * 0.1),
            "sb_waiting": max(0.0, 256.0 - apache_busy),
            "sb_logging": apache_busy * 0.04,
            "sb_closing": apache_busy * 0.08,
            "sb_dns": 0.0,
            "sb_open": 144.0,
            "sb_startup": 0.0,
            "sb_graceful_stop": 0.0,
            "sb_idle_cleanup": 0.0,
        }

        tomcat = {
            "requestcount": host.get("http_requests"),
            "errorcount": host.get("http_errors"),
            "processingtime": host.get("http_duration_ms"),
            "maxtime": state.max_service_ms,
            "bytessent": host.get("http_bytes_out"),
            "bytesreceived": host.get("http_bytes_in"),
            "currentthreadsbusy": busy_threads,
            "currentthreadcount": max(10.0, min(pool, busy_threads + 10.0)),
            "maxthreads": pool,
            "connectioncount": established + host.leaked_sockets,
            "keepalivecount": max(0.0, established * 0.35),
            "session_counter": host.get("sessions_created"),
            "session_expired": host.get("sessions_expired"),
            "session_rejected": host.get("sessions_rejected"),
        }

        jvm = {
            "heap_used": state.heap_used,
            "heap_committed": min(heap_max, state.heap_used * 1.28 + 64 * 1024 ** 2),
            "heap_max": heap_max,
            "nonheap_used": 190 * 1024 ** 2 + state.classes_loaded * 8_600.0,
            "nonheap_committed": 260 * 1024 ** 2 + state.classes_loaded * 9_200.0,
            "nonheap_max": -1.0,
            "old_used": live_set,
            "old_max": heap_max * 0.72,
            "eden_used": heap_max * 0.30 * headroom * state.young_fill,
            "eden_max": heap_max * 0.30,
            "metaspace_used": state.classes_loaded * 8_600.0,
            "metaspace_max": -1.0,
            "classes_loaded": state.classes_loaded,
            "threads_current": max(12.0, busy_threads + 34.0),
            "threads_daemon": max(10.0, busy_threads * 0.9 + 26.0),
            "threads_peak": max(40.0, pool * 0.5),
            "threads_started": host.get("jvm_threads_started") + 400.0,
            "threads_deadlocked": 0.0,
            "buffer_direct_used": 48 * 1024 ** 2 + established * 32_768.0,
            "buffer_mapped_used": 12 * 1024 ** 2,
        }

        thread_states = {
            "NEW": 0,
            "RUNNABLE": max(1, int(runnable + busy_threads * 0.08)),
            "BLOCKED": int(blocked),
            "WAITING": int(max(4.0, idle_threads * 0.25 + 12.0)),
            "TIMED_WAITING": int(max(4.0, idle_threads * 0.7)),
            "TERMINATED": 0,
        }

        gc = {
            "PS Scavenge": (host.get("jvm_young_gc_count"), host.get("jvm_young_gc_seconds")),
            "PS MarkSweep": (host.get("jvm_full_gc_count"), host.get("jvm_full_gc_seconds")),
        }

        return AppObservation(
            entity_id=server.entity_id,
            host=host_observation,
            jvm_up=jvm_up,
            offered_rps=offered_rps,
            completed_rps=completed_rps,
            error_rps=error_rps,
            service_ms=service_ms,
            busy_threads=int(busy_threads),
            pool_size=int(pool),
            apache=apache,
            tomcat=tomcat,
            jvm=jvm,
            thread_states=thread_states,
            gc=gc,
            latency=latency,
            conditions=tuple(sorted(set(conditions))),
        )

    # -- shared infrastructure nodes -------------------------------------
    def _step_node(
        self, tick: Tick, active: Sequence[ScenarioEvent], state: _NodeState
    ) -> NodeObservation:
        node = state.node
        host = state.host
        interval = tick.interval_s

        cpu_noise = self._noise_for(node.entity_id, "cpu", 0.16)
        io_noise = self._noise_for(node.entity_id, "io", 0.20)
        cpu_factor = cpu_noise.factor()
        io_factor = io_noise.factor()

        events = self._events_for(active, node.entity_id)
        conditions: list[str] = []
        patching = 0.0
        down = False
        for event in events:
            progress = event.progress(tick.nanos)
            if event.kind == "patching_window":
                patching = _patch_intensity(progress)
                down = _patch_is_rebooting(progress)
                conditions.append("patching_window")

        if down:
            if not host.down:
                host.reboot(tick.nanos)
            host.down = True
            return NodeObservation(
                entity_id=node.entity_id,
                host=HostObservation(
                    entity_id=node.entity_id, up=0, cores=host.cores,
                    boot_time_s=host.boot_nanos / 1e9,
                ),
                conditions=tuple(sorted(set(conditions))),
            )
        host.down = False

        demand = state.profile.demand_at(tick.moment, tick.nanos)
        user_cores = _clamp(demand / 9.0 * host.cores * cpu_factor, 0.0, host.cores * 0.95)
        system_cores = host.cores * (0.02 + 0.5 * patching)
        iowait_cores = host.cores * (0.03 * io_factor + 0.28 * patching)
        self._accumulate_host_cpu(host, interval, user_cores, system_cores, iowait_cores)

        read_ops = (demand * 9.0 + 3.0) * io_factor + 240.0 * patching * interval
        write_ops = (demand * 6.0 + 2.0) * io_factor + 170.0 * patching * interval
        self._accumulate_host_disk(
            host, interval, read_ops, write_ops, 0.0005 * (1.0 + 5.0 * patching)
        )
        self._accumulate_host_net(
            host, interval, rx_bytes=demand * 90_000 * interval,
            tx_bytes=demand * 70_000 * interval, loss=0.0,
        )

        runnable = max(1, int(user_cores) + 1)
        blocked = int(host.cores * patching * 2.2 + iowait_cores)
        self._advance_load(host, interval, runnable + blocked)

        host_observation = self._host_observation(
            host, tick, up=1, procs_running=runnable, procs_blocked=blocked,
            fd_used=int(2400 + demand * 40), tcp_inuse=120 + demand * 8,
            cached_fraction=0.5,
        )

        containers: dict[str, dict[str, float]] = {}
        if node.containers:
            # This host is a container worker. The roster and the CPU shares
            # are declared on the estate's ``Node`` so the cAdvisor series,
            # the container log stream and the identities published in
            # ``topology.json`` all describe the same set of workloads.
            for name, share in node.containers:
                host.add(f"container_cpu_{name}", user_cores * share * interval)
                host.add(f"container_periods_{name}", 10.0 * interval)
                throttled = max(0.0, user_cores * share / host.cores - 0.55)
                host.add(f"container_throttled_periods_{name}", throttled * 10.0 * interval)
                host.add(f"container_throttled_seconds_{name}", throttled * 0.9 * interval)
                host.add(f"container_rx_{name}", demand * 40_000 * share * interval)
                host.add(f"container_tx_{name}", demand * 30_000 * share * interval)
                containers[name] = {
                    "cpu_seconds": host.get(f"container_cpu_{name}"),
                    "cfs_periods": host.get(f"container_periods_{name}"),
                    "cfs_throttled_periods": host.get(f"container_throttled_periods_{name}"),
                    "cfs_throttled_seconds": host.get(f"container_throttled_seconds_{name}"),
                    "working_set_bytes": (1.6 * _GIB) * share * (0.6 + 0.4 * demand / 9.0),
                    "usage_bytes": (2.1 * _GIB) * share * (0.6 + 0.4 * demand / 9.0),
                    "rss": (1.2 * _GIB) * share,
                    "failcnt": 0.0,
                    "processes": 12.0 + share * 20.0,
                    "threads": 48.0 + share * 90.0,
                    "threads_max": 4096.0,
                    "rx_bytes": host.get(f"container_rx_{name}"),
                    "tx_bytes": host.get(f"container_tx_{name}"),
                    "rx_errors": 0.0,
                    "restarts": 0.0,
                }

        return NodeObservation(
            entity_id=node.entity_id,
            host=host_observation,
            containers=containers,
            conditions=tuple(sorted(set(conditions))),
        )

    # -- shared host accounting ------------------------------------------
    def _accumulate_latency(
        self,
        host: _HostState,
        interval: int,
        completed_rps: float,
        error_rps: float,
        service_ms: float,
    ) -> dict[str, tuple[tuple[int, ...], int, float]]:
        """Advance the Micrometer request-duration histogram.

        Requests are spread across the bucket boundaries using a log-normal
        with median equal to the current service time. A log-normal is the
        right shape — request latencies are multiplicative, not additive — and
        it means the p95 read off these buckets moves *further* than the mean
        when service time rises, which is exactly the behaviour a mean hides
        and the reason this family exists at all.

        Errors are given their own fast distribution: a rejected or refused
        request fails in milliseconds. That asymmetry matters, because an
        estate whose 5xx responses were as slow as its 2xx ones would make
        "latency up" and "errors up" indistinguishable.
        """
        out: dict[str, tuple[tuple[int, ...], int, float]] = {}
        success = max(0.0, completed_rps) * interval
        errors = max(0.0, error_rps) * interval
        series = (
            ("GET|SUCCESS|200|/api/v2/shipments", success * 0.62, service_ms / 1000.0, 0.55),
            ("POST|SUCCESS|200|/api/v2/shipments", success * 0.38, service_ms / 1000.0 * 1.25, 0.6),
            # Fail-fast path: refusal, not slowness.
            ("GET|SERVER_ERROR|503|/api/v2/shipments", errors, 0.014, 0.8),
        )
        for key, count, median_s, sigma in series:
            host.add(f"lat_{key}_count", count)
            host.add(f"lat_{key}_sum", count * median_s * math.exp(sigma * sigma / 2.0))
            total = host.get(f"lat_{key}_count")
            cumulative: list[int] = []
            for index, bound in enumerate(LATENCY_BUCKETS):
                fraction = _lognormal_cdf(bound, max(median_s, 1e-6), sigma)
                host.add(f"lat_{key}_b{index}", count * fraction)
                cumulative.append(min(int(host.get(f"lat_{key}_b{index}")), int(total)))
            # Buckets are cumulative and must be non-decreasing even after the
            # independent rounding above.
            for index in range(1, len(cumulative)):
                cumulative[index] = max(cumulative[index], cumulative[index - 1])
            out[key] = (tuple(cumulative), int(total), host.get(f"lat_{key}_sum"))
        return out

    def _accumulate_host_cpu(
        self,
        host: _HostState,
        interval: int,
        user_cores: float,
        system_cores: float,
        iowait_cores: float,
    ) -> None:
        """Advance ``node_cpu_seconds_total`` for every core.

        Invariant: across all modes each core accrues exactly ``interval``
        seconds, because that is what the kernel reports. Idle takes the
        remainder, which means idle falls automatically when work rises — no
        separate "set idle low" step exists to get out of step with reality.
        """
        cores = host.cores
        per_core_busy = _clamp((user_cores + system_cores + iowait_cores) / cores, 0.0, 1.0)
        scale = 1.0 if per_core_busy <= 0.0 else min(
            1.0, 1.0 / max(1e-9, (user_cores + system_cores + iowait_cores) / cores)
        )
        user = user_cores * scale / cores
        system = system_cores * scale / cores
        iowait = iowait_cores * scale / cores
        irq = 0.002
        softirq = 0.004
        nice = 0.0005
        steal = 0.0008
        accounted = user + system + iowait + irq + softirq + nice + steal
        idle = max(0.0, 1.0 - accounted)
        shares = {
            "user": user, "system": system, "iowait": iowait, "irq": irq,
            "softirq": softirq, "nice": nice, "steal": steal, "idle": idle,
        }
        for cpu in range(cores):
            # A slight per-core skew: real interrupt affinity is not uniform.
            skew = 1.0 + (0.06 if cpu == 0 else -0.02)
            for mode in _CPU_MODES:
                fraction = shares[mode]
                if mode == "idle":
                    continue
                host.add(f"cpu_{cpu}_{mode}", fraction * skew * interval)
            busy = sum(
                shares[mode] * skew for mode in _CPU_MODES if mode != "idle"
            )
            host.add(f"cpu_{cpu}_idle", max(0.0, 1.0 - busy) * interval)
        host.add("ctx_switches", (user_cores + system_cores) * 9_400.0 * interval)
        host.add("interrupts", (user_cores + system_cores) * 5_100.0 * interval + 120.0 * interval)

    def _accumulate_host_disk(
        self,
        host: _HostState,
        interval: int,
        read_ops: float,
        write_ops: float,
        service_s: float,
    ) -> None:
        host.add("disk_reads", read_ops)
        host.add("disk_writes", write_ops)
        host.add("disk_read_time", read_ops * service_s)
        host.add("disk_write_time", write_ops * service_s * 1.4)
        io_time = min(float(interval), (read_ops + write_ops) * service_s * 0.8)
        host.add("disk_io_time", io_time)
        # Weighted I/O time is the queue-depth integral, so it grows faster
        # than io_time once requests start queueing.
        host.add("disk_io_weighted", (read_ops + write_ops) * service_s * 1.9)
        host.add("pgpgin", read_ops * 42.0)
        host.add("pgpgout", write_ops * 38.0)
        host.add("pgmajfault", read_ops * 0.004)
        host.fs_used_bytes += write_ops * 220.0

    def _accumulate_host_net(
        self, host: _HostState, interval: int, rx_bytes: float, tx_bytes: float, loss: float
    ) -> None:
        host.add("net_rx_bytes", rx_bytes)
        host.add("net_tx_bytes", tx_bytes)
        host.add("net_rx_drop", rx_bytes / 1400.0 * loss)
        host.add("net_tx_drop", tx_bytes / 9600.0 * loss * 0.2)
        host.add("net_rx_errs", rx_bytes / 1400.0 * loss * 0.05)
        host.add("net_tx_errs", 0.0)

    def _advance_load(self, host: _HostState, interval: int, target: float) -> None:
        """Exponentially weighted run-queue averages, as the kernel computes them."""
        for attribute, window in (("load1", 60.0), ("load5", 300.0), ("load15", 900.0)):
            alpha = 1.0 - math.exp(-interval / window)
            current = getattr(host, attribute)
            setattr(host, attribute, current + alpha * (target - current))

    def _host_observation(
        self,
        host: _HostState,
        tick: Tick,
        up: int,
        procs_running: int,
        procs_blocked: int,
        fd_used: int,
        tcp_inuse: float,
        cached_fraction: float,
        heap_bytes: float = 0.0,
    ) -> HostObservation:
        total = float(host.mem_total)
        anonymous = max(heap_bytes * 1.35, total * 0.12) + host.leaked_sockets * 8_192.0
        cached = total * cached_fraction
        buffers = total * 0.03
        free = max(total * 0.02, total - anonymous - cached - buffers)
        available = min(total, free + cached * 0.72 + buffers)

        cpu_seconds = {
            f"{cpu}|{mode}": host.get(f"cpu_{cpu}_{mode}")
            for cpu in range(host.cores)
            for mode in _CPU_MODES
        }

        fs_size = 120.0 * _GIB if host.cores <= 4 else 900.0 * _GIB
        fs_used = min(host.fs_used_bytes, fs_size * 0.97)

        return HostObservation(
            entity_id=host.entity_id,
            up=up,
            cores=host.cores,
            boot_time_s=host.boot_nanos / 1e9,
            cpu_seconds=cpu_seconds,
            load1=host.load1,
            load5=host.load5,
            load15=host.load15,
            procs_running=procs_running,
            procs_blocked=procs_blocked,
            mem_total=int(total),
            mem_free=int(free),
            mem_available=int(available),
            mem_cached=int(cached),
            mem_buffers=int(buffers),
            swap_free=int(4 * _GIB),
            filefd_allocated=int(fd_used),
            filefd_maximum=host.fd_limit,
            sockstat={
                "sockets_used": tcp_inuse + 42.0,
                "tcp_inuse": tcp_inuse,
                "tcp_alloc": tcp_inuse + 18.0,
                "tcp_tw": max(0.0, tcp_inuse * 0.22),
                "tcp_orphan": max(0.0, host.leaked_sockets * 0.05),
                "tcp_mem_bytes": tcp_inuse * 4_096.0,
            },
            disk={
                "reads": host.get("disk_reads"),
                "writes": host.get("disk_writes"),
                "read_time": host.get("disk_read_time"),
                "write_time": host.get("disk_write_time"),
                "io_time": host.get("disk_io_time"),
                "io_weighted": host.get("disk_io_weighted"),
            },
            net={
                "rx_bytes": host.get("net_rx_bytes"),
                "tx_bytes": host.get("net_tx_bytes"),
                "rx_drop": host.get("net_rx_drop"),
                "tx_drop": host.get("net_tx_drop"),
                "rx_errs": host.get("net_rx_errs"),
                "tx_errs": host.get("net_tx_errs"),
            },
            vm={
                "pgpgin": host.get("pgpgin"),
                "pgpgout": host.get("pgpgout"),
                "pgmajfault": host.get("pgmajfault"),
            },
            ctx_switches=host.get("ctx_switches"),
            interrupts=host.get("interrupts"),
            fs={
                "size": fs_size,
                "avail": max(0.0, fs_size - fs_used),
                "files": 7_864_320.0,
                "files_free": max(0.0, 7_864_320.0 - fs_used / 32_768.0),
            },
        )


# ---------------------------------------------------------------------------
# Patching-window shape
# ---------------------------------------------------------------------------
def _patch_intensity(progress: float) -> float:
    """How hard the patch run is working, 0..1.

    Deliberately alarming in the middle: a static threshold on CPU, load,
    iowait or disk throughput fires here, and it should, because this is what
    an OS patch run genuinely looks like. The benign/genuine distinction is not
    "how big is the number" — if it were, the noise-suppression criterion would
    be untestable.
    """
    if progress < 0.08:
        return progress / 0.08 * 0.3        # package download
    if progress < 0.72:
        return 0.75 + 0.25 * math.sin(progress * 21.0)  # install, bursty
    if progress < 0.80:
        return 0.4                          # settling before reboot
    if progress < 0.88:
        return 0.0                          # rebooting, see _patch_is_rebooting
    return max(0.0, 0.55 * (1.0 - (progress - 0.88) / 0.12))  # cold-cache recovery


def _patch_is_rebooting(progress: float) -> bool:
    """The scrape window during which the host simply does not answer."""
    return 0.80 <= progress < 0.88
