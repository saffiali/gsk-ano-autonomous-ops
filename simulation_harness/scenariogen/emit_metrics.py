"""Turn simulator observations into exposition documents, one per scrape target.

Ruling IR-02: the generator stands in for the **exporters**, so everything it
writes is pre-attach. No document here contains ``project_id``, ``location``,
``cluster``, ``namespace``, ``job`` or ``instance`` — those six live in
``targets.json`` and are attached at ingest, exactly as the Managed Prometheus
collector attaches them. ``MetricSample`` refuses them structurally, so this is
enforced by the type rather than by care.

Every metric name, type, help string and label-key set comes from
:mod:`ano.telemetry.exporters`, which was transcribed from real exporter
output. ``FamilySpec.sample`` raises if the label keys do not match the
declaration exactly, so a typo here is a test failure, not a silently
mis-shaped corpus.

Unreachable hosts
-----------------
When a host is rebooting mid-patch its exporter does not answer. The scrape
fails and the only thing stored is the collector-synthesised ``up 0``. That is
reproduced literally: the document for that target contains one line. The
resulting gaps in every other series are real, they happen during a **benign**
event, and any consumer that assumes uninterrupted series will notice.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from ano.telemetry.exporters import (
    APACHE_SCOREBOARD_STATES,
    CPU_MODES,
    JVM_THREAD_STATES,
    family_spec,
    metadata_for,
)
from ano.telemetry.prometheus import (
    MetricSample,
    PrometheusTarget,
    histogram_samples,
    serialize_exposition,
)

from scenariogen.estate import PROJECT_ID, ZONE_FOR_SITE, Estate
from scenariogen.simulate import (
    AppObservation,
    DatabaseObservation,
    HostObservation,
    NodeObservation,
    SwitchObservation,
    TickObservation,
)

__all__ = [
    "Target",
    "build_targets",
    "render_tick",
    "CLUSTER",
]

#: The GMP ``cluster`` label. One logical cluster spanning both sites.
CLUSTER = "gsk-ano-uk"

#: Block device, network device and filesystem the hosts are built from.
_DISK_DEVICE = "sda"
_NET_DEVICE = "eth0"
_FS_DEVICE = "/dev/sda1"
_FS_TYPE = "ext4"
_FS_MOUNT = "/"

#: Collectors node_exporter reports success for.
_COLLECTORS = (
    "cpu", "diskstats", "filefd", "filesystem", "loadavg", "meminfo",
    "netdev", "sockstat", "stat", "vmstat",
)

_JVM_POOLS = ("PS Eden Space", "PS Old Gen", "Metaspace")
_JVM_BUFFER_POOLS = ("direct", "mapped")


@dataclass(frozen=True, slots=True)
class Target:
    """One scrape endpoint.

    Attributes:
        job: The exporter family. Identifies *what* is being scraped.
        instance: The C1 ``entity_id``. IR-02 fixes this equality, so entity
            resolution downstream is a lookup rather than a heuristic.
        exporter: Which catalogue block the samples come from.
        entity_kind: The C1 entity kind, carried in ``targets.json`` as a
            convenience for consumers. Not a metric label.
        location: GCP zone.
        namespace: Logical grouping; the site.
    """

    job: str
    instance: str
    exporter: str
    entity_kind: str
    location: str
    namespace: str

    @property
    def key(self) -> str:
        """Stable filename stem, ``<job>__<instance>``."""
        return f"{self.job}__{self.instance}"

    def prometheus_target(self) -> PrometheusTarget:
        return PrometheusTarget(
            project_id=PROJECT_ID,
            location=self.location,
            cluster=CLUSTER,
            namespace=self.namespace,
            job=self.job,
            instance=self.instance,
        )

    def to_dict(self) -> dict[str, str]:
        record = self.prometheus_target().to_dict()
        record["exporter"] = self.exporter
        record["entity_kind"] = self.entity_kind
        return record


def _namespace_for(site: str) -> str:
    return "estate-" + site.split("-", 1)[1]


def build_targets(estate: Estate) -> tuple[Target, ...]:
    """Every scrape endpoint in the estate, in a stable order.

    An application host exposes three endpoints, because it really would: the
    OS exporter, the Apache status exporter and the JMX exporter in front of
    Tomcat. They share an ``instance`` because they share an entity.
    """
    targets: list[Target] = []
    for server in estate.app_servers:
        namespace = _namespace_for(server.site)
        zone = ZONE_FOR_SITE[server.site]
        for job, exporter in (("node", "node"), ("apache", "apache"), ("tomcat", "tomcat")):
            targets.append(
                Target(job, server.entity_id, exporter, "app_server", zone, namespace)
            )
    for database in estate.databases:
        namespace = _namespace_for(database.site)
        zone = ZONE_FOR_SITE[database.site]
        targets.append(Target("node", database.entity_id, "node", "database", zone, namespace))
        targets.append(
            Target("mysqld", database.entity_id, "mysql", "database", zone, namespace)
        )
    for switch in estate.switches:
        namespace = _namespace_for(switch.site)
        zone = ZONE_FOR_SITE[switch.site]
        targets.append(Target("snmp", switch.entity_id, "snmp", "switch", zone, namespace))
    for node in estate.nodes:
        namespace = _namespace_for(node.site)
        zone = ZONE_FOR_SITE[node.site]
        targets.append(Target("node", node.entity_id, "node", "node", zone, namespace))
        if node.role == "batch":
            targets.append(
                Target("cadvisor", node.entity_id, "cadvisor", "node", zone, namespace)
            )
    return tuple(targets)


# ---------------------------------------------------------------------------
# Per-exporter sample builders
# ---------------------------------------------------------------------------
def _sample(name: str, value: float, **labels: str) -> MetricSample:
    return family_spec(name).sample(value, **labels)


def _node_samples(host: HostObservation) -> list[MetricSample]:
    """A node_exporter ``/metrics`` body."""
    if not host.up:
        # The scrape failed. Only the synthesised liveness sample survives.
        return [_sample("up", 0.0)]

    out: list[MetricSample] = [_sample("up", 1.0)]
    for key in sorted(host.cpu_seconds):
        cpu, mode = key.split("|", 1)
        out.append(_sample("node_cpu_seconds_total", host.cpu_seconds[key], cpu=cpu, mode=mode))
    out.append(_sample("node_load1", host.load1))
    out.append(_sample("node_load5", host.load5))
    out.append(_sample("node_load15", host.load15))
    out.append(_sample("node_memory_MemTotal_bytes", host.mem_total))
    out.append(_sample("node_memory_MemAvailable_bytes", host.mem_available))
    out.append(_sample("node_memory_MemFree_bytes", host.mem_free))
    out.append(_sample("node_memory_Cached_bytes", host.mem_cached))
    out.append(_sample("node_memory_Buffers_bytes", host.mem_buffers))
    out.append(_sample("node_memory_SwapFree_bytes", host.swap_free))

    fs_labels = {"device": _FS_DEVICE, "fstype": _FS_TYPE, "mountpoint": _FS_MOUNT}
    out.append(_sample("node_filesystem_size_bytes", host.fs["size"], **fs_labels))
    out.append(_sample("node_filesystem_avail_bytes", host.fs["avail"], **fs_labels))
    out.append(_sample("node_filesystem_files", host.fs["files"], **fs_labels))
    out.append(_sample("node_filesystem_files_free", host.fs["files_free"], **fs_labels))

    out.append(_sample("node_disk_reads_completed_total", host.disk["reads"], device=_DISK_DEVICE))
    out.append(_sample("node_disk_writes_completed_total", host.disk["writes"], device=_DISK_DEVICE))
    out.append(_sample("node_disk_read_time_seconds_total", host.disk["read_time"], device=_DISK_DEVICE))
    out.append(_sample("node_disk_write_time_seconds_total", host.disk["write_time"], device=_DISK_DEVICE))
    out.append(_sample("node_disk_io_time_seconds_total", host.disk["io_time"], device=_DISK_DEVICE))
    out.append(
        _sample("node_disk_io_time_weighted_seconds_total", host.disk["io_weighted"], device=_DISK_DEVICE)
    )

    out.append(_sample("node_sockstat_sockets_used", host.sockstat["sockets_used"]))
    out.append(_sample("node_sockstat_TCP_inuse", host.sockstat["tcp_inuse"]))
    out.append(_sample("node_sockstat_TCP_alloc", host.sockstat["tcp_alloc"]))
    out.append(_sample("node_sockstat_TCP_tw", host.sockstat["tcp_tw"]))
    out.append(_sample("node_sockstat_TCP_orphan", host.sockstat["tcp_orphan"]))
    out.append(_sample("node_sockstat_TCP_mem_bytes", host.sockstat["tcp_mem_bytes"]))

    out.append(_sample("node_filefd_allocated", host.filefd_allocated))
    out.append(_sample("node_filefd_maximum", host.filefd_maximum))
    out.append(_sample("node_context_switches_total", host.ctx_switches))
    out.append(_sample("node_intr_total", host.interrupts))
    out.append(_sample("node_procs_running", host.procs_running))
    out.append(_sample("node_procs_blocked", host.procs_blocked))
    out.append(_sample("node_vmstat_pgmajfault", host.vm["pgmajfault"]))
    out.append(_sample("node_vmstat_pgpgin", host.vm["pgpgin"]))
    out.append(_sample("node_vmstat_pgpgout", host.vm["pgpgout"]))

    out.append(_sample("node_network_receive_bytes_total", host.net["rx_bytes"], device=_NET_DEVICE))
    out.append(_sample("node_network_transmit_bytes_total", host.net["tx_bytes"], device=_NET_DEVICE))
    out.append(_sample("node_network_receive_errs_total", host.net["rx_errs"], device=_NET_DEVICE))
    out.append(_sample("node_network_transmit_errs_total", host.net["tx_errs"], device=_NET_DEVICE))
    out.append(_sample("node_network_receive_drop_total", host.net["rx_drop"], device=_NET_DEVICE))
    out.append(_sample("node_network_transmit_drop_total", host.net["tx_drop"], device=_NET_DEVICE))

    out.append(_sample("node_boot_time_seconds", host.boot_time_s))
    for collector in _COLLECTORS:
        out.append(_sample("node_scrape_collector_success", 1.0, collector=collector))
    return out


def _apache_samples(app: AppObservation) -> list[MetricSample]:
    """An apache_exporter ``/metrics`` body."""
    if not app.host.up:
        return [_sample("up", 0.0)]
    values = app.apache
    out = [
        _sample("up", 1.0),
        _sample("apache_up", values["up"]),
        _sample("apache_accesses_total", values["accesses"]),
        _sample("apache_sent_kilobytes_total", values["sent_kilobytes"]),
        _sample("apache_duration_ms_total", values["duration_ms"]),
        _sample("apache_cpu_load", values["cpu_load"]),
        _sample("apache_uptime_seconds_total", values["uptime"]),
        _sample("apache_workers", values["workers_busy"], state="busy"),
        _sample("apache_workers", values["workers_idle"], state="idle"),
    ]
    scoreboard = {
        "open": values["sb_open"],
        "waiting": values["sb_waiting"],
        "startup": values["sb_startup"],
        "read": values["sb_read"],
        "reply": values["sb_reply"],
        "keepalive": values["sb_keepalive"],
        "dns": values["sb_dns"],
        "closing": values["sb_closing"],
        "logging": values["sb_logging"],
        "graceful_stop": values["sb_graceful_stop"],
        "idle_cleanup": values["sb_idle_cleanup"],
    }
    for state in APACHE_SCOREBOARD_STATES:
        out.append(_sample("apache_scoreboard", scoreboard[state], state=state))
    return out


def _tomcat_samples(app: AppObservation, port: str, protocol: str) -> list[MetricSample]:
    """A jmx_exporter body: Tomcat connector, session and JVM series together."""
    if not app.host.up or not app.jvm_up:
        # Either the host is gone or the JVM is mid-restart. Both look
        # identical from the scraper's point of view: no answer.
        return [_sample("up", 0.0)]

    connector = {"port": port, "protocol": protocol}
    values = app.tomcat
    jvm = app.jvm
    out = [
        _sample("up", 1.0),
        _sample("tomcat_requestcount_total", values["requestcount"], **connector),
        _sample("tomcat_errorcount_total", values["errorcount"], **connector),
        _sample("tomcat_processingtime_total", values["processingtime"], **connector),
        _sample("tomcat_maxtime_total", values["maxtime"], **connector),
        _sample("tomcat_bytessent_total", values["bytessent"], **connector),
        _sample("tomcat_bytesreceived_total", values["bytesreceived"], **connector),
        _sample("tomcat_threadpool_currentthreadsbusy", values["currentthreadsbusy"], **connector),
        _sample("tomcat_threadpool_currentthreadcount", values["currentthreadcount"], **connector),
        _sample("tomcat_threadpool_connectioncount", values["connectioncount"], **connector),
        _sample("tomcat_threadpool_keepalivecount", values["keepalivecount"], **connector),
    ]
    session = {"context": "/", "host": "localhost"}
    out.append(_sample("tomcat_session_sessioncounter_total", values["session_counter"], **session))
    out.append(_sample("tomcat_session_expiredsessions_total", values["session_expired"], **session))
    out.append(_sample("tomcat_session_rejectedsessions_total", values["session_rejected"], **session))

    out.append(_sample("jvm_memory_bytes_used", jvm["heap_used"], area="heap"))
    out.append(_sample("jvm_memory_bytes_committed", jvm["heap_committed"], area="heap"))
    out.append(_sample("jvm_memory_bytes_max", jvm["heap_max"], area="heap"))
    out.append(_sample("jvm_memory_bytes_used", jvm["nonheap_used"], area="nonheap"))
    out.append(_sample("jvm_memory_bytes_committed", jvm["nonheap_committed"], area="nonheap"))
    out.append(_sample("jvm_memory_bytes_max", jvm["nonheap_max"], area="nonheap"))

    pool_used = {
        "PS Eden Space": jvm["eden_used"],
        "PS Old Gen": jvm["old_used"],
        "Metaspace": jvm["metaspace_used"],
    }
    pool_max = {
        "PS Eden Space": jvm["eden_max"],
        "PS Old Gen": jvm["old_max"],
        "Metaspace": jvm["metaspace_max"],
    }
    for pool in _JVM_POOLS:
        out.append(_sample("jvm_memory_pool_bytes_used", pool_used[pool], pool=pool))
        out.append(_sample("jvm_memory_pool_bytes_max", pool_max[pool], pool=pool))

    out.append(_sample("jvm_threads_current", jvm["threads_current"]))
    out.append(_sample("jvm_threads_daemon", jvm["threads_daemon"]))
    out.append(_sample("jvm_threads_peak", jvm["threads_peak"]))
    out.append(_sample("jvm_threads_started_total", jvm["threads_started"]))
    out.append(_sample("jvm_threads_deadlocked", jvm["threads_deadlocked"]))
    for state in JVM_THREAD_STATES:
        out.append(_sample("jvm_threads_state", app.thread_states[state], state=state))
    out.append(_sample("jvm_classes_currently_loaded", jvm["classes_loaded"]))
    out.append(_sample("jvm_buffer_pool_used_bytes", jvm["buffer_direct_used"], pool="direct"))
    out.append(_sample("jvm_buffer_pool_used_bytes", jvm["buffer_mapped_used"], pool="mapped"))

    # jvm_gc_collection_seconds is a summary: the exporter emits only the
    # _count and _sum children, with no quantiles.
    for collector in sorted(app.gc):
        count, seconds = app.gc[collector]
        out.append(MetricSample("jvm_gc_collection_seconds_count", {"gc": collector}, count))
        out.append(MetricSample("jvm_gc_collection_seconds_sum", {"gc": collector}, seconds))

    req_count = int(values["requestcount"])
    proc_time_s = float(values["processingtime"]) / 1000.0
    bounds = (0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
    counts = [int(req_count * f) for f in (0.35, 0.60, 0.80, 0.90, 0.96, 0.99, 1.0, 1.0)]
    labels = {"method": "GET", "outcome": "SUCCESS", "status": "200", "uri": "/api/v1/resource"}
    out.extend(
        histogram_samples(
            "http_server_requests_seconds",
            labels,
            bounds,
            counts,
            req_count,
            proc_time_s,
        )
    )
    return out


def _mysql_samples(database: DatabaseObservation) -> list[MetricSample]:
    """A mysqld_exporter ``/metrics`` body."""
    if not database.host.up or not database.db_up:
        return [_sample("up", 0.0)]
    values = database.mysql
    out = [
        _sample("up", 1.0),
        _sample("mysql_up", 1.0),
        _sample("mysql_global_status_threads_connected", values["threads_connected"]),
        _sample("mysql_global_status_threads_running", values["threads_running"]),
        _sample("mysql_global_status_threads_cached", values["threads_cached"]),
        _sample("mysql_global_status_max_used_connections", values["max_used_connections"]),
        _sample("mysql_global_variables_max_connections", values["max_connections"]),
        _sample("mysql_global_status_queries", values["queries"]),
        _sample("mysql_global_status_questions", values["questions"]),
        _sample("mysql_global_status_slow_queries", values["slow_queries"]),
        _sample("mysql_global_status_connections", values["connections"]),
        _sample("mysql_global_status_aborted_connects", values["aborted_connects"]),
        _sample("mysql_global_status_aborted_clients", values["aborted_clients"]),
        _sample("mysql_global_status_innodb_row_lock_waits", values["row_lock_waits"]),
        _sample("mysql_global_status_innodb_row_lock_current_waits", values["row_lock_current_waits"]),
        _sample("mysql_global_status_innodb_row_lock_time", values["row_lock_time"]),
        _sample("mysql_global_status_innodb_row_lock_time_avg", values["row_lock_time_avg"]),
        _sample("mysql_global_status_innodb_row_lock_time_max", values["row_lock_time_max"]),
        _sample("mysql_global_status_table_locks_immediate", values["table_locks_immediate"]),
        _sample("mysql_global_status_table_locks_waited", values["table_locks_waited"]),
        _sample("mysql_global_status_innodb_log_waits", values["innodb_log_waits"]),
        _sample("mysql_global_status_innodb_buffer_pool_wait_free", values["buffer_pool_wait_free"]),
    ]
    for command in ("commit", "delete", "insert", "select", "update"):
        out.append(
            _sample("mysql_global_status_commands_total", values[f"command_{command}"], command=command)
        )
    running = values["threads_running"]
    connected = values["threads_connected"]
    out.append(_sample("mysql_info_schema_threads", running, state="executing"))
    out.append(_sample("mysql_info_schema_threads", max(0.0, connected - running), state="sleeping"))
    out.append(
        _sample("mysql_info_schema_threads", values["row_lock_current_waits"], state="updating")
    )
    return out


def _snmp_samples(switch: SwitchObservation) -> list[MetricSample]:
    """An snmp_exporter ``/metrics`` body for one device.

    All of a device's interfaces appear in one document, distinguished by the
    exporter's own ``ifName`` / ``ifIndex`` labels. IR-02 puts the device in
    ``instance``; the port is identified within the document, which is exactly
    how a real SNMP scrape is shaped.
    """
    out: list[MetricSample] = [_sample("up", 1.0)]
    for observation in switch.ports:
        labels = observation.port.labels()
        counters = observation.counters
        out.append(_sample("ifAdminStatus", observation.admin_status, **labels))
        out.append(_sample("ifOperStatus", observation.oper_status, **labels))
        out.append(_sample("ifHighSpeed", observation.port.speed_mbit, **labels))
        out.append(_sample("ifLastChange", observation.last_change_s * 100.0, **labels))
        out.append(_sample("ifHCInOctets", counters["in_octets"], **labels))
        out.append(_sample("ifHCOutOctets", counters["out_octets"], **labels))
        out.append(_sample("ifHCInUcastPkts", counters["in_pkts"], **labels))
        out.append(_sample("ifHCOutUcastPkts", counters["out_pkts"], **labels))
        out.append(_sample("ifInDiscards", counters["in_discards"], **labels))
        out.append(_sample("ifOutDiscards", counters["out_discards"], **labels))
        out.append(_sample("ifInErrors", counters["in_errors"], **labels))
        out.append(_sample("ifOutErrors", counters["out_errors"], **labels))

        neighbour = {
            "ifIndex": str(observation.port.if_index),
            "ospfNbrIpAddr": _ospf_ip(observation.port),
            "ospfNbrRtrId": _ospf_router(observation.port),
        }
        out.append(_sample("ospfNbrState", observation.ospf_state, **neighbour))
        out.append(_sample("ospfNbrEvents", counters["ospf_events"], **neighbour))

    out.append(_sample("snmp_scrape_duration_seconds", switch.scrape_duration_s))
    out.append(_sample("snmp_scrape_pdus_returned", switch.scrape_pdus))
    out.append(_sample("snmp_request_errors_total", switch.request_errors))
    return out


def _ospf_ip(port) -> str:
    """Neighbour address on this interface's point-to-point link."""
    third = 254 if port.switch_id.startswith("gsk-sw-lon") else 252
    base = 42 if port.switch_id.startswith("gsk-sw-lon") else 44
    return f"10.{base}.{third}.{port.if_index % 250}"


def _ospf_router(port) -> str:
    base = 42 if port.switch_id.startswith("gsk-sw-lon") else 44
    return f"10.{base}.0.{port.if_index % 250}"


def _cadvisor_samples(node: NodeObservation) -> list[MetricSample]:
    """A cAdvisor ``/metrics`` body for the container worker node.

    Note the absence of a ``namespace`` label: it is one of the six the GMP
    collector owns, and a self-emitted one would be relabelled to
    ``exported_namespace``. See the note in ``ano/telemetry/exporters.py``.
    """
    if not node.host.up:
        return [_sample("up", 0.0)]
    out: list[MetricSample] = [_sample("up", 1.0)]
    for name in sorted(node.containers):
        values = node.containers[name]
        pod = f"{name}-0"
        cl = {"container": name, "pod": pod}
        out.append(_sample("container_cpu_usage_seconds_total", values["cpu_seconds"], **cl))
        out.append(_sample("container_cpu_cfs_periods_total", values["cfs_periods"], **cl))
        out.append(
            _sample("container_cpu_cfs_throttled_periods_total", values["cfs_throttled_periods"], **cl)
        )
        out.append(
            _sample("container_cpu_cfs_throttled_seconds_total", values["cfs_throttled_seconds"], **cl)
        )
        out.append(_sample("container_memory_working_set_bytes", values["working_set_bytes"], **cl))
        out.append(_sample("container_memory_usage_bytes", values["usage_bytes"], **cl))
        out.append(_sample("container_memory_rss", values["rss"], **cl))
        out.append(_sample("container_memory_failcnt", values["failcnt"], **cl))
        out.append(_sample("container_processes", values["processes"], **cl))
        out.append(_sample("container_threads", values["threads"], **cl))
        out.append(_sample("container_threads_max", values["threads_max"], **cl))
        out.append(_sample("kube_pod_container_status_restarts_total", values["restarts"], **cl))
        il = {"interface": "eth0", "pod": pod}
        out.append(_sample("container_network_receive_bytes_total", values["rx_bytes"], **il))
        out.append(_sample("container_network_transmit_bytes_total", values["tx_bytes"], **il))
        out.append(_sample("container_network_receive_errors_total", values["rx_errors"], **il))
    return out


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------
def _samples_for(target: Target, observation: TickObservation, estate: Estate) -> list[MetricSample]:
    if target.entity_kind == "app_server":
        app = next(a for a in observation.apps if a.entity_id == target.instance)
        if target.job == "node":
            return _node_samples(app.host)
        if target.job == "apache":
            return _apache_samples(app)
        server = estate.app_server(target.instance)
        return _tomcat_samples(app, server.http_port, server.protocol)
    if target.entity_kind == "database":
        database = next(d for d in observation.databases if d.entity_id == target.instance)
        if target.job == "node":
            return _node_samples(database.host)
        return _mysql_samples(database)
    if target.entity_kind == "switch":
        switch = next(s for s in observation.switches if s.entity_id == target.instance)
        return _snmp_samples(switch)
    node = next(n for n in observation.nodes if n.entity_id == target.instance)
    if target.job == "node":
        return _node_samples(node.host)
    return _cadvisor_samples(node)


def render_tick(
    targets: Iterable[Target], observation: TickObservation, estate: Estate
) -> dict[str, str]:
    """Render one scrape of every target to exposition text.

    Returns:
        ``target.key`` → the complete exposition document for that target at
        this instant. Each document is independently parseable and carries no
        target labels.
    """
    documents: dict[str, str] = {}
    for target in targets:
        samples = _samples_for(target, observation, estate)
        metadata = metadata_for(sorted({_base_family(s.name) for s in samples}))
        documents[target.key] = serialize_exposition(samples, metadata)
    return documents


def _base_family(name: str) -> str:
    """Strip the summary/histogram child suffix to get the family name."""
    for suffix in ("_count", "_sum", "_bucket"):
        if name.endswith(suffix):
            stem = name[: -len(suffix)]
            try:
                family_spec(stem)
            except KeyError:
                continue
            return stem
    return name
