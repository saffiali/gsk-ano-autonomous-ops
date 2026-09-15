"""Catalogue of **real** exporter metric families, names, types and label keys.

Everything here is transcribed from the verified sources catalogued in
``PROMETHEUS_SPEC.md`` Part 3 — a genuine captured ``/metrics`` payload for
node_exporter, the apache_exporter README, Google's own Managed Prometheus
Tomcat/JMX integration ConfigMap, a real mysqld_exporter capture, and an SNMP
metrics dictionary. Nothing is invented, because invented names are exactly
what makes synthetic telemetry read as fake.

Conventions preserved on purpose, all of which a plausible-but-wrong
implementation gets subtly different:

* ``node_memory_MemAvailable_bytes`` keeps the **CamelCase** middle segment —
  it mirrors ``/proc/meminfo`` keys verbatim.
* ``node_sockstat_TCP_alloc`` keeps the **UPPERCASE** protocol segment.
* ``tomcat_*`` names are **all-lowercase with no word separators**, because the
  jmx_exporter ConfigMap sets ``lowercaseOutputName: true``.
* SNMP names keep their MIB **camelCase** (``ifHCInOctets``, ``ospfNbrState``).
* Most ``mysql_global_status_*`` families are typed **untyped** and share the
  generic help text ``"Generic metric from SHOW GLOBAL STATUS."`` — that is
  what the real exporter emits, and reproducing it is more faithful than
  tidying it up.
* No family here carries ``project_id`` / ``location`` / ``cluster`` /
  ``namespace`` / ``job`` / ``instance``: Managed Prometheus attaches those.

One deliberate departure from a literal transcription
------------------------------------------------------
Real cAdvisor emits a ``namespace`` label. ``namespace`` is also one of the six
``prometheus_target`` labels the GMP collector attaches at export time
(``PROMETHEUS_SPEC.md`` §2.2), and rule 17 of the consolidated validator list
forbids an exporter payload from containing it. Real GMP resolves the collision
by renaming the exported one to ``exported_namespace``. Rather than model the
collision, the cAdvisor families here identify a workload by ``pod`` and
``container`` only, and the namespace is carried in the
:class:`~ano.telemetry.prometheus.PrometheusTarget` descriptor emitted beside
the exposition text — which is where GMP actually puts it. This is recorded
here rather than left as an unexplained difference from upstream cAdvisor.


Numeric encodings that downstream analysis depends on:

* ``ifOperStatus``: 1=up, 2=down, 3=testing, 4=unknown, 5=dormant,
  6=notPresent, 7=lowerLayerDown.
* ``ospfNbrState``: 1=down, 2=attempt, 3=init, 4=twoWay, 5=exchangeStart,
  6=exchange, 7=loading, 8=full.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from ano.telemetry.prometheus import MetricFamily, MetricSample

__all__ = [
    "FamilySpec",
    "NODE_EXPORTER",
    "CADVISOR",
    "APACHE_EXPORTER",
    "TOMCAT_EXPORTER",
    "JVM_EXPORTER",
    "MYSQLD_EXPORTER",
    "SNMP_EXPORTER",
    "ALL_FAMILIES",
    "IF_OPER_STATUS",
    "OSPF_NBR_STATE",
    "CPU_MODES",
    "APACHE_SCOREBOARD_STATES",
    "JVM_THREAD_STATES",
    "family_spec",
    "metadata_for",
    "build_families",
]


@dataclass(frozen=True, slots=True)
class FamilySpec:
    """Declaration of one exporter metric family."""

    name: str
    type: str
    help: str
    #: Label keys this family carries. Empty tuple means a bare scalar series.
    label_keys: tuple[str, ...] = ()
    #: Which exporter emits it — used to route samples to the right target.
    exporter: str = ""

    def sample(self, value: float, **labels: str) -> MetricSample:
        """Build a sample, checking the label keys match the declaration."""
        provided = set(labels)
        declared = set(self.label_keys)
        if provided != declared:
            raise ValueError(
                f"{self.name}: expected label keys {sorted(declared)}, "
                f"got {sorted(provided)}"
            )
        return MetricSample(self.name, labels, float(value))


def _spec(exporter: str, rows: Sequence[tuple[str, str, str, tuple[str, ...]]]):
    return tuple(
        FamilySpec(name, kind, help_text, labels, exporter)
        for name, kind, help_text, labels in rows
    )


#: ``node_cpu_seconds_total`` mode values, from the real capture.
CPU_MODES: tuple[str, ...] = (
    "idle",
    "iowait",
    "irq",
    "nice",
    "softirq",
    "steal",
    "system",
    "user",
)

#: ``apache_scoreboard`` states as emitted by apache_exporter (NOT collectd's
#: slightly different vocabulary — the two genuinely differ).
APACHE_SCOREBOARD_STATES: tuple[str, ...] = (
    "open",
    "waiting",
    "startup",
    "read",
    "reply",
    "keepalive",
    "dns",
    "closing",
    "logging",
    "graceful_stop",
    "idle_cleanup",
)

JVM_THREAD_STATES: tuple[str, ...] = (
    "NEW",
    "RUNNABLE",
    "BLOCKED",
    "WAITING",
    "TIMED_WAITING",
    "TERMINATED",
)

#: IF-MIB ifOperStatus enumeration.
IF_OPER_STATUS: Mapping[str, int] = {
    "up": 1,
    "down": 2,
    "testing": 3,
    "unknown": 4,
    "dormant": 5,
    "notPresent": 6,
    "lowerLayerDown": 7,
}

#: OSPF-MIB ospfNbrState enumeration.
OSPF_NBR_STATE: Mapping[str, int] = {
    "down": 1,
    "attempt": 2,
    "init": 3,
    "twoWay": 4,
    "exchangeStart": 5,
    "exchange": 6,
    "loading": 7,
    "full": 8,
}

_GENERIC_STATUS_HELP = "Generic metric from SHOW GLOBAL STATUS."
_GENERIC_VARIABLE_HELP = "Generic gauge metric from SHOW GLOBAL VARIABLES."

NODE_EXPORTER: tuple[FamilySpec, ...] = _spec(
    "node",
    [
        ("node_cpu_seconds_total", "counter", "Seconds the cpus spent in each mode.", ("cpu", "mode")),
        ("node_load1", "gauge", "1m load average.", ()),
        ("node_load5", "gauge", "5m load average.", ()),
        ("node_load15", "gauge", "15m load average.", ()),
        ("node_memory_MemTotal_bytes", "gauge", "Memory information field MemTotal_bytes.", ()),
        ("node_memory_MemAvailable_bytes", "gauge", "Memory information field MemAvailable_bytes.", ()),
        ("node_memory_MemFree_bytes", "gauge", "Memory information field MemFree_bytes.", ()),
        ("node_memory_Cached_bytes", "gauge", "Memory information field Cached_bytes.", ()),
        ("node_memory_Buffers_bytes", "gauge", "Memory information field Buffers_bytes.", ()),
        ("node_memory_SwapFree_bytes", "gauge", "Memory information field SwapFree_bytes.", ()),
        ("node_filesystem_avail_bytes", "gauge", "Filesystem space available to non-root users in bytes.", ("device", "fstype", "mountpoint")),
        ("node_filesystem_size_bytes", "gauge", "Filesystem size in bytes.", ("device", "fstype", "mountpoint")),
        ("node_filesystem_files", "gauge", "Filesystem total file nodes.", ("device", "fstype", "mountpoint")),
        ("node_filesystem_files_free", "gauge", "Filesystem total free file nodes.", ("device", "fstype", "mountpoint")),
        ("node_disk_io_time_seconds_total", "counter", "Total seconds spent doing I/Os.", ("device",)),
        ("node_disk_io_time_weighted_seconds_total", "counter", "The weighted # of seconds spent doing I/Os.", ("device",)),
        ("node_disk_read_time_seconds_total", "counter", "The total number of seconds spent by all reads.", ("device",)),
        ("node_disk_write_time_seconds_total", "counter", "This is the total number of seconds spent by all writes.", ("device",)),
        ("node_disk_reads_completed_total", "counter", "The total number of reads completed successfully.", ("device",)),
        ("node_disk_writes_completed_total", "counter", "The total number of writes completed successfully.", ("device",)),
        ("node_sockstat_sockets_used", "gauge", "Number of IPv4 sockets in use.", ()),
        ("node_sockstat_TCP_inuse", "gauge", "Number of TCP sockets in state inuse.", ()),
        ("node_sockstat_TCP_alloc", "gauge", "Number of TCP sockets in state alloc.", ()),
        ("node_sockstat_TCP_tw", "gauge", "Number of TCP sockets in state tw.", ()),
        ("node_sockstat_TCP_orphan", "gauge", "Number of TCP sockets in state orphan.", ()),
        ("node_sockstat_TCP_mem_bytes", "gauge", "Number of TCP sockets in state mem_bytes.", ()),
        ("node_filefd_allocated", "gauge", "File descriptor statistics: allocated.", ()),
        ("node_filefd_maximum", "gauge", "File descriptor statistics: maximum.", ()),
        ("node_context_switches_total", "counter", "Total number of context switches.", ()),
        ("node_intr_total", "counter", "Total number of interrupts serviced.", ()),
        ("node_procs_running", "gauge", "Number of processes in runnable state.", ()),
        ("node_procs_blocked", "gauge", "Number of processes blocked waiting for I/O to complete.", ()),
        ("node_vmstat_pgmajfault", "counter", "/proc/vmstat information field pgmajfault.", ()),
        ("node_vmstat_pgpgin", "counter", "/proc/vmstat information field pgpgin.", ()),
        ("node_vmstat_pgpgout", "counter", "/proc/vmstat information field pgpgout.", ()),
        ("node_network_receive_bytes_total", "counter", "Network device statistic receive_bytes.", ("device",)),
        ("node_network_transmit_bytes_total", "counter", "Network device statistic transmit_bytes.", ("device",)),
        ("node_network_receive_errs_total", "counter", "Network device statistic receive_errs.", ("device",)),
        ("node_network_transmit_errs_total", "counter", "Network device statistic transmit_errs.", ("device",)),
        ("node_network_receive_drop_total", "counter", "Network device statistic receive_drop.", ("device",)),
        ("node_network_transmit_drop_total", "counter", "Network device statistic transmit_drop.", ("device",)),
        ("node_boot_time_seconds", "gauge", "Node boot time, in unixtime.", ()),
        ("node_scrape_collector_success", "gauge", "node_exporter: Whether a collector succeeded.", ("collector",)),
        ("up", "gauge", "1 if the target is reachable, 0 otherwise.", ()),
    ],
)

CADVISOR: tuple[FamilySpec, ...] = _spec(
    "cadvisor",
    [
        ("container_cpu_usage_seconds_total", "counter", "Cumulative cpu time consumed in seconds.", ("container", "pod")),
        ("container_cpu_cfs_periods_total", "counter", "Number of elapsed enforcement period intervals.", ("container", "pod")),
        ("container_cpu_cfs_throttled_periods_total", "counter", "Number of throttled period intervals.", ("container", "pod")),
        ("container_cpu_cfs_throttled_seconds_total", "counter", "Total time duration the container has been throttled.", ("container", "pod")),
        ("container_memory_working_set_bytes", "gauge", "Current working set in bytes.", ("container", "pod")),
        ("container_memory_usage_bytes", "gauge", "Current memory usage in bytes, including all memory regardless of when it was accessed.", ("container", "pod")),
        ("container_memory_rss", "gauge", "Size of RSS in bytes.", ("container", "pod")),
        ("container_memory_failcnt", "counter", "Number of memory usage hits limits.", ("container", "pod")),
        ("container_processes", "gauge", "Number of processes running inside the container.", ("container", "pod")),
        ("container_threads", "gauge", "Number of threads running inside the container.", ("container", "pod")),
        ("container_threads_max", "gauge", "Maximum number of threads allowed inside the container.", ("container", "pod")),
        ("container_network_receive_bytes_total", "counter", "Cumulative count of bytes received.", ("interface", "pod")),
        ("container_network_transmit_bytes_total", "counter", "Cumulative count of bytes transmitted.", ("interface", "pod")),
        ("container_network_receive_errors_total", "counter", "Cumulative count of errors encountered while receiving.", ("interface", "pod")),
        ("kube_pod_container_status_restarts_total", "counter", "The number of container restarts per container.", ("container", "pod")),
    ],
)

APACHE_EXPORTER: tuple[FamilySpec, ...] = _spec(
    "apache",
    [
        ("apache_up", "gauge", "Could the apache server be reached", ()),
        ("apache_accesses_total", "counter", "Current total apache accesses (*)", ()),
        ("apache_sent_kilobytes_total", "counter", "Current total kbytes sent (*)", ()),
        ("apache_duration_ms_total", "gauge", "Total duration of all registered requests", ()),
        ("apache_cpu_load", "gauge", "CPU Load (*)", ()),
        ("apache_uptime_seconds_total", "counter", "Current uptime in seconds (*)", ()),
        ("apache_workers", "gauge", "Apache worker statuses", ("state",)),
        ("apache_scoreboard", "gauge", "Apache scoreboard statuses", ("state",)),
    ],
)

TOMCAT_EXPORTER: tuple[FamilySpec, ...] = _spec(
    "tomcat",
    [
        ("tomcat_requestcount_total", "counter", "Tomcat global requestCount", ("port", "protocol")),
        ("tomcat_errorcount_total", "counter", "Tomcat global errorCount", ("port", "protocol")),
        ("tomcat_processingtime_total", "counter", "Tomcat global processingTime", ("port", "protocol")),
        ("tomcat_bytessent_total", "counter", "Tomcat global bytesSent", ("port", "protocol")),
        ("tomcat_bytesreceived_total", "counter", "Tomcat global bytesReceived", ("port", "protocol")),
        ("tomcat_maxtime_total", "counter", "Tomcat global maxTime", ("port", "protocol")),
        ("tomcat_threadpool_currentthreadcount", "gauge", "Tomcat threadpool currentThreadCount", ("port", "protocol")),
        ("tomcat_threadpool_currentthreadsbusy", "gauge", "Tomcat threadpool currentThreadsBusy", ("port", "protocol")),
        # maxThreads is the configured ceiling on the connector's executor. It
        # is the denominator for pool saturation; without it currentThreadsBusy
        # is an unscaled count and saturation is not computable.
        ("tomcat_threadpool_maxthreads", "gauge", "Tomcat threadpool maxThreads", ("port", "protocol")),
        ("tomcat_threadpool_keepalivecount", "gauge", "Tomcat threadpool keepAliveCount", ("port", "protocol")),
        ("tomcat_threadpool_connectioncount", "gauge", "Tomcat threadpool connectionCount", ("port", "protocol")),
        ("tomcat_session_sessioncounter_total", "counter", "Tomcat session sessionCounter", ("context", "host")),
        ("tomcat_session_rejectedsessions_total", "counter", "Tomcat session rejectedSessions", ("context", "host")),
        ("tomcat_session_expiredsessions_total", "counter", "Tomcat session expiredSessions", ("context", "host")),
    ],
)

JVM_EXPORTER: tuple[FamilySpec, ...] = _spec(
    "jvm",
    [
        ("jvm_threads_current", "gauge", "Current thread count of a JVM", ()),
        ("jvm_threads_daemon", "gauge", "Daemon thread count of a JVM", ()),
        ("jvm_threads_peak", "gauge", "Peak thread count of a JVM", ()),
        ("jvm_threads_started_total", "counter", "Started thread count of a JVM", ()),
        ("jvm_threads_deadlocked", "gauge", "Cycles of JVM-threads that are in deadlock waiting to acquire object monitors or ownable synchronizers", ()),
        ("jvm_threads_state", "gauge", "Current count of threads by state", ("state",)),
        ("jvm_memory_bytes_used", "gauge", "Used bytes of a given JVM memory area.", ("area",)),
        ("jvm_memory_bytes_committed", "gauge", "Committed (bytes) of a given JVM memory area.", ("area",)),
        ("jvm_memory_bytes_max", "gauge", "Max (bytes) of a given JVM memory area.", ("area",)),
        ("jvm_memory_pool_bytes_used", "gauge", "Used bytes of a given JVM memory pool.", ("pool",)),
        ("jvm_memory_pool_bytes_max", "gauge", "Max bytes of a given JVM memory pool.", ("pool",)),
        ("jvm_gc_collection_seconds", "summary", "Time spent in a given JVM garbage collector in seconds.", ("gc",)),
        ("jvm_classes_currently_loaded", "gauge", "The number of classes that are currently loaded in the JVM", ()),
        ("jvm_buffer_pool_used_bytes", "gauge", "Used bytes of a given JVM buffer pool.", ("pool",)),
    ],
)

#: Micrometer / Spring Boot Actuator, exposed on the application's own
#: ``/actuator/prometheus`` endpoint and scraped alongside the jmx_exporter
#: series.
#:
#: This is the only family in the catalogue that carries a **latency
#: distribution** and a **status-code breakdown**. Tomcat's
#: ``processingtime_total`` / ``requestcount_total`` pair yields a mean only,
#: and a mean hides exactly the tail that matters during a stall; Apache's
#: mod_status exposes no status codes at all. Without this family the corpus
#: cannot answer "what is the p95?" or "what fraction of responses were 5xx?",
#: which are two of the most ordinary questions anyone asks of a web tier.
SPRINGBOOT_EXPORTER: tuple[FamilySpec, ...] = _spec(
    "springboot",
    [
        (
            "http_server_requests_seconds",
            "histogram",
            "Duration of HTTP server request handling",
            ("method", "outcome", "status", "uri"),
        ),
    ],
)

MYSQLD_EXPORTER: tuple[FamilySpec, ...] = _spec(
    "mysql",
    [
        ("mysql_up", "gauge", "Whether the MySQL server is up.", ()),
        ("mysql_global_status_innodb_row_lock_waits", "untyped", _GENERIC_STATUS_HELP, ()),
        ("mysql_global_status_innodb_row_lock_current_waits", "untyped", _GENERIC_STATUS_HELP, ()),
        ("mysql_global_status_innodb_row_lock_time", "untyped", _GENERIC_STATUS_HELP, ()),
        ("mysql_global_status_innodb_row_lock_time_avg", "untyped", _GENERIC_STATUS_HELP, ()),
        ("mysql_global_status_innodb_row_lock_time_max", "untyped", _GENERIC_STATUS_HELP, ()),
        ("mysql_global_status_table_locks_waited", "untyped", _GENERIC_STATUS_HELP, ()),
        ("mysql_global_status_table_locks_immediate", "untyped", _GENERIC_STATUS_HELP, ()),
        ("mysql_global_status_innodb_log_waits", "untyped", _GENERIC_STATUS_HELP, ()),
        ("mysql_global_status_innodb_buffer_pool_wait_free", "untyped", _GENERIC_STATUS_HELP, ()),
        ("mysql_global_status_threads_connected", "untyped", _GENERIC_STATUS_HELP, ()),
        ("mysql_global_status_threads_running", "untyped", _GENERIC_STATUS_HELP, ()),
        ("mysql_global_status_threads_cached", "untyped", _GENERIC_STATUS_HELP, ()),
        ("mysql_global_status_max_used_connections", "untyped", _GENERIC_STATUS_HELP, ()),
        ("mysql_global_variables_max_connections", "gauge", _GENERIC_VARIABLE_HELP, ()),
        ("mysql_global_status_aborted_connects", "untyped", _GENERIC_STATUS_HELP, ()),
        ("mysql_global_status_aborted_clients", "untyped", _GENERIC_STATUS_HELP, ()),
        ("mysql_global_status_connections", "untyped", _GENERIC_STATUS_HELP, ()),
        ("mysql_global_status_slow_queries", "untyped", _GENERIC_STATUS_HELP, ()),
        ("mysql_global_status_queries", "untyped", _GENERIC_STATUS_HELP, ()),
        ("mysql_global_status_questions", "untyped", _GENERIC_STATUS_HELP, ()),
        ("mysql_global_status_commands_total", "counter", "Total number of executed MySQL commands.", ("command",)),
        ("mysql_info_schema_threads", "gauge", "The number of threads (connections) split by current state.", ("state",)),
        # The --collect.info_schema.processlist collector. These two are the
        # only place session *age* appears: global status counters report lock
        # waits, not how long a query has been running, so a long-running
        # analytical query is invisible without them.
        ("mysql_info_schema_processlist_threads", "gauge", "The number of threads split by current state and command.", ("command", "state")),
        ("mysql_info_schema_processlist_seconds", "gauge", "The number of seconds threads have used split by current state and command.", ("command", "state")),
    ],
)

_IF_LABELS = ("ifAlias", "ifDescr", "ifIndex", "ifName")
_OSPF_LABELS = ("ifIndex", "ospfNbrIpAddr", "ospfNbrRtrId")

SNMP_EXPORTER: tuple[FamilySpec, ...] = _spec(
    "snmp",
    [
        ("ifHCInOctets", "counter", "The total number of octets received on the interface, including framing characters.", _IF_LABELS),
        ("ifHCOutOctets", "counter", "The total number of octets transmitted out of the interface, including framing characters.", _IF_LABELS),
        ("ifHCInUcastPkts", "counter", "The number of packets, delivered by this sub-layer to a higher sub-layer, that were not addressed to a multicast or broadcast address.", _IF_LABELS),
        ("ifHCOutUcastPkts", "counter", "The total number of packets that higher-level protocols requested be transmitted that were not addressed to a multicast or broadcast address.", _IF_LABELS),
        ("ifInErrors", "counter", "The number of inbound packets that contained errors preventing them from being deliverable to a higher-layer protocol.", _IF_LABELS),
        ("ifOutErrors", "counter", "The number of outbound packets that could not be transmitted because of errors.", _IF_LABELS),
        ("ifInDiscards", "counter", "The number of inbound packets which were chosen to be discarded even though no errors had been detected.", _IF_LABELS),
        ("ifOutDiscards", "counter", "The number of outbound packets which were chosen to be discarded even though no errors had been detected.", _IF_LABELS),
        ("ifOperStatus", "gauge", "The current operational state of the interface - 1=up, 2=down, 3=testing, 4=unknown, 5=dormant, 6=notPresent, 7=lowerLayerDown.", _IF_LABELS),
        ("ifAdminStatus", "gauge", "The desired state of the interface - 1=up, 2=down, 3=testing.", _IF_LABELS),
        ("ifHighSpeed", "gauge", "An estimate of the interface's current bandwidth in units of 1,000,000 bits per second.", _IF_LABELS),
        ("ifLastChange", "gauge", "The value of sysUpTime at the time the interface entered its current operational state.", _IF_LABELS),
        ("ospfNbrState", "gauge", "The state of the relationship with this neighbor - 1=down, 2=attempt, 3=init, 4=twoWay, 5=exchangeStart, 6=exchange, 7=loading, 8=full.", _OSPF_LABELS),
        ("ospfNbrEvents", "counter", "The number of times this neighbor relationship has changed state, or an error has occurred.", _OSPF_LABELS),
        ("snmp_scrape_duration_seconds", "gauge", "Total SNMP time scrape took (walk and processing).", ()),
        ("snmp_scrape_pdus_returned", "gauge", "PDUs returned from walk.", ()),
        ("snmp_request_errors_total", "counter", "Errors in requests to the SNMP exporter.", ()),
    ],
)

ALL_FAMILIES: tuple[FamilySpec, ...] = (
    NODE_EXPORTER
    + CADVISOR
    + APACHE_EXPORTER
    + TOMCAT_EXPORTER
    + JVM_EXPORTER
    + SPRINGBOOT_EXPORTER
    + MYSQLD_EXPORTER
    + SNMP_EXPORTER
)

_BY_NAME: dict[str, FamilySpec] = {spec.name: spec for spec in ALL_FAMILIES}
if len(_BY_NAME) != len(ALL_FAMILIES):  # pragma: no cover - catalogue authoring bug
    raise AssertionError("duplicate metric family name in the exporter catalogue")


def family_spec(name: str) -> FamilySpec:
    """Look up a family declaration by metric name."""
    return _BY_NAME[name]


def metadata_for(names: Iterable[str]) -> dict[str, tuple[str, str]]:
    """Build the ``{family: (type, help)}`` map ``serialize_exposition`` wants."""
    return {name: (_BY_NAME[name].type, _BY_NAME[name].help) for name in names}


def build_families(samples: Iterable[MetricSample]) -> list[MetricFamily]:
    """Group samples into :class:`MetricFamily` blocks using the catalogue.

    Families appear in first-appearance order, which keeps output stable and
    keeps each family's samples contiguous as the format requires.
    """
    order: list[str] = []
    grouped: dict[str, list[MetricSample]] = {}
    for sample in samples:
        family = _resolve_family(sample.name)
        if family not in grouped:
            grouped[family] = []
            order.append(family)
        grouped[family].append(sample)
    out: list[MetricFamily] = []
    for family in order:
        spec = _BY_NAME[family]
        out.append(
            MetricFamily(spec.name, spec.type, spec.help, tuple(grouped[family]))
        )
    return out


def _resolve_family(sample_name: str) -> str:
    if sample_name in _BY_NAME:
        return sample_name
    for suffix in ("_bucket", "_sum", "_count"):
        if sample_name.endswith(suffix):
            base = sample_name[: -len(suffix)]
            spec = _BY_NAME.get(base)
            if spec is not None and spec.type in ("histogram", "summary"):
                return base
    raise KeyError(f"{sample_name!r} is not in the exporter catalogue")
