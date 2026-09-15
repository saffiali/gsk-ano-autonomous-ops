"""Log emission: what the estate writes to Cloud Logging as things happen.

Requirement R2 asks for *semantic* understanding of unstructured logs, so the
logs here have to carry the meaning in the **prose**, the way real logs do.
A Tomcat stuck-thread warning, an InnoDB lock-wait warning and an OSPF
adjacency change are recognisable because of what they say, not because of a
field that says which kind of incident is in progress.

Integrity constraints this module holds itself to
-------------------------------------------------
* **No label leakage.** Nothing here writes an event id, a track, an
  ``is_genuine`` flag, or any C1 event-kind token. A test asserts that none of
  the ten C1 kind strings appears anywhere in the emitted corpus.
* **The fault vocabulary exists in history.** Every fault template below is
  also reachable from the unlabelled history micro-disturbances, so a novelty
  detector cannot win simply by noticing that fault-shaped words are new.
* **Exactly one event is genuinely novel.** :data:`NOVEL_TEMPLATES` is reachable
  only from the single in-window event flagged ``novel_log_signature``. Its
  vocabulary — a hardware security module, PKCS#11, TLS alert internals and
  native-memory arenas — appears nowhere else in the corpus, and that absence
  is asserted at token level by a test rather than merely intended.
* **The novel event also emits ordinary lines.** A real incident is a mixture.
  If the novel event emitted *only* unseen vocabulary it would be separable by
  a trivial "fraction of unknown tokens" rule, which would be a gift rather
  than a test.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass

from ano.contracts.determinism import rng, stable_id
from ano.telemetry.logentry import LogEntry, MonitoredResource, build_log_name

from scenariogen.estate import (
    PROJECT_ID,
    Estate,
    container_log_resource,
    gce_log_resource,
    switch_log_resource,
)
from scenariogen.simulate import TickObservation
from scenariogen.timeline import Tick

__all__ = [
    "NOVEL_TOKENS",
    "LogEmitter",
]

#: Base emission rate per second for each log source when nothing is wrong.
#: These are *error/event* logs, not access logs, so the healthy rate is a
#: handful per hour — which is what a production application log actually looks
#: like once request logging goes somewhere else.
_BASE_RATE = {
    "tomcat": 0.00040,
    "apache": 0.00014,
    "syslog": 0.00022,
    "mysql": 0.00020,
    "network": 0.00010,
    "container": 0.00030,
}

#: Tokens that appear **only** in the novel event's log signature. Curated
#: rather than derived, so the absence assertion is explicit and auditable.
NOVEL_TOKENS: tuple[str, ...] = (
    "bad_record_mac",
    "NativeMemoryTracking",
    "CKR_DEVICE_ERROR",
    "PKCS#11",
    "pkcs11",
    "HSM",
    "kickstart",
    "Thread_arena",
    "SSLProtocolException",
    "cryptoki",
)


@dataclass(frozen=True, slots=True)
class _Template:
    """One log line shape.

    Attributes:
        severity: Cloud Logging severity.
        text: Format string. Placeholders are filled from a per-line context.
        structured: True if this source writes ``jsonPayload`` rather than
            ``textPayload``. Application servers do; syslog and the network
            devices do not, which is the ordinary state of affairs and is what
            makes the semantic layer's job realistic.
        logger: Java logger name, for structured payloads.
    """

    severity: str
    text: str
    structured: bool = False
    logger: str = ""


# ---------------------------------------------------------------------------
# Healthy-operation templates
# ---------------------------------------------------------------------------
_NORMAL: dict[str, tuple[_Template, ...]] = {
    "tomcat": (
        _Template("INFO", "Scheduled cache refresh completed in {ms} ms, {n} entries reloaded",
                  True, "com.gsk.platform.cache.CacheRefreshJob"),
        _Template("INFO", "JDBC pool statistics: active={active} idle={idle} waiting=0 maxWait=0ms",
                  True, "com.zaxxer.hikari.pool.HikariPool"),
        _Template("INFO", "Health probe /actuator/health returned UP in {ms} ms",
                  True, "org.springframework.boot.actuate.health.HealthEndpoint"),
        _Template("INFO", "Expired {n} HTTP sessions during background processing",
                  True, "org.apache.catalina.session.ManagerBase"),
        _Template("INFO", "Reloaded application configuration revision {n} from config service",
                  True, "com.gsk.platform.config.ConfigWatcher"),
        _Template("DEBUG", "Outbound call to inventory-service completed in {ms} ms, status 200",
                  True, "com.gsk.clinsupply.client.InventoryClient"),
    ),
    "apache": (
        _Template("NOTICE", "[mpm_event:notice] [pid {pid}] AH00489: Apache/2.4.57 (Unix) "
                            "configured -- resuming normal operations"),
        _Template("INFO", "[core:info] [pid {pid}] AH00094: Command line: '/usr/sbin/httpd -D FOREGROUND'"),
        _Template("INFO", "[mpm_event:info] [pid {pid}] AH10159: server seems busy, spawning "
                          "{n} children, there are {idle} idle workers"),
    ),
    "syslog": (
        _Template("INFO", "CRON[{pid}]: (root) CMD (/usr/lib/sysstat/debian-sa1 1 1)"),
        _Template("INFO", "chronyd[812]: Selected source 10.42.0.9 (ntp.internal.gsk)"),
        _Template("INFO", "systemd[1]: Started Daily rotation of log files."),
        _Template("INFO", "sshd[{pid}]: Accepted publickey for deploy from 10.42.8.11 port {n} ssh2"),
        _Template("NOTICE", "audispd[{pid}]: node=localhost type=USER_ACCT msg=audit(1): pid={pid} res=success"),
    ),
    "mysql": (
        _Template("INFO", "[Note] [MY-011825] [InnoDB] Buffer pool(s) load completed, {n} pages"),
        _Template("INFO", "[Note] [MY-010914] Aborted connection {n} to db: '{schema}' user: 'appsvc' "
                          "host: '10.42.8.{octet}' (Got an error reading communication packets)"),
        _Template("INFO", "[Note] [MY-012976] [InnoDB] Checkpoint age {n} pages, flushing to disk"),
        _Template("INFO", "[Note] [MY-011332] Binary log rotated to binlog.{n}"),
    ),
    "network": (
        _Template("NOTICE", "%SYS-5-CONFIG_I: Configured from console by netops on vty0 (10.42.8.4)"),
        _Template("INFO", "%SNMP-5-COLDSTART: SNMP agent on host {host} is undergoing a cold start"),
        _Template("NOTICE", "%LLDP-5-NEIGHBOR_ADDED: Neighbor discovered on {ifname}"),
    ),
    "container": (
        _Template("INFO", "batch job {n} finished, {n2} rows written in {ms} ms", True,
                  "com.gsk.batch.EtlRunner"),
        _Template("INFO", "checkpoint written to gs://gsk-ano-demo-batch/checkpoints/{n}", True,
                  "com.gsk.batch.Checkpointer"),
    ),
}


# ---------------------------------------------------------------------------
# Fault templates. Reachable from history micro-disturbances AND from the
# labelled window events, which is what keeps their vocabulary non-novel.
# ---------------------------------------------------------------------------
_FAULT: dict[str, dict[str, tuple[_Template, ...]]] = {
    "thread_starvation": {
        "tomcat": (
            _Template("WARNING",
                      "Thread [http-nio-8080-exec-{n}] has been active for {secs} seconds "
                      "servicing request POST /api/v2/shipments and may be stuck", True,
                      "org.apache.catalina.valves.StuckThreadDetectionValve"),
            _Template("ERROR",
                      "java.net.SocketTimeoutException: Read timed out; at "
                      "java.base/java.net.SocketInputStream.socketRead0(Native Method)", True,
                      "com.gsk.clinsupply.client.InventoryClient"),
            _Template("WARNING",
                      "Timeout waiting for idle object from pool after 30000 ms; "
                      "{active} of {maxt} worker threads in use, {waiting} requests queued", True,
                      "org.apache.tomcat.util.threads.ThreadPoolExecutor"),
            _Template("ERROR",
                      "Rejecting request: work queue capacity 100 reached and all pool "
                      "threads are busy", True,
                      "org.apache.tomcat.util.threads.ThreadPoolExecutor"),
        ),
        "apache": (
            _Template("ERROR", "[mpm_event:error] [pid {pid}] AH00485: scoreboard is full, "
                               "not at MaxRequestWorkers"),
            _Template("ERROR", "[proxy_http:error] [pid {pid}] AH01102: error reading status "
                               "line from remote server 127.0.0.1:8080"),
        ),
        "syslog": (
            _Template("WARNING", "kernel: TCP: request_sock_TCP: Possible SYN flooding on port 8080. "
                                 "Sending cookies. Check SNMP counters."),
        ),
    },
    "memory_leak": {
        "tomcat": (
            _Template("WARNING",
                      "Full GC (Ergonomics) {heap_before}M->{heap_after}M({heap_max}M), "
                      "{gcsecs} secs", True, "gc"),
            _Template("WARNING",
                      "GC overhead high: {n} full collections in the last 60 seconds reclaimed "
                      "less than 3% of the old generation", True,
                      "com.gsk.platform.diagnostics.HeapMonitor"),
            _Template("ERROR",
                      "The web application registered a JDBC driver but failed to deregister it "
                      "when the context stopped, which is very likely to create a memory leak",
                      True, "org.apache.catalina.loader.WebappClassLoaderBase"),
            _Template("CRITICAL",
                      "java.lang.OutOfMemoryError: Java heap space; at "
                      "java.base/java.util.Arrays.copyOf(Arrays.java:3537)", True,
                      "org.apache.catalina.core.ContainerBase"),
        ),
        "syslog": (
            _Template("WARNING", "kernel: java invoked oom-killer: gfp_mask=0x140dca, order=0, "
                                 "oom_score_adj=0"),
        ),
    },
    "socket_exhaustion": {
        "tomcat": (
            _Template("ERROR",
                      "java.net.SocketException: Too many open files; at "
                      "java.base/sun.nio.ch.Net.accept(Native Method)", True,
                      "org.apache.tomcat.util.net.NioEndpoint"),
            _Template("ERROR",
                      "Socket accept failed: java.io.IOException: Too many open files (errno 24)",
                      True, "org.apache.tomcat.util.net.Acceptor"),
            _Template("WARNING",
                      "Descriptor usage {fds} of soft limit {fdmax}; {n} sockets in CLOSE_WAIT "
                      "held by pid {pid}", True,
                      "com.gsk.platform.diagnostics.DescriptorMonitor"),
        ),
        "syslog": (
            _Template("WARNING", "kernel: VFS: file-max limit {fdmax} reached"),
            _Template("WARNING", "kernel: TCP: too many orphaned sockets"),
            _Template("ERROR", "systemd[1]: httpd.service: Failed to fork off sandboxing "
                               "workspace: Too many open files"),
        ),
    },
    "db_lock_contention": {
        "mysql": (
            _Template("WARNING",
                      "[Warning] [MY-012241] [InnoDB] Transaction {n} has been waiting {secs} "
                      "seconds for a lock on index PRIMARY of table `{schema}`.`shipment_lines`"),
            _Template("ERROR",
                      "[ERROR] [MY-011971] [InnoDB] Lock wait timeout exceeded; try restarting "
                      "transaction (thread {n}, db '{schema}')"),
            _Template("WARNING",
                      "[Warning] [MY-012242] [InnoDB] transactions deadlock detected, dumping "
                      "detailed information for thread {n}"),
            _Template("WARNING",
                      "[Warning] [MY-010914] Aborted connection {n} to db: '{schema}' user: "
                      "'appsvc' (Got timeout reading communication packets)"),
        ),
        "tomcat": (
            _Template("ERROR",
                      "org.springframework.dao.CannotAcquireLockException: could not execute "
                      "statement; SQL [update shipment_lines set status=? where id=?]", True,
                      "com.gsk.clinsupply.repo.ShipmentRepository"),
            _Template("WARNING",
                      "JDBC statement exceeded the {secs}s slow-query threshold: "
                      "select ... from shipment_lines for update", True,
                      "com.gsk.platform.jdbc.SlowQueryListener"),
        ),
    },
    "ospf_flap": {
        "network": (
            _Template("NOTICE",
                      "%OSPF-5-ADJCHG: Process 100, Nbr {rtr} on {ifname} from FULL to DOWN, "
                      "Neighbor Down: Dead timer expired"),
            _Template("NOTICE",
                      "%OSPF-5-ADJCHG: Process 100, Nbr {rtr} on {ifname} from LOADING to FULL, "
                      "Loading Done"),
            _Template("ERROR", "%LINK-3-UPDOWN: Interface {ifname}, changed state to down"),
            _Template("NOTICE",
                      "%LINEPROTO-5-UPDOWN: Line protocol on Interface {ifname}, "
                      "changed state to up"),
        ),
        "tomcat": (
            _Template("WARNING",
                      "Retrying upstream request after transport failure (attempt 2 of 3): "
                      "java.net.NoRouteToHostException", True,
                      "com.gsk.platform.http.RetryingClient"),
        ),
    },
    "packet_loss": {
        "network": (
            _Template("WARNING",
                      "%ETHPORT-4-IF_RX_ERRORS: Interface {ifname} input error rate rising, "
                      "CRC errors {n} in the last interval"),
            _Template("WARNING",
                      "%PLATFORM-4-XCVR_MARGINAL: Transceiver on {ifname} reporting low optical "
                      "receive power ({dbm} dBm), below the warning threshold"),
            _Template("WARNING",
                      "%ETHPORT-4-IF_DISCARDS: Ingress discards rising on {ifname}, "
                      "{n} frames dropped"),
        ),
        "tomcat": (
            _Template("WARNING",
                      "java.net.SocketException: Connection reset; retrying idempotent request",
                      True, "com.gsk.platform.http.RetryingClient"),
        ),
        "syslog": (
            _Template("WARNING", "kernel: TCP: eth0: {n} retransmits in the last minute, "
                                 "congestion window collapsed"),
        ),
    },
    "patching_window": {
        "syslog": (
            _Template("INFO", "unattended-upgrades[{pid}]: Starting unattended upgrades script"),
            _Template("INFO", "dpkg: unpacking linux-image-6.8.0-52-generic over "
                              "(6.8.0-49-generic)"),
            _Template("NOTICE", "systemd[1]: Stopping Apache HTTP Server..."),
            _Template("INFO", "unattended-upgrades[{pid}]: Packages that were upgraded: "
                              "openssl libssl3 linux-image-generic curl"),
            _Template("NOTICE", "systemd-shutdown[1]: Rebooting."),
            _Template("NOTICE", "kernel: Linux version 6.8.0-52-generic "
                                "(buildd@lcy02) #52-Ubuntu SMP"),
            _Template("WARNING", "kernel: EXT4-fs (sda1): recovery complete after unclean shutdown"),
        ),
        "mysql": (
            _Template("NOTICE", "[System] [MY-010910] mysqld: Shutdown complete (mysqld 8.0.36)"),
            _Template("NOTICE", "[System] [MY-010931] mysqld: ready for connections. "
                                "Version: '8.0.36' port: 3306"),
        ),
    },
    "planned_deploy": {
        "tomcat": (
            _Template("NOTICE", "Received SIGTERM, initiating graceful shutdown; draining "
                                "{active} in-flight requests", True,
                      "org.apache.catalina.core.StandardServer"),
            _Template("INFO", "Deployment of web application archive "
                              "[/opt/tomcat/webapps/{appname}.war] has finished in [{ms}] ms",
                      True, "org.apache.catalina.startup.HostConfig"),
            _Template("INFO", "Starting Servlet engine: [Apache Tomcat/10.1.19]", True,
                      "org.apache.catalina.core.StandardEngine"),
            _Template("INFO", "Initializing Spring embedded WebApplicationContext, "
                              "{n} beans instantiated", True,
                      "org.springframework.web.context.ContextLoader"),
        ),
        "apache": (
            _Template("NOTICE", "[mpm_event:notice] [pid {pid}] AH00169: caught SIGTERM, "
                                "shutting down"),
            _Template("ERROR", "[proxy:error] [pid {pid}] AH00898: Error reading from remote "
                               "server returned by /api/v2/shipments"),
            _Template("ERROR", "[proxy_http:error] [pid {pid}] AH01114: HTTP: failed to make "
                               "connection to backend: 127.0.0.1"),
        ),
    },
    "traffic_surge": {
        "tomcat": (
            _Template("INFO", "Request rate {n}/s is above the 7-day median for this hour; "
                              "connector accepting normally", True,
                      "com.gsk.platform.diagnostics.LoadMonitor"),
            _Template("WARNING", "JDBC pool at {active} of {maxt} active connections, "
                                 "mean borrow wait {ms} ms", True,
                      "com.zaxxer.hikari.pool.HikariPool"),
            _Template("INFO", "Cache hit ratio fell to {n}% under elevated load, "
                              "{n2} evictions in the last minute", True,
                      "com.gsk.platform.cache.CacheMetrics"),
        ),
        "apache": (
            _Template("INFO", "[mpm_event:info] [pid {pid}] AH10159: server seems busy, "
                              "spawning {n} children, there are {idle} idle workers"),
        ),
    },
}


#: Reachable only from the one event flagged ``novel_log_signature``.
#: Hardware-security-module and native-memory vocabulary: plausible for a
#: pharmaceutical estate signing regulated records, and absent from every
#: other template above.
NOVEL_TEMPLATES: tuple[_Template, ...] = (
    _Template("ERROR",
              "javax.net.ssl.SSLProtocolException: Received fatal alert: bad_record_mac "
              "from signing gateway sign-gw.internal.gsk", True,
              "com.gsk.platform.crypto.SigningGatewayClient"),
    _Template("ERROR",
              "sun.security.ssl.Alert: Couldn't kickstart handshaking; the peer closed the "
              "TLS session before the ChangeCipherSpec", True,
              "com.gsk.platform.crypto.SigningGatewayClient"),
    _Template("CRITICAL",
              "PKCS#11 token returned CKR_DEVICE_ERROR during C_Sign; the cryptoki library "
              "reported the slot as unusable", True,
              "com.gsk.platform.crypto.Pkcs11SignatureProvider"),
    _Template("WARNING",
              "HSM session pool depleted: 0 of 8 pkcs11 sessions available, {waiting} callers "
              "parked awaiting a slot", True,
              "com.gsk.platform.crypto.Pkcs11SessionPool"),
    _Template("WARNING",
              "NativeMemoryTracking: arena chunk allocation failed while committing 262144 "
              "bytes in Thread_arena; malloc arena count {n}", True,
              "com.gsk.platform.diagnostics.NativeMemoryMonitor"),
)


def _poisson(generator, mean: float) -> int:
    """Draw a Poisson count.

    Knuth's product method below ~30, normal approximation above it, which is
    where Knuth stops being cheap. Both consume the generator deterministically.
    """
    if mean <= 0.0:
        return 0
    if mean < 30.0:
        limit = math.exp(-mean)
        count = 0
        product = 1.0
        while True:
            product *= generator.random()
            if product <= limit:
                return count
            count += 1
            if count > 400:
                return count
    return max(0, int(generator.gauss(mean, math.sqrt(mean)) + 0.5))


class LogEmitter:
    """Turns per-tick observations into Cloud Logging entries.

    Args:
        seed: Run seed.
        estate: Topology, for resource labels.
    """

    def __init__(self, seed: int, estate: Estate) -> None:
        self.seed = seed
        self.estate = estate
        self._generators: dict[str, object] = {}
        self._seen_insert_ids: set[str] = set()
        self._sequence = 0

    # -- resources -------------------------------------------------------
    #
    # All three delegate to ``scenariogen.estate``. That indirection is the
    # point: ``topology.json`` publishes the output of the same functions, so
    # the identity a consumer joins on is the identity actually emitted, by
    # construction rather than by two places agreeing to stay in step.
    def _gce_resource(self, entity_id: str, site: str) -> MonitoredResource:
        block = gce_log_resource(entity_id, site)
        return MonitoredResource(block["type"], dict(block["labels"]))

    def _switch_resource(self, entity_id: str, site: str) -> MonitoredResource:
        block = switch_log_resource(entity_id, site)
        return MonitoredResource(block["type"], dict(block["labels"]))

    def _container_resource(self, entity_id: str, site: str, container: str) -> MonitoredResource:
        del entity_id  # The container's identity does not name its host.
        block = container_log_resource(site, container)
        return MonitoredResource(block["type"], dict(block["labels"]))

    # -- emission --------------------------------------------------------
    def _generator_for(self, entity_id: str, source: str):
        key = f"{entity_id}|{source}"
        generator = self._generators.get(key)
        if generator is None:
            generator = rng(self.seed, "scenariogen", "logs", source, entity_id)
            self._generators[key] = generator
        return generator

    def _insert_id(self, entity_id: str, source: str) -> str:
        """A unique, deterministic Cloud Logging insert id.

        Uniqueness is *checked*, not assumed: a duplicate would make the whole
        batch fail L-rule validation, and silently emitting one would be a
        defect the corpus carries into every downstream measurement.
        """
        self._sequence += 1
        candidate = stable_id(
            f"{entity_id}|{source}|{self._sequence}",
            "scenariogen", "insert-id", str(self.seed), length=20,
        )
        if candidate in self._seen_insert_ids:
            raise RuntimeError(f"duplicate insertId generated: {candidate}")
        self._seen_insert_ids.add(candidate)
        return candidate

    def _context(self, generator, entity_id: str, extra: dict[str, object]) -> dict[str, object]:
        context: dict[str, object] = {
            "n": generator.randint(2, 9999),
            "n2": generator.randint(10, 5000),
            "pid": generator.randint(700, 32000),
            "ms": generator.randint(3, 900),
            "secs": generator.randint(2, 240),
            "octet": generator.randint(2, 250),
            "idle": generator.randint(1, 120),
            "dbm": round(generator.uniform(-19.5, -12.5), 1),
            "host": entity_id,
        }
        context.update(extra)
        return context

    def _emit(
        self,
        entries: list[LogEntry],
        generator,
        tick: Tick,
        entity_id: str,
        source: str,
        resource: MonitoredResource,
        log_id: str,
        templates: Sequence[_Template],
        mean_count: float,
        extra: dict[str, object],
        labels: dict[str, str],
    ) -> None:
        count = _poisson(generator, mean_count)
        if count == 0:
            return
        offsets = sorted(generator.random() for _ in range(count))
        for offset in offsets:
            template = templates[generator.randrange(len(templates))]
            context = self._context(generator, entity_id, extra)
            try:
                message = template.text.format(**context)
            except KeyError as exc:  # pragma: no cover - guards template typos
                raise KeyError(
                    f"log template for {source} references unknown placeholder {exc}"
                ) from exc
            nanos = tick.nanos + int(offset * tick.interval_s * 1_000_000_000)
            payload_kwargs: dict[str, object] = {}
            if template.structured:
                payload_kwargs["json_payload"] = {
                    "message": message,
                    "logger": template.logger,
                    "thread": f"http-nio-8080-exec-{generator.randint(1, 200)}",
                }
            else:
                payload_kwargs["text_payload"] = message
            entries.append(
                LogEntry(
                    log_name=build_log_name(PROJECT_ID, log_id),
                    resource=resource,
                    insert_id=self._insert_id(entity_id, source),
                    timestamp_nanos=nanos,
                    # Ingestion lag: real receiveTimestamp trails the event.
                    receive_timestamp_nanos=nanos + generator.randint(18_000_000, 900_000_000),
                    severity=template.severity,
                    labels=dict(labels),
                    **payload_kwargs,
                )
            )

    def emit(self, observation: TickObservation, novel_entities: frozenset[str]) -> list[LogEntry]:
        """Every log entry the estate writes during one tick.

        Args:
            observation: The simulated state at this instant.
            novel_entities: Entities currently hosting the novel-signature
                event. Passed in rather than read from the plan so this module
                never sees an event id, a track or a genuineness flag.
        """
        tick = observation.tick
        entries: list[LogEntry] = []
        interval = float(tick.interval_s)

        for app in observation.apps:
            server = self.estate.app_server(app.entity_id)
            resource = self._gce_resource(app.entity_id, server.site)
            labels = {"environment": "production", "service": server.service}
            extra = {
                "active": app.busy_threads,
                "maxt": app.pool_size,
                "waiting": max(0, int(app.offered_rps - app.completed_rps)),
                "fds": app.host.filefd_allocated,
                "fdmax": app.host.filefd_maximum,
                "appname": server.service.replace("-", ""),
                "heap_max": int(server.heap_max_bytes / 1024 ** 2),
                "heap_before": int(app.jvm.get("heap_used", 0.0) / 1024 ** 2) if app.jvm else 0,
                "heap_after": int(app.jvm.get("old_used", 0.0) / 1024 ** 2) if app.jvm else 0,
                "gcsecs": round(1.2 + len(app.conditions) * 0.4, 3),
            }
            for source, log_id in (("tomcat", "tomcat"), ("apache", "apache"), ("syslog", "syslog")):
                generator = self._generator_for(app.entity_id, source)
                mean = _BASE_RATE[source] * interval
                pool: list[_Template] = list(_NORMAL[source])
                for condition in app.conditions:
                    fault_templates = _FAULT.get(condition, {}).get(source)
                    if fault_templates:
                        pool.extend(fault_templates)
                        mean += _BASE_RATE[source] * interval * _fault_multiplier(condition)
                if source == "tomcat" and app.entity_id in novel_entities:
                    pool.extend(NOVEL_TEMPLATES)
                    mean += _BASE_RATE[source] * interval * 30.0
                self._emit(
                    entries, generator, tick, app.entity_id, source, resource, log_id,
                    pool, mean, extra, labels,
                )

        for database in observation.databases:
            record = self.estate.database(database.entity_id)
            resource = self._gce_resource(database.entity_id, record.site)
            labels = {"environment": "production", "service": "mysql"}
            extra = {"schema": record.schema, "maxt": record.max_connections,
                     "active": database.active_sessions}
            for source, log_id in (("mysql", "mysql"), ("syslog", "syslog")):
                generator = self._generator_for(database.entity_id, source)
                mean = _BASE_RATE[source] * interval
                pool = list(_NORMAL[source])
                for condition in database.conditions:
                    fault_templates = _FAULT.get(condition, {}).get(source)
                    if fault_templates:
                        pool.extend(fault_templates)
                        mean += _BASE_RATE[source] * interval * _fault_multiplier(condition)
                self._emit(
                    entries, generator, tick, database.entity_id, source, resource, log_id,
                    pool, mean, extra, labels,
                )

        for switch in observation.switches:
            record = next(s for s in self.estate.switches if s.entity_id == switch.entity_id)
            resource = self._switch_resource(switch.entity_id, record.site)
            labels = {"environment": "production", "device_role": "switch"}
            generator = self._generator_for(switch.entity_id, "network")
            mean = _BASE_RATE["network"] * interval
            pool = list(_NORMAL["network"])
            interface = record.ports[0].if_name
            for port in switch.ports:
                if port.conditions:
                    interface = port.port.if_name
                for condition in port.conditions:
                    fault_templates = _FAULT.get(condition, {}).get("network")
                    if fault_templates:
                        pool.extend(fault_templates)
                        mean += _BASE_RATE["network"] * interval * _fault_multiplier(condition)
            extra = {"ifname": interface, "rtr": record.ospf_router_id}
            self._emit(
                entries, generator, tick, switch.entity_id, "network", resource,
                "network-syslog", pool, mean, extra, labels,
            )

        for node in observation.nodes:
            record = next(n for n in self.estate.nodes if n.entity_id == node.entity_id)
            resource = self._gce_resource(node.entity_id, record.site)
            labels = {"environment": "production", "service": record.role}
            generator = self._generator_for(node.entity_id, "syslog")
            mean = _BASE_RATE["syslog"] * interval
            pool = list(_NORMAL["syslog"])
            for condition in node.conditions:
                fault_templates = _FAULT.get(condition, {}).get("syslog")
                if fault_templates:
                    pool.extend(fault_templates)
                    mean += _BASE_RATE["syslog"] * interval * _fault_multiplier(condition)
            self._emit(
                entries, generator, tick, node.entity_id, "syslog", resource, "syslog",
                pool, mean, {}, labels,
            )
            for container in sorted(node.containers):
                container_resource = self._container_resource(
                    node.entity_id, record.site, container
                )
                generator = self._generator_for(f"{node.entity_id}/{container}", "container")
                self._emit(
                    entries, generator, tick, f"{node.entity_id}/{container}", "container",
                    container_resource, "stdout", _NORMAL["container"],
                    _BASE_RATE["container"] * interval, {},
                    {"environment": "production", "workload": container},
                )

        entries.sort(key=lambda entry: (entry.timestamp_nanos or 0, entry.insert_id))
        return entries


def _fault_multiplier(condition: str) -> float:
    """How much louder a source gets while a condition is in progress.

    Chosen from what these failures actually do to log volume, not from what
    would be convenient: a reboot narrates itself heavily, a slow leak barely
    says anything until the very end, and a traffic surge is almost silent
    because nothing is failing.
    """
    return {
        "thread_starvation": 95.0,
        "memory_leak": 38.0,
        "socket_exhaustion": 70.0,
        "db_lock_contention": 60.0,
        "ospf_flap": 110.0,
        "packet_loss": 26.0,
        "patching_window": 85.0,
        "planned_deploy": 120.0,
        "traffic_surge": 9.0,
    }.get(condition, 1.0)
