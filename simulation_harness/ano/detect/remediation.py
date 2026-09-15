"""``ano.detect.remediation`` — the operator-facing recommended action.

Requirement R2 asks that a prediction arrive "with enough detail for an
operator to act preemptively". The signals answer *what is happening*; this
module answers *what to do about it*, which is the field contract C2 calls
``recommended_remediation``.

The one rule that matters
-------------------------

**The attributed root-cause domain is authoritative. Signal families refine
the advice within that domain; they never override it.**

That ordering is not stylistic. A database-contention cascade lights up
application-side symptoms on every app server querying the database, and a
genuine application fault will occasionally show a stray lock-wait reading. If
a signal family were allowed to outrank the attributed domain, the engine could
attribute an incident to ``application`` and, in the same payload, tell the
operator to go and terminate blocking SQL sessions — a self-contradicting alert
that sends the first responder to the wrong team. Whatever R3's attribution
decided is what the operator is told to act on. If the attribution is wrong,
the advice is wrong in the same direction, visibly, rather than wrong in a way
that hides the attribution error.

Advice is derived from the signal-family vocabulary in
:mod:`ano.detect.signals` — the same families that carry the log-odds mass —
rather than from substring matches on feature names. Feature names are an
internal naming convention; keying behaviour off them would break silently the
first time a feature is renamed, and would misfire on any name that happens to
contain the wrong word.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from ano.detect.candidate import Candidate

__all__ = ["APPLICATION_ADVICE", "remediation_for", "attach_remediations"]


#: Application-domain advice, in priority order. The first family present in
#: the candidate's evidence wins, so the order encodes which failure mode an
#: operator should act on when several are lit at once.
#:
#: Descriptor exhaustion leads because it is the hardest deadline — a process
#: out of file descriptors stops accepting connections outright, and the fix
#: (recycling sockets, raising the limit) is both fast and safe. Memory
#: pressure follows because an OOM kill is unrecoverable and the mitigation
#: needs lead time. Thread starvation is next: painful but usually survivable
#: and often self-clearing once load drops. The resource families come last —
#: they are real, but "CPU is high" is the least specific instruction on the
#: list.
APPLICATION_ADVICE: tuple[tuple[str, str], ...] = (
    (
        "descriptor_exhaustion",
        "Socket or file-descriptor exhaustion predicted on {entity}. Recycle "
        "idle TCP sockets, investigate the connection leak, and raise the "
        "process ulimit or ephemeral port range before new connections are "
        "refused.",
    ),
    (
        "memory_pressure",
        "Memory leak or GC stall predicted on {entity}. Capture a heap "
        "dump for leak analysis, raise the heap ceiling, and schedule a "
        "rolling restart before the process is OOM-killed.",
    ),
    (
        "thread_starvation",
        "Thread pool exhaustion predicted on {entity}. Take a thread dump to "
        "identify the blocking call, scale out the worker pool, and shed "
        "low-priority background requests.",
    ),
    (
        "cpu_without_throughput",
        "CPU saturation without matching throughput on {entity} — the host is "
        "busy but not doing useful work. Profile for a hot loop or lock "
        "convoy, and scale out or restart before requests time out.",
    ),
    (
        "io_wait",
        "Rising I/O wait on {entity}. Identify the process driving the I/O, "
        "check the backing storage for contention, and move or throttle the "
        "offending workload.",
    ),
    (
        "disk_latency",
        "Disk latency climbing on {entity}. Check the underlying device for "
        "faults or saturation and relocate latency-sensitive workloads.",
    ),
    (
        "kernel_pressure",
        "Kernel-level pressure on {entity} — context-switch and system-time "
        "load are abnormal. Look for a runaway process or interrupt storm.",
    ),
    (
        "service_health",
        "Service health degrading on {entity}: error rate and latency are "
        "rising against baseline. Check recent deployments and upstream "
        "dependencies, and prepare to roll back.",
    ),
)


_FAMILY_ALIASES: dict[str, tuple[str, ...]] = {
    "descriptor_exhaustion": ("descriptor_exhaustion", "descriptor"),
    "memory_pressure": ("memory_pressure", "memory"),
    "thread_starvation": ("thread_starvation", "thread"),
}


def _first_family(families: Mapping[str, float], table: Iterable[tuple[str, str]]) -> str | None:
    """Return the advice template for the first listed family that is present.

    Args:
        families: Family name to severity, from the candidate.
        table: Ordered ``(family, template)`` pairs.

    Returns:
        The matching template, or ``None`` if no listed family is present.
    """
    for family, template in table:
        aliases = _FAMILY_ALIASES.get(family, (family,))
        if any(families.get(alias, 0.0) > 0.0 for alias in aliases):
            return template
    return None


def remediation_for(candidate: Candidate) -> str:
    """Recommend an operator action for one candidate.

    The attributed root-cause domain selects the team and the class of action;
    the signal families refine the instruction within it. See the module
    docstring for why that ordering is fixed.

    Args:
        candidate: A candidate, ideally post-attribution. An unattributed
            candidate is treated as an application-domain one, which matches
            :attr:`ano.detect.candidate.Candidate.track`'s own default.

    Returns:
        A single-sentence-or-two instruction naming the responsible entity.
    """
    domain = candidate.cause_domain or "application"
    entity = candidate.responsible_entity
    families = candidate.family_severities

    if domain == "database":
        if families.get("db_session_pool", 0.0) > 0.0 and not families.get(
            "db_contention", 0.0
        ):
            return (
                f"Connection-pool saturation on {entity}. Raise "
                f"max_connections or the client pool ceiling, and find the "
                f"caller holding sessions open."
            )
        if families.get("db_long_running_sql", 0.0) > 0.0 and not families.get(
            "db_contention", 0.0
        ):
            return (
                f"Long-running queries degrading {entity}. Identify the slow "
                f"statements, add or repair the supporting indexes, and "
                f"consider a statement timeout."
            )
        return (
            f"Database contention on {entity}. Identify and terminate the "
            f"blocking sessions, inspect row-lock waits, and tune the "
            f"offending query's indexes to restore application throughput."
        )

    if domain == "network":
        if families.get("net_ospf_flap", 0.0) > 0.0:
            return (
                f"OSPF adjacency instability on {entity}. Verify the neighbour "
                f"state and interface stability, and damp or isolate the "
                f"flapping link before routing reconverges again."
            )
        if families.get("net_congestion", 0.0) > 0.0 and not families.get(
            "net_port_errors", 0.0
        ):
            return (
                f"Link congestion on {entity}. Rebalance traffic across the "
                f"uplink group or raise capacity before queuing turns into "
                f"loss."
            )
        return (
            f"Network degradation on {entity}. Reroute traffic away from the "
            f"failing interface, inspect the transceiver optics and cabling, "
            f"and verify OSPF adjacency state to prevent further loss."
        )

    template = _first_family(families, APPLICATION_ADVICE)
    if template is not None:
        return template.format(entity=entity)
    return (
        f"Impending unresponsiveness predicted on {entity}. Preemptively scale "
        f"out or begin a rolling restart before the instance requires a hard "
        f"reboot."
    )


def attach_remediations(candidates: Iterable[Candidate]) -> list[Candidate]:
    """Attach a recommended operator action to each candidate.

    Args:
        candidates: Candidates, ideally post-attribution.

    Returns:
        The same candidates, in the same order, each carrying a remediation.
    """
    return [item.with_remediation(remediation_for(item)) for item in candidates]
