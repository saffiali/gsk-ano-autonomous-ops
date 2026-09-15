"""Readers for the on-disk seed directory the scenario generator produces.

Layout (fixed by ruling IR-05)::

    <seed>/
      labels.json                      # C1 GROUND TRUTH — the harness only
      topology.json                    # operations input
      change_calendar.json             # operations input
      manifest.json                    # seed, profile, counts, sha256 per file
      logs.jsonl                       # one serialised LogEntry per line
      metrics/
        targets.json                   # scrape target descriptors
        scrapes/<job>__<instance>.jsonl  # AUTHORITATIVE: one scrape per line
        sample/<job>__<instance>.prom    # a raw /metrics document, for humans

Requirement R5 in one paragraph
-------------------------------
``labels.json`` sits in that directory. **Nothing under ``ano/`` may open it.**
Every read in this module names its file explicitly; the one directory listing
(:func:`iter_scrape_files`) is scoped to ``metrics/scrapes`` and additionally
filtered, and :func:`guard_path` raises on any path whose basename is a known
ground-truth artefact. Proximity is not permission, so the guard is code rather
than a comment — the E2E runtime audit hook watches for exactly this.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from typing import Any

from ano.ingest.records import (
    MetricScrape,
    ResourceIndex,
    coerce_target,
    rfc3339_to_millis,
)
from ano.telemetry import LogEntry, PrometheusTarget, parse_log_entries

__all__ = [
    "GROUND_TRUTH_BASENAMES",
    "PHASES",
    "Ground_Truth_Access_Error",
    "SeedDirectory",
    "guard_path",
    "read_targets",
    "iter_scrape_files",
    "iter_scrapes",
    "iter_log_entries",
    "read_topology",
    "read_resource_index",
    "read_change_calendar",
    "read_manifest",
]

#: File basenames that hold ground truth. Opening one from ``ano/`` is a
#: requirement-R5 violation, so this module refuses to.
GROUND_TRUTH_BASENAMES: frozenset[str] = frozenset(
    {"labels.json", "ground_truth.json", "labels.jsonl"}
)

#: Scrape phases the generator emits. ``history`` is the long unlabelled
#: baseline period; ``window`` is the evaluation period.
PHASES: tuple[str, ...] = ("history", "window")


class Ground_Truth_Access_Error(PermissionError):
    """Raised when something under ``ano/`` tries to open a label artefact.

    A hard failure, deliberately. Requirement R5 makes "the detector never sees
    the answers" an acceptance criterion, and an accidental directory glob is
    the realistic way that would be broken. Failing loudly at the moment of
    access is the only version of this guarantee worth having.
    """


def guard_path(path: str | os.PathLike[str]) -> str:
    """Return ``path`` as a string, refusing ground-truth artefacts.

    Args:
        path: The file about to be opened.

    Returns:
        The path, unchanged.

    Raises:
        Ground_Truth_Access_Error: if the basename is in
            :data:`GROUND_TRUTH_BASENAMES`.
    """
    text = os.fspath(path)
    if os.path.basename(text).lower() in GROUND_TRUTH_BASENAMES:
        raise Ground_Truth_Access_Error(
            f"{text!r} is a ground-truth label artefact; requirement R5 "
            "forbids anything under ano/ from reading it. Only harness/ may."
        )
    return text


@dataclass(frozen=True, slots=True)
class SeedDirectory:
    """Locates the artefacts of one generated seed. Pure path arithmetic.

    Attributes:
        root: The seed directory. Either the directory itself, or the
            ``logs.jsonl`` inside it — :meth:`locate` accepts both, because
            ruling IR-05 hands the detection entrypoint a ``logs_path`` and a
            ``metrics_path`` rather than a root.
    """

    root: str

    @classmethod
    def locate(
        cls,
        path: str | os.PathLike[str],
        metrics_path: str | os.PathLike[str] | None = None,
    ) -> "SeedDirectory":
        """Build from a seed root, or from the ``logs.jsonl`` path (IR-05).

        Args:
            path: The seed directory, or the logs file inside it.
            metrics_path: The metrics directory, when it is not the default
                ``<root>/metrics``. Only its parent is used to locate the root.
        """
        text = os.fspath(path)
        root = os.path.dirname(text) if os.path.isfile(text) else text
        if not root and metrics_path is not None:
            root = os.path.dirname(os.fspath(metrics_path).rstrip(os.sep))
        return cls(root=root or ".")

    @property
    def logs_path(self) -> str:
        """``<root>/logs.jsonl``."""
        return os.path.join(self.root, "logs.jsonl")

    @property
    def metrics_dir(self) -> str:
        """``<root>/metrics``."""
        return os.path.join(self.root, "metrics")

    @property
    def targets_path(self) -> str:
        """``<root>/metrics/targets.json``."""
        return os.path.join(self.metrics_dir, "targets.json")

    @property
    def scrapes_dir(self) -> str:
        """``<root>/metrics/scrapes``."""
        return os.path.join(self.metrics_dir, "scrapes")

    @property
    def topology_path(self) -> str:
        """``<root>/topology.json`` — an operations input (IR-03)."""
        return os.path.join(self.root, "topology.json")

    @property
    def change_calendar_path(self) -> str:
        """``<root>/change_calendar.json`` — an operations input (IR-03)."""
        return os.path.join(self.root, "change_calendar.json")

    @property
    def manifest_path(self) -> str:
        """``<root>/manifest.json``."""
        return os.path.join(self.root, "manifest.json")

    def exists(self) -> bool:
        """True if at least the log file or the metrics directory is present."""
        return os.path.exists(self.logs_path) or os.path.isdir(self.metrics_dir)


def _load_json(path: str | os.PathLike[str]) -> Any:
    """Read and parse a JSON file, through :func:`guard_path`."""
    with open(guard_path(path), "r", encoding="utf-8") as handle:
        return json.load(handle)


def read_manifest(path: str | os.PathLike[str]) -> dict[str, Any]:
    """Read ``manifest.json``. Returns ``{}`` when it is absent."""
    text = guard_path(path)
    if not os.path.exists(text):
        return {}
    obj = _load_json(text)
    return dict(obj) if isinstance(obj, Mapping) else {}


def read_targets(
    path: str | os.PathLike[str],
) -> dict[tuple[str, str], PrometheusTarget]:
    """Read ``metrics/targets.json`` into a ``(job, instance)`` index.

    The file's entries carry the six Managed Prometheus target labels plus
    descriptive keys (``exporter``, ``entity_kind``). Only the six become
    labels; see :func:`ano.ingest.records.coerce_target`.

    Returns:
        Target descriptors keyed by ``(job, instance)`` — which is exactly how
        the scrape filenames are formed, so a scrape file resolves to its
        target without a filename heuristic.

    Raises:
        ValueError: if two entries declare the same ``(job, instance)``.
    """
    obj = _load_json(path)
    entries = obj.get("targets", obj) if isinstance(obj, Mapping) else obj
    index: dict[tuple[str, str], PrometheusTarget] = {}
    for entry in entries:
        target = coerce_target(entry)
        key = (target.job, target.instance)
        if key in index:
            raise ValueError(
                f"targets.json declares {key} twice; a scrape target must be "
                "unique or a scrape file cannot be attributed"
            )
        index[key] = target
    return index


def iter_scrape_files(scrapes_dir: str | os.PathLike[str]) -> tuple[str, ...]:
    """Every ``<job>__<instance>.jsonl`` scrape file, in sorted order.

    Scoped to the scrapes directory and filtered to the expected extension, so
    no other artefact can be swept in. Sorted so ingest order is reproducible.
    """
    directory = os.fspath(scrapes_dir)
    if not os.path.isdir(directory):
        return ()
    names = sorted(
        name
        for name in os.listdir(directory)
        if name.endswith(".jsonl")
        and name.lower() not in GROUND_TRUTH_BASENAMES
    )
    return tuple(os.path.join(directory, name) for name in names)


def iter_scrapes(
    metrics_dir: str | os.PathLike[str],
    *,
    targets: Mapping[tuple[str, str], PrometheusTarget] | None = None,
    phases: Iterable[str] | None = None,
) -> Iterator[MetricScrape]:
    """Stream every scrape in ``metrics/scrapes``, resolved against its target.

    Args:
        metrics_dir: The ``metrics/`` directory (IR-05 ``metrics_path``).
        targets: A pre-read target index. Read from ``targets.json`` if omitted.
        phases: Keep only these phases (``history`` / ``window``). ``None``
            keeps all.

    Yields:
        :class:`ano.ingest.records.MetricScrape`, file by file in sorted file
        order and line by line within a file. Callers that need global time
        order sort afterwards; the ingest pipeline does.

    Raises:
        ValueError: if a scrape file's ``(job, instance)`` is not in
            ``targets.json``, or a line is missing a required field.
    """
    directory = os.fspath(metrics_dir)
    index = (
        dict(targets)
        if targets is not None
        else read_targets(os.path.join(directory, "targets.json"))
    )
    wanted = frozenset(phases) if phases is not None else None

    for file_path in iter_scrape_files(os.path.join(directory, "scrapes")):
        stem = os.path.basename(file_path)[: -len(".jsonl")]
        job, separator, instance = stem.partition("__")
        if not separator:
            raise ValueError(
                f"scrape file {file_path!r} is not named <job>__<instance>.jsonl"
            )
        target = index.get((job, instance))
        if target is None:
            raise ValueError(
                f"scrape file {file_path!r} names target ({job!r}, "
                f"{instance!r}), which targets.json does not declare"
            )
        with open(guard_path(file_path), "r", encoding="utf-8") as handle:
            for number, line in enumerate(handle, start=1):
                stripped = line.strip()
                if not stripped:
                    continue
                obj = json.loads(stripped)
                phase = str(obj.get("phase", ""))
                if wanted is not None and phase not in wanted:
                    continue
                try:
                    scrape_time = obj["scrape_time"]
                    exposition = obj["exposition"]
                except KeyError as error:
                    raise ValueError(
                        f"{file_path}:{number} is missing {error.args[0]!r}"
                    ) from error
                yield MetricScrape(
                    timestamp_ms=rfc3339_to_millis(scrape_time),
                    text=exposition,
                    target=target,
                    phase=phase,
                )


def iter_log_entries(
    logs_path: str | os.PathLike[str], *, validate: bool = True
) -> Iterator[LogEntry]:
    """Stream ``logs.jsonl`` through the M2 parser.

    Args:
        logs_path: Path to the JSON Lines log file.
        validate: Validate each entry against the ``LogEntry`` schema. Leave on
            unless profiling says otherwise; a malformed entry that reaches the
            store is far more expensive than the check.
    """
    yield from parse_log_entries(guard_path(logs_path), validate=validate)


def read_resource_index(path: str | os.PathLike[str]) -> ResourceIndex:
    """Build the monitored-resource join from ``topology.json``.

    Each entity in the estate inventory may declare the monitored resource it
    appears as::

        {"entity_id": "gsk-app-lon-tc-01", "kind": "app_server",
         "log_resource": {"type": "gce_instance",
                          "labels": {"instance_id": "1063...", "project_id": "..."}}}

    Cloud Logging entries identify their origin only by that resource, so
    without this join every log row would land under a numeric provider id
    while the metric stream used the entity name — the same host split across
    two namespaces. This is identity data of exactly the kind a CMDB holds; it
    says nothing about incidents and is an operations input under ruling IR-03.

    Returns:
        A populated :class:`ResourceIndex`, empty if the inventory declares no
        resources. Emptiness is not an error here — the caller reports the
        resulting unresolved count, which is the visible signal.

    Raises:
        FileNotFoundError: if the file is absent.
        ValueError: if two entities claim the same resource identity.
    """
    obj = _load_json(path)
    index = ResourceIndex()
    for item in obj.get("entities", ()):
        resource = item.get("log_resource")
        if not isinstance(resource, Mapping):
            continue
        labels = resource.get("labels")
        if not isinstance(labels, Mapping):
            continue
        index.add(
            str(item["entity_id"]), str(resource.get("type") or ""), labels
        )
    return index


def read_topology(
    path: str | os.PathLike[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Read ``topology.json`` into ``entities`` and ``topology`` store rows.

    An operations input, not ground truth (ruling IR-03): it describes what is
    connected to what, which any operations team knows about its own estate.

    Returns:
        ``(entity_rows, edge_rows)`` ready for
        :meth:`AnalyticalStore.insert_rows`. Entity rows carry the per-kind
        attributes (service, schema, thread pool size, port speed, ...) as
        ``labels``; edge rows carry the declared ``queries`` / ``traverses``
        relations plus the ``hosts`` edges from each switch to its ports.

    Raises:
        FileNotFoundError: if the file is absent — the caller decides whether
            that is fatal, but it is never papered over.
    """
    obj = _load_json(path)
    kinds: dict[str, str] = {
        str(item["entity_id"]): str(item["kind"])
        for item in obj.get("entities", ())
    }
    sites: dict[str, str] = {}
    labels: dict[str, dict[str, str]] = {}

    def absorb(block: str, extra_kind: str | None = None) -> None:
        for item in obj.get(block, ()):
            entity_id = str(item["entity_id"])
            if extra_kind is not None:
                kinds.setdefault(entity_id, extra_kind)
            if item.get("site"):
                sites[entity_id] = str(item["site"])
            attrs = {
                key: _label_value(value)
                for key, value in sorted(item.items())
                if key not in ("entity_id", "site", "ports")
            }
            if attrs:
                labels[entity_id] = attrs

    absorb("app_servers", "app_server")
    absorb("databases", "database")
    absorb("switches", "switch")
    absorb("nodes", "node")

    edges: list[dict[str, Any]] = []
    for switch in obj.get("switches", ()):
        switch_id = str(switch["entity_id"])
        site = sites.get(switch_id)
        for port in switch.get("ports", ()):
            port_id = str(port["entity_id"])
            kinds.setdefault(port_id, "switch")
            if site:
                sites.setdefault(port_id, site)
            labels[port_id] = {
                key: _label_value(value)
                for key, value in sorted(port.items())
                if key != "entity_id"
            } | {"switch": switch_id}
            edges.append(
                {
                    "src_entity": switch_id,
                    "src_kind": "switch",
                    "dst_entity": port_id,
                    "dst_kind": "switch",
                    "relation": "hosts",
                }
            )

    for edge in obj.get("edges", ()):
        relation, src, dst = (str(part) for part in edge)
        edges.append(
            {
                "src_entity": src,
                "src_kind": kinds.get(src),
                "dst_entity": dst,
                "dst_kind": kinds.get(dst),
                "relation": relation,
            }
        )

    entity_rows = [
        {
            "entity_id": entity_id,
            "kind": kinds[entity_id],
            "site": sites.get(entity_id),
            "labels": labels.get(entity_id, {}),
        }
        for entity_id in sorted(kinds)
    ]
    edges.sort(key=lambda row: (row["src_entity"], row["relation"], row["dst_entity"]))
    return entity_rows, edges


def read_change_calendar(path: str | os.PathLike[str]) -> list[dict[str, Any]]:
    """Read ``change_calendar.json`` into ``change_events`` store rows.

    An operations input, not ground truth (ruling IR-03): it names scheduled
    work, never an incident, a root cause or a genuineness flag.

    A calendar entry naming several entities becomes one row per entity, since
    ``change_events`` is keyed per entity. The row key is
    ``"<change_id>@<entity_id>"`` so the fan-out cannot collide on the primary
    key; :class:`ano.ingest.features.ChangeWindow` strips the suffix so callers
    still see the original id.

    Returns:
        Rows ready for :meth:`AnalyticalStore.insert_rows`, sorted by start
        time then row key.
    """
    obj = _load_json(path)
    rows: list[dict[str, Any]] = []
    for change in obj.get("changes", ()) if isinstance(obj, Mapping) else obj:
        change_id = str(change["change_id"])
        kind = str(change["kind"])
        start = str(change["start"])
        end = str(change["end"])
        description = str(change.get("description", ""))
        entities = [str(item) for item in change.get("entities", ())]
        if not entities:
            entities = [""]
        for entity_id in entities:
            rows.append(
                {
                    "change_id": f"{change_id}@{entity_id}" if entity_id else change_id,
                    "kind": kind,
                    "entity_id": entity_id or None,
                    "start_time": start,
                    "end_time": end,
                    "description": description,
                }
            )
    rows.sort(key=lambda row: (row["start_time"], row["change_id"]))
    return rows


def _label_value(value: Any) -> str:
    """Render a topology attribute as a label string."""
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return repr(value)
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def parse_phase_bounds(
    scrapes: Iterable[MetricScrape],
) -> dict[str, tuple[int, int]]:
    """Earliest and latest scrape time per phase, in epoch milliseconds.

    Used to record the ``history`` / ``window`` split in the store so a model
    can declare its training window from ingested data alone.
    """
    bounds: dict[str, tuple[int, int]] = {}
    for scrape in scrapes:
        phase = scrape.phase or "unknown"
        low, high = bounds.get(phase, (scrape.timestamp_ms, scrape.timestamp_ms))
        bounds[phase] = (
            min(low, scrape.timestamp_ms),
            max(high, scrape.timestamp_ms),
        )
    return bounds

