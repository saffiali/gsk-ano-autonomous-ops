"""Normalised ingest records, the GMP target attach, and entity resolution.

The ingest pipeline carries **one** element type — :class:`TelemetryEnvelope` —
so a single stage graph can handle Cloud Logging entries and Managed Prometheus
samples together (feature 26). This module defines that envelope, the flattened
store rows it decodes into, and the entity resolver that answers "which piece
of the estate is this telemetry about?".

The Managed Prometheus target attach (ruling IR-02)
---------------------------------------------------
A real exporter serves ``/metrics`` with **no** target labels; the Managed
Prometheus collector attaches ``project_id``, ``location``, ``cluster``,
``namespace``, ``job`` and ``instance`` at scrape time. ``ano.telemetry``
models that faithfully: :class:`ano.telemetry.MetricSample` refuses those six
keys, and the target lives beside the exposition text as a
:class:`ano.telemetry.PrometheusTarget`.

**This layer is the collector.** :func:`attach` calls
``ano.telemetry.attach_target_labels`` to turn a
:class:`ano.telemetry.MetricSample` into a
:class:`ano.telemetry.ScrapedSample`, and from that point on every consumer
sees post-attach samples in which ``instance`` is the entity id and ``job`` is
the exporter family. That is the contract M4 and M5 code against, so metric
entity resolution reads ``ScrapedSample.entity_id`` and nothing else.

Log entity resolution
---------------------
Cloud Logging has no equivalent out-of-band attach, so log entries are resolved
by label lookup: an ordered walk of candidate keys across the label maps an
entry actually carries (``LogEntry.labels``, ``LogEntry.resource.labels``, the
top level of ``jsonPayload``). The first key that yields a non-empty value
wins. Nothing is guessed and nothing is dropped: a record whose entity cannot
be resolved is still ingested with a NULL ``entity_id`` and counted, because a
silent drop is how a pipeline loses data without anybody noticing.
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from ano.contracts.common import dumps_canonical, format_rfc3339
from ano.contracts.determinism import stable_id
from ano.telemetry import (
    MetricSample,
    PrometheusTarget,
    ScrapedSample,
    attach_target_labels,
)

#: Where the collector attach comes from. Recorded in every
#: :class:`ano.ingest.pipeline.IngestResult` so the provenance of the
#: post-attach labels is visible in the run record and not merely assumed.
TARGET_ATTACH_SOURCE = "ano.telemetry.attach_target_labels"

__all__ = [
    "DEFAULT_ENTITY_LABEL_KEYS",
    "GMP_TARGET_LABELS",
    "METRIC_ENTITY_LABEL",
    "TARGET_ATTACH_SOURCE",
    "LOG_KIND",
    "METRIC_KIND",
    "TelemetryEnvelope",
    "LogRecord",
    "MetricRecord",
    "MetricScrape",
    "EntityResolver",
    "ResourceIndex",
    "OPAQUE_ENTITY_LABEL_KEYS",
    "attach",
    "coerce_target",
    "extract_message",
    "nanos_to_rfc3339",
    "rfc3339_to_millis",
]


#: Element kinds carried by the shared pipeline.
LOG_KIND = "log"
METRIC_KIND = "metric"

#: The six labels Managed Prometheus attaches at scrape time, in the order this
#: layer writes them.
GMP_TARGET_LABELS: tuple[str, ...] = (
    "project_id",
    "location",
    "cluster",
    "namespace",
    "job",
    "instance",
)

#: The post-attach label that carries the entity id for a metric sample.
#: Fixed by ruling IR-02: ``instance`` equals the C1 ``entity_id``. There is
#: deliberately no fallback chain for metrics — a sample that reaches the store
#: without an ``instance`` is a generator bug, and counting it is how it gets
#: found.
METRIC_ENTITY_LABEL = "instance"

#: Label keys consulted, in order, when resolving the entity a **log entry** is
#: about. The first group is what this project's own telemetry stamps; the rest
#: are the conventional Cloud Logging monitored-resource label names, so
#: realistically shaped log entries resolve without configuration.
DEFAULT_ENTITY_LABEL_KEYS: tuple[str, ...] = (
    "entity_id",
    "entity",
    "instance_id",
    "instance",
    "node_name",
    "node",
    "host",
    "hostname",
    "server",
    "database_id",
    "device",
    "target",
    "pod_name",
    "container_name",
    "cluster_name",
)


_UTC = _dt.timezone.utc


def nanos_to_rfc3339(nanos: int) -> str:
    """Render epoch nanoseconds as RFC 3339 UTC text.

    The store keeps timestamps as RFC 3339 text because that sorts
    lexicographically in chronological order in both SQLite and BigQuery.
    Nanosecond precision is preserved in the fractional part; Python's
    ``datetime`` would silently truncate it to microseconds, so the fraction is
    formatted from the integer directly.

    Args:
        nanos: Epoch nanoseconds.

    Returns:
        e.g. ``"2026-09-13T14:00:00.123456789Z"``. A whole second renders
        without a fractional part.
    """
    seconds, remainder = divmod(int(nanos), 1_000_000_000)
    moment = _dt.datetime.fromtimestamp(seconds, tz=_UTC)
    base = moment.strftime("%Y-%m-%dT%H:%M:%S")
    if remainder:
        return f"{base}.{remainder:09d}".rstrip("0") + "Z"
    return base + "Z"


def rfc3339_to_millis(text: str) -> int:
    """Parse RFC 3339 UTC text into epoch milliseconds (floor)."""
    cleaned = text.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    moment = _dt.datetime.fromisoformat(cleaned)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=_UTC)
    return int(moment.timestamp() * 1000)


def coerce_target(target: PrometheusTarget | Mapping[str, str]) -> PrometheusTarget:
    """Build a :class:`ano.telemetry.PrometheusTarget` from loose input.

    ``metrics/targets.json`` carries the six target labels **plus** descriptive
    keys (``exporter``, ``entity_kind``) that are documentation, not labels.
    ``PrometheusTarget.from_dict`` is strict about its key set — correctly so —
    which is exactly why this narrowing step exists and is explicit rather than
    hidden inside a loader.

    Args:
        target: An existing target, or a mapping containing at least the six
            target label names.

    Returns:
        The target.

    Raises:
        ValueError: if any of the six labels is missing or empty.
    """
    if isinstance(target, PrometheusTarget):
        return target
    mapping = dict(target)
    missing = [
        name for name in GMP_TARGET_LABELS if not str(mapping.get(name, "")).strip()
    ]
    if missing:
        raise ValueError(
            "target descriptor is missing "
            + ", ".join(repr(name) for name in missing)
            + "; the Managed Prometheus target labels are "
            + ", ".join(GMP_TARGET_LABELS)
        )
    return PrometheusTarget.from_dict(
        {name: str(mapping[name]) for name in GMP_TARGET_LABELS}
    )


def attach(
    sample: MetricSample, target: PrometheusTarget | Mapping[str, str]
) -> ScrapedSample:
    """Perform the Managed Prometheus scrape-time attach (ruling IR-02).

    This layer is the collector: it is the first point at which a sample can
    say which entity it came from. Everything downstream sees the result.

    Args:
        sample: The exporter-emitted sample, which by construction carries none
            of the six reserved target labels.
        target: The scrape target descriptor.

    Returns:
        The post-attach :class:`ano.telemetry.ScrapedSample`, whose
        ``entity_id`` is the target's ``instance``.
    """
    return attach_target_labels(sample, coerce_target(target))



@dataclass(frozen=True, slots=True)
class MetricScrape:
    """One Managed Prometheus scrape: exposition text plus its target and time.

    This mirrors a real scrape exactly. The exporter body carries no target
    labels (``ano.telemetry.MetricSample`` refuses them) and no per-sample
    timestamps; the collector supplies both. Modelling it this way is what
    makes the synthetic metric plane shape-faithful and gives every sample an
    identity without inventing a label real exporters do not emit.

    Attributes:
        timestamp_ms: Scrape time, epoch milliseconds. Applied to every sample
            in ``text`` that does not carry its own timestamp.
        text: Prometheus text-exposition content (version 0.0.4), pre-attach.
        target: The target descriptor — a
            :class:`ano.telemetry.PrometheusTarget` or a mapping containing the
            six target label names. ``instance`` is the entity id (IR-02);
            ``job`` identifies the exporter family.
        phase: ``"history"`` for the long unlabelled baseline period,
            ``"window"`` for the evaluation period, ``""`` when the source did
            not say. Carried through ingest so a model can declare which data
            it trained on without consulting anything it is not allowed to see.
    """

    timestamp_ms: int
    text: str
    target: PrometheusTarget | Mapping[str, str] = field(default_factory=dict)
    phase: str = ""

    def prometheus_target(self) -> PrometheusTarget:
        """The target descriptor as a :class:`ano.telemetry.PrometheusTarget`."""
        return coerce_target(self.target)

    def target_labels(self) -> dict[str, str]:
        """The target descriptor as a plain ``str -> str`` mapping."""
        return self.prometheus_target().to_dict()

    def entity_id(self) -> str:
        """The entity this scrape targets — its ``instance`` label (IR-02)."""
        return self.prometheus_target().instance




@dataclass(frozen=True, slots=True)
class TelemetryEnvelope:
    """The single element type flowing through the ingest pipeline.

    Attributes:
        kind: :data:`LOG_KIND` or :data:`METRIC_KIND`.
        timestamp_ms: Event time, epoch milliseconds. Used for ordering and for
            fixed-window assignment.
        key: A stable, unique-per-record key. Log entries use their
            ``insertId``; metric samples use a digest of
            (metric, labels, timestamp). This is the idempotency key.
        payload: The record itself, as plain data.

    The envelope is what is published to Pub/Sub, so it must round-trip through
    ``bytes``: see :meth:`to_bytes` and :meth:`from_bytes`.
    """

    kind: str
    timestamp_ms: int
    key: str
    payload: Mapping[str, Any]

    def __post_init__(self) -> None:
        if self.kind not in (LOG_KIND, METRIC_KIND):
            raise ValueError(
                f"envelope kind must be {LOG_KIND!r} or {METRIC_KIND!r}, "
                f"got {self.kind!r}"
            )
        if not self.key:
            raise ValueError("envelope key must be non-empty")

    def to_bytes(self) -> bytes:
        """Canonical JSON encoding — the Pub/Sub message body."""
        return dumps_canonical(
            {
                "kind": self.kind,
                "timestamp_ms": self.timestamp_ms,
                "key": self.key,
                "payload": self.payload,
            }
        ).encode("utf-8")

    def attributes(self) -> dict[str, str]:
        """Pub/Sub message attributes. Kept to strings, as the real service is."""
        return {"kind": self.kind, "key": self.key}

    @classmethod
    def from_bytes(cls, data: bytes) -> "TelemetryEnvelope":
        """Decode an envelope published by :meth:`to_bytes`."""
        import json

        obj = json.loads(data.decode("utf-8"))
        return cls(
            kind=obj["kind"],
            timestamp_ms=int(obj["timestamp_ms"]),
            key=obj["key"],
            payload=obj["payload"],
        )

    def sort_key(self) -> tuple[int, str, str]:
        """Total order: event time, then kind, then key. No ties, ever."""
        return (self.timestamp_ms, self.kind, self.key)


@dataclass(frozen=True, slots=True)
class LogRecord:
    """A Cloud Logging entry flattened into ``log_entries`` columns."""

    insert_id: str
    timestamp: str
    receive_timestamp: str | None
    severity: str
    log_name: str
    resource_type: str
    resource_labels: Mapping[str, str]
    labels: Mapping[str, str]
    text_payload: str | None
    json_payload: Mapping[str, Any] | None
    entity_id: str | None
    message: str

    def to_row(self) -> dict[str, Any]:
        """The ``log_entries`` row for :meth:`AnalyticalStore.insert_rows`."""
        return {
            "insert_id": self.insert_id,
            "timestamp": self.timestamp,
            "receive_timestamp": self.receive_timestamp,
            "severity": self.severity,
            "log_name": self.log_name,
            "resource_type": self.resource_type,
            "resource_labels": dict(self.resource_labels),
            "labels": dict(self.labels),
            "text_payload": self.text_payload,
            "json_payload": (
                dict(self.json_payload) if self.json_payload is not None else None
            ),
            "entity_id": self.entity_id,
            "message": self.message,
        }


@dataclass(frozen=True, slots=True)
class MetricRecord:
    """A Managed Prometheus sample flattened into ``metric_samples`` columns."""

    metric: str
    labels: Mapping[str, str]
    value: float
    timestamp_ms: int
    entity_id: str | None

    def to_row(self) -> dict[str, Any]:
        """The ``metric_samples`` row for :meth:`AnalyticalStore.insert_rows`."""
        return {
            "metric": self.metric,
            "labels": dict(self.labels),
            "value": float(self.value),
            "timestamp_ms": int(self.timestamp_ms),
            "entity_id": self.entity_id,
        }

    def dedupe_key(self) -> str:
        """Stable identity of this sample, for idempotent re-ingest.

        ``metric_samples`` has no natural primary key — a time series is
        identified by (name, label set, time). This folds those three into one
        content-derived id so a re-run recognises a sample it already stored.
        """
        canonical = dumps_canonical(
            {
                "metric": self.metric,
                "labels": dict(self.labels),
                "timestamp_ms": int(self.timestamp_ms),
            }
        )
        return stable_id(canonical, "ingest", "metric-sample", length=24)


#: Label keys whose value is a *provider-assigned* identifier rather than a
#: name an operator would recognise. A GCE ``instance_id`` is a decimal integer
#: that means nothing outside the cloud project, so accepting it as an entity
#: id would silently produce a second, parallel namespace for the same host —
#: metrics under ``gsk-app-lon-tc-01`` and logs under ``1090374129436298431``.
#: These keys therefore only resolve through :class:`ResourceIndex`; if the
#: estate inventory does not map them, the record is reported unresolved.
OPAQUE_ENTITY_LABEL_KEYS: frozenset[str] = frozenset({"instance_id"})


class ResourceIndex:
    """Maps a monitored-resource identity onto the entity that owns it.

    Cloud Logging entries name their origin with a ``MonitoredResource`` — a
    type plus a few labels — and for ``gce_instance`` those labels contain the
    numeric instance id and nothing else that identifies the host. The estate
    inventory is what closes the gap: it states, per entity, which monitored
    resource that entity appears as. This is the same join a real log pipeline
    performs against a CMDB or the Compute inventory, and it carries no
    information about incidents, only about identity.

    Matching is by **subset**: an inventory entry matches a log resource of the
    same type when every label the inventory names is present on the record
    with the same value. Extra labels on the record are ignored, so a provider
    adding a field does not break the join.
    """

    __slots__ = ("_by_type",)

    def __init__(self) -> None:
        # type -> label-key tuple -> label-value tuple -> entity id.
        self._by_type: dict[
            str, dict[tuple[str, ...], dict[tuple[str, ...], str]]
        ] = {}

    def add(
        self,
        entity_id: str,
        resource_type: str,
        labels: Mapping[str, Any],
    ) -> None:
        """Register that ``entity_id`` appears as this monitored resource.

        Raises:
            ValueError: if the identity is already claimed by a different
                entity. An ambiguous join is a defect in the inventory and
                must not be resolved by arbitrary precedence.
        """
        if not entity_id or not resource_type or not labels:
            return
        items = tuple(
            sorted(
                (str(key), str(value))
                for key, value in labels.items()
                if value is not None and str(value) != ""
            )
        )
        if not items:
            return
        keys = tuple(key for key, _ in items)
        values = tuple(value for _, value in items)
        slot = self._by_type.setdefault(resource_type, {}).setdefault(keys, {})
        existing = slot.get(values)
        if existing is not None and existing != entity_id:
            raise ValueError(
                f"monitored resource {resource_type} {dict(items)!r} is "
                f"claimed by both {existing!r} and {entity_id!r}; the estate "
                "inventory is ambiguous"
            )
        slot[values] = entity_id

    def resolve(
        self, resource_type: str, labels: Mapping[str, Any] | None
    ) -> str | None:
        """The entity owning this resource, or ``None`` if it is unknown."""
        if not resource_type or not labels:
            return None
        by_keys = self._by_type.get(resource_type)
        if not by_keys:
            return None
        for keys in sorted(by_keys):
            try:
                values = tuple(str(labels[key]) for key in keys)
            except KeyError:
                continue
            found = by_keys[keys].get(values)
            if found is not None:
                return found
        return None

    def __len__(self) -> int:
        return sum(
            len(slot)
            for by_keys in self._by_type.values()
            for slot in by_keys.values()
        )

    def __bool__(self) -> bool:
        return len(self) > 0


class EntityResolver:
    """Resolves the monitored entity a telemetry record belongs to.

    Resolution is tried in three steps, most trustworthy first:

    1. a label that names the entity outright (``entity_id``, ``instance``,
       ``host``, ...);
    2. the monitored-resource join, via :class:`ResourceIndex`, for records
       that identify their origin only as a cloud resource;
    3. nothing — the record is reported unresolved.

    There is deliberately no fourth step. A provider-assigned identifier such
    as a GCE ``instance_id`` is **not** accepted as an entity id when the
    inventory fails to map it, because doing so would put the same host under
    two keys — its name in the metric stream and its numeric id in the log
    stream — and nothing downstream would ever join them again. A missing
    mapping is a visible coverage number instead.

    Args:
        label_keys: Candidate label keys, most specific first. Defaults to
            :data:`DEFAULT_ENTITY_LABEL_KEYS`.
        aliases: Optional map from a resolved raw value to a canonical entity
            id, for estates where the telemetry label and the entity register
            disagree (a hostname versus an inventory id, say). Empty by
            default; this is configuration, not a heuristic.
        resource_index: Inventory join for monitored resources. Empty by
            default, which simply means step 2 never matches.
        opaque_keys: Label keys that must go through ``resource_index``.
            Defaults to :data:`OPAQUE_ENTITY_LABEL_KEYS`.
    """

    __slots__ = ("_label_keys", "_aliases", "_resource_index", "_opaque_keys")

    def __init__(
        self,
        label_keys: Sequence[str] = DEFAULT_ENTITY_LABEL_KEYS,
        *,
        aliases: Mapping[str, str] | None = None,
        resource_index: ResourceIndex | None = None,
        opaque_keys: Iterable[str] = OPAQUE_ENTITY_LABEL_KEYS,
    ) -> None:
        if not label_keys:
            raise ValueError("label_keys must not be empty")
        self._label_keys = tuple(label_keys)
        self._aliases = dict(aliases or {})
        self._resource_index = resource_index or ResourceIndex()
        self._opaque_keys = frozenset(opaque_keys)

    @property
    def label_keys(self) -> tuple[str, ...]:
        """The candidate keys, in priority order."""
        return self._label_keys

    @property
    def resource_index(self) -> ResourceIndex:
        """The monitored-resource inventory join."""
        return self._resource_index

    def resolve(
        self,
        *label_maps: Mapping[str, Any] | None,
        resource_type: str = "",
        resource_labels: Mapping[str, Any] | None = None,
    ) -> str | None:
        """The entity this record belongs to, or ``None``.

        Priority is by **key**, then by map order: ``entity_id`` in the second
        map beats ``host`` in the first, because a record that names its entity
        explicitly should not be overridden by a coincidental hostname label.

        Args:
            *label_maps: Label mappings to search, in tie-break order.
            resource_type: Monitored-resource type, for the inventory join.
            resource_labels: Monitored-resource labels, for the inventory
                join. Usually one of ``label_maps`` as well; passing it here
                too is what enables step 2.

        Returns:
            The canonical entity id, or ``None`` if nothing resolved it.
        """
        for key in self._label_keys:
            if key in self._opaque_keys:
                continue
            for labels in label_maps:
                if not labels:
                    continue
                value = labels.get(key)
                if isinstance(value, str) and value.strip():
                    resolved = value.strip()
                    return self._aliases.get(resolved, resolved)

        joined = self._resource_index.resolve(resource_type, resource_labels)
        if joined is not None:
            return joined

        # An opaque id is only usable if the caller configured an alias for it
        # explicitly. Otherwise the record stays unresolved on purpose.
        for key in self._label_keys:
            if key not in self._opaque_keys:
                continue
            for labels in label_maps:
                if not labels:
                    continue
                value = labels.get(key)
                if isinstance(value, str) and value.strip():
                    aliased = self._aliases.get(value.strip())
                    if aliased is not None:
                        return aliased
        return None



def extract_message(
    text_payload: str | None,
    json_payload: Mapping[str, Any] | None,
    *,
    message_keys: Sequence[str] = ("message", "msg", "event", "summary"),
) -> str:
    """Recover the human-readable message from a ``LogEntry`` payload.

    Cloud Logging carries the text either in ``textPayload`` or somewhere in
    ``jsonPayload``. This looks in the conventional places and, failing that,
    falls back to a canonical rendering of the whole JSON payload so a
    structured-only entry still contributes a stable, embeddable string rather
    than an empty one.

    Args:
        text_payload: ``LogEntry.textPayload``, if any.
        json_payload: ``LogEntry.jsonPayload``, if any.
        message_keys: Keys to check inside ``jsonPayload``, in order.

    Returns:
        The message text, possibly empty.
    """
    if text_payload:
        return text_payload
    if not json_payload:
        return ""
    for key in message_keys:
        value = json_payload.get(key)
        if isinstance(value, str) and value:
            return value
    # No conventional message field: render the payload deterministically so
    # the structure itself becomes the signature.
    return " ".join(
        f"{key}={_scalar(json_payload[key])}" for key in sorted(json_payload)
    )


def _scalar(value: Any) -> str:
    """Render a JSON value compactly for message reconstruction."""
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    if isinstance(value, (int, float)):
        return repr(value)
    return dumps_canonical(value)


def format_timestamp(value: _dt.datetime | str | int | None) -> str | None:
    """Coerce a timestamp of any accepted shape to RFC 3339 UTC text.

    Args:
        value: An aware ``datetime``, RFC 3339 text, epoch nanoseconds, or
            ``None``.

    Returns:
        RFC 3339 text, or ``None`` when ``value`` is ``None``.
    """
    if value is None:
        return None
    if isinstance(value, _dt.datetime):
        return format_rfc3339(value)
    if isinstance(value, int):
        return nanos_to_rfc3339(value)
    return str(value)


def iter_label_maps(
    payload: Mapping[str, Any]
) -> Iterable[Mapping[str, Any] | None]:
    """Yield the label maps of a decoded log payload, in resolution order."""
    yield payload.get("labels")
    resource = payload.get("resource")
    if isinstance(resource, Mapping):
        yield resource.get("labels")
    json_payload = payload.get("jsonPayload")
    if isinstance(json_payload, Mapping):
        yield json_payload
