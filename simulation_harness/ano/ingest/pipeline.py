"""The single ingestion pipeline: Cloud Logging **and** Managed Prometheus.

Requirement R1 asks for both telemetry planes to reach the analytical store
"through a single pipeline". Not two pipelines that happen to write to the same
place — one. That is what this module builds, and the shape of it is the
evidence:

.. code-block:: text

    logs.jsonl ─┐
                ├─▶ one Pub/Sub topic ─▶ one subscription ─▶ one stage graph ─▶ store
    scrapes/ ───┘        (ano.gcp)          (ano.gcp)          (ano.gcp)

Every element on the wire is a :class:`ano.ingest.records.TelemetryEnvelope`,
tagged ``log`` or ``metric``. The stage graph is a
:class:`ano.gcp.dataflow.Pipeline` — the local stand-in for Cloud Dataflow —
and the stages are the ordinary Beam vocabulary:

==========================  ====================================================
Stage                       What it does
==========================  ====================================================
``decode-envelope``         Pub/Sub ``Message`` bytes to a typed envelope
``normalise``               envelope to flattened store rows; a metric scrape
                            fans out to one row per sample, and this is where
                            the Managed Prometheus target attach happens
``deduplicate``             drops rows already in the store, or already seen in
                            this run — what makes re-ingest idempotent
``embed``                   computes the R1 log-message vector embedding
``write-store``             buffered writes into BigQuery / SQLite
==========================  ====================================================

Determinism
-----------
Envelopes are published in a total order — event time, then kind, then key —
and the Pub/Sub shim is FIFO, so the stage graph sees the same sequence on
every run. Within a scrape, samples are ordered by metric then canonical label
text. No element order anywhere depends on dictionary iteration, file system
order, or a clock.

Idempotency
-----------
Running the same seed twice inserts nothing the second time and reports the
duplicates it skipped. The key is ``insertId`` for logs and a digest of
(metric, labels, timestamp) for samples, since ``metric_samples`` has no
natural primary key. Offline the existing keys are loaded into memory; the
BigQuery deployment does the same job with a ``MERGE`` on that key, which is
why the key is content-derived and stable rather than a row number.

Requirement R5
--------------
Nothing here reads a ground-truth label file. :func:`ano.ingest.sources.guard_path`
refuses to open one, and no directory listing in this package is broad enough
to reach ``labels.json``.
"""

from __future__ import annotations

import datetime as _dt
import json
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from ano.config import Config, default_config
from ano.contracts.common import dumps_canonical
from ano.contracts.determinism import stable_id
from ano.gcp.bqstore import Column, TableSpec, register_table
from ano.gcp.dataflow import (
    FlatMap,
    Filter,
    LocalPipelineRunner,
    Map,
    Pipeline,
    Write,
)
from ano.gcp.messaging import LocalBroker, LocalPublisher, LocalSubscriber
from ano.ingest import sources as _sources
from ano.ingest.records import (
    DEFAULT_ENTITY_LABEL_KEYS,
    LOG_KIND,
    METRIC_KIND,
    TARGET_ATTACH_SOURCE,
    EntityResolver,
    ResourceIndex,
    LogRecord,
    MetricRecord,
    MetricScrape,
    TelemetryEnvelope,
    attach,
    extract_message,
    nanos_to_rfc3339,
)
from ano.semantic.embedding import EmbeddingConfig, HashedEmbedder
from ano.telemetry import LogEntry, parse_exposition, serialize_log_entry

__all__ = [
    "INGEST_PHASES_TABLE",
    "IngestOptions",
    "IngestResult",
    "TelemetryIngestor",
    "ingest_seed_directory",
]

#: Records the ``history`` / ``window`` split observed during ingest.
#:
#: The generator marks each scrape with its phase; the store has nowhere to put
#: that per row, and per row is the wrong granularity anyway. What a model
#: needs is the **bounds**, so it can say "I trained on the history period,
#: which ran from X to Y" — feature 34's "training window is bounded and
#: declared", satisfied from ingested data alone with no sight of ground truth.
#:
#: Rows are content-keyed, so a re-ingest of identical data re-derives the same
#: key and is skipped rather than duplicated.

#: Phase recorded when nothing in the data places a record in a phase. Seeing
#: this in the store is a signal that the scrape stream was absent or silent,
#: not a normal outcome.
UNKNOWN_PHASE = "unknown"

INGEST_PHASES_TABLE = TableSpec(
    name="ingest_phases",
    description=(
        "Observed time bounds of each telemetry phase (history / window), "
        "recorded at ingest so a model can declare an auditable training "
        "window without reading ground truth."
    ),
    columns=(
        Column("phase_key", "TEXT", "STRING", nullable=False,
               description="content-derived row key"),
        Column("phase", "TEXT", "STRING", nullable=False,
               description="history | window | unknown"),
        Column("kind", "TEXT", "STRING", nullable=False,
               description="log | metric"),
        Column("start_time", "TEXT", "TIMESTAMP", nullable=False,
               description="earliest record in the phase, RFC3339"),
        Column("end_time", "TEXT", "TIMESTAMP", nullable=False,
               description="latest record in the phase, RFC3339"),
        Column("row_count", "INTEGER", "INT64",
               description="records observed in the phase"),
    ),
    primary_key=("phase_key",),
    indexes=(("phase", "kind"),),
)

register_table(INGEST_PHASES_TABLE)


@dataclass(frozen=True, slots=True)
class IngestOptions:
    """Knobs for one ingest run. Every default is the one the demo uses.

    Attributes:
        topic: Pub/Sub topic the envelopes are published to. One topic, both
            telemetry kinds — that is the "single pipeline" of requirement R1.
        subscription: Subscription the pipeline pulls from.
        batch_size: Rows buffered before a write to the store.
        embed_logs: Compute and store the R1 log-message embeddings. Only
            switch this off to measure the cost of the embedding stage.
        embedding: Embedding configuration. Defaults to
            :class:`ano.semantic.embedding.EmbeddingConfig`'s defaults.
        entity_label_keys: Candidate label keys for **log** entity resolution.
            Metric entity resolution is fixed to the Managed Prometheus
            ``instance`` label by ruling IR-02 and is not configurable.
        validate_logs: Validate each log entry against the ``LogEntry`` schema
            on the way in.
        pull_batch: Messages pulled from the subscription per call.
    """

    topic: str = "telemetry"
    subscription: str = "telemetry-ingest"
    batch_size: int = 1000
    embed_logs: bool = True
    embedding: EmbeddingConfig | None = None
    entity_label_keys: tuple[str, ...] = DEFAULT_ENTITY_LABEL_KEYS
    validate_logs: bool = True
    pull_batch: int = 1000

    def __post_init__(self) -> None:
        if self.batch_size < 1:
            raise ValueError(f"batch_size must be >= 1, got {self.batch_size}")
        if self.pull_batch < 1:
            raise ValueError(f"pull_batch must be >= 1, got {self.pull_batch}")
        if not self.topic or not self.subscription:
            raise ValueError("topic and subscription must be non-empty")


@dataclass(frozen=True, slots=True)
class IngestResult:
    """What one ingest run did. Every number is measured, none is assumed."""

    run_id: str
    logs_ingested: int = 0
    logs_duplicate: int = 0
    logs_unresolved_entity: int = 0
    metrics_ingested: int = 0
    metrics_duplicate: int = 0
    scrapes_read: int = 0
    embeddings_written: int = 0
    entities_loaded: int = 0
    topology_edges_loaded: int = 0
    change_events_loaded: int = 0
    phase_bounds: Mapping[str, Mapping[str, str]] = field(default_factory=dict)
    stage_metrics: tuple[Mapping[str, Any], ...] = ()
    embedding_config: Mapping[str, Any] = field(default_factory=dict)
    embedding_fingerprint: str = ""
    target_attach_source: str = TARGET_ATTACH_SOURCE
    sources: tuple[str, ...] = ()
    elapsed_s: float = 0.0

    @property
    def rows_written(self) -> int:
        """Total telemetry rows inserted (excluding estate metadata)."""
        return self.logs_ingested + self.metrics_ingested + self.embeddings_written

    def to_dict(self) -> dict[str, Any]:
        """Plain data, for a manifest or a demo panel."""
        return {
            "run_id": self.run_id,
            "logs_ingested": self.logs_ingested,
            "logs_duplicate": self.logs_duplicate,
            "logs_unresolved_entity": self.logs_unresolved_entity,
            "metrics_ingested": self.metrics_ingested,
            "metrics_duplicate": self.metrics_duplicate,
            "scrapes_read": self.scrapes_read,
            "embeddings_written": self.embeddings_written,
            "entities_loaded": self.entities_loaded,
            "topology_edges_loaded": self.topology_edges_loaded,
            "change_events_loaded": self.change_events_loaded,
            "phase_bounds": {
                phase: dict(bounds) for phase, bounds in self.phase_bounds.items()
            },
            "stage_metrics": [dict(item) for item in self.stage_metrics],
            "embedding_config": dict(self.embedding_config),
            "embedding_fingerprint": self.embedding_fingerprint,
            "target_attach_source": self.target_attach_source,
            "sources": list(self.sources),
            "elapsed_s": round(self.elapsed_s, 6),
        }


class TelemetryIngestor:
    """Runs the unified ingest pipeline into a contract-C3 analytical store.

    Args:
        store: The analytical store (``ano.gcp.connect(cfg)``).
        config: Project configuration. Only used for naming; the store is
            supplied directly so a caller can ingest into a scratch database.
        options: See :class:`IngestOptions`.
        broker: Pub/Sub broker to use. A private one is created by default, so
            two ingest runs in the same process cannot see each other's
            messages.

    Example::

        store = connect(cfg)
        store.ensure_schema()
        ingestor = TelemetryIngestor(store, cfg)
        result = ingestor.ingest_directory("artifacts/seed-1234")
    """

    def __init__(
        self,
        store: Any,
        config: Config | None = None,
        *,
        options: IngestOptions | None = None,
        broker: LocalBroker | None = None,
    ) -> None:
        self._store = store
        self._config = config or default_config()
        self._options = options or IngestOptions()
        self._broker = broker or LocalBroker()
        self._embedder = HashedEmbedder(
            self._options.embedding or EmbeddingConfig()
        )
        self._resolver = EntityResolver(self._options.entity_label_keys)
        # Rebuilt from the scrape stream at the start of every run; logs are
        # placed against it because they carry no phase of their own.
        self._phase_index = _PhaseIndex(())

    # -- properties --------------------------------------------------------
    @property
    def embedder(self) -> HashedEmbedder:
        """The embedder this ingestor writes vectors with."""
        return self._embedder

    @property
    def options(self) -> IngestOptions:
        """The options this ingestor was built with."""
        return self._options

    # -- high-level entry points -------------------------------------------
    def ingest_directory(
        self,
        path: str,
        *,
        metrics_path: str | None = None,
        phases: Iterable[str] | None = None,
        load_operational_inputs: bool = True,
    ) -> IngestResult:
        """Ingest one generated seed directory.

        Args:
            path: The seed directory, or the ``logs.jsonl`` inside it — ruling
                IR-05 hands the detection entrypoint the latter.
            metrics_path: The ``metrics/`` directory, when it is not
                ``<root>/metrics``.
            phases: Restrict metric scrapes to these phases. ``None`` ingests
                both ``history`` and ``window``, which is what a model needs in
                order to train on history and score the window.
            load_operational_inputs: Also load ``topology.json`` and
                ``change_calendar.json`` (operations inputs, ruling IR-03).

        Returns:
            The :class:`IngestResult` for the run.
        """
        seed = _sources.SeedDirectory.locate(path, metrics_path)
        metrics_dir = metrics_path or seed.metrics_dir
        used: list[str] = []

        log_entries: Sequence[LogEntry] = ()
        if _exists(seed.logs_path):
            log_entries = tuple(
                _sources.iter_log_entries(
                    seed.logs_path, validate=self._options.validate_logs
                )
            )
            used.append(seed.logs_path)

        scrapes: Sequence[MetricScrape] = ()
        if _exists(metrics_dir):
            scrapes = tuple(
                _sources.iter_scrapes(metrics_dir, phases=phases)
            )
            used.append(metrics_dir)

        estate_counts = (0, 0, 0)
        if load_operational_inputs:
            estate_counts = self._load_operational_inputs(seed, used)

        result = self.ingest(log_entries=log_entries, metric_scrapes=scrapes)
        entities, edges, changes = estate_counts
        return _replace(
            result,
            entities_loaded=entities,
            topology_edges_loaded=edges,
            change_events_loaded=changes,
            sources=tuple(used),
        )

    def ingest(
        self,
        *,
        log_entries: Iterable[LogEntry | Mapping[str, Any]] = (),
        metric_scrapes: Iterable[MetricScrape] = (),
    ) -> IngestResult:
        """Ingest in-memory telemetry through the pipeline.

        Both arguments feed the **same** topic, subscription and stage graph.

        Args:
            log_entries: ``LogEntry`` objects, or already-serialised
                ``LogEntry`` dicts.
            metric_scrapes: Managed Prometheus scrapes.

        Returns:
            The :class:`IngestResult` for the run.
        """
        started = time.perf_counter()
        envelopes = self._build_envelopes(log_entries, metric_scrapes)
        run_id = stable_id(
            dumps_canonical([envelope.key for envelope in envelopes]),
            "ingest",
            "run",
            prefix="ingest",
            length=12,
        )

        publisher, subscriber = self._wire_messaging()
        for envelope in envelopes:
            publisher.publish(
                self._options.topic, envelope.to_bytes(), envelope.attributes()
            )

        counters = _Counters()
        dedupe = _DedupeIndex(self._store)
        writer = _StoreWriter(self._store, self._options.batch_size, counters)
        pipeline = self._build_pipeline(dedupe, counters, writer)

        messages = subscriber.pull_all(
            self._options.subscription, batch=self._options.pull_batch
        )
        runner = LocalPipelineRunner()
        outcome = runner.run(pipeline, messages)
        writer.flush()
        subscriber.ack_all(messages)

        phase_bounds = self._record_phases(counters, dedupe)
        return IngestResult(
            run_id=run_id,
            logs_ingested=counters.logs_written,
            logs_duplicate=counters.logs_duplicate,
            logs_unresolved_entity=counters.logs_unresolved,
            metrics_ingested=counters.metrics_written,
            metrics_duplicate=counters.metrics_duplicate,
            scrapes_read=counters.scrapes_read,
            embeddings_written=counters.embeddings_written,
            phase_bounds=phase_bounds,
            stage_metrics=tuple(item.to_dict() for item in outcome.metrics),
            embedding_config=self._embedder.config.to_dict(),
            embedding_fingerprint=self._embedder.fingerprint(),
            elapsed_s=time.perf_counter() - started,
        )

    # -- estate / operations inputs ----------------------------------------
    def install_resource_index(self, index: ResourceIndex) -> None:
        """Use ``index`` to attribute records that name only a cloud resource.

        Rebuilds the resolver rather than mutating it, so the resolver stays
        an immutable value object. Call before :meth:`ingest`.
        """
        self._resolver = EntityResolver(
            self._options.entity_label_keys, resource_index=index
        )

    def load_entities(self, rows: Sequence[Mapping[str, Any]]) -> int:
        """Insert estate rows, skipping entities already present."""
        return self._insert_new("entities", rows, "entity_id")

    def load_topology(self, rows: Sequence[Mapping[str, Any]]) -> int:
        """Insert topology edges, skipping edges already present."""
        existing = {
            (row["src_entity"], row["relation"], row["dst_entity"])
            for row in self._store.query(
                "SELECT src_entity, relation, dst_entity FROM topology"
            )
        }
        fresh = [
            row
            for row in rows
            if (row["src_entity"], row["relation"], row["dst_entity"])
            not in existing
        ]
        return self._store.insert_rows("topology", fresh) if fresh else 0

    def load_change_calendar(self, rows: Sequence[Mapping[str, Any]]) -> int:
        """Insert change-calendar rows, skipping ones already present."""
        return self._insert_new("change_events", rows, "change_id")

    # -- internals ---------------------------------------------------------
    def _load_operational_inputs(
        self, seed: "_sources.SeedDirectory", used: list[str]
    ) -> tuple[int, int, int]:
        """Load ``topology.json`` and ``change_calendar.json`` if present."""
        entities = edges = changes = 0
        if _exists(seed.topology_path):
            entity_rows, edge_rows = _sources.read_topology(seed.topology_path)
            entities = self.load_entities(entity_rows)
            edges = self.load_topology(edge_rows)
            # The estate inventory is also the join that lets a Cloud Logging
            # entry — which names only a monitored resource — be attributed to
            # an entity. Installed before ingest so the very first log record
            # can use it.
            self.install_resource_index(
                _sources.read_resource_index(seed.topology_path)
            )
            used.append(seed.topology_path)
        if _exists(seed.change_calendar_path):
            change_rows = _sources.read_change_calendar(
                seed.change_calendar_path
            )
            changes = self.load_change_calendar(change_rows)
            used.append(seed.change_calendar_path)
        return entities, edges, changes

    def _insert_new(
        self, table: str, rows: Sequence[Mapping[str, Any]], key: str
    ) -> int:
        """Insert only the rows whose ``key`` is not already in ``table``."""
        if not rows:
            return 0
        existing = {
            row[key]
            for row in self._store.query(f'SELECT "{key}" FROM "{table}"')
        }
        fresh = [row for row in rows if row[key] not in existing]
        return self._store.insert_rows(table, fresh) if fresh else 0

    def _wire_messaging(self) -> tuple[LocalPublisher, LocalSubscriber]:
        """Create the topic and subscription, mirroring real Pub/Sub setup."""
        publisher = LocalPublisher(self._broker)
        subscriber = publisher.subscriber()
        publisher.create_topic(self._options.topic)
        subscriber.create_subscription(
            self._options.subscription, self._options.topic
        )
        return publisher, subscriber

    def _build_envelopes(
        self,
        log_entries: Iterable[LogEntry | Mapping[str, Any]],
        metric_scrapes: Iterable[MetricScrape],
    ) -> tuple[TelemetryEnvelope, ...]:
        """Turn both sources into one totally ordered envelope stream.

        The scrape stream is walked first so :attr:`_phase_index` is populated
        before any log record is normalised: logs have no phase of their own
        and are placed against the intervals the scrapes define.
        """
        envelopes: list[TelemetryEnvelope] = []
        scrapes = tuple(metric_scrapes)
        self._phase_index = _PhaseIndex.from_bounds(
            _sources.parse_phase_bounds(scrapes)
        )

        for entry in log_entries:
            payload = (
                serialize_log_entry(entry)
                if isinstance(entry, LogEntry)
                else dict(entry)
            )
            insert_id = payload.get("insertId")
            if not insert_id:
                raise ValueError(
                    "log entry has no insertId; it is the de-duplication key "
                    "and ingest cannot be idempotent without it"
                )
            timestamp = payload.get("timestamp")
            if not timestamp:
                raise ValueError(
                    f"log entry {insert_id!r} has no timestamp; the pipeline "
                    "orders on event time and cannot place it"
                )
            envelopes.append(
                TelemetryEnvelope(
                    kind=LOG_KIND,
                    timestamp_ms=_sources.rfc3339_to_millis(timestamp),
                    key=str(insert_id),
                    payload=payload,
                )
            )

        for scrape in scrapes:
            target = scrape.prometheus_target().to_dict()
            envelopes.append(
                TelemetryEnvelope(
                    kind=METRIC_KIND,
                    timestamp_ms=scrape.timestamp_ms,
                    key=stable_id(
                        dumps_canonical(
                            {
                                "t": scrape.timestamp_ms,
                                "target": target,
                            }
                        ),
                        "ingest",
                        "scrape",
                        length=20,
                    ),
                    payload={
                        "exposition": scrape.text,
                        "target": target,
                        "phase": scrape.phase,
                    },
                )
            )

        envelopes.sort(key=TelemetryEnvelope.sort_key)
        return tuple(envelopes)

    def _build_pipeline(
        self,
        dedupe: "_DedupeIndex",
        counters: "_Counters",
        writer: "_StoreWriter",
    ) -> Pipeline:
        """Assemble the Dataflow-shaped stage graph."""
        return (
            Pipeline("ano-telemetry-ingest")
            .apply(Map("decode-envelope", _decode))
            .apply(
                FlatMap(
                    "normalise",
                    lambda envelope: self._normalise(envelope, counters),
                )
            )
            .apply(Filter("deduplicate", lambda row: dedupe.accept(row, counters)))
            .apply(Map("embed", lambda row: self._embed(row, counters)))
            .apply(Write("write-store", writer.write))
        )

    def _normalise(
        self, envelope: TelemetryEnvelope, counters: "_Counters"
    ) -> Iterable[LogRecord | MetricRecord]:
        """Envelope to store rows. A metric scrape fans out to its samples."""
        if envelope.kind == LOG_KIND:
            return (self._normalise_log(envelope, counters),)
        return self._normalise_scrape(envelope, counters)

    def _normalise_log(
        self, envelope: TelemetryEnvelope, counters: "_Counters"
    ) -> LogRecord:
        """Flatten one serialised ``LogEntry`` into a :class:`LogRecord`."""
        payload = envelope.payload
        resource = payload.get("resource") or {}
        resource_labels = dict(resource.get("labels") or {})
        labels = dict(payload.get("labels") or {})
        json_payload = payload.get("jsonPayload")
        text_payload = payload.get("textPayload")

        entity_id = self._resolver.resolve(
            labels,
            resource_labels,
            json_payload if isinstance(json_payload, Mapping) else None,
            resource_type=str(resource.get("type") or ""),
            resource_labels=resource_labels,
        )
        if entity_id is None:
            counters.logs_unresolved += 1

        declared = _phase_of(payload)
        counters.phase_observe(
            "log",
            declared or self._phase_index.classify(envelope.timestamp_ms),
            envelope.timestamp_ms,
        )
        return LogRecord(
            insert_id=envelope.key,
            timestamp=str(payload["timestamp"]),
            receive_timestamp=payload.get("receiveTimestamp"),
            severity=str(payload.get("severity") or "DEFAULT"),
            log_name=str(payload.get("logName") or ""),
            resource_type=str(resource.get("type") or ""),
            resource_labels=resource_labels,
            labels=labels,
            text_payload=text_payload,
            json_payload=(
                dict(json_payload) if isinstance(json_payload, Mapping) else None
            ),
            entity_id=entity_id,
            message=extract_message(text_payload, json_payload),
        )

    def _normalise_scrape(
        self, envelope: TelemetryEnvelope, counters: "_Counters"
    ) -> list[MetricRecord]:
        """Parse one scrape and attach its target labels (ruling IR-02)."""
        counters.scrapes_read += 1
        payload = envelope.payload
        target = payload["target"]
        phase = str(payload.get("phase") or "")
        counters.phase_observe("metric", phase, envelope.timestamp_ms)

        records: list[MetricRecord] = []
        for sample in parse_exposition(payload["exposition"]):
            scraped = attach(sample, target)
            timestamp_ms = (
                sample.timestamp
                if sample.timestamp is not None
                else envelope.timestamp_ms
            )
            records.append(
                MetricRecord(
                    metric=scraped.name,
                    labels=scraped.labels(),
                    value=scraped.value,
                    timestamp_ms=int(timestamp_ms),
                    entity_id=scraped.entity_id,
                )
            )
        # A total order within the scrape, so the write sequence is stable.
        records.sort(key=lambda row: (row.metric, dumps_canonical(dict(row.labels))))
        return records

    def _embed(
        self, row: LogRecord | MetricRecord, counters: "_Counters"
    ) -> tuple[LogRecord | MetricRecord, tuple[float, ...] | None]:
        """Attach the R1 embedding to a log row; pass metric rows through."""
        if not self._options.embed_logs or not isinstance(row, LogRecord):
            return (row, None)
        vector = self._embedder.embed(row.message)
        counters.embeddings_written += 1
        return (row, vector)

    def _record_phases(
        self, counters: "_Counters", dedupe: "_DedupeIndex"
    ) -> dict[str, dict[str, str]]:
        """Persist the observed phase bounds and return them."""
        rows: list[dict[str, Any]] = []
        summary: dict[str, dict[str, str]] = {}
        for (kind, phase), (low, high, count) in sorted(counters.phases.items()):
            start = _millis_to_rfc3339(low)
            end = _millis_to_rfc3339(high)
            key = stable_id(
                f"{phase}|{kind}|{start}|{end}",
                "ingest",
                "phase",
                length=20,
            )
            summary[f"{kind}:{phase}"] = {
                "start": start,
                "end": end,
                "count": str(count),
            }
            if key in dedupe.phase_keys:
                continue
            dedupe.phase_keys.add(key)
            rows.append(
                {
                    "phase_key": key,
                    "phase": phase,
                    "kind": kind,
                    "start_time": start,
                    "end_time": end,
                    "row_count": count,
                }
            )
        if rows:
            self._store.insert_rows(INGEST_PHASES_TABLE.name, rows)
        return summary


# ---------------------------------------------------------------------------
# Stage helpers. Kept module-level and small so each stage stays inspectable.
# ---------------------------------------------------------------------------


def _decode(message: Any) -> TelemetryEnvelope:
    """Pub/Sub ``Message`` to :class:`TelemetryEnvelope`."""
    return TelemetryEnvelope.from_bytes(message.data)


class _PhaseIndex:
    """Maps an arbitrary event time onto the collection phase it falls in.

    Only *metric scrapes* carry an explicit ``phase`` field — the exposition
    envelope has somewhere to put it. Cloud Logging entries have no such
    field, and inventing one would be unfaithful: a real log entry does not
    know which slice of a study it belongs to.

    The split is nonetheless observable, because ``history`` and ``window``
    are a **data-collection boundary** that the scrape stream states outright.
    Projecting log timestamps onto the intervals the scrapes define is the
    same join a real pipeline performs when it says "this log predates the
    evaluation period". It reads no label file and learns nothing about
    incidents: the boundary would be identical if the window contained no
    faults at all.

    Attributes:
        intervals: ``(low_ms, high_ms, phase)`` sorted by ``low_ms``.
    """

    __slots__ = ("intervals",)

    def __init__(self, intervals: Sequence[tuple[int, int, str]]) -> None:
        self.intervals = tuple(sorted(intervals))

    @classmethod
    def from_bounds(cls, bounds: Mapping[str, tuple[int, int]]) -> "_PhaseIndex":
        """Build from ``{phase: (low_ms, high_ms)}`` as the scrapes reported it.

        ``unknown`` is dropped rather than indexed: it is the absence of a
        phase, and letting it claim an interval would spread that absence to
        logs that the real phases can place perfectly well.
        """
        return cls(
            [
                (low, high, phase)
                for phase, (low, high) in bounds.items()
                if phase and phase != UNKNOWN_PHASE
            ]
        )

    def classify(self, timestamp_ms: int) -> str:
        """The phase ``timestamp_ms`` belongs to.

        Times before the first interval take the first phase and times after
        the last take the last, so the two streams cannot disagree merely
        because logging started a few seconds before the first scrape. A time
        in a gap between two phases takes the later one, which is the
        conservative choice: it never back-dates window activity into the
        training set.
        """
        if not self.intervals:
            return UNKNOWN_PHASE
        for _, high, phase in self.intervals:
            if timestamp_ms <= high:
                return phase
        return self.intervals[-1][2]

    def __bool__(self) -> bool:
        return bool(self.intervals)


def _phase_of(payload: Mapping[str, Any]) -> str:
    """The phase a log entry declares for itself, if it declares one.

    Returns ``""`` when the entry says nothing, which is the ordinary case for
    real Cloud Logging data; the caller then falls back to :class:`_PhaseIndex`
    rather than recording the row as ``unknown``.
    """
    labels = payload.get("labels") or {}
    value = labels.get("phase")
    return str(value) if value else ""


def _millis_to_rfc3339(value: int) -> str:
    """Epoch milliseconds to RFC 3339 UTC text."""
    moment = _dt.datetime.fromtimestamp(value / 1000.0, tz=_dt.timezone.utc)
    text = moment.strftime("%Y-%m-%dT%H:%M:%S")
    micros = moment.microsecond
    if micros:
        text += f".{micros:06d}".rstrip("0")
    return text + "Z"


def _exists(path: str) -> bool:
    """True if ``path`` names an existing file or directory."""
    import os

    return os.path.exists(path)


def _replace(result: IngestResult, **changes: Any) -> IngestResult:
    """Frozen-dataclass copy with fields replaced."""
    import dataclasses

    return dataclasses.replace(result, **changes)


class _Counters:
    """Mutable tallies shared by the stages. Not part of the public API."""

    __slots__ = (
        "logs_written",
        "logs_duplicate",
        "logs_unresolved",
        "metrics_written",
        "metrics_duplicate",
        "scrapes_read",
        "embeddings_written",
        "phases",
    )

    def __init__(self) -> None:
        self.logs_written = 0
        self.logs_duplicate = 0
        self.logs_unresolved = 0
        self.metrics_written = 0
        self.metrics_duplicate = 0
        self.scrapes_read = 0
        self.embeddings_written = 0
        # (kind, phase) -> [min_ms, max_ms, count]
        self.phases: dict[tuple[str, str], list[int]] = {}

    def phase_observe(self, kind: str, phase: str, timestamp_ms: int) -> None:
        """Widen the recorded bounds of one phase."""
        key = (kind, phase or "unknown")
        entry = self.phases.get(key)
        if entry is None:
            self.phases[key] = [timestamp_ms, timestamp_ms, 1]
            return
        if timestamp_ms < entry[0]:
            entry[0] = timestamp_ms
        if timestamp_ms > entry[1]:
            entry[1] = timestamp_ms
        entry[2] += 1


class _DedupeIndex:
    """Keys already in the store, plus keys written during this run.

    Loading the key set once is what makes re-ingest idempotent offline. A
    BigQuery deployment expresses the same rule as a ``MERGE`` on the same
    content-derived key rather than by holding it in memory, which is why the
    key is a digest of the record's identity and not a row number.
    """

    __slots__ = ("log_keys", "metric_keys", "embedding_keys", "phase_keys")

    def __init__(self, store: Any) -> None:
        self.log_keys: set[str] = {
            row["insert_id"] for row in store.query(
                "SELECT insert_id FROM log_entries"
            )
        }
        self.embedding_keys: set[str] = {
            row["insert_id"] for row in store.query(
                "SELECT insert_id FROM log_embeddings"
            )
        }
        self.phase_keys: set[str] = {
            row["phase_key"] for row in store.query(
                f'SELECT phase_key FROM "{INGEST_PHASES_TABLE.name}"'
            )
        }
        self.metric_keys: set[str] = set()
        for row in store.query(
            "SELECT metric, labels, timestamp_ms FROM metric_samples"
        ):
            self.metric_keys.add(
                _metric_key(
                    row["metric"],
                    json.loads(row["labels"]) if row["labels"] else {},
                    int(row["timestamp_ms"]),
                )
            )

    def accept(
        self, row: LogRecord | MetricRecord, counters: "_Counters"
    ) -> bool:
        """True if ``row`` has not been seen, recording it as seen if so."""
        if isinstance(row, LogRecord):
            if row.insert_id in self.log_keys:
                counters.logs_duplicate += 1
                return False
            self.log_keys.add(row.insert_id)
            return True
        key = row.dedupe_key()
        if key in self.metric_keys:
            counters.metrics_duplicate += 1
            return False
        self.metric_keys.add(key)
        return True


def _metric_key(metric: str, labels: Mapping[str, Any], timestamp_ms: int) -> str:
    """The sample identity used for idempotent re-ingest."""
    return stable_id(
        dumps_canonical(
            {
                "metric": metric,
                "labels": {str(k): str(v) for k, v in labels.items()},
                "timestamp_ms": int(timestamp_ms),
            }
        ),
        "ingest",
        "metric-sample",
        length=24,
    )


class _StoreWriter:
    """Buffered writer — the sink behind the ``write-store`` stage."""

    __slots__ = ("_store", "_batch_size", "_counters", "_buffers")

    def __init__(self, store: Any, batch_size: int, counters: _Counters) -> None:
        self._store = store
        self._batch_size = batch_size
        self._counters = counters
        self._buffers: dict[str, list[Mapping[str, Any]]] = {
            "log_entries": [],
            "metric_samples": [],
            "log_embeddings": [],
        }

    def write(
        self, element: tuple[LogRecord | MetricRecord, tuple[float, ...] | None]
    ) -> None:
        """Buffer one element's rows, flushing tables that reach the batch size."""
        row, vector = element
        if isinstance(row, LogRecord):
            self._buffers["log_entries"].append(row.to_row())
            self._counters.logs_written += 1
            if vector is not None:
                self._buffers["log_embeddings"].append(
                    {
                        "insert_id": row.insert_id,
                        "entity_id": row.entity_id,
                        "timestamp": row.timestamp,
                        "dim": len(vector),
                        "vector": list(vector),
                    }
                )
        else:
            self._buffers["metric_samples"].append(row.to_row())
            self._counters.metrics_written += 1
        for table, buffer in self._buffers.items():
            if len(buffer) >= self._batch_size:
                self._flush_table(table)

    def flush(self) -> None:
        """Write every buffered row."""
        for table in list(self._buffers):
            self._flush_table(table)

    def _flush_table(self, table: str) -> None:
        buffer = self._buffers[table]
        if not buffer:
            return
        self._store.insert_rows(table, buffer)
        buffer.clear()


def ingest_seed_directory(
    store: Any,
    path: str,
    *,
    config: Config | None = None,
    metrics_path: str | None = None,
    options: IngestOptions | None = None,
    phases: Iterable[str] | None = None,
) -> IngestResult:
    """Ingest one seed directory into ``store``. The one-call entry point.

    Args:
        store: A contract-C3 analytical store. :meth:`ensure_schema` is called
            for you, so a fresh database works.
        path: Seed directory, or the ``logs.jsonl`` inside it (ruling IR-05).
        config: Project configuration.
        metrics_path: The ``metrics/`` directory, if not ``<root>/metrics``.
        options: Ingest options.
        phases: Restrict metric scrapes to these phases.

    Returns:
        The :class:`IngestResult` for the run.
    """
    store.ensure_schema()
    ingestor = TelemetryIngestor(store, config, options=options)
    return ingestor.ingest_directory(
        path, metrics_path=metrics_path, phases=phases
    )
