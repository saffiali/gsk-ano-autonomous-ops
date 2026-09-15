"""``ano.ingest`` — the unified telemetry ingestion pipeline and feature reader.

Two things live here, and milestone M3 owns both:

1. **The pipeline** (:mod:`ano.ingest.pipeline`). Synthetic Cloud Logging
   entries and Managed Prometheus scrapes travel one Pub/Sub topic into one
   Dataflow-shaped stage graph and land in the analytical store. Ordering is
   total and reproducible; re-ingest is idempotent; the Managed Prometheus
   scrape-time target attach happens here, exactly where the collector does it
   (ruling IR-02).

2. **The feature-read API** (:mod:`ano.ingest.features`). A small, stable,
   read-only surface — time-windowed per-entity features, log rows, stored
   embeddings, topology and the operations change calendar — through which the
   detection, attribution and demo layers read everything they need. Treat it
   as a published contract: add to it freely, change it only by agreement.

Quick start::

    from ano.config import load_config
    from ano.gcp import connect
    from ano.ingest import FeatureReader, ingest_seed_directory

    cfg = load_config()
    store = connect(cfg)
    result = ingest_seed_directory(store, "artifacts/seed-1234", config=cfg)

    reader = FeatureReader(store)
    bounds = reader.time_bounds()
    for entity in reader.observed_entities():
        windows = reader.feature_windows(entity, bounds, window_s=300)

Requirement R5: nothing in this package reads a ground-truth label file, and
:func:`ano.ingest.sources.guard_path` raises if anything tries.
"""

from ano.ingest.features import (
    AGGREGATES,
    DEFAULT_AGGREGATES,
    DEFAULT_WINDOW_S,
    ChangeWindow,
    EmbeddingRow,
    EntityRow,
    FeatureReader,
    FeatureWindow,
    LogCountWindow,
    LogRow,
    SamplePoint,
    SeriesKey,
    TimeRange,
    TopologyEdge,
    feature_name,
)
from ano.ingest.pipeline import (
    INGEST_PHASES_TABLE,
    IngestOptions,
    IngestResult,
    TelemetryIngestor,
    ingest_seed_directory,
)
from ano.ingest.records import (
    DEFAULT_ENTITY_LABEL_KEYS,
    GMP_TARGET_LABELS,
    LOG_KIND,
    METRIC_ENTITY_LABEL,
    METRIC_KIND,
    TARGET_ATTACH_SOURCE,
    EntityResolver,
    LogRecord,
    MetricRecord,
    MetricScrape,
    TelemetryEnvelope,
    attach,
    coerce_target,
    extract_message,
)
from ano.ingest.sources import (
    GROUND_TRUTH_BASENAMES,
    PHASES,
    Ground_Truth_Access_Error,
    SeedDirectory,
    guard_path,
    iter_log_entries,
    iter_scrapes,
    read_change_calendar,
    read_manifest,
    read_targets,
    read_topology,
)

GroundTruthAccessError = Ground_Truth_Access_Error

__all__ = [
    # feature-read API (the published contract)
    "FeatureReader",
    "TimeRange",
    "FeatureWindow",
    "EntityRow",
    "SeriesKey",
    "SamplePoint",
    "LogRow",
    "LogCountWindow",
    "EmbeddingRow",
    "ChangeWindow",
    "TopologyEdge",
    "AGGREGATES",
    "DEFAULT_AGGREGATES",
    "DEFAULT_WINDOW_S",
    "feature_name",
    # pipeline
    "TelemetryIngestor",
    "IngestOptions",
    "IngestResult",
    "ingest_seed_directory",
    "INGEST_PHASES_TABLE",
    # records / collector attach
    "TelemetryEnvelope",
    "LogRecord",
    "MetricRecord",
    "MetricScrape",
    "EntityResolver",
    "attach",
    "coerce_target",
    "extract_message",
    "DEFAULT_ENTITY_LABEL_KEYS",
    "GMP_TARGET_LABELS",
    "METRIC_ENTITY_LABEL",
    "TARGET_ATTACH_SOURCE",
    "LOG_KIND",
    "METRIC_KIND",
    # on-disk sources
    "SeedDirectory",
    "iter_log_entries",
    "iter_scrapes",
    "read_targets",
    "read_topology",
    "read_change_calendar",
    "read_manifest",
    "guard_path",
    "GroundTruthAccessError",
    "GROUND_TRUTH_BASENAMES",
    "PHASES",
]
