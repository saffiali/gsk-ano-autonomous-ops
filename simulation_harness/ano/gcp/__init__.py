"""``ano.gcp`` — Google Cloud service adapters with offline substitutes.

Every component on the production path is a Google Cloud managed service. Every
one of them also has an offline local substitute here, behind a GCP-shaped
interface, so the whole system runs on a laptop with no credentials and no
network (requirement R6) and deploys to a real project with **configuration
changes alone**.

Service mapping
---------------

=====================  ===========================  ==========================
Production GCP service Offline substitute            Module
=====================  ===========================  ==========================
Cloud Logging          JSONL ``LogEntry`` file       :mod:`ano.gcp.logsink`
Managed Prometheus     exposition scrape -> JSONL    :mod:`ano.gcp.metricsink`
Pub/Sub                in-process broker             :mod:`ano.gcp.messaging`
Dataflow               deterministic stage runner    :mod:`ano.gcp.dataflow`
BigQuery               stdlib ``sqlite3``            :mod:`ano.gcp.bqstore`
Cloud Monitoring       local alert store             :mod:`ano.gcp.alertsink`
=====================  ===========================  ==========================

Each module's docstring states the exact service it stands in for and what
changes on deployment.

Usage — never name a backend in application code::

    from ano.config import load_config
    from ano.gcp import build_log_sink, connect

    cfg = load_config()
    store = connect(cfg)              # sqlite offline, BigQuery deployed
    store.ensure_schema()
    with build_log_sink(cfg) as logs:
        logs.write(entry)

Importing this package registers the offline substitutes. Selecting a ``gcp``
backend without a deployment plugin raises
:class:`ano.gcp.registry.BackendUnavailableError` — it never silently falls back
to the local substitute.
"""

from ano.gcp.alertsink import (
    SEVERITIES,
    Alert,
    AlertSink,
    LocalAlertSink,
    build_alert_sink,
    read_alerts,
)
from ano.gcp.bqstore import (
    CORE_TABLES,
    AnalyticalStore,
    Column,
    SqliteAnalyticalStore,
    TableSpec,
    build_analytical_store,
    connect,
    register_table,
    registered_tables,
    series_cte,
)
from ano.gcp.dataflow import (
    CombinePerKey,
    Filter,
    FixedWindows,
    FlatMap,
    GroupByKey,
    LocalPipelineRunner,
    Map,
    Partition,
    Pipeline,
    PipelineResult,
    Stage,
    StageMetrics,
    WindowedValue,
    Write,
    build_pipeline_runner,
)
from ano.gcp.logsink import (
    LocalLogSink,
    LogSink,
    build_log_sink,
    read_entries,
)
from ano.gcp.messaging import (
    LocalBroker,
    LocalPublisher,
    LocalSubscriber,
    Message,
    Publisher,
    Subscriber,
    SubscriptionNotFoundError,
    TopicNotFoundError,
    build_publisher,
    build_subscriber,
    get_broker,
    reset_broker,
)
from ano.gcp.metricsink import (
    LocalMetricSink,
    MetricSample,
    MetricSink,
    build_metric_sink,
    format_exposition_line,
    parse_exposition,
    read_samples,
)
from ano.gcp.registry import (
    GCP_CLIENT_LIBRARIES,
    GCP_SERVICE_NAMES,
    BackendUnavailableError,
    build,
    register_backend,
    registered_backends,
    unregister_backend,
)

__all__ = [
    # registry
    "build",
    "register_backend",
    "unregister_backend",
    "registered_backends",
    "BackendUnavailableError",
    "GCP_SERVICE_NAMES",
    "GCP_CLIENT_LIBRARIES",
    # Cloud Logging
    "LogSink",
    "LocalLogSink",
    "build_log_sink",
    "read_entries",
    # Managed Prometheus
    "MetricSink",
    "LocalMetricSink",
    "MetricSample",
    "parse_exposition",
    "format_exposition_line",
    "read_samples",
    "build_metric_sink",
    # Pub/Sub
    "Publisher",
    "Subscriber",
    "Message",
    "LocalBroker",
    "LocalPublisher",
    "LocalSubscriber",
    "TopicNotFoundError",
    "SubscriptionNotFoundError",
    "build_publisher",
    "build_subscriber",
    "get_broker",
    "reset_broker",
    # Dataflow
    "Pipeline",
    "PipelineResult",
    "Stage",
    "StageMetrics",
    "Map",
    "FlatMap",
    "Filter",
    "Partition",
    "GroupByKey",
    "CombinePerKey",
    "FixedWindows",
    "WindowedValue",
    "Write",
    "LocalPipelineRunner",
    "build_pipeline_runner",
    # BigQuery
    "AnalyticalStore",
    "SqliteAnalyticalStore",
    "TableSpec",
    "Column",
    "CORE_TABLES",
    "register_table",
    "registered_tables",
    "series_cte",
    "connect",
    "build_analytical_store",
    # Cloud Monitoring
    "AlertSink",
    "LocalAlertSink",
    "Alert",
    "SEVERITIES",
    "build_alert_sink",
    "read_alerts",
]
