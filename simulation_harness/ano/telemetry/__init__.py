"""``ano.telemetry`` — genuine Cloud Logging and Prometheus/GMP wire formats.

This package is **contract C7** (``PROJECT.md``). It is consumed by M3
(ingestion), M5 (harness conformance counters) and M6 (demo), and produced by
``scenariogen``. It contains no scenario logic and no detection logic: it is
purely the format layer.

Public surface
--------------

Cloud Logging::

    LogEntry, MonitoredResource, HttpRequest, LogEntryOperation,
    LogEntrySourceLocation, LogSplit
    serialize_log_entry(entry)  -> dict
    parse_log_entries(src)      -> Iterator[LogEntry]
    validate_log_entry(obj)     -> None          # raises LogEntryError
    validate_log_entry_batch(entries) -> int     # + insertId uniqueness
    build_log_name(project_id, log_id)
    SEVERITIES, resource_label_keys(type)

Prometheus / GMP::

    MetricSample(name, labels, value, timestamp)
    MetricFamily(name, type, help, samples)
    serialize_exposition(samples, metadata=None) -> str
    serialize_families(families)                 -> str
    parse_exposition(text)      -> Iterator[MetricSample]
    validate_exposition(text)   -> None          # raises ExpositionError
    assert_roundtrip(text)      -> None
    histogram_samples(...)      -> list[MetricSample]
    PrometheusTarget            # the six GMP-attached target labels
    ScrapedSample               # post-attach sample (IR-02)
    attach_target_labels(sample, target) -> ScrapedSample
    TEXT_VERSION, CONTENT_TYPE, GMP_TARGET_LABELS

Exporter catalogue (``ano.telemetry.exporters``) supplies verified real metric
names, types, help strings and label keys for node_exporter, cAdvisor,
apache_exporter, Tomcat/JVM via jmx_exporter, mysqld_exporter and
snmp_exporter.

Design note — producer/checker independence
-------------------------------------------
:mod:`ano.telemetry.prometheus` writes; :mod:`ano.telemetry.promparse` checks.
They share no grammar constants, on purpose: a validator that reuses the
writer's tables cannot see the writer's bugs
(``VALIDATION_STRATEGY.md`` §3.1). Likewise the LogEntry validator is driven by
a declarative schema in ``ano/telemetry/schemas/`` that is mechanically pinned
to the vendored Apache-2.0 ``log_entry.proto`` by a drift test, rather than by
reading the serialiser.
"""

from __future__ import annotations

from ano.telemetry.errors import ExpositionError, LogEntryError, TelemetryError
from ano.telemetry.exporters import FamilySpec, build_families, metadata_for
from ano.telemetry.gofloat import format_go_float
from ano.telemetry.logentry import (
    HttpRequest,
    LogEntry,
    LogEntryOperation,
    LogEntrySourceLocation,
    LogSplit,
    MonitoredResource,
    build_log_name,
    encode_log_id,
    format_duration_seconds,
    log_entries_to_jsonl,
    parse_log_entries,
    parse_log_entry,
    serialize_log_entry,
)
from ano.telemetry.logentry_validator import (
    SEVERITIES,
    resource_label_keys,
    validate_log_entry,
    validate_log_entry_batch,
)
from ano.telemetry.prometheus import (
    CONTENT_TYPE,
    GMP_TARGET_LABELS,
    METRIC_TYPES,
    TEXT_VERSION,
    MetricFamily,
    MetricSample,
    PrometheusTarget,
    ScrapedSample,
    attach_target_labels,
    escape_help,
    escape_label_value,
    histogram_samples,
    serialize_exposition,
    serialize_families,
    serialize_sample,
)
from ano.telemetry.promparse import (
    assert_roundtrip,
    parse_document,
    parse_exposition,
    validate_exposition,
)
from ano.telemetry.timefmt import format_rfc3339, parse_rfc3339

__all__ = [
    # errors
    "TelemetryError",
    "LogEntryError",
    "ExpositionError",
    # Cloud Logging
    "LogEntry",
    "MonitoredResource",
    "HttpRequest",
    "LogEntryOperation",
    "LogEntrySourceLocation",
    "LogSplit",
    "serialize_log_entry",
    "parse_log_entry",
    "parse_log_entries",
    "log_entries_to_jsonl",
    "validate_log_entry",
    "validate_log_entry_batch",
    "build_log_name",
    "encode_log_id",
    "format_duration_seconds",
    "SEVERITIES",
    "resource_label_keys",
    # Prometheus / GMP
    "MetricSample",
    "MetricFamily",
    "PrometheusTarget",
    "ScrapedSample",
    "attach_target_labels",
    "serialize_exposition",
    "serialize_families",
    "serialize_sample",
    "parse_exposition",
    "parse_document",
    "validate_exposition",
    "assert_roundtrip",
    "histogram_samples",
    "escape_label_value",
    "escape_help",
    "TEXT_VERSION",
    "CONTENT_TYPE",
    "METRIC_TYPES",
    "GMP_TARGET_LABELS",
    # exporter catalogue
    "FamilySpec",
    "build_families",
    "metadata_for",
    # time / float
    "format_rfc3339",
    "parse_rfc3339",
    "format_go_float",
]
