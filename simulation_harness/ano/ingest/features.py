"""The feature-read API — a published contract for the analytical layers.

This is the **only** interface the detection, attribution and demo layers need
in order to read telemetry. It exists so that nothing downstream has to know
that the analytical store is BigQuery in production and SQLite offline, that
timestamps are RFC 3339 text in one table and epoch milliseconds in another, or
that Managed Prometheus attaches its target labels at scrape time.

Stability
---------
Treat every public name here as frozen. Additive change (a new method, a new
keyword argument with a default) is fine; changing a signature or a return
shape is not, because four milestones read through it. The
:class:`FeatureReader` never writes, so it is safe to hold open for the life of
a run.

Entity identity
---------------
Fixed by ruling IR-02: a metric sample's entity is its Managed Prometheus
``instance`` label, attached by :mod:`ano.ingest.pipeline`. ``entity_id`` in
``metric_samples`` is that value. There is no guess-the-key fallback.

Time
----
Every time argument and every time in a returned record is a timezone-aware
UTC :class:`datetime.datetime`. Windows are half-open ``[start, end)``, which
is what makes consecutive windows tile without double-counting a boundary
sample.

Typical use::

    from ano.gcp import connect
    from ano.ingest import FeatureReader, TimeRange

    reader = FeatureReader(connect(cfg))
    bounds = reader.time_bounds()
    for entity in reader.entities(kinds=("app_server",)):
        for window in reader.feature_windows(
            entity.entity_id, bounds, window_s=300,
            metrics=("jvm_threads_current", "node_load1"),
        ):
            print(window.window_start, window.get("node_load1__mean"))
"""

from __future__ import annotations

import datetime as _dt
import json
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from ano.contracts.common import format_rfc3339

__all__ = [
    "AGGREGATES",
    "DEFAULT_AGGREGATES",
    "DEFAULT_WINDOW_S",
    "TimeRange",
    "EntityRow",
    "SeriesKey",
    "SamplePoint",
    "FeatureWindow",
    "LogRow",
    "LogCountWindow",
    "EmbeddingRow",
    "ChangeWindow",
    "TopologyEdge",
    "FeatureReader",
    "feature_name",
]

#: Every aggregate :meth:`FeatureReader.feature_windows` can compute.
#:
#: ``delta`` and ``rate_per_s`` exist for Prometheus counters:
#: ``rate_per_s = (last - first) / (last_time - first_time)``. They are the
#: naive form and do **not** correct for a counter reset inside the window —
#: stated here rather than discovered later.
AGGREGATES: tuple[str, ...] = (
    "mean",
    "min",
    "max",
    "sum",
    "count",
    "stddev",
    "first",
    "last",
    "delta",
    "rate_per_s",
)

#: The aggregates computed when the caller does not say.
DEFAULT_AGGREGATES: tuple[str, ...] = ("mean", "max", "min", "last")

#: Default feature-window width, in seconds.
DEFAULT_WINDOW_S = 300

_UTC = _dt.timezone.utc


def feature_name(metric: str, aggregate: str, group: str = "") -> str:
    """Compose the canonical feature key for a metric/aggregate/group triple.

    Args:
        metric: Prometheus metric name.
        aggregate: One of :data:`AGGREGATES`.
        group: Optional label-group discriminator, e.g. ``'ifName=Gi0/12'``.

    Returns:
        ``"node_load1__mean"``, or ``"ifHCInOctets{ifName=Gi0/12}__rate_per_s"``
        when grouped. The double underscore separates metric from aggregate and
        cannot appear in a Prometheus metric name, so the key is unambiguous.
    """
    base = f"{metric}{{{group}}}" if group else metric
    return f"{base}__{aggregate}"


@dataclass(frozen=True, slots=True)
class TimeRange:
    """A half-open UTC interval ``[start, end)``."""

    start: _dt.datetime
    end: _dt.datetime

    def __post_init__(self) -> None:
        for name, value in (("start", self.start), ("end", self.end)):
            if value.tzinfo is None:
                raise ValueError(f"TimeRange.{name} must be timezone-aware")
        if self.end < self.start:
            raise ValueError(
                f"TimeRange end {self.end!r} precedes start {self.start!r}"
            )

    @property
    def duration_s(self) -> float:
        """Width of the interval in seconds."""
        return (self.end - self.start).total_seconds()

    def contains(self, moment: _dt.datetime) -> bool:
        """True if ``moment`` falls in ``[start, end)``."""
        return self.start <= moment < self.end

    def clamp(self, other: "TimeRange") -> "TimeRange":
        """Intersection with ``other``; empty if they do not overlap."""
        start = max(self.start, other.start)
        end = min(self.end, other.end)
        return TimeRange(start, max(start, end))

    def split(self, window_s: int) -> tuple["TimeRange", ...]:
        """Tile the interval into consecutive ``window_s``-wide windows.

        The last window is truncated at :attr:`end` rather than overhanging it.
        """
        if window_s <= 0:
            raise ValueError(f"window_s must be > 0, got {window_s}")
        step = _dt.timedelta(seconds=window_s)
        windows: list[TimeRange] = []
        cursor = self.start
        while cursor < self.end:
            nxt = min(cursor + step, self.end)
            windows.append(TimeRange(cursor, nxt))
            cursor = nxt
        return tuple(windows)

    @classmethod
    def of(
        cls, start: _dt.datetime, duration_s: float
    ) -> "TimeRange":
        """Build a range from a start and a width in seconds."""
        return cls(start, start + _dt.timedelta(seconds=duration_s))

    def to_dict(self) -> dict[str, str]:
        """RFC 3339 rendering, for reports and artifacts."""
        return {
            "start": format_rfc3339(self.start),
            "end": format_rfc3339(self.end),
        }


@dataclass(frozen=True, slots=True)
class EntityRow:
    """One row of the monitored estate."""

    entity_id: str
    kind: str
    site: str | None = None
    labels: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SeriesKey:
    """Identity of one time series: metric name plus its post-attach labels."""

    metric: str
    labels: Mapping[str, str] = field(default_factory=dict)

    @property
    def instance(self) -> str | None:
        """The ``instance`` target label — the entity id (IR-02)."""
        return self.labels.get("instance")

    @property
    def job(self) -> str | None:
        """The ``job`` target label — the exporter family."""
        return self.labels.get("job")


@dataclass(frozen=True, slots=True)
class SamplePoint:
    """One metric sample."""

    timestamp: _dt.datetime
    value: float
    labels: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class FeatureWindow:
    """Aggregated metric features for one entity over one time window.

    Attributes:
        entity_id: The entity these features describe.
        window: The half-open interval they cover.
        values: Feature key (see :func:`feature_name`) to value. A metric with
            no sample in the window is **absent**, never zero — zero is a
            measurement, absence is not, and conflating them is how a gap
            becomes a false signal.
        sample_count: Feature-base name to the number of samples behind it.
    """

    entity_id: str
    window: TimeRange
    values: Mapping[str, float] = field(default_factory=dict)
    sample_count: Mapping[str, int] = field(default_factory=dict)

    @property
    def window_start(self) -> _dt.datetime:
        """Inclusive window start."""
        return self.window.start

    @property
    def window_end(self) -> _dt.datetime:
        """Exclusive window end."""
        return self.window.end

    def get(self, name: str, default: float | None = None) -> float | None:
        """Feature ``name``, or ``default`` when it was not measured."""
        return self.values.get(name, default)

    def names(self) -> tuple[str, ...]:
        """Feature keys present, sorted."""
        return tuple(sorted(self.values))

    def __contains__(self, name: object) -> bool:
        return name in self.values


@dataclass(frozen=True, slots=True)
class LogRow:
    """One ingested Cloud Logging entry, flattened."""

    insert_id: str
    timestamp: _dt.datetime
    entity_id: str | None
    severity: str
    message: str
    resource_type: str | None = None
    labels: Mapping[str, str] = field(default_factory=dict)
    resource_labels: Mapping[str, str] = field(default_factory=dict)
    json_payload: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class LogCountWindow:
    """Log volume for one entity over one window, split by severity."""

    entity_id: str | None
    window: TimeRange
    total: int
    by_severity: Mapping[str, int] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class EmbeddingRow:
    """A stored log-message embedding (requirement R1)."""

    insert_id: str
    entity_id: str | None
    timestamp: _dt.datetime
    vector: tuple[float, ...]

    @property
    def dim(self) -> int:
        """Vector dimensionality."""
        return len(self.vector)


@dataclass(frozen=True, slots=True)
class ChangeWindow:
    """A scheduled change from the operations calendar.

    An operations input, not ground truth: real teams know their own change
    schedule. It carries no incident kind, no root cause and no genuineness
    flag (ruling IR-03), so reading it at inference time does not violate R5.
    """

    change_id: str
    kind: str
    entity_id: str | None
    window: TimeRange
    description: str = ""


@dataclass(frozen=True, slots=True)
class TopologyEdge:
    """A directed relation between two entities."""

    src_entity: str
    dst_entity: str
    relation: str
    src_kind: str | None = None
    dst_kind: str | None = None


class FeatureReader:
    """Read-only, time-windowed access to everything the pipeline ingested.

    Args:
        store: Any contract-C3 ``AnalyticalStore`` (``ano.gcp.connect(cfg)``).
        default_window_s: Window width used when a caller omits one.

    The reader issues ``ORDER BY`` on every query whose order a caller could
    observe, so two runs over the same store return identical sequences.
    """

    def __init__(self, store: Any, *, default_window_s: int = DEFAULT_WINDOW_S) -> None:
        if default_window_s <= 0:
            raise ValueError(
                f"default_window_s must be > 0, got {default_window_s}"
            )
        self._store = store
        self._default_window_s = default_window_s

    # -- properties --------------------------------------------------------
    @property
    def store(self) -> Any:
        """The underlying analytical store, for callers that need raw SQL."""
        return self._store

    @property
    def default_window_s(self) -> int:
        """Default feature-window width in seconds."""
        return self._default_window_s

    # -- estate ------------------------------------------------------------
    def entities(
        self, kinds: Iterable[str] | None = None
    ) -> tuple[EntityRow, ...]:
        """Every monitored entity, ordered by id.

        Args:
            kinds: Restrict to these entity kinds (``app_server``,
                ``database``, ``switch``, ``node``). ``None`` means all.
        """
        sql = "SELECT entity_id, kind, site, labels FROM entities"
        params: list[Any] = []
        selected = tuple(kinds) if kinds is not None else ()
        if selected:
            sql += " WHERE kind IN (" + ",".join("?" * len(selected)) + ")"
            params.extend(selected)
        sql += " ORDER BY entity_id"
        return tuple(
            EntityRow(
                entity_id=row["entity_id"],
                kind=row["kind"],
                site=row["site"],
                labels=_json_map(row["labels"]),
            )
            for row in self._store.query(sql, params)
        )

    def entity_kinds(self) -> dict[str, str]:
        """Map of entity id to kind, for the whole estate."""
        return {row.entity_id: row.kind for row in self.entities()}

    def observed_entities(self) -> tuple[str, ...]:
        """Entity ids that actually appear in telemetry, ordered.

        The union of the entities seen in ``metric_samples`` and
        ``log_entries``. Use this rather than :meth:`entities` when you want
        "what did we receive data about", which is not necessarily the same as
        "what is in the register".
        """
        rows = self._store.query(
            "SELECT DISTINCT entity_id FROM metric_samples "
            "WHERE entity_id IS NOT NULL "
            "UNION SELECT DISTINCT entity_id FROM log_entries "
            "WHERE entity_id IS NOT NULL "
            "ORDER BY entity_id"
        )
        return tuple(row["entity_id"] for row in rows)

    # -- time --------------------------------------------------------------
    def time_bounds(self) -> TimeRange | None:
        """The full ingested interval across logs and metrics.

        Returns:
            A :class:`TimeRange` spanning the earliest to the latest telemetry
            (end exclusive, nudged one millisecond past the last sample so the
            last sample is inside it), or ``None`` when the store is empty.
        """
        metric_row = self._store.query_one(
            "SELECT min(timestamp_ms) AS lo, max(timestamp_ms) AS hi "
            "FROM metric_samples"
        ) or {}
        log_row = self._store.query_one(
            "SELECT min(timestamp) AS lo, max(timestamp) AS hi FROM log_entries"
        ) or {}

        lows: list[_dt.datetime] = []
        highs: list[_dt.datetime] = []
        if metric_row.get("lo") is not None:
            lows.append(_from_millis(int(metric_row["lo"])))
            highs.append(_from_millis(int(metric_row["hi"])))
        if log_row.get("lo"):
            lows.append(_parse_ts(log_row["lo"]))
            highs.append(_parse_ts(log_row["hi"]))
        if not lows:
            return None
        return TimeRange(min(lows), max(highs) + _dt.timedelta(milliseconds=1))

    def training_range(
        self, fraction: float, *, bounds: TimeRange | None = None
    ) -> TimeRange:
        """The leading ``fraction`` of the ingested interval.

        This is how a model declares its training window from the data alone —
        no ground truth, no configuration file naming incident times — which is
        what makes feature 34 ("training window is bounded and declared")
        auditable and keeps requirement R5 intact.

        Args:
            fraction: Share of the interval to train on, in ``(0, 1]``.
            bounds: Interval to split. Defaults to :meth:`time_bounds`.

        Raises:
            ValueError: for a fraction outside ``(0, 1]``, or an empty store.
        """
        if not 0.0 < fraction <= 1.0:
            raise ValueError(f"fraction must be in (0, 1], got {fraction}")
        span = bounds if bounds is not None else self.time_bounds()
        if span is None:
            raise ValueError("store holds no telemetry; no training range exists")
        return TimeRange.of(span.start, span.duration_s * fraction)

    # -- metrics -----------------------------------------------------------
    def metric_names(self, entity_id: str | None = None) -> tuple[str, ...]:
        """Distinct metric names, optionally restricted to one entity."""
        sql = "SELECT DISTINCT metric FROM metric_samples"
        params: list[Any] = []
        if entity_id is not None:
            sql += " WHERE entity_id = ?"
            params.append(entity_id)
        sql += " ORDER BY metric"
        return tuple(row["metric"] for row in self._store.query(sql, params))

    def series_keys(
        self, entity_id: str, metric: str | None = None
    ) -> tuple[SeriesKey, ...]:
        """Distinct (metric, label set) series for one entity."""
        sql = (
            "SELECT DISTINCT metric, labels FROM metric_samples "
            "WHERE entity_id = ?"
        )
        params: list[Any] = [entity_id]
        if metric is not None:
            sql += " AND metric = ?"
            params.append(metric)
        sql += " ORDER BY metric, labels"
        return tuple(
            SeriesKey(metric=row["metric"], labels=_json_map(row["labels"]))
            for row in self._store.query(sql, params)
        )

    def metric_series(
        self,
        entity_id: str,
        metric: str,
        window: TimeRange | None = None,
        *,
        label_filter: Mapping[str, str] | None = None,
    ) -> tuple[SamplePoint, ...]:
        """Raw samples of one metric for one entity, in time order.

        Args:
            entity_id: Entity to read.
            metric: Prometheus metric name.
            window: Restrict to ``[start, end)``. ``None`` means everything.
            label_filter: Keep only samples whose labels match every pair. Use
                this to pick one series out of a per-CPU or per-port family.

        Returns:
            Samples ordered by timestamp then by canonical label text, so two
            samples at the same instant have a defined order.
        """
        sql = (
            "SELECT timestamp_ms, value, labels FROM metric_samples "
            "WHERE entity_id = ? AND metric = ?"
        )
        params: list[Any] = [entity_id, metric]
        if window is not None:
            sql += " AND timestamp_ms >= ? AND timestamp_ms < ?"
            params.extend([_to_millis(window.start), _to_millis(window.end)])
        sql += " ORDER BY timestamp_ms, labels"
        points = []
        for row in self._store.query(sql, params):
            labels = _json_map(row["labels"])
            if label_filter and any(
                labels.get(key) != value for key, value in label_filter.items()
            ):
                continue
            points.append(
                SamplePoint(
                    timestamp=_from_millis(int(row["timestamp_ms"])),
                    value=float(row["value"]),
                    labels=labels,
                )
            )
        return tuple(points)

    def feature_windows(
        self,
        entity_id: str,
        window: TimeRange | None = None,
        *,
        window_s: int | None = None,
        metrics: Sequence[str] | None = None,
        aggregates: Sequence[str] = DEFAULT_AGGREGATES,
        group_labels: Sequence[str] = (),
        include_empty: bool = False,
    ) -> tuple[FeatureWindow, ...]:
        """Time-windowed aggregated metric features for one entity.

        This is the method the detection layer is expected to live on.

        Args:
            entity_id: Entity to read.
            window: Interval to cover. Defaults to :meth:`time_bounds`.
            window_s: Window width in seconds. Defaults to
                :attr:`default_window_s`.
            metrics: Restrict to these metric names. ``None`` means every
                metric the entity reports.
            aggregates: Which of :data:`AGGREGATES` to compute.
            group_labels: Split each metric by these label keys, so a per-port
                or per-CPU family yields one feature per member instead of one
                blended average. Feature keys then read
                ``metric{ifName=Gi0/12}__mean``.
            include_empty: Emit a :class:`FeatureWindow` with no values for
                windows in which the entity reported nothing. Off by default,
                because an absent window and an all-zero window are different
                facts.

        Returns:
            Windows in ascending time order, tiling ``window``.

        Raises:
            ValueError: for an unknown aggregate name or a non-positive width.
        """
        unknown = sorted(set(aggregates) - set(AGGREGATES))
        if unknown:
            raise ValueError(
                f"unknown aggregate(s) {unknown}; supported: {list(AGGREGATES)}"
            )
        if not aggregates:
            raise ValueError("at least one aggregate is required")
        span = window if window is not None else self.time_bounds()
        if span is None:
            return ()
        width = window_s if window_s is not None else self._default_window_s
        if width <= 0:
            raise ValueError(f"window_s must be > 0, got {width}")

        origin_ms = _to_millis(span.start)
        end_ms = _to_millis(span.end)
        width_ms = width * 1000

        buckets = self._aggregate_buckets(
            entity_id=entity_id,
            origin_ms=origin_ms,
            end_ms=end_ms,
            width_ms=width_ms,
            metrics=metrics,
            group_labels=tuple(group_labels),
        )

        windows: list[FeatureWindow] = []
        for index, sub in enumerate(span.split(width)):
            rows = buckets.get(index, ())
            if not rows and not include_empty:
                continue
            values: dict[str, float] = {}
            counts: dict[str, int] = {}
            for row in rows:
                base = (
                    f"{row['metric']}{{{row['group']}}}"
                    if row["group"]
                    else row["metric"]
                )
                counts[base] = int(row["n"])
                for aggregate in aggregates:
                    computed = _compute(aggregate, row)
                    if computed is not None:
                        values[f"{base}__{aggregate}"] = computed
            windows.append(
                FeatureWindow(
                    entity_id=entity_id,
                    window=sub,
                    values=values,
                    sample_count=counts,
                )
            )
        return tuple(windows)

    def feature_matrix(
        self,
        entity_ids: Iterable[str] | None = None,
        window: TimeRange | None = None,
        **kwargs: Any,
    ) -> dict[str, tuple[FeatureWindow, ...]]:
        """:meth:`feature_windows` for several entities at once.

        Args:
            entity_ids: Entities to read. ``None`` means every entity that
                appears in the telemetry (:meth:`observed_entities`).
            window: Passed through.
            **kwargs: Passed through to :meth:`feature_windows`.

        Returns:
            Entity id to its windows, iteration-ordered by entity id.
        """
        selected = (
            tuple(entity_ids)
            if entity_ids is not None
            else self.observed_entities()
        )
        return {
            entity_id: self.feature_windows(entity_id, window, **kwargs)
            for entity_id in sorted(selected)
        }

    # -- logs --------------------------------------------------------------
    def log_rows(
        self,
        entity_id: str | None = None,
        window: TimeRange | None = None,
        *,
        severities: Iterable[str] | None = None,
        limit: int | None = None,
    ) -> tuple[LogRow, ...]:
        """Ingested log entries, ordered by timestamp then ``insertId``.

        Args:
            entity_id: Restrict to one entity. ``None`` means all.
            window: Restrict to ``[start, end)``.
            severities: Restrict to these ``LogSeverity`` names.
            limit: Cap the number of rows returned.
        """
        sql = (
            "SELECT insert_id, timestamp, entity_id, severity, message, "
            "resource_type, labels, resource_labels, json_payload "
            "FROM log_entries WHERE 1 = 1"
        )
        params: list[Any] = []
        if entity_id is not None:
            sql += " AND entity_id = ?"
            params.append(entity_id)
        if window is not None:
            sql += " AND timestamp >= ? AND timestamp < ?"
            params.extend(
                [format_rfc3339(window.start), format_rfc3339(window.end)]
            )
        chosen = tuple(severities) if severities is not None else ()
        if chosen:
            sql += " AND severity IN (" + ",".join("?" * len(chosen)) + ")"
            params.extend(chosen)
        sql += " ORDER BY timestamp, insert_id"
        if limit is not None:
            if limit < 0:
                raise ValueError(f"limit must be >= 0, got {limit}")
            sql += " LIMIT ?"
            params.append(limit)
        return tuple(
            LogRow(
                insert_id=row["insert_id"],
                timestamp=_parse_ts(row["timestamp"]),
                entity_id=row["entity_id"],
                severity=row["severity"] or "DEFAULT",
                message=row["message"] or "",
                resource_type=row["resource_type"],
                labels=_json_map(row["labels"]),
                resource_labels=_json_map(row["resource_labels"]),
                json_payload=_json_obj(row["json_payload"]),
            )
            for row in self._store.query(sql, params)
        )

    def log_count_windows(
        self,
        entity_id: str | None = None,
        window: TimeRange | None = None,
        *,
        window_s: int | None = None,
        include_empty: bool = False,
    ) -> tuple[LogCountWindow, ...]:
        """Log volume per window, split by severity.

        Cheap enough to call per entity per run: the counting happens in the
        store, not in Python.
        """
        span = window if window is not None else self.time_bounds()
        if span is None:
            return ()
        width = window_s if window_s is not None else self._default_window_s
        if width <= 0:
            raise ValueError(f"window_s must be > 0, got {width}")

        rows = self.log_rows(entity_id, span)
        width_delta = _dt.timedelta(seconds=width)
        tally: dict[int, dict[str, int]] = {}
        for row in rows:
            index = int(
                (row.timestamp - span.start).total_seconds() // width
            )
            bucket = tally.setdefault(index, {})
            bucket[row.severity] = bucket.get(row.severity, 0) + 1

        out: list[LogCountWindow] = []
        for index, sub in enumerate(span.split(width)):
            bucket = tally.get(index)
            if bucket is None and not include_empty:
                continue
            by_severity = dict(sorted((bucket or {}).items()))
            out.append(
                LogCountWindow(
                    entity_id=entity_id,
                    window=sub,
                    total=sum(by_severity.values()),
                    by_severity=by_severity,
                )
            )
        del width_delta
        return tuple(out)

    def embeddings(
        self,
        entity_id: str | None = None,
        window: TimeRange | None = None,
        *,
        limit: int | None = None,
    ) -> tuple[EmbeddingRow, ...]:
        """Stored log-message embeddings, ordered by timestamp then id.

        The semantic layer reads these rather than recomputing them, which is
        what makes "outlier detection operates on the embeddings" (AC-14) a
        fact about the data flow and not just about the code.
        """
        sql = (
            "SELECT insert_id, entity_id, timestamp, vector "
            "FROM log_embeddings WHERE 1 = 1"
        )
        params: list[Any] = []
        if entity_id is not None:
            sql += " AND entity_id = ?"
            params.append(entity_id)
        if window is not None:
            sql += " AND timestamp >= ? AND timestamp < ?"
            params.extend(
                [format_rfc3339(window.start), format_rfc3339(window.end)]
            )
        sql += " ORDER BY timestamp, insert_id"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        return tuple(
            EmbeddingRow(
                insert_id=row["insert_id"],
                entity_id=row["entity_id"],
                timestamp=_parse_ts(row["timestamp"]),
                vector=tuple(float(v) for v in json.loads(row["vector"])),
            )
            for row in self._store.query(sql, params)
        )

    def embedded_log_rows(
        self,
        entity_id: str | None = None,
        window: TimeRange | None = None,
        *,
        limit: int | None = None,
    ) -> tuple[tuple[LogRow, tuple[float, ...]], ...]:
        """Log entries joined to their stored embedding vectors.

        The join happens in the store, so the semantic layer reads the vectors
        the ingest pipeline persisted instead of recomputing them from strings.
        That is what makes "outlier detection operates on those embeddings"
        (AC-14) an observable property of the data flow.

        Args:
            entity_id: Restrict to one entity.
            window: Restrict to ``[start, end)``.
            limit: Cap the number of rows.

        Returns:
            ``(LogRow, vector)`` pairs ordered by timestamp then ``insertId``.
            Entries with no stored embedding are omitted.
        """
        sql = (
            "SELECT e.insert_id AS insert_id, e.timestamp AS timestamp, "
            "e.entity_id AS entity_id, e.severity AS severity, "
            "e.message AS message, e.resource_type AS resource_type, "
            "e.labels AS labels, e.resource_labels AS resource_labels, "
            "e.json_payload AS json_payload, v.vector AS vector "
            "FROM log_entries AS e "
            "JOIN log_embeddings AS v ON v.insert_id = e.insert_id "
            "WHERE 1 = 1"
        )
        params: list[Any] = []
        if entity_id is not None:
            sql += " AND e.entity_id = ?"
            params.append(entity_id)
        if window is not None:
            sql += " AND e.timestamp >= ? AND e.timestamp < ?"
            params.extend(
                [format_rfc3339(window.start), format_rfc3339(window.end)]
            )
        sql += " ORDER BY e.timestamp, e.insert_id"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        return tuple(
            (
                LogRow(
                    insert_id=row["insert_id"],
                    timestamp=_parse_ts(row["timestamp"]),
                    entity_id=row["entity_id"],
                    severity=row["severity"] or "DEFAULT",
                    message=row["message"] or "",
                    resource_type=row["resource_type"],
                    labels=_json_map(row["labels"]),
                    resource_labels=_json_map(row["resource_labels"]),
                    json_payload=_json_obj(row["json_payload"]),
                ),
                tuple(float(v) for v in json.loads(row["vector"])),
            )
            for row in self._store.query(sql, params)
        )

    def phase_range(self, phase: str, kind: str = "log") -> TimeRange | None:
        """The observed bounds of one ingest phase, or ``None`` if unrecorded.

        The generator marks telemetry as ``history`` (the long unlabelled
        baseline period) or ``window`` (the evaluation period), and the ingest
        pipeline records the bounds it saw. A model can therefore say exactly
        which interval it trained on, from ingested data only — feature 34,
        without going near ground truth.

        Args:
            phase: ``"history"``, ``"window"`` or ``"unknown"``.
            kind: ``"log"`` or ``"metric"``.
        """
        row = self._store.query_one(
            "SELECT start_time, end_time FROM ingest_phases "
            "WHERE phase = ? AND kind = ? "
            "ORDER BY ingest_seq DESC LIMIT 1",
            [phase, kind],
        )
        if not row:
            return None
        return TimeRange(
            _parse_ts(row["start_time"]),
            _parse_ts(row["end_time"]) + _dt.timedelta(milliseconds=1),
        )

    def phases(self) -> tuple[str, ...]:
        """Distinct phases recorded during ingest, sorted."""
        rows = self._store.query(
            "SELECT DISTINCT phase FROM ingest_phases ORDER BY phase"
        )
        return tuple(row["phase"] for row in rows)

    # -- operational inputs ------------------------------------------------

    def change_windows(
        self,
        entity_id: str | None = None,
        window: TimeRange | None = None,
        *,
        include_estate_wide: bool = True,
    ) -> tuple[ChangeWindow, ...]:
        """Scheduled changes overlapping ``window``, ordered by start time.

        Args:
            entity_id: Restrict to changes affecting this entity.
            window: Keep only changes that overlap ``[start, end)``.
            include_estate_wide: Also return changes with a NULL entity, which
                apply to the whole estate.
        """
        sql = (
            "SELECT change_id, kind, entity_id, start_time, end_time, "
            "description FROM change_events WHERE 1 = 1"
        )
        params: list[Any] = []
        if entity_id is not None:
            if include_estate_wide:
                sql += " AND (entity_id = ? OR entity_id IS NULL)"
            else:
                sql += " AND entity_id = ?"
            params.append(entity_id)
        if window is not None:
            sql += " AND start_time < ? AND end_time > ?"
            params.extend(
                [format_rfc3339(window.end), format_rfc3339(window.start)]
            )
        sql += " ORDER BY start_time, change_id"
        return tuple(
            ChangeWindow(
                change_id=row["change_id"],
                kind=row["kind"],
                entity_id=row["entity_id"],
                window=TimeRange(
                    _parse_ts(row["start_time"]), _parse_ts(row["end_time"])
                ),
                description=row["description"] or "",
            )
            for row in self._store.query(sql, params)
        )

    def topology_edges(
        self,
        *,
        src_entity: str | None = None,
        dst_entity: str | None = None,
        relation: str | None = None,
    ) -> tuple[TopologyEdge, ...]:
        """Directed topology relations, ordered deterministically."""
        sql = (
            "SELECT src_entity, src_kind, dst_entity, dst_kind, relation "
            "FROM topology WHERE 1 = 1"
        )
        params: list[Any] = []
        if src_entity is not None:
            sql += " AND src_entity = ?"
            params.append(src_entity)
        if dst_entity is not None:
            sql += " AND dst_entity = ?"
            params.append(dst_entity)
        if relation is not None:
            sql += " AND relation = ?"
            params.append(relation)
        sql += " ORDER BY src_entity, relation, dst_entity"
        return tuple(
            TopologyEdge(
                src_entity=row["src_entity"],
                dst_entity=row["dst_entity"],
                relation=row["relation"],
                src_kind=row["src_kind"],
                dst_kind=row["dst_kind"],
            )
            for row in self._store.query(sql, params)
        )

    # -- diagnostics -------------------------------------------------------
    def row_counts(self) -> dict[str, int]:
        """Row count per table this API reads. Cheap ingest sanity check."""
        tables = (
            "log_entries",
            "metric_samples",
            "log_embeddings",
            "entities",
            "topology",
            "change_events",
        )
        counts: dict[str, int] = {}
        for table in tables:
            row = self._store.query_one(f'SELECT count(*) AS n FROM "{table}"')
            counts[table] = int(row["n"]) if row else 0
        return counts

    # -- internals ---------------------------------------------------------
    def _aggregate_buckets(
        self,
        *,
        entity_id: str,
        origin_ms: int,
        end_ms: int,
        width_ms: int,
        metrics: Sequence[str] | None,
        group_labels: tuple[str, ...],
    ) -> dict[int, tuple[dict[str, Any], ...]]:
        """Run the windowed aggregation and index the rows by bucket."""
        group_expr = "''"
        if group_labels:
            # json_extract is available in SQLite (JSON1) and BigQuery alike;
            # the label map is canonical JSON text in both backends.
            pieces = [
                f"'{key}=' || coalesce(json_extract(labels, '$.\"{key}\"'), '')"
                for key in group_labels
                if _safe_label_key(key)
            ]
            if pieces:
                group_expr = " || ',' || ".join(pieces)

        where = ["entity_id = ?", "timestamp_ms >= ?", "timestamp_ms < ?"]
        params: list[Any] = [entity_id, origin_ms, end_ms]
        if metrics is not None:
            chosen = tuple(metrics)
            if not chosen:
                return {}
            where.append("metric IN (" + ",".join("?" * len(chosen)) + ")")
            params.extend(chosen)

        sql = (
            "WITH bucketed AS ("
            "  SELECT metric, value, timestamp_ms,"
            f"   {group_expr} AS grp,"
            "    CAST(FLOOR((timestamp_ms - ?) * 1.0 / ?) AS INTEGER) AS bucket"
            "  FROM metric_samples"
            f"  WHERE {' AND '.join(where)}"
            "), ranked AS ("
            "  SELECT metric, grp, bucket, value, timestamp_ms,"
            "    row_number() OVER ("
            "      PARTITION BY metric, grp, bucket"
            "      ORDER BY timestamp_ms ASC, value ASC) AS rn_first,"
            "    row_number() OVER ("
            "      PARTITION BY metric, grp, bucket"
            "      ORDER BY timestamp_ms DESC, value DESC) AS rn_last"
            "  FROM bucketed"
            ") SELECT metric, grp, bucket,"
            "  avg(value) AS mean, min(value) AS lo, max(value) AS hi,"
            "  sum(value) AS total, count(*) AS n, stddev(value) AS sd,"
            "  max(CASE WHEN rn_first = 1 THEN value END) AS first_value,"
            "  max(CASE WHEN rn_last = 1 THEN value END) AS last_value,"
            "  min(timestamp_ms) AS first_ms, max(timestamp_ms) AS last_ms"
            " FROM ranked GROUP BY metric, grp, bucket"
            " ORDER BY bucket, metric, grp"
        )
        # The two bucket-arithmetic parameters bind before the WHERE clause's.
        bound = [origin_ms, width_ms] + params

        out: dict[int, list[dict[str, Any]]] = {}
        for row in self._store.query(sql, bound):
            out.setdefault(int(row["bucket"]), []).append(
                {
                    "metric": row["metric"],
                    "group": row["grp"] or "",
                    "mean": row["mean"],
                    "lo": row["lo"],
                    "hi": row["hi"],
                    "total": row["total"],
                    "n": row["n"],
                    "sd": row["sd"],
                    "first_value": row["first_value"],
                    "last_value": row["last_value"],
                    "first_ms": row["first_ms"],
                    "last_ms": row["last_ms"],
                }
            )
        return {index: tuple(rows) for index, rows in out.items()}


def _compute(aggregate: str, row: Mapping[str, Any]) -> float | None:
    """Derive one aggregate from an aggregation row, or ``None`` if undefined."""
    if aggregate == "mean":
        return _float(row["mean"])
    if aggregate == "min":
        return _float(row["lo"])
    if aggregate == "max":
        return _float(row["hi"])
    if aggregate == "sum":
        return _float(row["total"])
    if aggregate == "count":
        return float(row["n"])
    if aggregate == "stddev":
        # NULL for a single sample: a standard deviation of one point is
        # undefined, and reporting 0.0 would understate the uncertainty.
        return _float(row["sd"])
    if aggregate == "first":
        return _float(row["first_value"])
    if aggregate == "last":
        return _float(row["last_value"])
    first = _float(row["first_value"])
    last = _float(row["last_value"])
    if first is None or last is None:
        return None
    if aggregate == "delta":
        return last - first
    if aggregate == "rate_per_s":
        span_ms = int(row["last_ms"]) - int(row["first_ms"])
        if span_ms <= 0:
            return None
        return (last - first) / (span_ms / 1000.0)
    raise ValueError(f"unhandled aggregate {aggregate!r}")


def _float(value: Any) -> float | None:
    """Coerce a store value to float, mapping NULL and NaN to ``None``."""
    if value is None:
        return None
    number = float(value)
    return None if math.isnan(number) else number


def _safe_label_key(key: str) -> bool:
    """True if ``key`` is safe to inline into a JSON path expression."""
    if not key or not key.replace("_", "").isalnum():
        raise ValueError(
            f"label key {key!r} must be alphanumeric with underscores; it is "
            "inlined into a JSON path and cannot be parameterised"
        )
    return True


def _json_map(value: Any) -> dict[str, str]:
    """Decode a JSON label column into a ``str -> str`` mapping."""
    if not value:
        return {}
    if isinstance(value, Mapping):
        return {str(k): str(v) for k, v in value.items()}
    obj = json.loads(value)
    return {str(k): str(v) for k, v in obj.items()} if obj else {}


def _json_obj(value: Any) -> dict[str, Any] | None:
    """Decode a JSON object column, or ``None``."""
    if not value:
        return None
    if isinstance(value, Mapping):
        return dict(value)
    obj = json.loads(value)
    return dict(obj) if isinstance(obj, dict) else None


def _from_millis(value: int) -> _dt.datetime:
    """Epoch milliseconds to an aware UTC datetime."""
    return _dt.datetime.fromtimestamp(value / 1000.0, tz=_UTC)


def _to_millis(value: _dt.datetime) -> int:
    """Aware datetime to epoch milliseconds (floor)."""
    return int(value.timestamp() * 1000)


def _parse_ts(text: str) -> _dt.datetime:
    """Parse RFC 3339 store text into an aware UTC datetime."""
    cleaned = text.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    moment = _dt.datetime.fromisoformat(cleaned)
    return moment.replace(tzinfo=_UTC) if moment.tzinfo is None else moment.astimezone(_UTC)
