"""``AnalyticalStore`` — the BigQuery adapter (contract C3).

Stands in for
-------------
**BigQuery**. In production the ``gcp`` backend wraps
``google.cloud.bigquery.Client``: :meth:`AnalyticalStore.insert_rows` becomes
``insert_rows_json`` (or a load job), :meth:`AnalyticalStore.query` becomes
``client.query(sql, job_config=...)``, and :meth:`ensure_schema` becomes dataset
and table creation. Tables carry the same names and columns in both, so the SQL
the analytical layers write is the same SQL.

What changes on deployment
--------------------------
Configuration only: ``store_backend`` becomes ``"gcp"``, ``project_id`` is set
and ``dataset`` names the BigQuery dataset. ``sqlite_path`` stops being used.

Offline substrate
-----------------
Standard-library ``sqlite3`` (SQLite 3.53.4 on this machine). The recon survey
verified the analytical features this project needs: **window functions, CTEs,
JSON1 ``json_extract``, ``ntile`` and FTS5 all work**
(``.agents/explorer_survey_env/handoff.md`` §1.8). Two gaps and their fixes:

* **``stddev`` does not exist.** :meth:`AnalyticalStore.connect` registers
  ``stddev``/``stddev_samp`` (sample, N-1), ``stddev_pop`` and ``variance``
  via ``create_aggregate``, computed with Welford's algorithm so long series do
  not lose precision. Rolling baselines (requirement R4) depend on these.
* **``generate_series`` does not exist.** Use :func:`series_cte` to build the
  recursive CTE that replaces it.

Writing portable SQL
--------------------
Keep queries dialect-portable so the same text runs against BigQuery:

* parameters are ``?`` placeholders here; a BigQuery-backed implementation maps
  the same positional parameters to query parameters;
* JSON columns are TEXT holding canonical JSON, read with ``json_extract`` —
  which BigQuery also provides;
* timestamps are stored as **RFC 3339 text**, which sorts and compares
  lexicographically in exactly the chronological order, and casts to BigQuery
  ``TIMESTAMP`` on load.

Determinism
-----------
Neither SQLite nor BigQuery promises row order without ``ORDER BY``. Every
query whose result feeds a score must order explicitly. :meth:`insert_rows`
stamps a monotonic ``ingest_seq`` on core tables so "insertion order" is
available as an explicit, orderable column rather than an assumption.
"""

from __future__ import annotations

import datetime as _dt
import math
import os
import sqlite3
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from ano.config import LOCAL, Config
from ano.contracts.common import dumps_canonical, format_rfc3339
from ano.gcp.registry import register_backend

__all__ = [
    "Column",
    "TableSpec",
    "AnalyticalStore",
    "SqliteAnalyticalStore",
    "CORE_TABLES",
    "register_table",
    "registered_tables",
    "series_cte",
    "connect",
    "build_analytical_store",
]


@dataclass(frozen=True, slots=True)
class Column:
    """One column of a table.

    Attributes:
        name: Column name.
        sql_type: SQLite type (``TEXT``, ``INTEGER``, ``REAL``).
        bigquery_type: The corresponding BigQuery type, recorded so the
            deployed schema is derivable from this one declaration rather than
            maintained twice.
        nullable: Whether NULL is allowed.
        json: True if the column holds canonical JSON text. Values that are
            dicts or lists are encoded automatically on insert.
        description: What the column means.
    """

    name: str
    sql_type: str = "TEXT"
    bigquery_type: str = "STRING"
    nullable: bool = True
    json: bool = False
    description: str = ""


@dataclass(frozen=True, slots=True)
class TableSpec:
    """A table definition, shared between the offline store and BigQuery.

    Attributes:
        name: Table name, identical in both backends.
        columns: Column definitions.
        primary_key: Column names forming the primary key, if any.
        indexes: Tuples of column names to index.
        description: What the table holds.
        ingest_seq: Add a monotonic ``ingest_seq INTEGER`` column so insertion
            order is an explicit, orderable fact.
    """

    name: str
    columns: tuple[Column, ...]
    primary_key: tuple[str, ...] = ()
    indexes: tuple[tuple[str, ...], ...] = ()
    description: str = ""
    ingest_seq: bool = True

    def column_names(self) -> tuple[str, ...]:
        """Declared column names, including ``ingest_seq`` when present."""
        names = tuple(column.name for column in self.columns)
        return names + ("ingest_seq",) if self.ingest_seq else names

    def json_columns(self) -> frozenset[str]:
        """Names of columns holding canonical JSON text."""
        return frozenset(column.name for column in self.columns if column.json)

    def create_sql(self) -> str:
        """``CREATE TABLE IF NOT EXISTS`` statement for SQLite."""
        parts: list[str] = []
        for column in self.columns:
            piece = f'"{column.name}" {column.sql_type}'
            if not column.nullable:
                piece += " NOT NULL"
            parts.append(piece)
        if self.ingest_seq:
            parts.append('"ingest_seq" INTEGER')
        if self.primary_key:
            keys = ", ".join(f'"{name}"' for name in self.primary_key)
            parts.append(f"PRIMARY KEY ({keys})")
        body = ",\n  ".join(parts)
        return f'CREATE TABLE IF NOT EXISTS "{self.name}" (\n  {body}\n)'

    def index_sql(self) -> list[str]:
        """``CREATE INDEX IF NOT EXISTS`` statements."""
        statements: list[str] = []
        for columns in self.indexes:
            suffix = "_".join(columns)
            cols = ", ".join(f'"{name}"' for name in columns)
            statements.append(
                f'CREATE INDEX IF NOT EXISTS "idx_{self.name}_{suffix}" '
                f'ON "{self.name}" ({cols})'
            )
        return statements

    def bigquery_schema(self) -> list[dict[str, str]]:
        """The equivalent BigQuery schema, as plain data.

        The deployed table definition is derived from the same declaration the
        offline store uses, so the two cannot drift apart.
        """
        schema = [
            {
                "name": column.name,
                "type": column.bigquery_type,
                "mode": "NULLABLE" if column.nullable else "REQUIRED",
                "description": column.description,
            }
            for column in self.columns
        ]
        if self.ingest_seq:
            schema.append(
                {
                    "name": "ingest_seq",
                    "type": "INT64",
                    "mode": "NULLABLE",
                    "description": "monotonic insertion sequence",
                }
            )
        return schema


#: The core tables every milestone shares. Milestone-specific tables are added
#: with :func:`register_table` rather than by editing this file.
CORE_TABLES: tuple[TableSpec, ...] = (
    TableSpec(
        name="log_entries",
        description="Cloud Logging LogEntry records, flattened for analysis.",
        columns=(
            Column("insert_id", "TEXT", "STRING", nullable=False,
                   description="LogEntry.insertId; the de-duplication key"),
            Column("timestamp", "TEXT", "TIMESTAMP", nullable=False,
                   description="LogEntry.timestamp, RFC3339"),
            Column("receive_timestamp", "TEXT", "TIMESTAMP",
                   description="LogEntry.receiveTimestamp, RFC3339"),
            Column("severity", "TEXT", "STRING",
                   description="LogEntry.severity"),
            Column("log_name", "TEXT", "STRING",
                   description="LogEntry.logName"),
            Column("resource_type", "TEXT", "STRING",
                   description="LogEntry.resource.type"),
            Column("resource_labels", "TEXT", "JSON", json=True,
                   description="LogEntry.resource.labels"),
            Column("labels", "TEXT", "JSON", json=True,
                   description="LogEntry.labels"),
            Column("text_payload", "TEXT", "STRING",
                   description="LogEntry.textPayload"),
            Column("json_payload", "TEXT", "JSON", json=True,
                   description="LogEntry.jsonPayload"),
            Column("entity_id", "TEXT", "STRING",
                   description="resolved monitored entity"),
            Column("message", "TEXT", "STRING",
                   description="the human-readable message the embedding is "
                               "computed from"),
        ),
        primary_key=("insert_id",),
        indexes=(("timestamp",), ("entity_id", "timestamp"), ("severity",)),
    ),
    TableSpec(
        name="metric_samples",
        description="Managed Prometheus samples.",
        columns=(
            Column("metric", "TEXT", "STRING", nullable=False,
                   description="metric name"),
            Column("labels", "TEXT", "JSON", json=True,
                   description="label set"),
            Column("value", "REAL", "FLOAT64", nullable=False,
                   description="sample value"),
            Column("timestamp_ms", "INTEGER", "INT64", nullable=False,
                   description="sample time, epoch milliseconds"),
            Column("entity_id", "TEXT", "STRING",
                   description="resolved monitored entity"),
        ),
        indexes=(
            ("metric", "timestamp_ms"),
            ("entity_id", "metric", "timestamp_ms"),
        ),
    ),
    TableSpec(
        name="log_embeddings",
        description="Vector embeddings of log messages (requirement R1).",
        columns=(
            Column("insert_id", "TEXT", "STRING", nullable=False,
                   description="the log entry this embeds"),
            Column("entity_id", "TEXT", "STRING",
                   description="resolved monitored entity"),
            Column("timestamp", "TEXT", "TIMESTAMP",
                   description="log entry timestamp, RFC3339"),
            Column("dim", "INTEGER", "INT64", nullable=False,
                   description="vector dimensionality"),
            Column("vector", "TEXT", "JSON", json=True, nullable=False,
                   description="the embedding, as a JSON array of floats"),
        ),
        primary_key=("insert_id",),
        indexes=(("entity_id", "timestamp"),),
    ),
    TableSpec(
        name="entities",
        description="The monitored estate.",
        columns=(
            Column("entity_id", "TEXT", "STRING", nullable=False,
                   description="stable entity identifier"),
            Column("kind", "TEXT", "STRING", nullable=False,
                   description="app_server | database | switch | node"),
            Column("site", "TEXT", "STRING", description="site or region"),
            Column("labels", "TEXT", "JSON", json=True,
                   description="arbitrary entity labels"),
        ),
        primary_key=("entity_id",),
        indexes=(("kind",),),
    ),
    TableSpec(
        name="topology",
        description=(
            "Directed relations between entities (requirement R3): which app "
            "servers query which databases, which switch ports they traverse."
        ),
        columns=(
            Column("src_entity", "TEXT", "STRING", nullable=False,
                   description="source entity id"),
            Column("src_kind", "TEXT", "STRING", description="source kind"),
            Column("dst_entity", "TEXT", "STRING", nullable=False,
                   description="destination entity id"),
            Column("dst_kind", "TEXT", "STRING", description="destination kind"),
            Column("relation", "TEXT", "STRING", nullable=False,
                   description="queries | traverses | hosts | peers"),
        ),
        indexes=(("src_entity",), ("dst_entity",), ("relation",)),
    ),
    TableSpec(
        name="change_events",
        description=(
            "The scheduled-change calendar used for change-awareness "
            "(requirement R4): patch windows, maintenance, planned deploys. "
            "This is a legitimate operational input, not ground truth - real "
            "operations teams know their own change schedule."
        ),
        columns=(
            Column("change_id", "TEXT", "STRING", nullable=False,
                   description="change identifier"),
            Column("kind", "TEXT", "STRING", nullable=False,
                   description="patching_window | maintenance | planned_deploy"),
            Column("entity_id", "TEXT", "STRING",
                   description="affected entity, or NULL for estate-wide"),
            Column("start_time", "TEXT", "TIMESTAMP", nullable=False,
                   description="window start, RFC3339"),
            Column("end_time", "TEXT", "TIMESTAMP", nullable=False,
                   description="window end, RFC3339"),
            Column("description", "TEXT", "STRING", description="free text"),
        ),
        primary_key=("change_id",),
        indexes=(("entity_id", "start_time"), ("start_time",)),
    ),
    TableSpec(
        name="predictions",
        description="Emitted C2 prediction records.",
        columns=(
            Column("prediction_id", "TEXT", "STRING", nullable=False,
                   description="C2 prediction_id"),
            Column("emitted_at", "TEXT", "TIMESTAMP", nullable=False,
                   description="decision time, RFC3339"),
            Column("track", "TEXT", "STRING", nullable=False,
                   description="unresponsiveness | cross_domain"),
            Column("entity", "TEXT", "STRING", nullable=False,
                   description="subject entity"),
            Column("predicted_onset", "TEXT", "TIMESTAMP",
                   description="forecast onset, RFC3339"),
            Column("time_to_failure_s", "INTEGER", "INT64",
                   description="seconds from emitted_at to predicted_onset"),
            Column("confidence", "REAL", "FLOAT64", description="0..1"),
            Column("root_cause_domain", "TEXT", "STRING",
                   description="attributed domain"),
            Column("root_cause_entity", "TEXT", "STRING",
                   description="attributed entity"),
            Column("suppressed", "INTEGER", "BOOL",
                   description="1 if suppressed"),
            Column("suppression_reason", "TEXT", "STRING",
                   description="why, when suppressed"),
            Column("incident_id", "TEXT", "STRING", nullable=False,
                   description="collapse key"),
            Column("payload", "TEXT", "JSON", json=True, nullable=False,
                   description="the complete C2 record"),
        ),
        primary_key=("prediction_id",),
        indexes=(("incident_id",), ("entity", "emitted_at"), ("track",)),
    ),
    TableSpec(
        name="alerts",
        description="Alerts emitted to the alerting plane.",
        columns=(
            Column("alert_id", "TEXT", "STRING", nullable=False,
                   description="stable alert identifier"),
            Column("created_at", "TEXT", "TIMESTAMP",
                   description="emission time, RFC3339"),
            Column("incident_id", "TEXT", "STRING",
                   description="the incident this alert belongs to"),
            Column("entity", "TEXT", "STRING", description="subject entity"),
            Column("severity", "TEXT", "STRING",
                   description="alert severity"),
            Column("summary", "TEXT", "STRING", description="one-line summary"),
            Column("payload", "TEXT", "JSON", json=True,
                   description="the complete alert record"),
        ),
        primary_key=("alert_id",),
        indexes=(("incident_id",), ("created_at",)),
    ),
)

_EXTRA_TABLES: dict[str, TableSpec] = {}


def register_table(spec: TableSpec) -> None:
    """Register an additional table.

    Milestones that need their own tables call this at import time instead of
    editing :data:`CORE_TABLES` — the store module is owned by milestone M1 and
    must not become a merge point for five parallel tracks.

    Raises:
        ValueError: if the name collides with a core or already-registered
            table with a different definition.
    """
    if not isinstance(spec, TableSpec):
        raise TypeError(f"expected a TableSpec, got {type(spec).__name__}")
    for core in CORE_TABLES:
        if core.name == spec.name:
            raise ValueError(
                f"{spec.name!r} is a core table and may not be redefined"
            )
    existing = _EXTRA_TABLES.get(spec.name)
    if existing is not None and existing != spec:
        raise ValueError(
            f"table {spec.name!r} is already registered with a different "
            "definition"
        )
    _EXTRA_TABLES[spec.name] = spec


def registered_tables() -> tuple[TableSpec, ...]:
    """Core tables plus every table registered by another milestone."""
    return CORE_TABLES + tuple(
        _EXTRA_TABLES[name] for name in sorted(_EXTRA_TABLES)
    )


def series_cte(
    alias: str = "series",
    column: str = "n",
    start: str = ":start",
    stop: str = ":stop",
    step: str = "1",
) -> str:
    """Build the recursive CTE that replaces SQLite's missing ``generate_series``.

    Args:
        alias: CTE name.
        column: Generated column name.
        start: Expression or placeholder for the first value.
        stop: Expression or placeholder for the inclusive last value.
        step: Expression for the increment.

    Returns:
        A ``WITH RECURSIVE ...`` prefix. Append your ``SELECT``.

    Example::

        sql = series_cte("grid", "bucket", "?", "?", "300") + (
            " SELECT bucket FROM grid ORDER BY bucket")
    """
    return (
        f"WITH RECURSIVE {alias}({column}) AS ("
        f" SELECT {start}"
        f" UNION ALL"
        f" SELECT {column} + ({step}) FROM {alias} WHERE {column} + ({step}) <= {stop}"
        f")"
    )


class _Welford:
    """Streaming mean/variance accumulator shared by the stddev aggregates."""

    __slots__ = ("count", "mean", "m2")

    def __init__(self) -> None:
        self.count = 0
        self.mean = 0.0
        self.m2 = 0.0

    def step(self, value: Any) -> None:
        """Accumulate one value; NULL and non-numeric input are skipped."""
        if value is None:
            return
        try:
            number = float(value)
        except (TypeError, ValueError):
            return
        if not math.isfinite(number):
            return
        self.count += 1
        delta = number - self.mean
        self.mean += delta / self.count
        self.m2 += delta * (number - self.mean)


class _StdDevSample(_Welford):
    """Sample standard deviation (N-1), matching BigQuery's ``STDDEV``."""

    def finalize(self) -> float | None:
        """Return the sample standard deviation, or NULL for fewer than 2 rows."""
        if self.count < 2:
            return None
        return math.sqrt(self.m2 / (self.count - 1))


class _StdDevPop(_Welford):
    """Population standard deviation (N), matching ``STDDEV_POP``."""

    def finalize(self) -> float | None:
        """Return the population standard deviation, or NULL for no rows."""
        if self.count < 1:
            return None
        return math.sqrt(self.m2 / self.count)


class _VarianceSample(_Welford):
    """Sample variance (N-1), matching BigQuery's ``VARIANCE``."""

    def finalize(self) -> float | None:
        """Return the sample variance, or NULL for fewer than 2 rows."""
        if self.count < 2:
            return None
        return self.m2 / (self.count - 1)


class AnalyticalStore:
    """Contract C3: ``connect``, ``ensure_schema``, ``insert_rows``, ``query``.

    This class is the interface. :class:`SqliteAnalyticalStore` is the offline
    implementation; a deployment plugin registers a BigQuery-backed one with the
    same four methods.
    """

    def ensure_schema(self, extra_tables: Iterable[TableSpec] = ()) -> None:
        """Create every registered table and index if absent. Idempotent."""
        raise NotImplementedError

    def insert_rows(
        self, table: str, rows: Iterable[Mapping[str, Any]]
    ) -> int:
        """Insert rows into ``table``.

        Returns:
            The number of rows inserted.
        """
        raise NotImplementedError

    def query(
        self, sql: str, params: Sequence[Any] | Mapping[str, Any] = ()
    ) -> list[dict[str, Any]]:
        """Run ``sql`` and return the rows as dicts."""
        raise NotImplementedError

    def close(self) -> None:
        """Release resources."""
        raise NotImplementedError

    def __enter__(self) -> "AnalyticalStore":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


class SqliteAnalyticalStore(AnalyticalStore):
    """Offline BigQuery substitute backed by standard-library ``sqlite3``.

    Args:
        path: Database file, or ``":memory:"``. Parent directories are created.
        dataset: Dataset name; recorded for parity with BigQuery and reported
            by :attr:`dataset`.
        timeout: SQLite busy timeout in seconds.
    """

    def __init__(
        self,
        path: str = ":memory:",
        *,
        dataset: str = "ano",
        timeout: float = 30.0,
    ) -> None:
        self._path = path
        self._dataset = dataset
        if path != ":memory:":
            parent = os.path.dirname(path)
            if parent:
                os.makedirs(parent, exist_ok=True)
        self._conn = sqlite3.connect(path, timeout=timeout)
        self._conn.row_factory = sqlite3.Row
        self._register_functions()
        # WAL is unavailable for :memory:; it makes file-backed reads cheaper
        # and is harmless if it fails.
        try:
            self._conn.execute("PRAGMA journal_mode=WAL")
        except sqlite3.DatabaseError:  # pragma: no cover - platform dependent
            pass
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._seq = 0

    # -- properties --------------------------------------------------------
    @property
    def path(self) -> str:
        """The database file path, or ``":memory:"``."""
        return self._path

    @property
    def dataset(self) -> str:
        """The dataset name (the BigQuery dataset this stands in for)."""
        return self._dataset

    @property
    def connection(self) -> sqlite3.Connection:
        """The raw connection, for callers that need cursor-level control."""
        return self._conn

    # -- setup -------------------------------------------------------------
    def _register_functions(self) -> None:
        """Register the aggregates SQLite lacks but BigQuery provides."""
        self._conn.create_aggregate("stddev", 1, _StdDevSample)
        self._conn.create_aggregate("stddev_samp", 1, _StdDevSample)
        self._conn.create_aggregate("stddev_pop", 1, _StdDevPop)
        self._conn.create_aggregate("variance", 1, _VarianceSample)
        self._conn.create_aggregate("var_samp", 1, _VarianceSample)

    def ensure_schema(self, extra_tables: Iterable[TableSpec] = ()) -> None:
        """Create every registered table and index if absent. Idempotent."""
        specs = list(registered_tables()) + list(extra_tables)
        cursor = self._conn.cursor()
        for spec in specs:
            cursor.execute(spec.create_sql())
            for statement in spec.index_sql():
                cursor.execute(statement)
        self._conn.commit()

    def table_spec(self, table: str) -> TableSpec | None:
        """The registered spec for ``table``, or ``None`` if unregistered."""
        for spec in registered_tables():
            if spec.name == table:
                return spec
        return None

    # -- writes ------------------------------------------------------------
    def insert_rows(self, table: str, rows: Iterable[Mapping[str, Any]]) -> int:
        """Insert ``rows`` into ``table``.

        Values are adapted for storage: ``dict``/``list`` become canonical JSON,
        aware ``datetime`` becomes RFC 3339 text, ``bool`` becomes 0/1. A
        monotonic ``ingest_seq`` is stamped when the table declares one.

        Args:
            table: Table name.
            rows: Mappings keyed by column name. Every row must have the same
                key set as the first — a ragged batch means the producer has a
                bug, and SQLite would otherwise paper over it with NULLs.

        Returns:
            The number of rows inserted.

        Raises:
            ValueError: for an unknown column, or a ragged batch.
        """
        materialised = list(rows)
        if not materialised:
            return 0
        spec = self.table_spec(table)
        first_keys = tuple(materialised[0])
        if not first_keys:
            raise ValueError(f"cannot insert an empty row into {table!r}")
        if spec is not None:
            known = set(spec.column_names())
            unknown = sorted(set(first_keys) - known)
            if unknown:
                raise ValueError(
                    f"table {table!r} has no column(s) "
                    + ", ".join(repr(name) for name in unknown)
                    + "; declared columns are "
                    + ", ".join(sorted(known))
                )
            json_columns = spec.json_columns()
            stamp_seq = spec.ingest_seq and "ingest_seq" not in first_keys
        else:
            json_columns = frozenset()
            stamp_seq = False

        columns = list(first_keys) + (["ingest_seq"] if stamp_seq else [])
        placeholders = ", ".join("?" for _ in columns)
        quoted = ", ".join(f'"{name}"' for name in columns)
        statement = f'INSERT INTO "{table}" ({quoted}) VALUES ({placeholders})'

        payload: list[tuple[Any, ...]] = []
        for index, row in enumerate(materialised):
            if tuple(row) != first_keys:
                raise ValueError(
                    f"row {index} of the batch for {table!r} has keys "
                    f"{sorted(row)} but row 0 has {sorted(first_keys)}; every "
                    "row in a batch must declare the same columns"
                )
            values = [
                _adapt(row[name], name in json_columns) for name in first_keys
            ]
            if stamp_seq:
                self._seq += 1
                values.append(self._seq)
            payload.append(tuple(values))

        self._conn.executemany(statement, payload)
        self._conn.commit()
        return len(payload)

    def execute(
        self, sql: str, params: Sequence[Any] | Mapping[str, Any] = ()
    ) -> int:
        """Run a statement that returns no rows (DDL, DELETE, UPDATE).

        Returns:
            ``cursor.rowcount``.
        """
        cursor = self._conn.execute(sql, params)
        self._conn.commit()
        return cursor.rowcount

    # -- reads -------------------------------------------------------------
    def query(
        self, sql: str, params: Sequence[Any] | Mapping[str, Any] = ()
    ) -> list[dict[str, Any]]:
        """Run ``sql`` and return every row as a dict.

        Args:
            sql: SQL text with ``?`` (positional) or ``:name`` (named)
                placeholders. Never interpolate values into the text.
            params: Parameter values.

        Returns:
            Rows as dicts, in the order SQLite returned them. **Add an
            ``ORDER BY`` if the order matters** — neither SQLite nor BigQuery
            promises one otherwise.
        """
        cursor = self._conn.execute(sql, params)
        return [dict(row) for row in cursor.fetchall()]

    def query_one(
        self, sql: str, params: Sequence[Any] | Mapping[str, Any] = ()
    ) -> dict[str, Any] | None:
        """Run ``sql`` and return the first row, or ``None``."""
        cursor = self._conn.execute(sql, params)
        row = cursor.fetchone()
        return dict(row) if row is not None else None

    def scalar(
        self, sql: str, params: Sequence[Any] | Mapping[str, Any] = ()
    ) -> Any:
        """Run ``sql`` and return the first column of the first row."""
        cursor = self._conn.execute(sql, params)
        row = cursor.fetchone()
        return None if row is None else row[0]

    def count(self, table: str) -> int:
        """Number of rows in ``table``."""
        if not table.replace("_", "").isalnum():
            raise ValueError(f"unsafe table name {table!r}")
        return int(self.scalar(f'SELECT count(*) FROM "{table}"') or 0)

    def tables(self) -> tuple[str, ...]:
        """Every table present in the database, sorted."""
        rows = self.query(
            "SELECT name FROM sqlite_master WHERE type = 'table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        return tuple(row["name"] for row in rows)

    # -- lifecycle ---------------------------------------------------------
    def commit(self) -> None:
        """Commit the current transaction."""
        self._conn.commit()

    def close(self) -> None:
        """Commit and close the connection."""
        try:
            self._conn.commit()
        finally:
            self._conn.close()

    @classmethod
    def from_config(cls, config: Config) -> "SqliteAnalyticalStore":
        """Build the offline store from a :class:`ano.config.Config`."""
        return cls(config.resolved_sqlite_path(), dataset=config.dataset)


def _adapt(value: Any, as_json: bool) -> Any:
    """Convert a Python value to something SQLite can store."""
    if as_json:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        return dumps_canonical(value)
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, _dt.datetime):
        return format_rfc3339(value)
    if isinstance(value, (dict, list, tuple)):
        return dumps_canonical(list(value) if isinstance(value, tuple) else value)
    return value


def connect(config: Config) -> AnalyticalStore:
    """Contract C3 ``connect(cfg)``: build the configured analytical store."""
    from ano.gcp.registry import build

    return build("store", config)


#: Alias matching the other adapters' naming.
build_analytical_store = connect


register_backend("store", LOCAL, SqliteAnalyticalStore.from_config)
