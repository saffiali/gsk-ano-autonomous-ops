"""``LogSink`` — the Cloud Logging adapter.

Stands in for
-------------
**Google Cloud Logging** (``google.cloud.logging_v2``). In production the
``gcp`` backend wraps ``logging_v2.Client(project=...).logger(log_id)`` and each
:meth:`LogSink.write` becomes one ``logger.log_struct(...)`` /
``entries.write`` call carrying the same ``LogEntry`` dict.

What changes on deployment
--------------------------
Configuration only:

* ``logging_backend`` (or the global ``backend``) becomes ``"gcp"``;
* ``project_id`` is set;
* ``log_name`` becomes the Cloud Logging log id.

``log_path`` stops being used — entries go to the Cloud Logging API instead of
a local JSONL file. No application code changes: callers hold a :class:`LogSink`
and call :meth:`write`.

Behaviour deliberately mirrored from the real service
-----------------------------------------------------
* **``logName`` is populated by the service.** Cloud Logging derives it from the
  project and log id; the local sink does the same, so an entry written locally
  has the same ``logName`` it would have in the cloud.
* **``insertId`` de-duplication.** Cloud Logging treats two entries with the same
  ``insertId`` in the same log as duplicates and keeps one. The local sink does
  the same, so replaying a scenario twice cannot inflate log volume — which
  would quietly change every downstream count.
* **Append-only, ordered.** Entries are stored in write order. Cloud Logging
  orders by ``timestamp``; the offline reader exposes both (:meth:`entries` in
  write order, :meth:`entries_by_timestamp` in timestamp order) so callers state
  which they mean rather than depending on an accident.

This module does **not** validate ``LogEntry`` structure. That is
``ano.telemetry``'s job (milestone M2) — the sink is transport, and duplicating
schema rules in two places is how they drift apart. The sink checks only what it
must to do its job: the entry is a JSON object and ``insertId``, if present, is
a string.
"""

from __future__ import annotations

import abc
import json
import os
from collections.abc import Iterable, Iterator, Mapping
from typing import Any

from ano.config import LOCAL, Config
from ano.contracts.common import dumps_canonical
from ano.gcp.registry import register_backend

__all__ = ["LogSink", "LocalLogSink", "read_entries", "build_log_sink"]


class LogSink(abc.ABC):
    """Write ``LogEntry``-shaped records to the log plane.

    The surface mirrors ``google.cloud.logging_v2.Logger``: write one entry,
    write a batch, flush, close.
    """

    @abc.abstractmethod
    def write(self, entry: Mapping[str, Any]) -> None:
        """Write one ``LogEntry``-shaped dict."""

    def write_many(self, entries: Iterable[Mapping[str, Any]]) -> int:
        """Write a batch of entries.

        Returns:
            The number of entries accepted (duplicates suppressed by
            ``insertId`` are not counted).
        """
        accepted = 0
        before = self.count()
        for entry in entries:
            self.write(entry)
        accepted = self.count() - before
        return accepted

    @abc.abstractmethod
    def count(self) -> int:
        """Number of entries accepted so far."""

    def flush(self) -> None:
        """Flush buffered entries. No-op unless overridden."""

    def close(self) -> None:
        """Flush and release resources."""
        self.flush()

    def __enter__(self) -> "LogSink":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


class LocalLogSink(LogSink):
    """Offline Cloud Logging substitute backed by a JSONL file.

    Args:
        path: Destination file. Parent directories are created. Use ``None``
            for an in-memory-only sink (tests).
        log_name: Cloud Logging log id.
        project_id: Project id used to build ``logName``. Defaults to
            ``"local"`` offline, which makes it obvious in an artifact that the
            entry did not come from a real project.
        dedupe_by_insert_id: Mirror Cloud Logging's ``insertId`` de-duplication.
        keep_in_memory: Retain entries for :meth:`entries`. Always true when
            ``path`` is ``None``.
        truncate: Truncate an existing file on open. Default true, so a rerun
            reproduces the artifact rather than appending to the last run's.
    """

    def __init__(
        self,
        path: str | os.PathLike[str] | None,
        *,
        log_name: str = "ano-telemetry",
        project_id: str | None = None,
        dedupe_by_insert_id: bool = True,
        keep_in_memory: bool = True,
        truncate: bool = True,
    ) -> None:
        self._path = os.fspath(path) if path is not None else None
        self._log_name = log_name
        self._project_id = project_id or "local"
        self._dedupe = dedupe_by_insert_id
        self._keep = keep_in_memory or self._path is None
        self._entries: list[dict[str, Any]] = []
        self._seen_insert_ids: set[str] = set()
        self._count = 0
        self._handle = None
        if self._path is not None:
            parent = os.path.dirname(self._path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            self._handle = open(
                self._path, "w" if truncate else "a", encoding="utf-8"
            )

    # -- properties --------------------------------------------------------
    @property
    def path(self) -> str | None:
        """The JSONL destination, or ``None`` for in-memory only."""
        return self._path

    @property
    def log_name(self) -> str:
        """The fully qualified ``logName`` this sink stamps on entries."""
        return f"projects/{self._project_id}/logs/{self._log_name}"

    # -- LogSink -----------------------------------------------------------
    def write(self, entry: Mapping[str, Any]) -> None:
        """Write one entry, stamping ``logName`` and de-duplicating.

        Raises:
            TypeError: if ``entry`` is not a mapping.
            ValueError: if ``insertId`` is present but not a string.
        """
        if not isinstance(entry, Mapping):
            raise TypeError(
                f"a LogEntry must be a JSON object, got {type(entry).__name__}"
            )
        insert_id = entry.get("insertId")
        if insert_id is not None and not isinstance(insert_id, str):
            raise ValueError(
                f"insertId must be a string, got {type(insert_id).__name__}"
            )
        if self._dedupe and isinstance(insert_id, str):
            if insert_id in self._seen_insert_ids:
                # Cloud Logging keeps the first of a duplicate pair.
                return
            self._seen_insert_ids.add(insert_id)

        record = dict(entry)
        record.setdefault("logName", self.log_name)
        # Cloud Logging stamps receiveTimestamp at ingestion. In a deterministic
        # replay there is no meaningful wall clock, so the event timestamp is
        # used; this is documented rather than silently invented.
        if "timestamp" in record:
            record.setdefault("receiveTimestamp", record["timestamp"])

        if self._keep:
            self._entries.append(record)
        if self._handle is not None:
            self._handle.write(dumps_canonical(record) + "\n")
        self._count += 1

    def count(self) -> int:
        """Number of entries accepted (duplicates excluded)."""
        return self._count

    def flush(self) -> None:
        """Flush the underlying file handle."""
        if self._handle is not None:
            self._handle.flush()

    def close(self) -> None:
        """Flush and close the underlying file handle."""
        if self._handle is not None:
            self._handle.flush()
            self._handle.close()
            self._handle = None

    # -- offline read-back -------------------------------------------------
    def entries(self) -> tuple[dict[str, Any], ...]:
        """Entries in write order.

        Only available when ``keep_in_memory`` is set; otherwise use
        :func:`read_entries` against :attr:`path`.
        """
        if not self._keep:
            raise RuntimeError(
                "this sink was created with keep_in_memory=False; read the "
                f"artifact at {self._path!r} instead"
            )
        return tuple(self._entries)

    def entries_by_timestamp(self) -> tuple[dict[str, Any], ...]:
        """Entries sorted by ``timestamp`` then ``insertId``.

        The tie-break on ``insertId`` makes the order total, so two runs that
        produce the same entries produce the same sequence.
        """
        return tuple(
            sorted(
                self.entries(),
                key=lambda item: (
                    str(item.get("timestamp", "")),
                    str(item.get("insertId", "")),
                ),
            )
        )

    @classmethod
    def from_config(cls, config: Config) -> "LocalLogSink":
        """Build the offline sink from a :class:`ano.config.Config`."""
        return cls(
            config.resolved_log_path(),
            log_name=config.log_name,
            project_id=config.project_id,
        )


def read_entries(path: str | os.PathLike[str]) -> Iterator[dict[str, Any]]:
    """Stream ``LogEntry`` dicts from a JSONL artifact written by this sink."""
    with open(path, "r", encoding="utf-8") as handle:
        for number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                yield json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{number}: invalid JSON: {exc}") from exc


def build_log_sink(config: Config) -> LogSink:
    """Build the configured log sink (see :func:`ano.gcp.registry.build`)."""
    from ano.gcp.registry import build

    return build("logging", config)


register_backend("logging", LOCAL, LocalLogSink.from_config)
