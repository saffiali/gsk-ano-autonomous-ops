"""``MetricSink`` — the Managed Prometheus adapter.

Stands in for
-------------
**Google Cloud Managed Service for Prometheus (GMP)**. In production the
``gcp`` backend is the managed collector: targets are scraped and samples land
in Monarch, queryable through the Cloud Monitoring / PromQL API
(``google-cloud-monitoring``).

What changes on deployment
--------------------------
Configuration only: ``metrics_backend`` (or the global ``backend``) becomes
``"gcp"`` and ``project_id`` is set. ``metric_path`` stops being used. The
caller still holds a :class:`MetricSink` and still calls :meth:`scrape` with
exposition text or :meth:`write` with samples — which is exactly the shape of a
GMP collector, so the substitution is structural, not cosmetic.

Why this module parses exposition text at all
---------------------------------------------
Because a scraper must: that is what a collector *is*. The parser here is the
transport-level one — it turns ``name{labels} value [timestamp]`` into
:class:`MetricSample` records. It is deliberately minimal.

The authoritative Prometheus/GMP **format model and validator** belong to
``ano.telemetry`` (milestone M2); once that exists, inject it::

    sink = LocalMetricSink(path, parser=ano.telemetry.parse_exposition)

:meth:`MetricSink.scrape` takes the parser as a constructor argument precisely
so there is one authoritative implementation and this one is only the default.

Determinism
-----------
Samples are stored in scrape order, and within one scrape in the order the
exposition text listed them. Prometheus itself makes no ordering promise, so
anything that depends on sample order must sort explicitly;
:meth:`LocalMetricSink.samples_sorted` provides a total order
(``metric``, ``labels``, ``timestamp``).
"""

from __future__ import annotations

import abc
import json
import math
import os
import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from ano.config import LOCAL, Config
from ano.contracts.common import dumps_canonical
from ano.gcp.registry import register_backend

__all__ = [
    "MetricSample",
    "MetricSink",
    "LocalMetricSink",
    "parse_exposition",
    "format_exposition_line",
    "build_metric_sink",
]


@dataclass(frozen=True, slots=True)
class MetricSample:
    """One Prometheus/GMP sample.

    Attributes:
        metric: Metric name, e.g. ``node_cpu_seconds_total``.
        labels: Label set. Stored sorted so two equal label sets serialise
            identically.
        value: Sample value.
        timestamp_ms: Sample time in milliseconds since the epoch, or ``None``
            when the exposition text carried no timestamp (in which case the
            collector's scrape time applies).
    """

    metric: str
    labels: Mapping[str, str]
    value: float
    timestamp_ms: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.metric, str) or not self.metric:
            raise ValueError("metric must be a non-empty string")
        if not isinstance(self.labels, Mapping):
            raise TypeError("labels must be a mapping of string to string")
        for key, value in self.labels.items():
            if not isinstance(key, str) or not isinstance(value, str):
                raise TypeError(
                    f"label keys and values must be strings: {key!r}={value!r}"
                )
        if isinstance(self.value, bool) or not isinstance(self.value, (int, float)):
            raise TypeError("value must be a number")
        if self.timestamp_ms is not None and (
            isinstance(self.timestamp_ms, bool)
            or not isinstance(self.timestamp_ms, int)
        ):
            raise TypeError("timestamp_ms must be an integer or None")
        # Freeze the label order so serialisation is canonical.
        object.__setattr__(
            self, "labels", dict(sorted((str(k), str(v)) for k, v in self.labels.items()))
        )
        object.__setattr__(self, "value", float(self.value))

    def to_dict(self) -> dict[str, Any]:
        """Serialise to the sink's storage form."""
        return {
            "metric": self.metric,
            "labels": dict(self.labels),
            "value": self.value,
            "timestamp_ms": self.timestamp_ms,
        }

    @classmethod
    def from_dict(cls, obj: Mapping[str, Any]) -> "MetricSample":
        """Rebuild a sample from its storage form."""
        return cls(
            metric=obj["metric"],
            labels=dict(obj.get("labels") or {}),
            value=float(obj["value"]),
            timestamp_ms=obj.get("timestamp_ms"),
        )

    def sort_key(self) -> tuple[str, str, int]:
        """Total ordering key: metric, canonical labels, timestamp."""
        return (
            self.metric,
            dumps_canonical(dict(self.labels)),
            -1 if self.timestamp_ms is None else self.timestamp_ms,
        )


# Exposition grammar. Metric and label names match Prometheus' own charset.
_NAME = r"[a-zA-Z_:][a-zA-Z0-9_:]*"
_LABEL_NAME = r"[a-zA-Z_][a-zA-Z0-9_]*"
_SAMPLE_RE = re.compile(
    rf"^(?P<metric>{_NAME})"
    r"(?:\{(?P<labels>.*)\})?"
    r"[ \t]+(?P<value>[^ \t]+)"
    r"(?:[ \t]+(?P<timestamp>-?\d+))?[ \t]*$"
)
_LABEL_RE = re.compile(
    rf'(?P<name>{_LABEL_NAME})[ \t]*=[ \t]*"(?P<value>(?:[^"\\]|\\.)*)"'
)
_ESCAPES = {"\\": "\\", '"': '"', "n": "\n"}


def _unescape(text: str) -> str:
    """Undo Prometheus label-value escaping (``\\\\``, ``\\"``, ``\\n``)."""
    out: list[str] = []
    index = 0
    while index < len(text):
        char = text[index]
        if char == "\\" and index + 1 < len(text):
            nxt = text[index + 1]
            out.append(_ESCAPES.get(nxt, "\\" + nxt))
            index += 2
            continue
        out.append(char)
        index += 1
    return "".join(out)


def _escape(text: str) -> str:
    """Apply Prometheus label-value escaping."""
    return text.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _parse_value(text: str, line_number: int) -> float:
    lowered = text.lower()
    if lowered in ("nan", "+nan", "-nan"):
        return math.nan
    if lowered in ("+inf", "inf"):
        return math.inf
    if lowered == "-inf":
        return -math.inf
    try:
        return float(text)
    except ValueError as exc:
        raise ValueError(
            f"line {line_number}: {text!r} is not a valid sample value"
        ) from exc


def _parse_labels(label_text: str, line_number: int) -> dict[str, str]:
    """Parse a ``{a="1",b="2"}`` label block, refusing anything unrecognised.

    The scan is positional: after each ``name="value"`` the next non-space
    character must be a comma (or the end of the block). That proves every
    character was understood, rather than merely that *some* labels matched.
    """
    labels: dict[str, str] = {}
    position = 0
    length = len(label_text)
    while True:
        while position < length and label_text[position] in " \t":
            position += 1
        if position >= length:
            return labels
        match = _LABEL_RE.match(label_text, position)
        if match is None:
            raise ValueError(
                f"line {line_number}: could not parse label set at offset "
                f"{position}: {label_text!r}"
            )
        name = match.group("name")
        if name in labels:
            raise ValueError(
                f"line {line_number}: duplicate label {name!r} in {label_text!r}"
            )
        labels[name] = _unescape(match.group("value"))
        position = match.end()
        while position < length and label_text[position] in " \t":
            position += 1
        if position >= length:
            return labels
        if label_text[position] != ",":
            raise ValueError(
                f"line {line_number}: expected ',' between labels at offset "
                f"{position}: {label_text!r}"
            )
        position += 1


def parse_exposition(text: str) -> list[MetricSample]:
    """Parse Prometheus text-exposition content into samples.

    Handles ``# HELP`` / ``# TYPE`` comments (skipped), label sets with escaped
    values, the special values ``NaN``/``+Inf``/``-Inf``, and the optional
    trailing millisecond timestamp.

    Args:
        text: Exposition body, as a GMP collector would receive it from a
            ``/metrics`` endpoint.

    Returns:
        Samples in the order they appeared.

    Raises:
        ValueError: on a line that is not a comment, not blank and not a valid
            sample. A collector that silently drops malformed lines hides
            generator bugs, so this one refuses.
    """
    samples: list[MetricSample] = []
    for number, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        match = _SAMPLE_RE.match(line)
        if match is None:
            raise ValueError(f"line {number}: not a valid exposition sample: {raw!r}")
        labels: dict[str, str] = {}
        label_text = match.group("labels")
        if label_text is not None and label_text.strip():
            labels = _parse_labels(label_text, number)
        timestamp = match.group("timestamp")
        samples.append(
            MetricSample(
                metric=match.group("metric"),
                labels=labels,
                value=_parse_value(match.group("value"), number),
                timestamp_ms=int(timestamp) if timestamp is not None else None,
            )
        )
    return samples


def format_exposition_line(sample: MetricSample) -> str:
    """Render one sample back to exposition text.

    Useful for round-trip tests and for the demo surface. The authoritative
    serialiser is ``ano.telemetry``'s (milestone M2); this one exists so the
    sink can be tested without depending on a module that does not exist yet.
    """
    if sample.labels:
        label_text = ",".join(
            f'{key}="{_escape(value)}"' for key, value in sample.labels.items()
        )
        head = f"{sample.metric}{{{label_text}}}"
    else:
        head = sample.metric
    value = sample.value
    if math.isnan(value):
        rendered = "NaN"
    elif math.isinf(value):
        rendered = "+Inf" if value > 0 else "-Inf"
    else:
        rendered = repr(value)
    if sample.timestamp_ms is None:
        return f"{head} {rendered}"
    return f"{head} {rendered} {sample.timestamp_ms}"


class MetricSink(abc.ABC):
    """Ingest Prometheus/GMP samples."""

    @abc.abstractmethod
    def write(self, sample: MetricSample) -> None:
        """Ingest one sample."""

    def write_many(self, samples: Iterable[MetricSample]) -> int:
        """Ingest a batch of samples.

        Returns:
            The number ingested.
        """
        ingested = 0
        for sample in samples:
            self.write(sample)
            ingested += 1
        return ingested

    @abc.abstractmethod
    def scrape(self, text: str) -> int:
        """Ingest a block of exposition text, as a collector scrape would.

        Returns:
            The number of samples ingested.
        """

    @abc.abstractmethod
    def count(self) -> int:
        """Number of samples ingested so far."""

    def flush(self) -> None:
        """Flush buffered samples. No-op unless overridden."""

    def close(self) -> None:
        """Flush and release resources."""
        self.flush()

    def __enter__(self) -> "MetricSink":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


class LocalMetricSink(MetricSink):
    """Offline Managed Prometheus substitute backed by a JSONL file.

    Args:
        path: Destination file, or ``None`` for in-memory only.
        parser: Exposition parser. Defaults to :func:`parse_exposition`; inject
            ``ano.telemetry``'s authoritative parser once it exists.
        keep_in_memory: Retain samples for :meth:`samples`.
        truncate: Truncate an existing file on open.
    """

    def __init__(
        self,
        path: str | os.PathLike[str] | None,
        *,
        parser: Callable[[str], Sequence[MetricSample]] | None = None,
        keep_in_memory: bool = True,
        truncate: bool = True,
    ) -> None:
        self._path = os.fspath(path) if path is not None else None
        self._parser = parser or parse_exposition
        self._keep = keep_in_memory or self._path is None
        self._samples: list[MetricSample] = []
        self._count = 0
        self._handle = None
        if self._path is not None:
            parent = os.path.dirname(self._path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            self._handle = open(
                self._path, "w" if truncate else "a", encoding="utf-8"
            )

    @property
    def path(self) -> str | None:
        """The JSONL destination, or ``None``."""
        return self._path

    def write(self, sample: MetricSample) -> None:
        """Ingest one sample."""
        if not isinstance(sample, MetricSample):
            raise TypeError(
                f"expected a MetricSample, got {type(sample).__name__}"
            )
        if self._keep:
            self._samples.append(sample)
        if self._handle is not None:
            # allow_nan is off in canonical JSON, so non-finite values are
            # stored as their exposition spelling rather than invalid JSON.
            payload = sample.to_dict()
            if not math.isfinite(payload["value"]):
                payload["value"] = (
                    "NaN"
                    if math.isnan(payload["value"])
                    else ("+Inf" if payload["value"] > 0 else "-Inf")
                )
            self._handle.write(dumps_canonical(payload) + "\n")
        self._count += 1

    def scrape(self, text: str) -> int:
        """Parse and ingest a block of exposition text."""
        samples = self._parser(text)
        return self.write_many(samples)

    def count(self) -> int:
        """Number of samples ingested."""
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

    def samples(self) -> tuple[MetricSample, ...]:
        """Samples in ingestion order."""
        if not self._keep:
            raise RuntimeError(
                "this sink was created with keep_in_memory=False; read the "
                f"artifact at {self._path!r} instead"
            )
        return tuple(self._samples)

    def samples_sorted(self) -> tuple[MetricSample, ...]:
        """Samples in a total order (metric, labels, timestamp)."""
        return tuple(sorted(self.samples(), key=MetricSample.sort_key))

    @classmethod
    def from_config(cls, config: Config) -> "LocalMetricSink":
        """Build the offline sink from a :class:`ano.config.Config`."""
        return cls(config.resolved_metric_path())


def read_samples(path: str | os.PathLike[str]) -> list[MetricSample]:
    """Read samples back from a JSONL artifact written by this sink."""
    out: list[MetricSample] = []
    with open(path, "r", encoding="utf-8") as handle:
        for number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                payload = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{number}: invalid JSON: {exc}") from exc
            value = payload.get("value")
            if isinstance(value, str):
                payload["value"] = _parse_value(value, number)
            out.append(MetricSample.from_dict(payload))
    return out


def build_metric_sink(config: Config) -> MetricSink:
    """Build the configured metric sink (see :func:`ano.gcp.registry.build`)."""
    from ano.gcp.registry import build

    return build("metrics", config)


register_backend("metrics", LOCAL, LocalMetricSink.from_config)
