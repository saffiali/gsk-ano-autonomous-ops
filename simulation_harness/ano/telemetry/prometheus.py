"""Prometheus text exposition format 0.0.4 — the **writer** side.

Contract **C7** fixes::

    MetricSample(name, labels: dict[str,str], value: float, timestamp: int|None)
    serialize_exposition(samples) -> str

This module is the *producer*. The *checker* lives in
:mod:`ano.telemetry.promparse` and deliberately shares **no code and no
constants** with this file — ``VALIDATION_STRATEGY.md`` §3.1: *"Shared code
between producer and checker is how format bugs survive."* The duplication
below is therefore intentional and must not be "cleaned up".

Traps honoured here (``PROMETHEUS_SPEC.md``):

* Label values escape **exactly three** sequences: ``\\\\``, ``\\n``, ``\\"``.
  There is **no** ``\\t`` and no ``\\r`` escape — emitting one makes the payload
  unparseable. A literal tab inside the quotes is fine (§1.5).
* ``# HELP`` uses the *unquoted* escaper, so a ``"`` in help text is **not**
  escaped (escaping it would produce an invalid sequence outside quotes).
* Values use Go ``%g`` shortest formatting (:mod:`ano.telemetry.gofloat`).
* Sample timestamps are **omitted**; the scraper stamps samples (§1.7).
* Histograms always carry ``le="+Inf"``, and that bucket equals ``_count``
  (§1.9).
* Exporters never emit ``project_id`` / ``location`` / ``cluster`` /
  ``namespace`` / ``job`` / ``instance`` — GMP attaches those at export time
  (§2.2). Emitting them is the classic tell of hand-faked telemetry.
* Line terminator is LF and the document ends with LF (§1.2).
"""

from __future__ import annotations

import math
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from ano.telemetry.errors import ExpositionError
from ano.telemetry.gofloat import format_go_float

__all__ = [
    "TEXT_VERSION",
    "CONTENT_TYPE",
    "METRIC_TYPES",
    "GMP_TARGET_LABELS",
    "MetricSample",
    "MetricFamily",
    "escape_label_value",
    "escape_help",
    "serialize_sample",
    "serialize_exposition",
    "serialize_families",
    "histogram_samples",
    "PrometheusTarget",
    "ScrapedSample",
    "attach_target_labels",
]

#: ``expfmt.TextVersion`` — the format ANO targets, and what GMP scrapes.
TEXT_VERSION = "0.0.4"
CONTENT_TYPE = f"text/plain; version={TEXT_VERSION}; charset=utf-8"

#: The five legal ``# TYPE`` tokens.
METRIC_TYPES: frozenset[str] = frozenset(
    {"counter", "gauge", "histogram", "summary", "untyped"}
)

#: Target labels the GMP collector attaches at export time. An exporter's
#: ``/metrics`` payload must never contain them as metric labels.
GMP_TARGET_LABELS: frozenset[str] = frozenset(
    {"project_id", "location", "cluster", "namespace", "job", "instance"}
)

# Exporter-safe charsets. Metric names may legally contain ``:`` but that is
# reserved for recording rules, so exporters must not use it (§1.3).
_METRIC_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_LABEL_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

#: GMP hard limits (§2.3): system limits, not configurable.
MAX_LABEL_KEY_CHARS = 100
MAX_LABEL_VALUE_CHARS = 1024

_FROZEN: dict[str, Any] = {"frozen": True, "slots": True}


def escape_label_value(value: str) -> str:
    """Escape a label value for the quoted context.

    Exactly three replacements, matching the reference writer's
    ``quotedEscaper``. Backslash **must** be replaced first, otherwise the
    backslashes introduced by the other two replacements would be re-escaped.
    """
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def escape_help(text: str) -> str:
    """Escape ``# HELP`` text, which is *unquoted*.

    Only ``\\`` and newline are escaped. A ``"`` is left literal: HELP is not
    a quoted context, so ``\\"`` there would be an invalid escape sequence.
    """
    return text.replace("\\", "\\\\").replace("\n", "\\n")


@dataclass(**_FROZEN)
class MetricSample:
    """One exposition sample line: ``name{labels} value [timestamp]``.

    Args:
        name: Metric name; exporter charset ``[a-zA-Z_][a-zA-Z0-9_]*``.
        labels: Label name → value. Values are escaped on serialisation.
        value: Sample value. ``float('nan')`` and infinities are legal and
            render as ``NaN`` / ``+Inf`` / ``-Inf``.
        timestamp: Optional integer **milliseconds** since the epoch. ANO
            leaves this ``None``: real exporters do not stamp samples, and a
            historical timestamp would be rejected by a live scrape path.
    """

    name: str
    labels: Mapping[str, str] = field(default_factory=dict)
    value: float = 0.0
    timestamp: int | None = None

    def __post_init__(self) -> None:
        if _METRIC_NAME_RE.match(self.name) is None:
            raise ExpositionError(
                "PROM-NAME",
                f"invalid metric name {self.name!r}: exporters must match "
                "^[a-zA-Z_][a-zA-Z0-9_]*$ (no ':' — that is reserved for "
                "recording rules, and no dashes/dots: the GMP collector does "
                "not scrape UTF-8 names)",
            )
        labels = dict(self.labels)
        for key, value in labels.items():
            if _LABEL_NAME_RE.match(key) is None:
                raise ExpositionError(
                    "PROM-LABEL", f"invalid label name {key!r} on {self.name!r}"
                )
            if key.startswith("__"):
                raise ExpositionError(
                    "PROM-RESERVED",
                    f"label name {key!r} uses the reserved '__' prefix",
                )
            if key in GMP_TARGET_LABELS:
                raise ExpositionError(
                    "PROM-GMP",
                    f"label {key!r} is a GMP target label attached by the "
                    "collector at export time; an exporter must not emit it",
                )
            if not isinstance(value, str):
                raise ExpositionError(
                    "PROM-LABEL",
                    f"label {key!r} value must be a str, got {type(value).__name__}",
                )
            if len(key) > MAX_LABEL_KEY_CHARS:
                raise ExpositionError(
                    "PROM-LIMIT", f"label key {key!r} exceeds {MAX_LABEL_KEY_CHARS} chars"
                )
            if len(value) > MAX_LABEL_VALUE_CHARS:
                raise ExpositionError(
                    "PROM-LIMIT",
                    f"label {key!r} value exceeds {MAX_LABEL_VALUE_CHARS} chars",
                )
        if self.timestamp is not None and not isinstance(self.timestamp, int):
            raise ExpositionError(
                "PROM-TS", "sample timestamp must be integer milliseconds or None"
            )
        object.__setattr__(self, "labels", labels)
        object.__setattr__(self, "value", float(self.value))

    @property
    def series_key(self) -> tuple[str, tuple[tuple[str, str], ...]]:
        """Identity of the time series: name plus the sorted label set."""
        return (self.name, tuple(sorted(self.labels.items())))


def _label_sort_key(item: tuple[str, str]) -> tuple[int, str]:
    """Sort labels alphabetically but keep ``le`` / ``quantile`` last.

    That is what the reference writer produces: it writes the family's own
    labels and then appends the bucket / quantile label.
    """
    name = item[0]
    return (1, name) if name in ("le", "quantile") else (0, name)


def serialize_sample(sample: MetricSample) -> str:
    """Render one sample line **without** its trailing newline."""
    if sample.labels:
        pairs = ",".join(
            f'{key}="{escape_label_value(value)}"'
            for key, value in sorted(sample.labels.items(), key=_label_sort_key)
        )
        head = f"{sample.name}{{{pairs}}}"
    else:
        head = sample.name
    line = f"{head} {format_go_float(sample.value)}"
    if sample.timestamp is not None:
        line += f" {int(sample.timestamp)}"
    return line


@dataclass(**_FROZEN)
class MetricFamily:
    """A ``# HELP`` / ``# TYPE`` block and the samples that belong to it."""

    name: str
    type: str
    help: str
    samples: tuple[MetricSample, ...] = ()

    def __post_init__(self) -> None:
        if _METRIC_NAME_RE.match(self.name) is None:
            raise ExpositionError("PROM-NAME", f"invalid family name {self.name!r}")
        if self.type not in METRIC_TYPES:
            raise ExpositionError(
                "PROM-TYPE",
                f"{self.type!r} is not a metric type; permitted: "
                f"{sorted(METRIC_TYPES)}",
            )
        object.__setattr__(self, "samples", tuple(self.samples))

    def render(self) -> list[str]:
        """Render the family as a list of lines (no trailing newlines)."""
        lines = []
        if self.help:
            lines.append(f"# HELP {self.name} {escape_help(self.help)}")
        lines.append(f"# TYPE {self.name} {self.type}")
        lines.extend(serialize_sample(sample) for sample in self.samples)
        return lines


def serialize_families(families: Iterable[MetricFamily]) -> str:
    """Render metric families as a complete exposition document.

    Returns:
        The document text. LF line endings, terminated by LF — a missing final
        newline is ``unexpected end of input stream`` to the reference parser.
    """
    lines: list[str] = []
    for family in families:
        lines.extend(family.render())
    return "".join(line + "\n" for line in lines)


def serialize_exposition(
    samples: Iterable[MetricSample],
    metadata: Mapping[str, tuple[str, str]] | None = None,
) -> str:
    """Render samples as a complete exposition document (contract C7).

    Args:
        samples: The samples to emit. Families are emitted in first-appearance
            order, and all samples of a family are grouped contiguously, as
            §1.2 rule 7 requires.
        metadata: Optional ``family name -> (type, help)`` map. When supplied,
            each family gets its ``# HELP`` / ``# TYPE`` header and histogram
            / summary suffixes (``_bucket``, ``_sum``, ``_count``) are folded
            into their parent family. When omitted, bare sample lines are
            emitted — still valid 0.0.4, just untyped.

    Returns:
        The exposition document text, LF-terminated.
    """
    samples = list(samples)
    metadata = dict(metadata or {})

    # Map each concrete sample name onto its declaring family.
    family_of: dict[str, str] = {}
    for family_name, (kind, _help) in metadata.items():
        family_of[family_name] = family_name
        if kind == "histogram":
            suffixes = ("_bucket", "_sum", "_count")
        elif kind == "summary":
            suffixes = ("_sum", "_count")
        else:
            suffixes = ()
        for suffix in suffixes:
            family_of[family_name + suffix] = family_name

    order: list[str] = []
    grouped: dict[str, list[MetricSample]] = {}
    for sample in samples:
        key = family_of.get(sample.name, sample.name)
        if key not in grouped:
            grouped[key] = []
            order.append(key)
        grouped[key].append(sample)

    lines: list[str] = []
    for key in order:
        if key in metadata:
            kind, help_text = metadata[key]
            if kind not in METRIC_TYPES:
                raise ExpositionError(
                    "PROM-TYPE", f"{kind!r} is not a metric type for family {key!r}"
                )
            if help_text:
                lines.append(f"# HELP {key} {escape_help(help_text)}")
            lines.append(f"# TYPE {key} {kind}")
        lines.extend(serialize_sample(sample) for sample in grouped[key])
    return "".join(line + "\n" for line in lines)


def histogram_samples(
    name: str,
    labels: Mapping[str, str],
    upper_bounds: Sequence[float],
    cumulative_counts: Sequence[int],
    total_count: int,
    total_sum: float,
) -> list[MetricSample]:
    """Build a conformant histogram family's samples.

    Exists so the generator physically cannot produce the three histogram
    defects the spec calls out: a missing ``le="+Inf"`` bucket, an ``+Inf``
    bucket that disagrees with ``_count``, or non-monotonic bucket counts.

    Args:
        name: Family name, e.g. ``http_request_duration_seconds``.
        labels: Labels shared by every series in the family.
        upper_bounds: Finite bucket upper bounds, strictly ascending.
        cumulative_counts: Cumulative observation counts, one per bound,
            non-decreasing, each ``<= total_count``.
        total_count: Total observations; becomes ``le="+Inf"`` and ``_count``.
        total_sum: Sum of observed values; becomes ``_sum``.

    Returns:
        Bucket samples (including ``+Inf``) followed by ``_sum`` and ``_count``.
    """
    if len(upper_bounds) != len(cumulative_counts):
        raise ExpositionError(
            "PROM-HIST", "upper_bounds and cumulative_counts must be the same length"
        )
    previous = 0
    for bound, count in zip(upper_bounds, cumulative_counts):
        if math.isinf(bound):
            raise ExpositionError(
                "PROM-HIST",
                "do not pass +Inf in upper_bounds; it is appended automatically",
            )
        if count < previous:
            raise ExpositionError(
                "PROM-HIST",
                f"bucket counts must be non-decreasing with ascending le "
                f"(got {count} after {previous})",
            )
        if count > total_count:
            raise ExpositionError(
                "PROM-HIST",
                f"bucket count {count} exceeds total_count {total_count}",
            )
        previous = count
    for earlier, later in zip(upper_bounds, upper_bounds[1:]):
        if later <= earlier:
            raise ExpositionError(
                "PROM-HIST", "upper_bounds must be strictly ascending"
            )

    out: list[MetricSample] = []
    for bound, count in zip(upper_bounds, cumulative_counts):
        out.append(
            MetricSample(
                f"{name}_bucket",
                {**labels, "le": format_go_float(bound)},
                float(count),
            )
        )
    out.append(
        MetricSample(f"{name}_bucket", {**labels, "le": "+Inf"}, float(total_count))
    )
    out.append(MetricSample(f"{name}_sum", dict(labels), float(total_sum)))
    out.append(MetricSample(f"{name}_count", dict(labels), float(total_count)))
    return out


@dataclass(**_FROZEN)
class PrometheusTarget:
    """The six ``prometheus_target`` labels GMP attaches at export time.

    Modelled **alongside** the exposition text, never inside it. This is the
    honest way to represent the GMP path: the collector adds these, so the
    exporter payload must not contain them, but a faithful reference
    implementation still has to say what they would be.
    """

    project_id: str
    location: str
    cluster: str
    namespace: str
    job: str
    instance: str

    def to_dict(self) -> dict[str, str]:
        return {
            "project_id": self.project_id,
            "location": self.location,
            "cluster": self.cluster,
            "namespace": self.namespace,
            "job": self.job,
            "instance": self.instance,
        }

    @classmethod
    def from_dict(cls, obj: Mapping[str, str]) -> "PrometheusTarget":
        """Rebuild a target descriptor from its wire form.

        Strict about the key set: a descriptor missing ``instance`` or carrying
        an extra key is a broken contract, not something to paper over.
        """
        missing = sorted(set(GMP_TARGET_LABELS) - set(obj))
        if missing:
            raise ExpositionError(
                "PROM-TARGET", f"target descriptor is missing {missing}"
            )
        extra = sorted(set(obj) - set(GMP_TARGET_LABELS))
        if extra:
            raise ExpositionError(
                "PROM-TARGET", f"target descriptor has unexpected keys {extra}"
            )
        return cls(
            project_id=obj["project_id"],
            location=obj["location"],
            cluster=obj["cluster"],
            namespace=obj["namespace"],
            job=obj["job"],
            instance=obj["instance"],
        )


@dataclass(**_FROZEN)
class ScrapedSample:
    """One sample **after** the collector has attached the target labels.

    Ruling IR-02. The two-type split mirrors what actually happens in Google
    Managed Prometheus:

    * :class:`MetricSample` is what the exporter exposes on ``/metrics``. It
      may **not** carry ``project_id`` / ``location`` / ``cluster`` /
      ``namespace`` / ``job`` / ``instance`` — the collector owns those, and an
      exporter that emits one would have it relabelled to ``exported_*``.
    * ``ScrapedSample`` is what the collector stores. It legitimately carries
      both sets, and is therefore the first point at which a sample can say
      which entity it came from.

    Keeping these as distinct types means "which host is this sample from?"
    cannot be answered before ingest, which is exactly the real constraint and
    stops anyone quietly inventing an ``entity`` label on the exporter side.

    Attributes:
        sample: The exporter-emitted sample, unchanged.
        target: The six target labels the collector attached.
    """

    sample: MetricSample
    target: PrometheusTarget

    @property
    def name(self) -> str:
        """The metric name, unchanged by the attach."""
        return self.sample.name

    @property
    def value(self) -> float:
        """The sample value, unchanged by the attach."""
        return self.sample.value

    @property
    def entity_id(self) -> str:
        """The C1 entity this sample belongs to.

        IR-02 fixes ``instance`` == ``entity_id``, so resolution is a lookup,
        not a guess. Per-port SNMP series are distinguished *within* an entity
        by the exporter's own ``ifName`` / ``ifIndex`` labels.
        """
        return self.target.instance

    def labels(self) -> dict[str, str]:
        """The full post-attach label set: exporter labels plus target labels.

        No collision is possible because :class:`MetricSample` refuses the six
        target label names at construction time.
        """
        merged = dict(self.sample.labels)
        merged.update(self.target.to_dict())
        return merged

    @property
    def series_key(self) -> tuple[str, tuple[tuple[str, str], ...]]:
        """Post-attach series identity, unique across the whole estate."""
        return (self.sample.name, tuple(sorted(self.labels().items())))


def attach_target_labels(
    sample: MetricSample, target: PrometheusTarget
) -> ScrapedSample:
    """Perform the collector-side attach (IR-02).

    This is the only sanctioned way to turn exporter output into stored
    telemetry. ``ano.ingest`` calls it; the generator never does, because the
    generator is standing in for the exporters.
    """
    return ScrapedSample(sample=sample, target=target)

