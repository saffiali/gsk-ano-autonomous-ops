"""Raw exporter telemetry -> per-entity derived feature series.

What this layer is for
----------------------
The detectors must not reason about ``node_cpu_seconds_total{cpu="3",
mode="iowait"}``. They reason about *features*: bounded, comparable, per-entity
quantities such as "fraction of CPU time spent waiting on I/O" or "requests
served per busy worker thread". This module is the only place that knows the
exporter vocabulary, so a change in the telemetry shape lands here and nowhere
else.

Three jobs, in order:

1. **Entity resolution.** A Prometheus sample says nothing about which host it
   came from — GMP attaches ``instance`` out of band, and contract C7's
   ``MetricSample`` deliberately refuses that label. We therefore resolve the
   owning entity from the first present of a configurable list of label keys,
   and compose ``"<device>:<ifName>"`` for SNMP interface samples so that a
   switch *port* is a first-class entity (feature #55).

2. **Alignment.** Raw series are resampled onto the entity's own observation
   timeline by last-observation-carried-forward with a staleness limit, which
   is exactly Prometheus' own lookback semantics. Counters are differenced into
   rates with counter-reset detection, because a restart must not read as a
   negative rate spike.

3. **Derivation.** A declarative catalogue turns aligned series into the named
   features the signal layer consumes.

The divergence features
-----------------------
Requirement R2 singles out "CPU saturation *without corresponding throughput*".
That is not a threshold on CPU; it is a statement about the *ratio* of useful
work to resource consumption. So the catalogue carries explicit efficiency
features — :data:`WORK_PER_CPU`, :data:`WORK_PER_BUSY_THREAD`,
:data:`DB_WORK_PER_SESSION` — and marks them as *downward*-directional. A
traffic surge raises work and utilisation together and leaves efficiency flat;
thread starvation pins the workers while throughput falls away and efficiency
collapses. Baselining the ratio rather than either term is what lets the system
tell those two apart without being told which is which.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Callable, Protocol

from ano.detect.robust import safe_ratio

__all__ = [
    "DEFAULT_ENTITY_LABEL_KEYS",
    "GMP_TARGET_LABELS",
    "DEFAULT_STALENESS_FACTOR",
    "PHASE_HISTORY",
    "PHASE_WINDOW",
    "Observation",
    "observations_from_scrape",
    "observations_from_samples",
    "group_observations",
    "build_entity_frame",
    "SampleLike",
    "FeatureError",
    "Direction",
    "Ceiling",
    "FeatureSpec",
    "AlignedSeries",
    "EntityFrame",
    "FeatureFrame",
    "FEATURES",
    "feature_spec",
    "features_for_kind",
    "build_frames",
    "resolve_entity",
    "WORK_PER_CPU",
    "WORK_PER_BUSY_THREAD",
    "DB_WORK_PER_SESSION",
]

#: The label carrying the entity id, per integration ruling IR-02.
#:
#: This is the Google Managed Prometheus model reproduced faithfully: an
#: exporter's ``/metrics`` body carries no target labels at all, and the
#: collector attaches ``project_id``, ``location``, ``cluster``, ``namespace``,
#: ``job`` and ``instance`` at scrape time. ``instance`` is defined to equal the
#: contract-C1 ``entity_id``, so post-attach samples identify their entity
#: exactly and no guess-the-key fallback chain is needed or wanted.
#:
#: Kept as a tuple, and configurable, purely so the ingest layer can hand us an
#: alternative attach convention without a code change here.
DEFAULT_ENTITY_LABEL_KEYS: tuple[str, ...] = ("instance",)

#: A carried-forward observation goes stale after this many scrape intervals.
#: Prometheus' own default lookback delta is 5 minutes, which at a 30 s scrape
#: interval is ten intervals; four is a tighter, more conservative choice for a
#: replay corpus where a genuine gap means the exporter stopped answering.
DEFAULT_STALENESS_FACTOR = 4.0

#: Feature names. Defined as constants because the signal catalogue, the
#: attribution layer and the tests all refer to them, and a typo in a string
#: literal would silently disable a signal family.
WORK_PER_CPU = "app.work_per_cpu_second"
WORK_PER_BUSY_THREAD = "app.work_per_busy_thread"
DB_WORK_PER_SESSION = "db.work_per_running_session"


class FeatureError(ValueError):
    """The telemetry cannot be turned into feature series."""


class SampleLike(Protocol):
    """Structural type of a telemetry sample (contract C7 ``MetricSample``).

    Declared structurally so this module works against ``ano.telemetry`` and
    against test fixtures without either importing the other.
    """

    name: str
    labels: Mapping[str, str]
    value: float
    timestamp: int | None


Direction = str
"""``"up"`` if a rise is pathological, ``"down"`` if a fall is."""

Ceiling = str
"""How a feature's failure ceiling is obtained.

``"unit"``
    The feature is a fraction of a hard limit; the ceiling is 1.0. Extrapolating
    to it yields a physically meaningful time-to-exhaustion.
``"learned"``
    The ceiling is derived from the entity's own learned distribution. Used for
    unbounded signals (lock waits, latency) where "how bad can it get" is a
    property of the entity, not a universal constant.
``"none"``
    No meaningful ceiling; the feature contributes anomaly evidence only.
"""


@dataclass(frozen=True, slots=True)
class FeatureSpec:
    """One derived feature.

    Attributes:
        name: Canonical feature name, e.g. ``"host.cpu_iowait_ratio"``.
        title: Human-readable name for the operator-facing signal record.
        unit: Unit of the feature's values.
        direction: ``"up"`` or ``"down"`` — which way is pathological.
        ceiling: Ceiling policy, see :data:`Ceiling`.
        kinds: Entity kinds the feature is meaningful for. Empty means any.
        compute: ``frame -> sequence of value-or-None``, one per frame time.
        description: Why an operator should care. Surfaced in evidence.
        absolute_floor: Minimum scale for the robust z-score, in the feature's
            own units. Stops a dead-flat counter from producing infinite
            z-scores the first time it moves.
        relative_floor: Minimum scale as a fraction of the learned centre.
    """

    name: str
    title: str
    unit: str
    direction: Direction
    ceiling: Ceiling
    kinds: frozenset[str]
    compute: Callable[["EntityFrame"], Sequence[float | None]]
    description: str
    absolute_floor: float = 0.0
    relative_floor: float = 0.05

    def __post_init__(self) -> None:
        if self.direction not in ("up", "down"):
            raise ValueError(f"{self.name}: direction must be 'up' or 'down'")
        if self.ceiling not in ("unit", "learned", "none"):
            raise ValueError(f"{self.name}: unknown ceiling policy {self.ceiling!r}")

    def applies_to(self, kind: str) -> bool:
        """Whether this feature is meaningful for an entity of ``kind``."""
        return not self.kinds or kind in self.kinds


# --------------------------------------------------------------------------
# Entity resolution
# --------------------------------------------------------------------------


def resolve_entity(
    labels: Mapping[str, str],
    *,
    entity_label_keys: Sequence[str] = DEFAULT_ENTITY_LABEL_KEYS,
) -> str | None:
    """Work out which entity a sample belongs to from its post-attach labels.

    Per ruling IR-02 the collector-attached ``instance`` label equals the
    contract-C1 ``entity_id``, so the base case is a direct read.

    SNMP interface families are the one composite case. Their ``instance`` is
    the *device*, and the exporter's own ``ifName``/``ifDescr`` label separates
    the per-port series. Requirement R3 demands attribution name "this port",
    not merely the switch it sits in (feature #55), so a port becomes a
    first-class entity with the composite id ``"<device>:<ifName>"`` — which is
    the same id the topology uses. A sample whose ``instance`` already contains
    a colon is taken to name the port directly and is passed through.

    Args:
        labels: The sample's label set, after target-label attach.
        entity_label_keys: Keys to read the target identity from, in order.

    Returns:
        The entity id, or ``None`` when no label identifies one.
    """
    explicit: str | None = None
    for key in entity_label_keys:
        value = labels.get(key)
        if value:
            explicit = value
            break

    if_name = labels.get("ifName") or labels.get("ifDescr")
    if if_name:
        if explicit and ":" not in explicit:
            return f"{explicit}:{if_name}"
        if explicit:
            return explicit
        return if_name
    return explicit


#: The six labels Google Managed Prometheus attaches at scrape time. They
#: identify the *target*, not the series, so they are excluded from a series'
#: identity — otherwise re-pointing a scrape job would look like a new series.
GMP_TARGET_LABELS: frozenset[str] = frozenset(
    {"project_id", "location", "cluster", "namespace", "job", "instance"}
)


def _canonical_labels(
    labels: Mapping[str, str], entity_label_keys: Sequence[str]
) -> tuple[tuple[str, str], ...]:
    """Label set minus the target labels, which identify the scrape not the series."""
    dropped = GMP_TARGET_LABELS | set(entity_label_keys)
    return tuple(
        sorted((k, v) for k, v in labels.items() if k not in dropped)
    )


#: The generator's 3-month unlabelled training prefix.
PHASE_HISTORY = "history"

#: The evaluation period the detector is scored over.
PHASE_WINDOW = "window"


@dataclass(frozen=True, slots=True)
class Observation:
    """One telemetry sample, normalised for the detection stack.

    The single input type for everything above this module. Whether a sample
    arrived through :mod:`ano.ingest` as a contract-C7 ``ScrapedSample`` or
    through the pipeline's fallback reader, it becomes one of these — so no
    downstream layer has to know which.

    Attributes:
        entity: The contract-C1 entity id this sample belongs to.
        metric: Metric family name.
        labels: Exporter labels only. The six collector-attached target labels
            are stripped: they identify the scrape, not the series.
        value: The sample value.
        moment: Observation time in epoch seconds.
        phase: ``"history"`` for the unlabelled training prefix the baselines
            learn from, ``"window"`` for the evaluation period. Carried on the
            observation rather than inferred from a cut-off time so a corpus
            can declare it directly and the split is never guessed.
    """

    entity: str
    metric: str
    labels: Mapping[str, str]
    value: float
    moment: float
    phase: str = PHASE_WINDOW

    def is_history(self) -> bool:
        """Whether this observation belongs to the training prefix."""
        return self.phase == PHASE_HISTORY


def observations_from_scrape(
    scraped: Iterable[Any],
    *,
    moment: float,
    phase: str = PHASE_WINDOW,
    entity_label_keys: Sequence[str] = DEFAULT_ENTITY_LABEL_KEYS,
) -> list[Observation]:
    """Normalise contract-C7 ``ScrapedSample`` objects from a single scrape.

    A scrape document carries one ``scrape_time`` for its whole body, and the
    exposition inside it deliberately carries no per-sample timestamps — which
    is what a real ``/metrics`` endpoint looks like. The scrape time is
    therefore supplied by the caller rather than read off the samples.

    Args:
        scraped: ``ScrapedSample``-shaped objects exposing ``entity_id``,
            ``name``, ``value`` and either a ``labels()`` method or a ``labels``
            mapping.
        moment: The scrape time, in epoch seconds.
        phase: ``"history"`` or ``"window"``.
        entity_label_keys: Fallback keys if an object does not expose
            ``entity_id`` directly.

    Returns:
        Normalised observations. Samples that cannot be attributed to an entity
        are dropped rather than guessed at.
    """
    out: list[Observation] = []
    for item in scraped:
        raw_labels = item.labels
        labels: Mapping[str, str] = raw_labels() if callable(raw_labels) else raw_labels
        entity = getattr(item, "entity_id", None)
        if not entity:
            entity = resolve_entity(labels, entity_label_keys=entity_label_keys)
        if not entity:
            continue
        out.append(
            Observation(
                entity=entity,
                metric=item.name,
                labels=dict(_canonical_labels(labels, entity_label_keys)),
                value=float(item.value),
                moment=moment,
                phase=phase,
            )
        )
    return out


def observations_from_samples(
    samples: Iterable[SampleLike],
    *,
    entity: str | None = None,
    moment: float | None = None,
    phase: str = PHASE_WINDOW,
    entity_label_keys: Sequence[str] = DEFAULT_ENTITY_LABEL_KEYS,
) -> list[Observation]:
    """Normalise exporter-side ``MetricSample`` objects.

    Args:
        samples: Samples exposing ``name``, ``labels``, ``value`` and
            ``timestamp``.
        entity: Entity every sample belongs to. Supply this when the samples
            come from one scrape target, which is the normal case — the
            exporter body cannot name its own host.
        moment: Observation time in epoch seconds. Falls back to each sample's
            own millisecond ``timestamp`` when omitted.
        phase: ``"history"`` or ``"window"``.
        entity_label_keys: Keys to resolve the entity from when ``entity`` is
            not supplied.

    Returns:
        Normalised observations, skipping any sample with no resolvable entity
        or no resolvable time.

    Raises:
        FeatureError: If every sample was skipped for want of a timestamp,
            which means the corpus cannot be turned into a time series at all.
    """
    out: list[Observation] = []
    seen = 0
    undated = 0
    for item in samples:
        seen += 1
        owner = entity or resolve_entity(item.labels, entity_label_keys=entity_label_keys)
        if not owner:
            continue
        if moment is not None:
            when = moment
        elif item.timestamp is not None:
            # Prometheus exposition timestamps are integer milliseconds.
            when = float(item.timestamp) / 1000.0
        else:
            undated += 1
            continue
        out.append(
            Observation(
                entity=owner,
                metric=item.name,
                labels=dict(_canonical_labels(item.labels, entity_label_keys)),
                value=float(item.value),
                moment=when,
                phase=phase,
            )
        )
    if seen and undated == seen:
        raise FeatureError(
            "no telemetry sample carried a timestamp and no scrape time was "
            "supplied; a replay corpus must be timestamped for the detector to "
            "reconstruct a time series"
        )
    return out



# --------------------------------------------------------------------------
# Alignment
# --------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class AlignedSeries:
    """One raw metric series resampled onto its entity's timeline.

    Attributes:
        metric: Metric family name.
        labels: The series' distinguishing labels (entity labels removed).
        values: Level at each frame time, ``None`` where stale or unobserved.
        rates: Per-second first difference, ``None`` at the first point, on a
            counter reset, or where the level is unknown. ``None`` throughout
            for gauges.
        is_counter: Whether the source metric is a counter.
    """

    metric: str
    labels: Mapping[str, str]
    values: tuple[float | None, ...]
    rates: tuple[float | None, ...]
    is_counter: bool

    def matches(self, **selector: str) -> bool:
        """Whether every ``key=value`` in ``selector`` is present on this series."""
        return all(self.labels.get(key) == value for key, value in selector.items())


def _counter_metric_types() -> dict[str, str]:
    """Metric name -> Prometheus type, from the verified exporter catalogue.

    Falls back to the ``_total`` suffix convention for names the catalogue does
    not know, so an unfamiliar metric is still differenced correctly.
    """
    try:
        from ano.telemetry import exporters  # local import: optional dependency
    except ImportError:  # pragma: no cover - only if M2 is absent
        return {}
    return {family.name: family.type for family in exporters.ALL_FAMILIES}


_METRIC_TYPES: dict[str, str] = _counter_metric_types()


def _is_counter(metric: str) -> bool:
    """Whether ``metric`` accumulates and must be differenced."""
    declared = _METRIC_TYPES.get(metric)
    if declared is not None:
        return declared == "counter"
    base = metric
    for suffix in ("_sum", "_count", "_bucket"):
        if base.endswith(suffix):
            base = base[: -len(suffix)]
            if base in _METRIC_TYPES:
                return _METRIC_TYPES[base] in ("counter", "summary", "histogram")
            return suffix in ("_sum", "_count")
    return base.endswith("_total")


@dataclass(frozen=True, slots=True)
class EntityFrame:
    """Every aligned series for one entity, on one shared timeline.

    Attributes:
        entity: The entity id.
        kind: Entity kind if known (``app_server``/``database``/``switch``/
            ``node``), else ``"unknown"``. Supplied by the topology, never by
            a ground-truth label file.
        times: Observation times in epoch seconds, strictly increasing.
        series: The aligned series.
        step_s: Median spacing of ``times``, the effective scrape interval.
    """

    entity: str
    kind: str
    times: tuple[float, ...]
    series: tuple[AlignedSeries, ...]
    step_s: float
    _by_metric: dict[str, tuple[AlignedSeries, ...]] = field(
        default_factory=dict, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        index: dict[str, list[AlignedSeries]] = {}
        for item in self.series:
            index.setdefault(item.metric, []).append(item)
        object.__setattr__(
            self, "_by_metric", {k: tuple(v) for k, v in index.items()}
        )

    def __len__(self) -> int:
        return len(self.times)

    def select(self, metric: str, **selector: str) -> tuple[AlignedSeries, ...]:
        """All series for ``metric`` whose labels satisfy ``selector``."""
        return tuple(
            item for item in self._by_metric.get(metric, ()) if item.matches(**selector)
        )

    def has(self, metric: str, **selector: str) -> bool:
        """Whether any series matches."""
        return bool(self.select(metric, **selector))

    def _combine(
        self, chosen: Sequence[AlignedSeries], attribute: str
    ) -> list[float | None]:
        """Sum an attribute across series, propagating unknown as ``None``.

        A partially observed sum is not a sum. If any contributing series is
        unknown at time ``i`` the result is unknown, rather than silently
        understated — which would look exactly like a drop in load and could
        manufacture a phantom efficiency collapse.
        """
        if not chosen:
            return [None] * len(self.times)
        out: list[float | None] = []
        for i in range(len(self.times)):
            total = 0.0
            known = False
            for item in chosen:
                value = getattr(item, attribute)[i]
                if value is None:
                    known = False
                    break
                total += value
                known = True
            out.append(total if known else None)
        return out

    def sum_value(self, metric: str, **selector: str) -> list[float | None]:
        """Sum of levels across matching series, per frame time."""
        return self._combine(self.select(metric, **selector), "values")

    def sum_rate(self, metric: str, **selector: str) -> list[float | None]:
        """Sum of per-second rates across matching series, per frame time."""
        return self._combine(self.select(metric, **selector), "rates")

    def value(self, metric: str, **selector: str) -> list[float | None]:
        """Level of the single matching series, or all-``None`` if absent."""
        chosen = self.select(metric, **selector)
        if not chosen:
            return [None] * len(self.times)
        return list(chosen[0].values)

    def rate(self, metric: str, **selector: str) -> list[float | None]:
        """Rate of the single matching series, or all-``None`` if absent."""
        chosen = self.select(metric, **selector)
        if not chosen:
            return [None] * len(self.times)
        return list(chosen[0].rates)

    def label_values(self, metric: str, key: str) -> tuple[str, ...]:
        """Distinct values of label ``key`` across ``metric``'s series."""
        return tuple(
            sorted({item.labels[key] for item in self._by_metric.get(metric, ()) if key in item.labels})
        )


def _align(
    times: Sequence[float],
    points: Sequence[tuple[float, float]],
    staleness_s: float,
    is_counter: bool,
) -> tuple[tuple[float | None, ...], tuple[float | None, ...]]:
    """Resample one raw series onto ``times``.

    Last observation carried forward, expiring after ``staleness_s``. Counters
    additionally produce a per-second rate; a decrease is treated as a counter
    reset (a process restart) and yields ``None`` for that step rather than a
    large negative rate.

    Args:
        times: The entity timeline, strictly increasing.
        points: ``(time, value)`` observations, sorted by time.
        staleness_s: How long a carried-forward observation stays valid.
        is_counter: Whether to difference the series.

    Returns:
        ``(levels, rates)``, both the length of ``times``.
    """
    levels: list[float | None] = []
    cursor = 0
    last_time: float | None = None
    last_value: float | None = None
    for moment in times:
        while cursor < len(points) and points[cursor][0] <= moment:
            last_time, last_value = points[cursor]
            cursor += 1
        if last_value is None or last_time is None or moment - last_time > staleness_s:
            levels.append(None)
        else:
            levels.append(last_value)

    if not is_counter:
        return tuple(levels), tuple([None] * len(times))

    rates: list[float | None] = [None]
    for i in range(1, len(times)):
        current = levels[i]
        previous = levels[i - 1]
        delta_t = times[i] - times[i - 1]
        if current is None or previous is None or delta_t <= 0.0:
            rates.append(None)
        elif current < previous:
            rates.append(None)  # counter reset: the interval is uninterpretable
        else:
            rates.append((current - previous) / delta_t)
    return tuple(levels), tuple(rates)


@dataclass(frozen=True, slots=True)
class FeatureFrame:
    """Derived feature series for the whole estate.

    Attributes:
        frames: Entity id -> its aligned raw frame.
        values: ``(entity, feature)`` -> per-time values, ``None`` where the
            feature could not be computed.
        times: Entity id -> its timeline, in epoch seconds.
        step_s: Entity id -> its effective scrape interval.
    """

    frames: Mapping[str, EntityFrame]
    values: Mapping[tuple[str, str], tuple[float | None, ...]]
    times: Mapping[str, tuple[float, ...]]
    step_s: Mapping[str, float]

    def entities(self) -> tuple[str, ...]:
        """Entity ids, sorted, for deterministic iteration."""
        return tuple(sorted(self.frames))

    def feature_names(self, entity: str) -> tuple[str, ...]:
        """Features actually computed for ``entity``, sorted."""
        return tuple(
            sorted(name for (owner, name) in self.values if owner == entity)
        )

    def series(self, entity: str, feature: str) -> tuple[float | None, ...]:
        """Values of ``feature`` on ``entity``; all-``None`` if not computed."""
        found = self.values.get((entity, feature))
        if found is not None:
            return found
        return tuple([None] * len(self.times.get(entity, ())))

    def at(self, entity: str, feature: str, index: int) -> float | None:
        """Single value, or ``None`` if out of range or uncomputed."""
        data = self.values.get((entity, feature))
        if data is None or not -len(data) <= index < len(data):
            return None
        return data[index]

    def kind(self, entity: str) -> str:
        """Entity kind, or ``"unknown"``."""
        frame = self.frames.get(entity)
        return frame.kind if frame is not None else "unknown"

    def window(self, entity: str) -> tuple[float, float] | None:
        """``(first, last)`` observation time for ``entity``, if any."""
        stamps = self.times.get(entity, ())
        if not stamps:
            return None
        return stamps[0], stamps[-1]


# --------------------------------------------------------------------------
# Feature computations
# --------------------------------------------------------------------------


def _elementwise(
    left: Sequence[float | None],
    right: Sequence[float | None],
    operation: Callable[[float, float], float],
) -> list[float | None]:
    """Apply ``operation`` pairwise, propagating ``None``."""
    out: list[float | None] = []
    for a, b in zip(left, right):
        if a is None or b is None:
            out.append(None)
        else:
            try:
                out.append(operation(a, b))
            except (ZeroDivisionError, ValueError, OverflowError):
                out.append(None)
    return out


def _ratio(
    numerator: Sequence[float | None], denominator: Sequence[float | None]
) -> list[float | None]:
    """Elementwise ratio with a floored denominator."""
    return _elementwise(numerator, denominator, lambda a, b: safe_ratio(a, b))


def _fraction_of_total(
    part: Sequence[float | None], total: Sequence[float | None]
) -> list[float | None]:
    """Elementwise ``part / total``, unknown where the total is not positive.

    Distinct from :func:`_ratio`: a zero total here means "no denominator", not
    "an enormous ratio", so the answer is unknown rather than floored.
    """
    out: list[float | None] = []
    for a, b in zip(part, total):
        if a is None or b is None or b <= 0.0:
            out.append(None)
        else:
            out.append(a / b)
    return out


def _sum_series(*parts: Sequence[float | None]) -> list[float | None]:
    """Elementwise sum that tolerates a missing part.

    Unlike :meth:`EntityFrame._combine` this is used to add *alternative*
    sources of the same quantity (Apache accesses and Tomcat requests both
    count as served work), so a missing term contributes zero and the result is
    unknown only when every term is missing.
    """
    length = max((len(p) for p in parts), default=0)
    out: list[float | None] = []
    for i in range(length):
        total = 0.0
        known = False
        for part in parts:
            if i < len(part) and part[i] is not None:
                total += float(part[i])  # type: ignore[arg-type]
                known = True
        out.append(total if known else None)
    return out


def _cpu_mode_fraction(frame: EntityFrame, mode: str) -> list[float | None]:
    """Fraction of wall-clock CPU time spent in ``mode``, averaged over cores.

    ``node_cpu_seconds_total`` is per-core seconds-in-mode. Dividing the summed
    rate by the core count gives a 0..1 fraction that is comparable across
    hosts of different sizes — which matters because the baseline is per-entity
    but the *severity mapping* is shared.
    """
    cores = len(frame.label_values("node_cpu_seconds_total", "cpu"))
    if cores == 0:
        return [None] * len(frame)
    mode_rate = frame.sum_rate("node_cpu_seconds_total", mode=mode)
    return [None if v is None else v / cores for v in mode_rate]


def _cpu_busy_ratio(frame: EntityFrame) -> list[float | None]:
    """Non-idle CPU fraction, averaged over cores."""
    idle = _cpu_mode_fraction(frame, "idle")
    return [None if v is None else max(0.0, min(1.0, 1.0 - v)) for v in idle]


def _cpu_count(frame: EntityFrame) -> int:
    """Number of cores the node exporter reports."""
    return len(frame.label_values("node_cpu_seconds_total", "cpu"))


def _runq_per_cpu(frame: EntityFrame) -> list[float | None]:
    """1-minute load average per core — the classic run-queue pressure index."""
    cores = _cpu_count(frame)
    if cores == 0:
        return [None] * len(frame)
    return [None if v is None else v / cores for v in frame.value("node_load1")]


def _memory_used_ratio(frame: EntityFrame) -> list[float | None]:
    """Fraction of RAM unavailable to new allocations."""
    total = frame.value("node_memory_MemTotal_bytes")
    available = frame.value("node_memory_MemAvailable_bytes")
    used = _fraction_of_total(available, total)
    return [None if v is None else max(0.0, min(1.0, 1.0 - v)) for v in used]


def _disk_latency_seconds(frame: EntityFrame) -> list[float | None]:
    """Mean I/O service time: weighted busy-seconds per completed operation.

    ``node_disk_io_time_weighted_seconds_total`` accumulates queue-depth-times-
    time, so dividing its rate by the completion rate yields the average time an
    operation spent in flight. This is the quantity an operator calls "disk
    latency", and it rises before throughput collapses.
    """
    weighted = frame.sum_rate("node_disk_io_time_weighted_seconds_total")
    reads = frame.sum_rate("node_disk_reads_completed_total")
    writes = frame.sum_rate("node_disk_writes_completed_total")
    operations = _sum_series(reads, writes)
    return _fraction_of_total(weighted, operations)


def _disk_busy_ratio(frame: EntityFrame) -> list[float | None]:
    """Fraction of wall-clock time the busiest device spent doing I/O."""
    devices = frame.select("node_disk_io_time_seconds_total")
    if not devices:
        return [None] * len(frame)
    out: list[float | None] = []
    for i in range(len(frame)):
        best: float | None = None
        for device in devices:
            value = device.rates[i]
            if value is not None and (best is None or value > best):
                best = value
        out.append(None if best is None else min(1.0, max(0.0, best)))
    return out


def _disk_ops_rate(frame: EntityFrame) -> list[float | None]:
    """Completed disk operations per second — a host-level 'useful work' proxy."""
    return _sum_series(
        frame.sum_rate("node_disk_reads_completed_total"),
        frame.sum_rate("node_disk_writes_completed_total"),
    )


def _fd_utilisation(frame: EntityFrame) -> list[float | None]:
    """Allocated file descriptors as a fraction of the kernel maximum.

    Sockets consume descriptors, so this is the shared ceiling that both
    file-descriptor and socket exhaustion run into — the R2 signal family that
    has a genuine hard limit and therefore a genuine time-to-exhaustion.
    """
    return _fraction_of_total(
        frame.value("node_filefd_allocated"), frame.value("node_filefd_maximum")
    )


def _socket_pressure(frame: EntityFrame) -> list[float | None]:
    """TCP sockets in use as a fraction of allocated descriptors.

    Rising toward 1 means the descriptor table is being consumed by sockets
    specifically, which distinguishes socket exhaustion from a file-handle leak
    and changes the remediation.
    """
    return _fraction_of_total(
        frame.value("node_sockstat_TCP_inuse"), frame.value("node_filefd_allocated")
    )


def _orphan_ratio(frame: EntityFrame) -> list[float | None]:
    """Orphaned TCP sockets relative to sockets in use.

    Orphans accumulate when an application stops reading from connections it
    has closed — an early and specific precursor of socket exhaustion.
    """
    return _fraction_of_total(
        frame.value("node_sockstat_TCP_orphan"), frame.value("node_sockstat_TCP_inuse")
    )


def _timewait_ratio(frame: EntityFrame) -> list[float | None]:
    """TIME_WAIT sockets relative to sockets in use."""
    return _fraction_of_total(
        frame.value("node_sockstat_TCP_tw"), frame.value("node_sockstat_TCP_inuse")
    )


def _request_rate(frame: EntityFrame) -> list[float | None]:
    """Served requests per second, from whichever web tier is instrumented."""
    return _sum_series(
        frame.sum_rate("tomcat_requestcount_total"),
        frame.sum_rate("apache_accesses_total"),
    )


def _error_ratio(frame: EntityFrame) -> list[float | None]:
    """Fraction of requests that errored."""
    return _fraction_of_total(
        frame.sum_rate("tomcat_errorcount_total"), frame.sum_rate("tomcat_requestcount_total")
    )


def _mean_latency_seconds(frame: EntityFrame) -> list[float | None]:
    """Mean request service time over the interval.

    ``tomcat_processingtime_total`` accumulates milliseconds of processing, so
    its increment divided by the request increment is the interval's mean
    latency. Derived from counters rather than a gauge so it is an honest
    interval average and not a sampled instant.
    """
    processing_ms = frame.sum_rate("tomcat_processingtime_total")
    requests = frame.sum_rate("tomcat_requestcount_total")
    per_request_ms = _fraction_of_total(processing_ms, requests)
    return [None if v is None else v / 1000.0 for v in per_request_ms]


def _thread_busy_ratio(frame: EntityFrame) -> list[float | None]:
    """Busy Tomcat worker threads as a fraction of the live pool.

    The R2 thread-starvation family. A pool pinned at 1.0 cannot accept work,
    which is what "unresponsive" means from a user's point of view.
    """
    return _fraction_of_total(
        frame.sum_value("tomcat_threadpool_currentthreadsbusy"),
        frame.sum_value("tomcat_threadpool_currentthreadcount"),
    )


def _threads_busy(frame: EntityFrame) -> list[float | None]:
    """Absolute count of busy worker threads."""
    return frame.sum_value("tomcat_threadpool_currentthreadsbusy")


def _apache_busy_ratio(frame: EntityFrame) -> list[float | None]:
    """Busy Apache workers as a fraction of the configured worker set."""
    busy = frame.value("apache_workers", state="busy")
    idle = frame.value("apache_workers", state="idle")
    return _fraction_of_total(busy, _sum_series(busy, idle))


def _scoreboard_state_ratio(frame: EntityFrame, state: str) -> list[float | None]:
    """Fraction of Apache scoreboard slots in ``state``."""
    slots = frame.select("apache_scoreboard")
    if not slots:
        return [None] * len(frame)
    chosen = frame.value("apache_scoreboard", state=state)
    totals: list[float | None] = []
    for i in range(len(frame)):
        total = 0.0
        known = False
        for item in slots:
            value = item.values[i]
            if value is not None:
                total += value
                known = True
        totals.append(total if known else None)
    return _fraction_of_total(chosen, totals)


def _jvm_heap_ratio(frame: EntityFrame) -> list[float | None]:
    """JVM heap in use as a fraction of the configured maximum."""
    return _fraction_of_total(
        frame.value("jvm_memory_bytes_used", area="heap"),
        frame.value("jvm_memory_bytes_max", area="heap"),
    )


def _jvm_gc_fraction(frame: EntityFrame) -> list[float | None]:
    """Fraction of wall-clock time spent in garbage collection.

    Rising toward 1 is the signature of a memory-leak slow death: the heap is
    full, every collection reclaims almost nothing, and the application stops
    making progress while consuming all available CPU — high utilisation, no
    throughput.
    """
    return frame.sum_rate("jvm_gc_collection_seconds_sum")


def _jvm_state_ratio(frame: EntityFrame, state: str) -> list[float | None]:
    """Fraction of JVM threads in ``state``."""
    states = frame.select("jvm_threads_state")
    if not states:
        return [None] * len(frame)
    chosen = frame.value("jvm_threads_state", state=state)
    totals: list[float | None] = []
    for i in range(len(frame)):
        total = 0.0
        known = False
        for item in states:
            value = item.values[i]
            if value is not None:
                total += value
                known = True
        totals.append(total if known else None)
    return _fraction_of_total(chosen, totals)


def _work_per_cpu_second(frame: EntityFrame) -> list[float | None]:
    """Requests served per CPU-second consumed. **The R2 divergence feature.**

    Requirement R2 asks for "CPU saturation without corresponding throughput".
    Utilisation alone cannot express that: a busy host serving a surge and a
    hung host spinning on lock contention both read 95%. The ratio separates
    them. A surge leaves this flat — twice the work for twice the CPU. A hang
    drives it toward zero, because the CPU keeps burning while the work stops.

    Scored *downwards* against a per-entity, per-hour, per-day-of-week
    baseline, so "normal efficiency" is whatever this host normally achieves at
    this time of week rather than a number someone chose.
    """
    return _fraction_of_total(_request_rate(frame), _cpu_busy_ratio(frame))


def _work_per_busy_thread(frame: EntityFrame) -> list[float | None]:
    """Requests served per busy worker thread — the thread-starvation analogue.

    Threads held by a blocked downstream dependency are busy but idle-in-fact.
    This falls long before the pool is fully pinned, which is where the 15-30
    minute lead time comes from.
    """
    return _fraction_of_total(_request_rate(frame), _threads_busy(frame))


def _host_work_per_cpu_second(frame: EntityFrame) -> list[float | None]:
    """Disk and network operations per CPU-second, for hosts with no web tier.

    Feature #37 requires *server* unresponsiveness to be a covered target, not
    only application servers, so the divergence idea needs a work proxy that
    exists on a bare node.
    """
    work = _sum_series(
        _disk_ops_rate(frame),
        frame.sum_rate("node_network_receive_bytes_total"),
        frame.sum_rate("node_network_transmit_bytes_total"),
    )
    return _fraction_of_total(work, _cpu_busy_ratio(frame))


def _db_session_pool_ratio(frame: EntityFrame) -> list[float | None]:
    """Connected sessions as a fraction of ``max_connections``."""
    return _fraction_of_total(
        frame.value("mysql_global_status_threads_connected"),
        frame.value("mysql_global_variables_max_connections"),
    )


def _db_table_lock_contention(frame: EntityFrame) -> list[float | None]:
    """Fraction of table-lock acquisitions that had to wait."""
    waited = frame.rate("mysql_global_status_table_locks_waited")
    immediate = frame.rate("mysql_global_status_table_locks_immediate")
    return _fraction_of_total(waited, _sum_series(waited, immediate))


def _db_work_per_running_session(frame: EntityFrame) -> list[float | None]:
    """Queries completed per running session — the database efficiency index.

    Lock contention shows up here first and unambiguously: sessions pile up
    while completed work per session collapses. A genuine workload increase
    raises both terms and leaves the ratio alone, which is what keeps a benign
    traffic surge from being attributed to the database.
    """
    return _fraction_of_total(
        frame.rate("mysql_global_status_questions"),
        frame.value("mysql_global_status_threads_running"),
    )


def _port_loss_ratio(frame: EntityFrame) -> list[float | None]:
    """Discarded packets as a fraction of all packets offered to the port."""
    discards = _sum_series(
        frame.sum_rate("ifInDiscards"), frame.sum_rate("ifOutDiscards")
    )
    delivered = _sum_series(
        frame.sum_rate("ifHCInUcastPkts"), frame.sum_rate("ifHCOutUcastPkts")
    )
    return _fraction_of_total(discards, _sum_series(delivered, discards))


def _port_error_rate(frame: EntityFrame) -> list[float | None]:
    """Interface errors per second, both directions."""
    return _sum_series(frame.sum_rate("ifInErrors"), frame.sum_rate("ifOutErrors"))


def _port_down(frame: EntityFrame) -> list[float | None]:
    """1.0 when the interface is not operationally up.

    ``ifOperStatus`` is an enumeration in which 1 means ``up``; anything else is
    a hard fault rather than a degradation, which the suppression layer treats
    as unexplainable by a change window.
    """
    status = frame.value("ifOperStatus")
    return [None if v is None else (0.0 if int(v) == 1 else 1.0) for v in status]


def _port_utilisation(frame: EntityFrame) -> list[float | None]:
    """Offered load as a fraction of the port's nominal speed."""
    octets = _sum_series(frame.sum_rate("ifHCInOctets"), frame.sum_rate("ifHCOutOctets"))
    speed = frame.value("ifHighSpeed")
    bits = [None if v is None else v * 8.0 for v in octets]
    capacity = [None if v is None else v * 1e6 for v in speed]
    return _fraction_of_total(bits, capacity)


def _ospf_event_rate(frame: EntityFrame) -> list[float | None]:
    """OSPF neighbour state transitions per minute."""
    per_second = frame.sum_rate("ospfNbrEvents")
    return [None if v is None else v * 60.0 for v in per_second]


def _ospf_not_full(frame: EntityFrame) -> list[float | None]:
    """1.0 when an OSPF adjacency is not in the ``full`` state.

    ``ospfNbrState`` 8 is ``full``; a neighbour sitting anywhere else has lost
    adjacency and traffic is being reconverged around it.
    """
    out: list[float | None] = []
    neighbours = frame.select("ospfNbrState")
    if not neighbours:
        return [None] * len(frame)
    for i in range(len(frame)):
        worst: float | None = None
        for neighbour in neighbours:
            value = neighbour.values[i]
            if value is None:
                continue
            flag = 0.0 if int(value) == 8 else 1.0
            worst = flag if worst is None else max(worst, flag)
        out.append(worst)
    return out


_HOST_KINDS = frozenset({"app_server", "database", "node", "unknown"})
_APP_KINDS = frozenset({"app_server", "unknown"})
_DB_KINDS = frozenset({"database", "unknown"})
_NET_KINDS = frozenset({"switch", "unknown"})


#: The feature catalogue. Requirement R2 calls its five named signal families a
#: minimum rather than a closed list (feature #48), so extra families are
#: present and declared; every one of the five named families is here.
FEATURES: tuple[FeatureSpec, ...] = (
    # ---- host / OS ------------------------------------------------------
    FeatureSpec(
        name="host.cpu_busy_ratio",
        title="CPU utilisation",
        unit="ratio",
        direction="up",
        ceiling="unit",
        kinds=_HOST_KINDS,
        compute=_cpu_busy_ratio,
        description="Non-idle CPU time averaged over cores.",
        absolute_floor=0.01,
    ),
    FeatureSpec(
        name="host.cpu_iowait_ratio",
        title="I/O wait",
        unit="ratio",
        direction="up",
        ceiling="unit",
        kinds=_HOST_KINDS,
        compute=lambda frame: _cpu_mode_fraction(frame, "iowait"),
        description="CPU time blocked on storage. R2 'rising I/O wait'.",
        absolute_floor=0.005,
    ),
    FeatureSpec(
        name="host.cpu_system_ratio",
        title="Kernel CPU",
        unit="ratio",
        direction="up",
        ceiling="unit",
        kinds=_HOST_KINDS,
        compute=lambda frame: _cpu_mode_fraction(frame, "system"),
        description="CPU time in the kernel; rises with descriptor and socket churn.",
        absolute_floor=0.005,
    ),
    FeatureSpec(
        name="host.runq_per_cpu",
        title="Run-queue pressure",
        unit="runnable/core",
        direction="up",
        ceiling="learned",
        kinds=_HOST_KINDS,
        compute=_runq_per_cpu,
        description="1-minute load average per core.",
        absolute_floor=0.05,
    ),
    FeatureSpec(
        name="host.procs_blocked",
        title="Processes in uninterruptible sleep",
        unit="processes",
        direction="up",
        ceiling="learned",
        kinds=_HOST_KINDS,
        compute=lambda frame: frame.value("node_procs_blocked"),
        description="D-state processes: the OS-level face of a storage stall.",
        absolute_floor=0.5,
    ),
    FeatureSpec(
        name="host.disk_latency_seconds",
        title="Disk service time",
        unit="seconds",
        direction="up",
        ceiling="learned",
        kinds=_HOST_KINDS,
        compute=_disk_latency_seconds,
        description="Mean time an I/O spent in flight. R2 'disk latency'.",
        absolute_floor=0.0005,
    ),
    FeatureSpec(
        name="host.disk_busy_ratio",
        title="Disk utilisation",
        unit="ratio",
        direction="up",
        ceiling="unit",
        kinds=_HOST_KINDS,
        compute=_disk_busy_ratio,
        description="Fraction of time the busiest device was servicing I/O.",
        absolute_floor=0.01,
    ),
    FeatureSpec(
        name="host.fd_utilisation",
        title="File-descriptor utilisation",
        unit="ratio",
        direction="up",
        ceiling="unit",
        kinds=_HOST_KINDS,
        compute=_fd_utilisation,
        description="Allocated descriptors over the kernel maximum. "
        "R2 'socket/file-descriptor exhaustion'; has a hard ceiling, so it "
        "yields a genuine time-to-exhaustion.",
        absolute_floor=0.15,
    ),
    FeatureSpec(
        name="host.socket_pressure",
        title="Socket share of descriptors",
        unit="ratio",
        direction="up",
        ceiling="unit",
        kinds=_HOST_KINDS,
        compute=_socket_pressure,
        description="TCP sockets as a share of allocated descriptors; "
        "separates socket exhaustion from a plain file-handle leak.",
        absolute_floor=0.90,
    ),
    FeatureSpec(
        name="host.tcp_orphan_ratio",
        title="Orphaned TCP sockets",
        unit="ratio",
        direction="up",
        ceiling="unit",
        kinds=_HOST_KINDS,
        compute=_orphan_ratio,
        description="Sockets the application closed but never drained.",
        absolute_floor=0.05,
    ),
    FeatureSpec(
        name="host.tcp_timewait_ratio",
        title="TIME_WAIT sockets",
        unit="ratio",
        direction="up",
        ceiling="unit",
        kinds=_HOST_KINDS,
        compute=_timewait_ratio,
        description="Connection churn pressure on the ephemeral port range.",
        absolute_floor=0.01,
    ),
    FeatureSpec(
        name="host.memory_used_ratio",
        title="Memory utilisation",
        unit="ratio",
        direction="up",
        ceiling="unit",
        kinds=_HOST_KINDS,
        compute=_memory_used_ratio,
        description="RAM unavailable to new allocations.",
        absolute_floor=0.01,
    ),
    FeatureSpec(
        name="host.major_fault_rate",
        title="Major page faults",
        unit="faults/second",
        direction="up",
        ceiling="learned",
        kinds=_HOST_KINDS,
        compute=lambda frame: frame.rate("node_vmstat_pgmajfault"),
        description="Faults served from disk: memory pressure turning into I/O.",
        absolute_floor=0.5,
    ),
    FeatureSpec(
        name="host.context_switch_rate",
        title="Context switches",
        unit="switches/second",
        direction="up",
        ceiling="learned",
        kinds=_HOST_KINDS,
        compute=lambda frame: frame.rate("node_context_switches_total"),
        description="Scheduler churn; rises sharply under lock convoying.",
        absolute_floor=10.0,
    ),
    FeatureSpec(
        name="host.work_per_cpu_second",
        title="Host work per CPU-second",
        unit="operations/cpu-second",
        direction="down",
        ceiling="none",
        kinds=frozenset({"node", "database", "unknown"}),
        compute=_host_work_per_cpu_second,
        description="Disk and network operations delivered per unit of CPU "
        "burned. Falls when a host consumes CPU without doing work.",
        absolute_floor=1.0,
    ),
    # ---- application tier ----------------------------------------------
    FeatureSpec(
        name="app.request_rate",
        title="Request throughput",
        unit="requests/second",
        direction="down",
        ceiling="none",
        kinds=_APP_KINDS,
        compute=_request_rate,
        description="Served requests per second: the useful-work term.",
        absolute_floor=0.5,
    ),
    FeatureSpec(
        name="app.error_ratio",
        title="Request error ratio",
        unit="ratio",
        direction="up",
        ceiling="unit",
        kinds=_APP_KINDS,
        compute=_error_ratio,
        description="Fraction of requests returning an error.",
        absolute_floor=0.002,
    ),
    FeatureSpec(
        name="app.mean_latency_seconds",
        title="Mean request latency",
        unit="seconds",
        direction="up",
        ceiling="learned",
        kinds=_APP_KINDS,
        compute=_mean_latency_seconds,
        description="Interval-mean service time. A symptom, never evidence "
        "that the application itself is the cause.",
        absolute_floor=0.002,
    ),
    FeatureSpec(
        name="app.thread_busy_ratio",
        title="Worker-thread saturation",
        unit="ratio",
        direction="up",
        ceiling="unit",
        kinds=_APP_KINDS,
        compute=_thread_busy_ratio,
        description="Busy worker threads over the live pool. "
        "R2 'OS thread starvation'.",
        absolute_floor=0.02,
    ),
    FeatureSpec(
        name="app.apache_busy_ratio",
        title="Apache worker saturation",
        unit="ratio",
        direction="up",
        ceiling="unit",
        kinds=_APP_KINDS,
        compute=_apache_busy_ratio,
        description="Busy Apache workers over the worker set.",
        absolute_floor=0.02,
    ),
    FeatureSpec(
        name="app.scoreboard_wait_ratio",
        title="Apache slots awaiting a connection",
        unit="ratio",
        direction="down",
        ceiling="none",
        kinds=_APP_KINDS,
        compute=lambda frame: _scoreboard_state_ratio(frame, "idle"),
        description="Headroom in the Apache scoreboard; collapses before the "
        "server stops accepting connections.",
        absolute_floor=0.02,
    ),
    FeatureSpec(
        name="app.jvm_heap_ratio",
        title="JVM heap utilisation",
        unit="ratio",
        direction="up",
        ceiling="unit",
        kinds=_APP_KINDS,
        compute=_jvm_heap_ratio,
        description="Heap in use over the configured maximum.",
        absolute_floor=0.01,
    ),
    FeatureSpec(
        name="app.jvm_gc_fraction",
        title="Time in garbage collection",
        unit="ratio",
        direction="up",
        ceiling="unit",
        kinds=_APP_KINDS,
        compute=_jvm_gc_fraction,
        description="Wall-clock fraction spent collecting. The memory-leak "
        "slow-death signature: all CPU, no progress.",
        absolute_floor=0.005,
    ),
    FeatureSpec(
        name="app.jvm_blocked_ratio",
        title="Blocked JVM threads",
        unit="ratio",
        direction="up",
        ceiling="unit",
        kinds=_APP_KINDS,
        compute=lambda frame: _jvm_state_ratio(frame, "BLOCKED"),
        description="Threads blocked on a monitor: contention, not work.",
        absolute_floor=0.01,
    ),
    FeatureSpec(
        name="app.jvm_waiting_ratio",
        title="Waiting JVM threads",
        unit="ratio",
        direction="up",
        ceiling="unit",
        kinds=_APP_KINDS,
        compute=lambda frame: _jvm_state_ratio(frame, "WAITING"),
        description="Threads parked awaiting a downstream response.",
        absolute_floor=0.01,
    ),
    FeatureSpec(
        name="app.jvm_deadlocked",
        title="Deadlocked JVM threads",
        unit="threads",
        direction="up",
        ceiling="learned",
        kinds=_APP_KINDS,
        compute=lambda frame: frame.value("jvm_threads_deadlocked"),
        description="A hard, unrecoverable application fault.",
        absolute_floor=0.5,
    ),
    FeatureSpec(
        name="app.session_reject_rate",
        title="Rejected sessions",
        unit="sessions/second",
        direction="up",
        ceiling="learned",
        kinds=_APP_KINDS,
        compute=lambda frame: frame.sum_rate("tomcat_session_rejectedsessions_total"),
        description="Sessions the container refused to create.",
        absolute_floor=0.05,
    ),
    FeatureSpec(
        name=WORK_PER_CPU,
        title="Requests per CPU-second",
        unit="requests/cpu-second",
        direction="down",
        ceiling="none",
        kinds=_APP_KINDS,
        compute=_work_per_cpu_second,
        description="R2's 'CPU saturation without corresponding throughput', "
        "expressed as the quantity that actually distinguishes it from load.",
        absolute_floor=0.5,
    ),
    FeatureSpec(
        name=WORK_PER_BUSY_THREAD,
        title="Requests per busy thread",
        unit="requests/second/thread",
        direction="down",
        ceiling="none",
        kinds=_APP_KINDS,
        compute=_work_per_busy_thread,
        description="Throughput each occupied worker is actually delivering. "
        "Collapses while the pool still looks healthy.",
        absolute_floor=0.02,
    ),
    # ---- database -------------------------------------------------------
    FeatureSpec(
        name="db.lock_wait_rate",
        title="Row-lock waits",
        unit="waits/second",
        direction="up",
        ceiling="learned",
        kinds=_DB_KINDS,
        compute=lambda frame: frame.rate("mysql_global_status_innodb_row_lock_waits"),
        description="R3 'lock waits'. Primary evidence for database causation.",
        absolute_floor=0.05,
    ),
    FeatureSpec(
        name="db.lock_current_waits",
        title="Sessions currently waiting on a row lock",
        unit="sessions",
        direction="up",
        ceiling="learned",
        kinds=_DB_KINDS,
        compute=lambda frame: frame.value(
            "mysql_global_status_innodb_row_lock_current_waits"
        ),
        description="Instantaneous blocked-session count.",
        absolute_floor=0.5,
    ),
    FeatureSpec(
        name="db.lock_time_avg_ms",
        title="Mean row-lock wait",
        unit="milliseconds",
        direction="up",
        ceiling="learned",
        kinds=_DB_KINDS,
        compute=lambda frame: frame.value(
            "mysql_global_status_innodb_row_lock_time_avg"
        ),
        description="How long a blocked statement waits.",
        absolute_floor=25.0,
    ),
    FeatureSpec(
        name="db.table_lock_contention",
        title="Table-lock contention",
        unit="ratio",
        direction="up",
        ceiling="unit",
        kinds=_DB_KINDS,
        compute=_db_table_lock_contention,
        description="Share of table locks that had to wait.",
        absolute_floor=0.005,
    ),
    FeatureSpec(
        name="db.slow_query_rate",
        title="Slow queries",
        unit="queries/second",
        direction="up",
        ceiling="learned",
        kinds=_DB_KINDS,
        compute=lambda frame: frame.rate("mysql_global_status_slow_queries"),
        description="R3 'long-running SQL'.",
        absolute_floor=0.02,
    ),
    FeatureSpec(
        name="db.session_pool_ratio",
        title="Session-pool utilisation",
        unit="ratio",
        direction="up",
        ceiling="unit",
        kinds=_DB_KINDS,
        compute=_db_session_pool_ratio,
        description="R3 'session-pool spikes'. Hard ceiling at max_connections.",
        absolute_floor=0.01,
    ),
    FeatureSpec(
        name="db.threads_running",
        title="Actively running sessions",
        unit="sessions",
        direction="up",
        ceiling="learned",
        kinds=_DB_KINDS,
        compute=lambda frame: frame.value("mysql_global_status_threads_running"),
        description="Concurrency actually executing inside the engine.",
        absolute_floor=0.5,
    ),
    FeatureSpec(
        name="db.aborted_connect_rate",
        title="Aborted connections",
        unit="connections/second",
        direction="up",
        ceiling="learned",
        kinds=_DB_KINDS,
        compute=lambda frame: frame.rate("mysql_global_status_aborted_connects"),
        description="Clients failing to establish a session.",
        absolute_floor=0.02,
    ),
    FeatureSpec(
        name="db.buffer_pool_wait_rate",
        title="Buffer-pool waits",
        unit="waits/second",
        direction="up",
        ceiling="learned",
        kinds=_DB_KINDS,
        compute=lambda frame: frame.rate(
            "mysql_global_status_innodb_buffer_pool_wait_free"
        ),
        description="Statements waiting for a free page: I/O pressure inside "
        "the engine.",
        absolute_floor=0.02,
    ),
    FeatureSpec(
        name="db.query_rate",
        title="Query throughput",
        unit="queries/second",
        direction="down",
        ceiling="none",
        kinds=_DB_KINDS,
        compute=lambda frame: frame.rate("mysql_global_status_questions"),
        description="The database's useful-work term.",
        absolute_floor=1.0,
    ),
    FeatureSpec(
        name=DB_WORK_PER_SESSION,
        title="Queries per running session",
        unit="queries/second/session",
        direction="down",
        ceiling="none",
        kinds=_DB_KINDS,
        compute=_db_work_per_running_session,
        description="Database efficiency. Contention collapses it; a genuine "
        "workload increase leaves it flat.",
        absolute_floor=0.5,
    ),
    # ---- network --------------------------------------------------------
    FeatureSpec(
        name="net.port_error_rate",
        title="Interface errors",
        unit="errors/second",
        direction="up",
        ceiling="learned",
        kinds=_NET_KINDS,
        compute=_port_error_rate,
        description="R3 'port errors'.",
        absolute_floor=0.05,
    ),
    FeatureSpec(
        name="net.loss_ratio",
        title="Packet loss",
        unit="ratio",
        direction="up",
        ceiling="unit",
        kinds=_NET_KINDS,
        compute=_port_loss_ratio,
        description="R3 'packet loss'. Discards over offered packets.",
        absolute_floor=0.0005,
    ),
    FeatureSpec(
        name="net.discard_rate",
        title="Discarded packets",
        unit="packets/second",
        direction="up",
        ceiling="learned",
        kinds=_NET_KINDS,
        compute=lambda frame: _sum_series(
            frame.sum_rate("ifInDiscards"), frame.sum_rate("ifOutDiscards")
        ),
        description="Absolute discard volume.",
        absolute_floor=0.05,
    ),
    FeatureSpec(
        name="net.port_down",
        title="Interface not operationally up",
        unit="boolean",
        direction="up",
        ceiling="unit",
        kinds=_NET_KINDS,
        compute=_port_down,
        description="A hard link fault. Never explainable by a change window "
        "that does not name this device.",
        absolute_floor=0.25,
    ),
    FeatureSpec(
        name="net.port_utilisation",
        title="Link utilisation",
        unit="ratio",
        direction="up",
        ceiling="unit",
        kinds=_NET_KINDS,
        compute=_port_utilisation,
        description="Offered load over nominal port speed.",
        absolute_floor=0.01,
    ),
    FeatureSpec(
        name="net.ospf_event_rate",
        title="OSPF adjacency transitions",
        unit="changes/minute",
        direction="up",
        ceiling="learned",
        kinds=_NET_KINDS,
        compute=_ospf_event_rate,
        description="R3 'OSPF flaps'.",
        absolute_floor=0.02,
    ),
    FeatureSpec(
        name="net.ospf_not_full",
        title="OSPF adjacency down",
        unit="boolean",
        direction="up",
        ceiling="unit",
        kinds=_NET_KINDS,
        compute=_ospf_not_full,
        description="A neighbour outside the full state; traffic is "
        "reconverging around it.",
        absolute_floor=0.25,
    ),
)

_FEATURE_INDEX: dict[str, FeatureSpec] = {spec.name: spec for spec in FEATURES}


def feature_spec(name: str) -> FeatureSpec:
    """Look up a feature by name.

    Args:
        name: Canonical feature name.

    Returns:
        Its specification.

    Raises:
        KeyError: If no such feature exists.
    """
    return _FEATURE_INDEX[name]


def features_for_kind(kind: str) -> tuple[FeatureSpec, ...]:
    """Features meaningful for an entity of ``kind``, in catalogue order."""
    return tuple(spec for spec in FEATURES if spec.applies_to(kind))


# --------------------------------------------------------------------------
# Frame construction
# --------------------------------------------------------------------------


def _median_step(times: Sequence[float]) -> float:
    """Effective scrape interval: the median gap between observations."""
    if len(times) < 2:
        return 0.0
    gaps = sorted(times[i + 1] - times[i] for i in range(len(times) - 1))
    middle = len(gaps) // 2
    if len(gaps) % 2:
        return gaps[middle]
    return (gaps[middle - 1] + gaps[middle]) / 2.0


def group_observations(
    observations: Iterable["Observation"],
) -> dict[str, list["Observation"]]:
    """Bucket observations by entity, preserving arrival order within a bucket.

    Args:
        observations: Normalised observations.

    Returns:
        Entity id -> its observations.
    """
    grouped: dict[str, list[Observation]] = {}
    for item in observations:
        grouped.setdefault(item.entity, []).append(item)
    return grouped


def build_entity_frame(
    entity: str,
    observations: Iterable["Observation"],
    *,
    kind: str = "unknown",
    staleness_factor: float = DEFAULT_STALENESS_FACTOR,
) -> tuple[EntityFrame, dict[str, tuple[float | None, ...]]]:
    """Align one entity's observations and derive its features.

    Exposed separately from :func:`build_frames` so a caller can stream a long
    history phase entity by entity and discard each frame after folding it into
    the baselines. Materialising three months of aligned series for the whole
    estate at once would cost gigabytes for no benefit.

    Args:
        entity: Entity id.
        observations: That entity's observations, in any order.
        kind: Entity kind, which selects the applicable features.
        staleness_factor: Carried-forward observations expire after this many
            scrape intervals.

    Returns:
        ``(frame, features)`` where ``features`` maps feature name to its
        per-time values. Features that are entirely uncomputable are omitted.
    """
    series_points: dict[tuple[str, tuple[tuple[str, str], ...]], list[tuple[float, float]]] = {}
    for item in observations:
        key = (item.metric, tuple(sorted(item.labels.items())))
        series_points.setdefault(key, []).append((item.moment, item.value))

    times = tuple(
        sorted({moment for points in series_points.values() for moment, _ in points})
    )
    if not times:
        empty = EntityFrame(
            entity=entity, kind=kind, times=(), series=(), step_s=0.0
        )
        return empty, {}

    step = _median_step(times)
    staleness = staleness_factor * step if step > 0.0 else math.inf

    aligned: list[AlignedSeries] = []
    for (metric, label_items), points in sorted(series_points.items()):
        points.sort(key=lambda item: item[0])
        counter = _is_counter(metric)
        levels, rates = _align(times, points, staleness, counter)
        aligned.append(
            AlignedSeries(
                metric=metric,
                labels=dict(label_items),
                values=levels,
                rates=rates,
                is_counter=counter,
            )
        )

    frame = EntityFrame(
        entity=entity, kind=kind, times=times, series=tuple(aligned), step_s=step
    )

    features: dict[str, tuple[float | None, ...]] = {}
    for spec in features_for_kind(kind):
        computed = spec.compute(frame)
        if any(item is not None for item in computed):
            features[spec.name] = tuple(computed)
    return frame, features


def build_frames(
    observations: Iterable["Observation"],
    *,
    entity_kinds: Mapping[str, str] | None = None,
    staleness_factor: float = DEFAULT_STALENESS_FACTOR,
) -> FeatureFrame:
    """Turn normalised observations into derived per-entity feature series.

    Args:
        observations: Normalised observations. Use :func:`observations_from_scrape`
            or :func:`observations_from_samples` to produce them.
        entity_kinds: ``entity_id -> kind`` from the topology, used to decide
            which features are meaningful. This is CMDB data, not ground truth.
            Unknown entities get every computable feature.
        staleness_factor: Carried-forward observations expire after this many
            scrape intervals.

    Returns:
        The derived :class:`FeatureFrame`.
    """
    kinds = dict(entity_kinds or {})
    grouped = group_observations(observations)

    frames: dict[str, EntityFrame] = {}
    values: dict[tuple[str, str], tuple[float | None, ...]] = {}
    timelines: dict[str, tuple[float, ...]] = {}
    steps: dict[str, float] = {}

    for entity in sorted(grouped):
        frame, features = build_entity_frame(
            entity,
            grouped[entity],
            kind=kinds.get(entity, "unknown"),
            staleness_factor=staleness_factor,
        )
        if not frame.times:
            continue
        frames[entity] = frame
        timelines[entity] = frame.times
        steps[entity] = frame.step_s
        for name, computed in features.items():
            values[(entity, name)] = computed

    return FeatureFrame(frames=frames, values=values, times=timelines, step_s=steps)


_ = (Any, dataclass)  # referenced by the annotations above
