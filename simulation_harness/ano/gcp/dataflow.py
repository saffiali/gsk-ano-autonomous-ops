"""Local pipeline runner — the Dataflow adapter.

Stands in for
-------------
**Cloud Dataflow**. In production the same logical stages run as an Apache Beam
pipeline on the ``DataflowRunner``: :class:`Map` is ``beam.Map``,
:class:`FlatMap` is ``beam.FlatMap``, :class:`Filter` is ``beam.Filter``,
:class:`GroupByKey` is ``beam.GroupByKey``, :class:`FixedWindows` is
``beam.WindowInto(beam.window.FixedWindows(...))``, :class:`CombinePerKey` is
``beam.CombinePerKey``, and :class:`Write` is a sink transform.

What changes on deployment
--------------------------
Configuration only: ``pipeline_backend`` becomes ``"gcp"`` and a deployment
plugin registers a Beam-backed runner that maps each stage onto its Beam
transform. The *stage graph* — which is where the business logic lives — is
unchanged, because it is data: a list of stage objects, not a Beam program.

Why not Beam locally
--------------------
Beam is not installed and cannot be (no package manager; see
``.agents/teamwork_preview_orchestrator/DECISIONS.md`` D1). Equally important,
Beam's local runner gives no ordering or determinism guarantee, and "same seed
produces identical scores" is an acceptance criterion. This runner is
single-threaded and strictly ordered, so a pipeline run twice produces
byte-identical output.

Determinism rules this runner enforces
--------------------------------------
* Elements flow through stages in order.
* :class:`GroupByKey` emits groups **sorted by key**, and values within a group
  keep their arrival order. Beam makes neither promise; relying on the runner's
  incidental order is the classic way a pipeline becomes irreproducible, so
  this runner makes the order explicit and total.
* :class:`FixedWindows` assigns windows from an event-time function the caller
  supplies. There is no processing-time trigger and no watermark heuristic,
  because both are sources of run-to-run variation.

Metrics
-------
:meth:`Pipeline.run` returns a :class:`PipelineResult` carrying per-stage input
and output counts — the local analogue of Dataflow's step element counters, and
what makes "the ingest pipeline dropped 12 records" visible instead of silent.
"""

from __future__ import annotations

import abc
from collections.abc import Callable, Hashable, Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from typing import Any

from ano.config import LOCAL, Config
from ano.gcp.registry import register_backend

__all__ = [
    "Stage",
    "Map",
    "FlatMap",
    "Filter",
    "Partition",
    "GroupByKey",
    "CombinePerKey",
    "FixedWindows",
    "WindowedValue",
    "Write",
    "Pipeline",
    "PipelineResult",
    "StageMetrics",
    "LocalPipelineRunner",
    "build_pipeline_runner",
]


class Stage(abc.ABC):
    """One pipeline step. Mirrors a Beam ``PTransform``."""

    #: Human-readable step name, as it would appear in the Dataflow job graph.
    name: str

    def __init__(self, name: str) -> None:
        if not name:
            raise ValueError("a stage must have a non-empty name")
        self.name = name

    @abc.abstractmethod
    def expand(self, elements: Iterable[Any]) -> Iterator[Any]:
        """Transform the input stream into the output stream."""

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.name!r})"


class Map(Stage):
    """One output element per input element (``beam.Map``)."""

    def __init__(self, name: str, fn: Callable[[Any], Any]) -> None:
        super().__init__(name)
        self._fn = fn

    def expand(self, elements: Iterable[Any]) -> Iterator[Any]:
        """Apply ``fn`` to each element."""
        for element in elements:
            yield self._fn(element)


class FlatMap(Stage):
    """Zero or more output elements per input element (``beam.FlatMap``)."""

    def __init__(self, name: str, fn: Callable[[Any], Iterable[Any]]) -> None:
        super().__init__(name)
        self._fn = fn

    def expand(self, elements: Iterable[Any]) -> Iterator[Any]:
        """Apply ``fn`` and flatten the results."""
        for element in elements:
            yield from self._fn(element)


class Filter(Stage):
    """Keep elements for which ``predicate`` is true (``beam.Filter``)."""

    def __init__(self, name: str, predicate: Callable[[Any], bool]) -> None:
        super().__init__(name)
        self._predicate = predicate

    def expand(self, elements: Iterable[Any]) -> Iterator[Any]:
        """Yield only the elements that satisfy the predicate."""
        for element in elements:
            if self._predicate(element):
                yield element


class Partition(Stage):
    """Tag each element with a partition name (``beam.Partition``).

    Emits ``(partition, element)`` pairs so a later stage can route them. The
    local runner has a single output stream, so partitioning is expressed as a
    key rather than as multiple ``PCollection`` outputs.
    """

    def __init__(self, name: str, fn: Callable[[Any], str]) -> None:
        super().__init__(name)
        self._fn = fn

    def expand(self, elements: Iterable[Any]) -> Iterator[Any]:
        """Yield ``(partition_name, element)`` pairs."""
        for element in elements:
            yield (self._fn(element), element)


class GroupByKey(Stage):
    """Group elements by key (``beam.GroupByKey``).

    Emits ``(key, [values...])`` **sorted by key**. Values keep arrival order.
    This stage is blocking: it consumes the whole input before emitting, exactly
    as a Beam ``GroupByKey`` does within a window.
    """

    def __init__(self, name: str, key_fn: Callable[[Any], Hashable]) -> None:
        super().__init__(name)
        self._key_fn = key_fn

    def expand(self, elements: Iterable[Any]) -> Iterator[Any]:
        """Group, then emit in sorted key order."""
        groups: dict[Any, list[Any]] = {}
        for element in elements:
            groups.setdefault(self._key_fn(element), []).append(element)
        for key in sorted(groups, key=_total_order_key):
            yield (key, groups[key])


class CombinePerKey(Stage):
    """Reduce the values of each key (``beam.CombinePerKey``).

    Emits ``(key, combined)`` in sorted key order.
    """

    def __init__(
        self,
        name: str,
        key_fn: Callable[[Any], Hashable],
        combine_fn: Callable[[Sequence[Any]], Any],
    ) -> None:
        super().__init__(name)
        self._key_fn = key_fn
        self._combine_fn = combine_fn

    def expand(self, elements: Iterable[Any]) -> Iterator[Any]:
        """Group by key, then combine each group."""
        groups: dict[Any, list[Any]] = {}
        for element in elements:
            groups.setdefault(self._key_fn(element), []).append(element)
        for key in sorted(groups, key=_total_order_key):
            yield (key, self._combine_fn(groups[key]))


@dataclass(frozen=True, slots=True)
class WindowedValue:
    """An element tagged with its fixed event-time window.

    Attributes:
        window_start: Inclusive window start, in seconds since the epoch,
            aligned to a multiple of ``window_size_s``.
        window_size_s: Window width in seconds.
        value: The original element.
    """

    window_start: int
    window_size_s: int
    value: Any

    @property
    def window_end(self) -> int:
        """Exclusive window end, in seconds since the epoch."""
        return self.window_start + self.window_size_s


class FixedWindows(Stage):
    """Assign each element to a fixed event-time window.

    Mirrors ``beam.WindowInto(beam.window.FixedWindows(size))``.

    Args:
        name: Step name.
        timestamp_fn: Returns the element's **event time** in seconds since the
            epoch. Event time, never processing time — processing time is not
            reproducible.
        size_s: Window width in seconds.
        offset_s: Window alignment offset in seconds.
    """

    def __init__(
        self,
        name: str,
        timestamp_fn: Callable[[Any], float],
        size_s: int,
        offset_s: int = 0,
    ) -> None:
        super().__init__(name)
        if size_s <= 0:
            raise ValueError(f"window size must be > 0 seconds, got {size_s}")
        self._timestamp_fn = timestamp_fn
        self._size_s = size_s
        self._offset_s = offset_s

    def expand(self, elements: Iterable[Any]) -> Iterator[Any]:
        """Yield :class:`WindowedValue` for each element."""
        for element in elements:
            event_time = float(self._timestamp_fn(element))
            shifted = event_time - self._offset_s
            start = int(shifted // self._size_s) * self._size_s + self._offset_s
            yield WindowedValue(
                window_start=start, window_size_s=self._size_s, value=element
            )


class Write(Stage):
    """Terminal side-effecting stage (a Beam sink transform).

    ``sink_fn`` is called once per element; the element is passed through
    unchanged so later stages and the runner's counters still see it.
    """

    def __init__(self, name: str, sink_fn: Callable[[Any], Any]) -> None:
        super().__init__(name)
        self._sink_fn = sink_fn

    def expand(self, elements: Iterable[Any]) -> Iterator[Any]:
        """Write each element, then pass it through."""
        for element in elements:
            self._sink_fn(element)
            yield element


def _total_order_key(key: Any) -> tuple[str, str]:
    """Sort key giving a total order over mixed key types.

    Grouping keys are usually strings, but a pipeline may legitimately key on
    tuples or numbers. Comparing those directly raises ``TypeError`` for mixed
    types, and falling back to insertion order would reintroduce
    non-determinism, so keys are ordered by (type name, repr).
    """
    return (type(key).__name__, repr(key))


@dataclass(frozen=True, slots=True)
class StageMetrics:
    """Element counters for one stage — the Dataflow step-counter analogue."""

    name: str
    stage_type: str
    elements_in: int
    elements_out: int

    @property
    def dropped(self) -> int:
        """How many elements the stage removed (negative means it expanded)."""
        return self.elements_in - self.elements_out

    def to_dict(self) -> dict[str, Any]:
        """Serialise for inclusion in an artifact."""
        return {
            "name": self.name,
            "stage_type": self.stage_type,
            "elements_in": self.elements_in,
            "elements_out": self.elements_out,
        }


@dataclass(frozen=True, slots=True)
class PipelineResult:
    """The outcome of a pipeline run.

    Attributes:
        elements: The final output elements, in order.
        metrics: Per-stage counters, in stage order.
    """

    elements: tuple[Any, ...]
    metrics: tuple[StageMetrics, ...] = field(default=())

    def __len__(self) -> int:
        return len(self.elements)

    def __iter__(self) -> Iterator[Any]:
        return iter(self.elements)

    def metric(self, name: str) -> StageMetrics:
        """Counters for the stage named ``name``."""
        for item in self.metrics:
            if item.name == name:
                return item
        raise KeyError(name)

    def to_dict(self) -> dict[str, Any]:
        """Serialise the counters (not the elements) for an artifact."""
        return {
            "element_count": len(self.elements),
            "stages": [item.to_dict() for item in self.metrics],
        }


class Pipeline:
    """An ordered stage graph. Mirrors a Beam ``Pipeline``.

    The graph is data, not code: it can be built, inspected, logged and handed
    to a different runner without change. That is what makes the Dataflow swap
    a configuration change.

    Example::

        pipeline = (
            Pipeline("ingest")
            .apply(Map("parse", parse_entry))
            .apply(Filter("drop_debug", lambda e: e["severity"] != "DEBUG"))
            .apply(Write("store", store_writer))
        )
        result = runner.run(pipeline, raw_lines)
    """

    def __init__(self, name: str = "pipeline") -> None:
        if not name:
            raise ValueError("a pipeline must have a non-empty name")
        self.name = name
        self._stages: list[Stage] = []

    def apply(self, stage: Stage) -> "Pipeline":
        """Append ``stage``. Returns ``self`` so calls can be chained."""
        if not isinstance(stage, Stage):
            raise TypeError(f"expected a Stage, got {type(stage).__name__}")
        if any(existing.name == stage.name for existing in self._stages):
            raise ValueError(
                f"pipeline {self.name!r} already has a stage named {stage.name!r}; "
                "stage names must be unique so metrics are unambiguous"
            )
        self._stages.append(stage)
        return self

    @property
    def stages(self) -> tuple[Stage, ...]:
        """The stages, in order."""
        return tuple(self._stages)

    def describe(self) -> list[dict[str, str]]:
        """The job graph as plain data, for logging or a demo surface."""
        return [
            {"name": stage.name, "type": type(stage).__name__}
            for stage in self._stages
        ]

    def __repr__(self) -> str:
        return f"Pipeline({self.name!r}, stages={len(self._stages)})"


class LocalPipelineRunner:
    """Deterministic single-process runner for a :class:`Pipeline`.

    Args:
        collect_metrics: Count elements in and out of every stage. Costs one
            extra pass-through generator per stage; on by default because the
            counters are how a silent drop gets noticed.
    """

    def __init__(self, collect_metrics: bool = True) -> None:
        self._collect_metrics = collect_metrics

    def run(self, pipeline: Pipeline, source: Iterable[Any]) -> PipelineResult:
        """Execute ``pipeline`` over ``source``.

        Args:
            pipeline: The stage graph.
            source: Input elements.

        Returns:
            A :class:`PipelineResult` with the output elements and per-stage
            counters.
        """
        stream: Iterable[Any] = source
        counters: list[list[int]] = []
        for stage in pipeline.stages:
            if self._collect_metrics:
                counter = [0, 0]
                counters.append(counter)
                stream = stage.expand(_count(stream, counter, 0))
                stream = _count(stream, counter, 1)
            else:
                stream = stage.expand(stream)
        elements = tuple(stream)
        metrics = tuple(
            StageMetrics(
                name=stage.name,
                stage_type=type(stage).__name__,
                elements_in=counter[0],
                elements_out=counter[1],
            )
            for stage, counter in zip(pipeline.stages, counters)
        )
        return PipelineResult(elements=elements, metrics=metrics)

    @classmethod
    def from_config(cls, config: Config) -> "LocalPipelineRunner":
        """Build the offline runner from a :class:`ano.config.Config`."""
        return cls(collect_metrics=config.option_bool("pipeline_metrics", True))


def _count(stream: Iterable[Any], counter: list[int], index: int) -> Iterator[Any]:
    """Pass elements through, incrementing ``counter[index]`` for each."""
    for element in stream:
        counter[index] += 1
        yield element


def build_pipeline_runner(config: Config) -> LocalPipelineRunner:
    """Build the configured runner (see :func:`ano.gcp.registry.build`)."""
    from ano.gcp.registry import build

    return build("pipeline", config)


register_backend("pipeline", LOCAL, LocalPipelineRunner.from_config)
