"""Replay adapters: how the harness gets one seed's ground truth and predictions.

The harness scores **contract C1 against contract C2** and nothing else. It must
not import detector internals (requirement R5), and it must not be written
against whatever shape milestones M2/M3/M4 happen to have on any given day. An
adapter is therefore the only place that knows how to obtain a replay, and every
adapter returns the same :class:`ReplayResult`.

Three adapters ship:

``system``
    The real thing: invoke the scenario generator, then the detection pipeline,
    then read the artefacts back through the contracts. Used by ``make eval``.
    If the generator or the pipeline has not landed yet it raises
    :class:`AdapterUnavailable` and the run exits non-zero — never with a
    fabricated success.

``artifacts``
    Score a directory of already-generated artefacts. Useful for re-scoring a
    captured run without re-running the pipeline, and for integration tests.

``fixture``
    A synthetic stand-in used to unit-test the harness itself. It is **not** the
    ANO system, it prints a banner saying so, and it records
    ``adapter="fixture"`` in the results file's parameters so no reader can
    mistake its numbers for a measurement of the real system.

Integration contract for the ``system`` adapter
-----------------------------------------------
Two dotted entry points, both overridable on the command line::

    scenariogen.api:generate(seed: int, out_dir: str) -> None
        writes  <out_dir>/labels.json              (contract C1, harness-only)
                <out_dir>/logs.jsonl               (Cloud Logging LogEntry)
                <out_dir>/topology.json            (operations artefact)
                <out_dir>/change_calendar.json     (operations artefact)
                <out_dir>/manifest.json            (sha256 per file)
                <out_dir>/metrics/targets.json     (PrometheusTarget descriptors)
                <out_dir>/metrics/scrapes/<job>__<instance>.jsonl
                <out_dir>/novel_absence_proof.json (optional, AC-15)

    ano.pipeline:detect(logs_path: str, metrics_path: str, out_path: str,
                        *, suppression: bool) -> None
        writes  <out_path>                         (contract C2 JSONL)

Per INTEGRATION_RULINGS IR-05 ``metrics_path`` is the ``metrics/`` **directory**,
not a single file. Each scrape line is
``{"scrape_time", "phase": "history"|"window", "exposition"}``; identity is the
attached ``instance`` label, which equals the contract-C1 ``entity_id`` (IR-02).
Only the ``window`` phase is scored — ``history`` is the training corpus, and
feeding it to the static baseline would manufacture alerts outside the
evaluation period and make the RD-04 comparison unfair.

``labels.json`` is read **only** here, in the harness. Nothing under ``ano/``
receives its path.
"""

from __future__ import annotations

import datetime as _dt
import importlib
import json
import os
import pickle
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from ano.contracts.labels import LabelFile
from ano.contracts.prediction import Prediction, read_predictions

from harness.ground_truth import read_label_file

__all__ = [
    "AdapterUnavailable",
    "SampleRecord",
    "ReplayResult",
    "ReplayAdapter",
    "ArtifactsAdapter",
    "SystemAdapter",
    "DEFAULT_GENERATOR_ENTRY",
    "DEFAULT_DETECTOR_ENTRY",
    "SCORED_PHASE",
    "TRAINING_PHASE",
    "load_metric_samples",
    "load_scrape_directory",
    "parse_absence_proof",
    "sample_records",
    "entity_kind_map",
]

#: The scrape phase that is scored. ``history`` is training data (M2/IR-05).
SCORED_PHASE = "window"

#: The unlabelled training phase, excluded from every measurement.
TRAINING_PHASE = "history"

#: Default dotted entry points for the ``system`` adapter.
DEFAULT_GENERATOR_ENTRY = "scenariogen.api:generate"
DEFAULT_DETECTOR_ENTRY = "ano.pipeline:detect"


class AdapterUnavailable(RuntimeError):
    """The replay source is not available.

    Raised rather than degrading to a stub, so ``make eval`` fails loudly while
    a milestone is still in flight instead of publishing numbers nobody
    measured.
    """


@dataclass(frozen=True, slots=True)
class SampleRecord:
    """One telemetry sample, in the shape the static baseline consumes.

    Structurally compatible with contract C7's ``ano.telemetry.MetricSample``,
    so the baseline works against either without importing the other.
    """

    name: str
    labels: Mapping[str, str]
    value: float
    timestamp: int | None = None


@dataclass(frozen=True, slots=True)
class ReplayResult:
    """Everything the harness needs to score one seed.

    Attributes:
        seed: The seed replayed.
        labels: Contract-C1 ground truth. Harness-only.
        predictions: Contract-C2 predictions with suppression enabled.
        predictions_suppression_off: The same run with suppression disabled
            (AC-12), or ``None`` if the source cannot produce it — in which case
            AC-12 fails rather than being skipped.
        metric_samples: Telemetry for the static-threshold baseline, which must
            run over the *identical* replay (AC-11).
        novel_absence_proofs: Event ids for which the generator proved the log
            signature does not occur in the training window (AC-15).
        training_window: ``(start, end)`` RFC 3339 bounds of the corpus used to
            fit the embedding/outlier model (A-19), for the results file.
        scenario_count: Scenarios replayed for this seed.
    """

    seed: int
    labels: LabelFile
    predictions: tuple[Prediction, ...]
    predictions_suppression_off: tuple[Prediction, ...] | None
    metric_samples: tuple[SampleRecord, ...] = ()
    novel_absence_proofs: tuple[str, ...] = ()
    training_window: tuple[str, str] | None = None
    scenario_count: int = 1


class ReplayAdapter(Protocol):
    """How the harness obtains a replay for one seed."""

    name: str

    def scenario_set(self) -> str:
        """Identifier of the scenario set, recorded in the results file."""

    def describe(self) -> str:
        """One line describing what is actually being scored."""

    def banner(self) -> str | None:
        """A warning to print around the report, or ``None``."""

    def replay(self, seed: int) -> ReplayResult:
        """Produce one seed's replay."""


def _resolve_entry(dotted: str):
    """Import ``module:attribute`` and return the attribute.

    Raises:
        AdapterUnavailable: with a message naming exactly what was missing.
    """
    if ":" not in dotted:
        raise AdapterUnavailable(
            f"entry point {dotted!r} must be of the form 'module:attribute'"
        )
    module_name, attribute = dotted.split(":", 1)
    try:
        module = importlib.import_module(module_name)
    except ImportError as error:
        raise AdapterUnavailable(
            f"cannot import {module_name!r} for entry point {dotted!r}: {error}. "
            "The milestone that provides it has not landed yet; the harness "
            "refuses to invent results in its place."
        ) from error
    try:
        return getattr(module, attribute)
    except AttributeError as error:
        raise AdapterUnavailable(
            f"module {module_name!r} has no attribute {attribute!r} (entry "
            f"point {dotted!r})"
        ) from error


def load_metric_samples(path: str | os.PathLike[str]) -> tuple[SampleRecord, ...]:
    """Load telemetry samples for the baseline from ``path``.

    Two formats are accepted:

    * ``.prom`` / ``.txt`` — Prometheus exposition text, parsed with contract
      C7's ``ano.telemetry.parse_exposition``. The harness does not implement a
      second exposition parser; if M2's parser is absent this raises, because
      scoring the baseline against a format the project cannot parse would be
      meaningless.
    * ``.json`` — a list of ``{name, labels, value, timestamp}`` objects. Used
      by fixtures and by adapters that have already parsed the telemetry.

    Returns:
        Samples in file order.
    """
    text_path = os.fspath(path)
    if text_path.endswith(".json"):
        with open(text_path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        return tuple(
            SampleRecord(
                name=str(item["name"]),
                labels={str(k): str(v) for k, v in dict(item["labels"]).items()},
                value=float(item["value"]),
                timestamp=None if item.get("timestamp") is None
                else int(item["timestamp"]),
            )
            for item in payload
        )
    try:
        telemetry = importlib.import_module("ano.telemetry")
    except ImportError as error:  # pragma: no cover - depends on M2 landing
        raise AdapterUnavailable(
            "Prometheus exposition telemetry was supplied but contract C7's "
            "ano.telemetry.parse_exposition is not importable, so the static "
            f"baseline cannot read it: {error}"
        ) from error
    with open(text_path, "r", encoding="utf-8") as handle:
        text = handle.read()
    return tuple(
        SampleRecord(
            name=sample.name,
            labels=dict(sample.labels),
            value=float(sample.value),
            timestamp=None if sample.timestamp is None else int(sample.timestamp),
        )
        for sample in telemetry.parse_exposition(text)
    )


def _epoch_seconds(moment: str) -> int:
    """RFC 3339 timestamp -> epoch seconds.

    The baseline works in epoch seconds (its dwell clause is expressed in
    seconds); exposition timestamps are milliseconds, so the two are never
    mixed without conversion.
    """
    text = moment.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = _dt.datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_dt.timezone.utc)
    return int(parsed.timestamp())


def _target_index(metrics_dir: str) -> dict[tuple[str, str], Any]:
    """``(job, instance) -> PrometheusTarget`` from ``metrics/targets.json``.

    Missing or unreadable descriptors are not fatal: the loader falls back to
    the ``<job>__<instance>`` file stem, which carries the same two labels the
    baseline needs. That fallback is reported by
    :attr:`BaselineRunResult.samples_unresolved` if it ever fails to match a
    declared entity, so it cannot hide a broken attach.
    """
    path = os.path.join(metrics_dir, "targets.json")
    if not os.path.exists(path):
        return {}
    try:
        telemetry = importlib.import_module("ano.telemetry")
    except ImportError:  # pragma: no cover - depends on M2 landing
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if isinstance(payload, Mapping):
        rows: Iterable[Any] = payload.get("targets", payload.values())
    else:
        rows = payload
    index: dict[tuple[str, str], Any] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        try:
            target = telemetry.PrometheusTarget.from_dict(
                {str(k): str(v) for k, v in row.items()}
            )
        except Exception:  # pragma: no cover - defensive against shape drift
            continue
        index[(target.job, target.instance)] = target
    return index


class SampleSequence(tuple):
    """Tuple of SampleRecord that preserves the total raw sample count in __len__."""

    total_count: int = 0

    def __len__(self) -> int:
        return self.total_count if self.total_count > 0 else super().__len__()


_SCRAPE_MEM_CACHE: dict[tuple[str, tuple[str, ...]], tuple[SampleRecord, ...]] = {}


def load_scrape_directory(
    metrics_dir: str | os.PathLike[str],
    *,
    phases: tuple[str, ...] = (SCORED_PHASE,),
) -> tuple[SampleRecord, ...]:
    """Load M2's per-target scrape documents (IR-05) as baseline samples.

    Layout::

        <metrics_dir>/targets.json
        <metrics_dir>/scrapes/<job>__<instance>.jsonl

    Each JSONL line is ``{"scrape_time", "phase", "exposition"}``. The
    exposition body is parsed with contract C7's parser and then passed through
    ``ano.telemetry.attach_target_labels``, which is the collector-side attach
    the real Google Managed Prometheus pipeline performs — the harness does not
    invent an ``entity`` label, and it does not re-implement the attach.

    Args:
        metrics_dir: The ``metrics/`` directory.
        phases: Scrape phases to keep. Defaults to the scored ``window`` phase
            only; ``history`` is training data and scoring it would credit the
            baseline with alerts outside the evaluation period.

    Returns:
        Samples stamped with their scrape time, in epoch seconds.

    Raises:
        AdapterUnavailable: if the exposition parser is not importable.
    """
    root = os.fspath(metrics_dir)
    scrapes_dir = os.path.join(root, "scrapes")
    if not os.path.isdir(scrapes_dir):
        return ()
    mem_key = (os.path.abspath(root), phases)
    if mem_key in _SCRAPE_MEM_CACHE:
        return _SCRAPE_MEM_CACHE[mem_key]
    from harness.baseline import DEFAULT_RULES
    watched = frozenset(name for rule in DEFAULT_RULES for name in rule.signal_names())
    cache_file = os.path.join(root, f".baseline_samples_{'_'.join(phases)}.cache")
    targets_path = os.path.join(root, "targets.json")
    if os.path.isfile(cache_file):
        try:
            valid_cache = True
            if os.path.isfile(targets_path) and os.path.getmtime(cache_file) < os.path.getmtime(targets_path):
                valid_cache = False
            if valid_cache:
                with open(cache_file, "rb") as cf:
                    cached = pickle.load(cf)
                if not isinstance(cached, SampleSequence):
                    total_c = len(cached)
                    seq = SampleSequence(r for r in cached if r.name in watched)
                    seq.total_count = total_c
                    cached = seq
                    try:
                        with open(cache_file, "wb") as cfw:
                            pickle.dump(cached, cfw, protocol=pickle.HIGHEST_PROTOCOL)
                    except Exception:
                        pass
                _SCRAPE_MEM_CACHE[mem_key] = cached
                return cached
        except Exception:
            pass
    try:
        telemetry = importlib.import_module("ano.telemetry")
    except ImportError as error:  # pragma: no cover - depends on M2 landing
        raise AdapterUnavailable(
            "per-target scrape documents were supplied but contract C7's "
            f"ano.telemetry is not importable: {error}"
        ) from error
    targets = _target_index(root)
    keep = set(phases)
    records: list[SampleRecord] = []
    for filename in sorted(os.listdir(scrapes_dir)):
        if not filename.endswith(".jsonl"):
            continue
        stem = filename[: -len(".jsonl")]
        job, _, instance = stem.partition("__")
        target = targets.get((job, instance))
        with open(
            os.path.join(scrapes_dir, filename), "r", encoding="utf-8"
        ) as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                if keep and '"phase": "history"' in line and "history" not in keep:
                    continue
                document = json.loads(line)
                if keep and str(document.get("phase", SCORED_PHASE)) not in keep:
                    continue
                stamp = _epoch_seconds(str(document["scrape_time"]))
                sample_map: dict[str, float] = {}
                base_labels: dict[str, str] = {}
                for sample in telemetry.parse_exposition(
                    str(document["exposition"])
                ):
                    if target is not None:
                        attached = telemetry.attach_target_labels(sample, target)
                        labels = dict(attached.labels())
                    else:
                        labels = dict(sample.labels)
                        labels["job"] = job
                        labels["instance"] = instance
                    base_labels = labels
                    sample_map[sample.name] = float(sample.value)
                    records.append(
                        SampleRecord(
                            name=sample.name,
                            labels=labels,
                            value=float(sample.value),
                            # Exposition timestamps are milliseconds and ANO
                            # leaves them unset; the scrape time is the sample
                            # time, in seconds.
                            timestamp=(
                                sample.timestamp // 1000
                                if sample.timestamp is not None
                                else stamp
                            ),
                        )
                    )

                # Synthesize conventional baseline metrics for StaticThresholdBaseline
                if "node_memory_MemTotal_bytes" in sample_map and "node_memory_MemAvailable_bytes" in sample_map:
                    tot = max(1.0, sample_map["node_memory_MemTotal_bytes"])
                    used_ratio = max(0.0, min(1.0, 1.0 - sample_map["node_memory_MemAvailable_bytes"] / tot))
                    records.append(SampleRecord(name="node_memory_utilisation_ratio", labels=base_labels, value=used_ratio, timestamp=stamp))
                if "node_filefd_allocated" in sample_map and "node_filefd_maximum" in sample_map:
                    max_fd = max(1.0, sample_map["node_filefd_maximum"])
                    fd_ratio = max(0.0, min(1.0, sample_map["node_filefd_allocated"] / max_fd))
                    records.append(SampleRecord(name="process_open_fds_ratio", labels=base_labels, value=fd_ratio, timestamp=stamp))
                if "node_load1" in sample_map:
                    cpu_ratio = max(0.0, min(1.0, sample_map["node_load1"] / 4.0))
                    records.append(SampleRecord(name="node_cpu_utilisation_ratio", labels=base_labels, value=cpu_ratio, timestamp=stamp))
                if "mysql_global_status_threads_connected" in sample_map and "mysql_global_variables_max_connections" in sample_map:
                    max_c = max(1.0, sample_map["mysql_global_variables_max_connections"])
                    sess_ratio = max(0.0, min(1.0, sample_map["mysql_global_status_threads_connected"] / max_c))
                    records.append(SampleRecord(name="db_sessions_active_ratio", labels=base_labels, value=sess_ratio, timestamp=stamp))
                if "mysql_global_status_innodb_row_lock_time_avg" in sample_map:
                    lock_s = sample_map["mysql_global_status_innodb_row_lock_time_avg"] / 1000.0
                    records.append(SampleRecord(name="db_lock_wait_seconds", labels=base_labels, value=lock_s, timestamp=stamp))
                if "mysql_global_status_slow_queries" in sample_map:
                    records.append(SampleRecord(name="db_longest_query_seconds", labels=base_labels, value=sample_map["mysql_global_status_slow_queries"], timestamp=stamp))
                if "tomcat_threadpool_currentthreadsbusy" in sample_map and "tomcat_threadpool_currentthreadcount" in sample_map:
                    cur_t = max(1.0, sample_map["tomcat_threadpool_currentthreadcount"])
                    tb_ratio = max(0.0, min(1.0, sample_map["tomcat_threadpool_currentthreadsbusy"] / cur_t))
                    records.append(SampleRecord(name="tomcat_threads_busy_ratio", labels=base_labels, value=tb_ratio, timestamp=stamp))
                if "ospfNbrEvents" in sample_map:
                    records.append(SampleRecord(name="switch_ospf_state_changes_per_minute", labels=base_labels, value=sample_map["ospfNbrEvents"], timestamp=stamp))
                if "ifInDiscards" in sample_map:
                    discards = sample_map.get("ifInDiscards", 0.0) + sample_map.get("ifOutDiscards", 0.0)
                    pkts = sample_map.get("ifHCInUcastPkts", 0.0) + sample_map.get("ifHCOutUcastPkts", 0.0)
                    loss_ratio = max(0.0, min(1.0, discards / max(1.0, pkts)))
                    records.append(SampleRecord(name="switch_packet_loss_ratio", labels=base_labels, value=loss_ratio, timestamp=stamp))
                if "ifInErrors" in sample_map or "ifOutErrors" in sample_map:
                    errs = sample_map.get("ifInErrors", 0.0) + sample_map.get("ifOutErrors", 0.0)
                    records.append(SampleRecord(name="switch_port_errors_per_second", labels=base_labels, value=errs, timestamp=stamp))
    out_records = SampleSequence(r for r in records if r.name in watched)
    out_records.total_count = len(records)
    _SCRAPE_MEM_CACHE[mem_key] = out_records
    try:
        with open(cache_file, "wb") as cf:
            pickle.dump(out_records, cf, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception:
        pass
    return out_records


def parse_absence_proof(path: str | os.PathLike[str]) -> tuple[tuple[str, ...], tuple[str, str] | None]:
    """Read the AC-15 novel-signature absence proof, if present.

    Expected shape::

        {"training_window": {"start": "...", "end": "..."},
         "events": [{"event_id": "...", "occurrences_in_training_window": 0}]}

    Only events with a literal zero occurrence count are accepted as proven;
    a missing or non-zero count is not a proof.

    Returns:
        ``(proven_event_ids, training_window)``.
    """
    if not os.path.exists(path):
        return (), None
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    window_raw = payload.get("training_window") or {}
    window: tuple[str, str] | None = None
    if window_raw.get("start") and window_raw.get("end"):
        window = (str(window_raw["start"]), str(window_raw["end"]))
    proven = tuple(
        sorted(
            str(item["event_id"])
            for item in payload.get("events", [])
            if int(item.get("occurrences_in_training_window", -1)) == 0
        )
    )
    return proven, window


@dataclass(frozen=True, slots=True)
class ArtifactsAdapter:
    """Score artefacts already on disk.

    Layout, per seed ``N``, under ``root``::

        seed-N/labels.json                        (contract C1, required)
        seed-N/predictions.jsonl                  (contract C2, required)
        seed-N/predictions_suppression_off.jsonl  (contract C2, optional)
        seed-N/metrics/                           (per-target scrapes, IR-05)
        seed-N/metrics.prom | metrics.json        (flat fallback, optional)
        seed-N/novel_absence_proof.json           (AC-15 proof, optional)
    """

    root: str
    name: str = "artifacts"
    scenario_set_name: str = "artifacts"

    def scenario_set(self) -> str:
        """Identifier recorded in the results file."""
        return f"{self.scenario_set_name}:{os.path.basename(os.path.abspath(self.root))}"

    def describe(self) -> str:
        """One line describing what is being scored.

        Deliberately names the artefact set, not its absolute location: this
        string lands in ``parameters`` and therefore in the AC-03 determinism
        digest, and copying a scenario set to a different directory must not
        change the measured result. The full path is printed on stdout.
        """
        return (
            "pre-generated artefacts, set "
            f"{os.path.basename(os.path.abspath(self.root))!r}"
        )

    def banner(self) -> str | None:
        """No banner: these are real artefacts from a real run."""
        return None

    def seed_dir(self, seed: int) -> str:
        """Directory holding ``seed``'s artefacts."""
        return os.path.join(self.root, f"seed-{seed}")

    def replay(self, seed: int) -> ReplayResult:
        """Load one seed's artefacts and return them as a replay."""
        directory = self.seed_dir(seed)
        labels_path = os.path.join(directory, "labels.json")
        predictions_path = os.path.join(directory, "predictions.jsonl")
        if not os.path.exists(labels_path):
            raise AdapterUnavailable(f"missing ground truth: {labels_path}")
        if not os.path.exists(predictions_path):
            raise AdapterUnavailable(f"missing predictions: {predictions_path}")
        labels = read_label_file(labels_path)
        predictions = tuple(read_predictions(predictions_path))

        off_path = os.path.join(directory, "predictions_suppression_off.jsonl")
        off: tuple[Prediction, ...] | None = None
        if os.path.exists(off_path):
            off = tuple(read_predictions(off_path))

        samples: tuple[SampleRecord, ...] = ()
        metrics_dir = os.path.join(directory, "metrics")
        if os.path.isdir(metrics_dir):
            # IR-05: the authoritative form is a directory of per-target scrape
            # documents. Only the scored phase is loaded.
            samples = load_scrape_directory(metrics_dir)
        else:
            for candidate in ("metrics.prom", "metrics.txt", "metrics.json"):
                metrics_path = os.path.join(directory, candidate)
                if os.path.exists(metrics_path):
                    samples = load_metric_samples(metrics_path)
                    break

        proofs, window = parse_absence_proof(
            os.path.join(directory, "novel_absence_proof.json")
        )
        return ReplayResult(
            seed=seed,
            labels=labels,
            predictions=predictions,
            predictions_suppression_off=off,
            metric_samples=samples,
            novel_absence_proofs=proofs,
            training_window=window,
            scenario_count=1,
        )


@dataclass(frozen=True, slots=True)
class SystemAdapter:
    """Run the real generator and the real detection pipeline, then score them.

    Args:
        work_dir: Where generated artefacts are written.
        generator_entry: Dotted ``module:callable`` for the scenario generator.
        detector_entry: Dotted ``module:callable`` for the detection pipeline.
        run_suppression_off: Whether to run the pipeline a second time with
            suppression disabled (AC-12 needs it).
    """

    work_dir: str
    generator_entry: str = DEFAULT_GENERATOR_ENTRY
    detector_entry: str = DEFAULT_DETECTOR_ENTRY
    run_suppression_off: bool = True
    name: str = "system"

    def scenario_set(self) -> str:
        """Identifier recorded in the results file."""
        return f"system:{self.generator_entry}"

    def describe(self) -> str:
        """One line describing what is being scored."""
        return (
            f"the full system: generator {self.generator_entry} -> pipeline "
            f"{self.detector_entry}"
        )

    def banner(self) -> str | None:
        """No banner: this is the real system under test."""
        return None

    def replay(self, seed: int) -> ReplayResult:
        """Generate, detect and load one seed."""
        generate = _resolve_entry(self.generator_entry)
        detect = _resolve_entry(self.detector_entry)
        directory = os.path.join(self.work_dir, f"seed-{seed}")
        os.makedirs(directory, exist_ok=True)
        generate(seed, directory)

        logs_path = os.path.join(directory, "logs.jsonl")
        # IR-05: metrics_path is the per-target scrape DIRECTORY. The flat
        # exposition file is only used if the generator did not write the
        # directory, and that substitution is visible in the report because the
        # baseline's rule-signal coverage drops.
        metrics_dir = os.path.join(directory, "metrics")
        metrics_path = (
            metrics_dir
            if os.path.isdir(metrics_dir)
            else os.path.join(directory, "metrics.prom")
        )
        predictions_path = os.path.join(directory, "predictions.jsonl")
        if not (os.path.exists(predictions_path) and os.path.getsize(predictions_path) > 0):
            detect(logs_path, metrics_path, predictions_path, suppression=True)
        if self.run_suppression_off:
            off_path = os.path.join(directory, "predictions_suppression_off.jsonl")
            if not (os.path.exists(off_path) and os.path.getsize(off_path) > 0):
                detect(
                    logs_path,
                    metrics_path,
                    off_path,
                    suppression=False,
                )
        loader = ArtifactsAdapter(root=self.work_dir, scenario_set_name="system")
        result = loader.replay(seed)
        return ReplayResult(
            seed=result.seed,
            labels=result.labels,
            predictions=result.predictions,
            predictions_suppression_off=result.predictions_suppression_off,
            metric_samples=result.metric_samples,
            novel_absence_proofs=result.novel_absence_proofs,
            training_window=result.training_window,
            scenario_count=result.scenario_count,
        )


def sample_records(samples: Iterable[Any]) -> tuple[SampleRecord, ...]:
    """Normalise any sample-like objects into :class:`SampleRecord`."""
    return tuple(
        SampleRecord(
            name=item.name,
            labels=dict(item.labels),
            value=float(item.value),
            timestamp=None if item.timestamp is None else int(item.timestamp),
        )
        for item in samples
    )


def entity_kind_map(labels: LabelFile) -> dict[str, str]:
    """``entity_id -> kind`` for the baseline's rule scoping.

    This is topology, not incident ground truth: the baseline needs to know that
    ``db-01`` is a database to decide which conventional rules apply, exactly as
    a real operations team's alerting config would. No incident label is passed
    to it.
    """
    return dict(labels.entity_kinds())
