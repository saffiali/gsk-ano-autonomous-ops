"""A synthetic stand-in used to test the harness itself.

**This is not the ANO system.** It is a deterministic, parameterised stand-in
that produces a contract-C1 label file, contract-C2 prediction streams and
Prometheus-shaped telemetry, so that:

* every metric can be unit tested against hand-computed expectations,
* adversarial cases (a detector that never fires, one that fires constantly,
  zero genuine incidents, zero benign events, everything suppressed) can be
  exercised before the real detectors land, and
* the harness can be run end to end and its printed report inspected while M2,
  M3 and M4 are still in flight.

Every run through this adapter prints a banner and records ``adapter="fixture"``
plus an explicit ``system_under_test`` string in the results file's parameters,
so a fixture score can never be read as a measurement of the real system.

Determinism: all randomness comes from ``ano.contracts.determinism.rng``. The
builtin ``hash()`` is never used.
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Sequence
from dataclasses import dataclass, replace

from ano.contracts.common import UTC
from ano.contracts.determinism import rng
from ano.contracts.labels import (
    Entity,
    GroundTruthEvent,
    LabelFile,
    RootCause,
    Window,
)
from ano.contracts.prediction import Attribution, Evidence, Prediction, Signal

from harness.adapters import ReplayResult, SampleRecord

__all__ = [
    "FIXTURE_BANNER",
    "EPOCH",
    "FixtureProfile",
    "FixtureAdapter",
    "build_labels",
    "build_samples",
    "build_predictions",
]

FIXTURE_BANNER = (
    "FIXTURE ADAPTER — the numbers in this report score a SYNTHETIC STAND-IN, "
    "not the ANO system. They exercise the harness's own scoring logic and "
    "prove nothing about the detectors. A real measurement requires "
    "--adapter system (or --adapter artifacts over real generated artefacts)."
)

#: Fixed start of every fixture window; the fixture is a test article, so its
#: absolute time base is arbitrary but must be constant for determinism.
EPOCH = _dt.datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)

#: Replay length, in minutes.
WINDOW_MINUTES = 720

#: Telemetry scrape interval, in seconds.
SCRAPE_INTERVAL_S = 60

_ENTITIES: tuple[tuple[str, str], ...] = (
    ("app-01", "app_server"),
    ("app-02", "app_server"),
    ("app-03", "app_server"),
    ("app-04", "app_server"),
    ("app-05", "app_server"),
    ("app-06", "app_server"),
    ("app-07", "app_server"),
    ("db-01", "database"),
    ("db-02", "database"),
    ("sw-01", "switch"),
    ("sw-02", "switch"),
    ("sw-03", "switch"),
    ("node-01", "node"),
    ("node-02", "node"),
    ("node-03", "node"),
    ("node-04", "node"),
)

# (event_id, kind, onset_min, duration_min, root_domain, root_entity,
#  affected, track, novel)
_EVENT_PLAN: tuple[tuple[str, str, int, int, str, str, tuple[str, ...], str, bool], ...] = (
    (
        "E1",
        "thread_starvation",
        120,
        30,
        "application",
        "app-01",
        ("app-01",),
        "unresponsiveness",
        False,
    ),
    (
        "E2",
        "memory_leak",
        240,
        40,
        "application",
        "node-01",
        ("node-01",),
        "unresponsiveness",
        False,
    ),
    (
        "E3",
        "socket_exhaustion",
        380,
        30,
        "application",
        "app-02",
        ("app-02",),
        "unresponsiveness",
        True,
    ),
    (
        "E4",
        "thread_starvation",
        520,
        30,
        "application",
        "node-02",
        ("node-02",),
        "unresponsiveness",
        False,
    ),
    (
        "E5",
        "db_lock_contention",
        200,
        50,
        "database",
        "db-01",
        ("db-01", "app-03", "app-04", "app-05"),
        "cross_domain",
        False,
    ),
    (
        "E6",
        "db_lock_contention",
        430,
        40,
        "database",
        "db-02",
        ("db-02", "app-03", "app-04"),
        "cross_domain",
        False,
    ),
    (
        "E7",
        "ospf_flap",
        600,
        35,
        "network",
        "sw-01",
        ("sw-01", "sw-02", "app-01"),
        "cross_domain",
        False,
    ),
    (
        "B1",
        "patching_window",
        660,
        40,
        "application",
        "node-03",
        ("node-03", "node-04", "app-06", "app-07"),
        "benign",
        False,
    ),
    (
        "B2",
        "planned_deploy",
        20,
        25,
        "application",
        "app-06",
        ("app-06", "app-07"),
        "benign",
        False,
    ),
    (
        "B3",
        "traffic_surge",
        320,
        30,
        "application",
        "app-06",
        ("app-06", "node-03", "sw-03"),
        "benign",
        False,
    ),
    (
        "N1",
        "normal",
        470,
        40,
        "application",
        "node-04",
        (),
        "normal",
        False,
    ),
)

# entity kind -> (signal name, quiet value, peak value)
_SIGNALS: dict[str, tuple[tuple[str, float, float], ...]] = {
    "node": (
        ("node_cpu_utilisation_ratio", 0.40, 0.98),
        ("node_memory_utilisation_ratio", 0.45, 0.97),
    ),
    "app_server": (
        ("node_cpu_utilisation_ratio", 0.40, 0.98),
        ("tomcat_threads_busy_ratio", 0.35, 0.99),
        ("process_open_fds_ratio", 0.30, 0.95),
    ),
    "database": (
        ("db_sessions_active_ratio", 0.45, 0.97),
        ("db_lock_wait_seconds", 0.20, 14.0),
    ),
    "switch": (
        ("switch_packet_loss_ratio", 0.0005, 0.04),
        ("switch_ospf_state_changes_per_minute", 0.0, 3.0),
    ),
}

#: How far before onset the degradation starts, and when it reaches full
#: intensity. A real fault ramps; a step change would give the static baseline
#: no chance at all and would make it a strawman.
_RAMP_START_S = 3600
_RAMP_FULL_S = 1800

#: Intensity a benign event drives the signals to. Patching and deploys peg the
#: host; a traffic surge is high but not pegged. Both are above the
#: conventional static thresholds, which is precisely why a static baseline is
#: noisy during change windows and a change-aware system need not be.
_BENIGN_INTENSITY: dict[str, float] = {
    "patching_window": 1.0,
    "planned_deploy": 0.98,
    "traffic_surge": 0.95,
}


@dataclass(frozen=True, slots=True)
class FixtureProfile:
    """Behaviour of the synthetic stand-in "detector".

    Every field exists so a test can drive the harness into a specific corner.

    Attributes:
        detect_probability: Chance the stand-in predicts a given genuine event.
        domain_accuracy: Chance it attributes the correct root-cause domain.
        entity_accuracy: Chance it attributes the correct root-cause entity,
            given the domain was right.
        lead_min_s: Minimum lead time of a prediction.
        lead_max_s: Maximum lead time of a prediction.
        benign_false_positive_probability: Chance a benign event raises one.
        collapse: Whether downstream alerts are collapsed into one incident.
        suppress_change_window_alerts: Whether alerts inside a patching or
            deploy window are marked suppressed.
        reactive_only: Emit every prediction just *after* onset — the purely
            reactive detector RD-03 exists to exclude.
        never_fire: Emit nothing at all.
        always_fire: Emit an alert on every entity at every scrape — the
            permanently-firing detector RD-02 and RD-01 exist to catch.
        pre_horizon_only: Emit each prediction earlier than the 7200 s horizon.
        flag_novel_signature: Whether to carry a semantic-outlier signal on the
            novel-signature incident.
    """

    detect_probability: float = 1.0
    domain_accuracy: float = 0.90
    entity_accuracy: float = 0.95
    lead_min_s: int = 1000
    lead_max_s: int = 3000
    benign_false_positive_probability: float = 0.05
    collapse: bool = True
    suppress_change_window_alerts: bool = True
    reactive_only: bool = False
    never_fire: bool = False
    always_fire: bool = False
    pre_horizon_only: bool = False
    flag_novel_signature: bool = True


def _moment(minutes: float) -> _dt.datetime:
    """Absolute time ``minutes`` after :data:`EPOCH`."""
    return EPOCH + _dt.timedelta(minutes=minutes)


def build_labels(
    seed: int, *, plan: Sequence[tuple] = _EVENT_PLAN, jitter: bool = True
) -> LabelFile:
    """Build one seed's contract-C1 ground truth.

    Args:
        seed: Seed; drives per-seed onset jitter so the five seeds genuinely
            differ (AC-03 requires distinct scenario content, not five copies).
        plan: The event plan to realise.
        jitter: Whether to apply the per-seed onset jitter.

    Returns:
        A validated :class:`LabelFile`.
    """
    generator = rng(seed, "harness", "fixture", "labels")
    events: list[GroundTruthEvent] = []
    for row in plan:
        (
            event_id,
            kind,
            onset_min,
            duration_min,
            domain,
            root_entity,
            affected,
            track,
            novel,
        ) = row
        offset = generator.randint(-5, 5) if jitter else 0
        onset = _moment(onset_min + offset)
        events.append(
            GroundTruthEvent(
                event_id=f"{event_id}-s{seed}",
                is_genuine=kind
                in {
                    "thread_starvation",
                    "memory_leak",
                    "socket_exhaustion",
                    "db_lock_contention",
                    "ospf_flap",
                    "packet_loss",
                },
                kind=kind,
                onset=onset,
                end=onset + _dt.timedelta(minutes=duration_min),
                root_cause=RootCause(domain=domain, entity=root_entity),
                affected_entities=tuple(affected),
                track=track,
                novel_log_signature=novel,
            )
        )
    return LabelFile(
        seed=seed,
        window=Window(start=EPOCH, end=_moment(WINDOW_MINUTES)),
        entities=tuple(
            Entity(entity_id=name, kind=kind) for name, kind in _ENTITIES
        ),
        events=tuple(events),
    )


def _intensity(event: GroundTruthEvent, moment: _dt.datetime) -> float:
    """How hard ``event`` is driving its entities' signals at ``moment``."""
    if event.kind == "normal":
        return 0.0
    if not event.is_genuine:
        if event.onset <= moment <= event.end:
            return _BENIGN_INTENSITY.get(event.kind, 0.0)
        return 0.0
    ramp_start = event.onset - _dt.timedelta(seconds=_RAMP_START_S)
    ramp_full = event.onset - _dt.timedelta(seconds=_RAMP_FULL_S)
    if moment < ramp_start:
        return 0.0
    if moment >= ramp_full and moment <= event.end:
        return 1.0
    if moment > event.end:
        return 0.0
    span = (ramp_full - ramp_start).total_seconds()
    return (moment - ramp_start).total_seconds() / span


def build_samples(labels: LabelFile) -> tuple[SampleRecord, ...]:
    """Telemetry for the static-threshold baseline, derived from the labels.

    The generator that will replace this fixture derives telemetry from its own
    scenario definition in exactly the same way; the harness itself never uses
    these samples for scoring, only the baseline does, and the baseline sees
    only metric values and entity kinds — never an incident label.
    """
    kinds = labels.entity_kinds()
    per_entity: dict[str, list[GroundTruthEvent]] = {name: [] for name in kinds}
    for event in labels.events:
        touched = {event.root_cause.entity, *event.affected_entities}
        for name in touched:
            if name in per_entity:
                per_entity[name].append(event)

    samples: list[SampleRecord] = []
    total_steps = int(
        (labels.window.end - labels.window.start).total_seconds()
        // SCRAPE_INTERVAL_S
    )
    for step in range(total_steps):
        moment = labels.window.start + _dt.timedelta(seconds=step * SCRAPE_INTERVAL_S)
        stamp = int(moment.timestamp())
        for name in sorted(kinds):
            kind = kinds[name]
            intensity = 0.0
            for event in per_entity[name]:
                intensity = max(intensity, _intensity(event, moment))
            for signal, quiet, peak in _SIGNALS.get(kind, ()):
                value = quiet + (peak - quiet) * intensity
                samples.append(
                    SampleRecord(
                        name=signal,
                        labels={"instance": name, "job": kind},
                        value=round(value, 6),
                        timestamp=stamp,
                    )
                )
    return tuple(samples)


def _signals_for(event: GroundTruthEvent, novel: bool) -> tuple[Signal, ...]:
    """Driving signals carried on a stand-in prediction (contract C2)."""
    base = (
        Signal(
            name="thread_pool_saturation",
            value=0.94,
            baseline=0.41,
            contribution=0.6,
        ),
        Signal(
            name="io_wait_ratio", value=0.28, baseline=0.06, contribution=0.4
        ),
    )
    if novel:
        return base + (
            Signal(
                name="semantic_outlier_score",
                value=0.93,
                baseline=0.12,
                contribution=0.8,
            ),
        )
    return base


def _attribution(
    event: GroundTruthEvent, generator, profile: FixtureProfile
) -> Attribution:
    """The stand-in's root-cause call, right with the profile's probability."""
    wrong_domain = {
        "database": "application",
        "network": "application",
        "application": "database",
    }
    domain = event.root_cause.domain
    if generator.random() >= profile.domain_accuracy:
        domain = wrong_domain[event.root_cause.domain]
    entity = event.root_cause.entity
    if generator.random() >= profile.entity_accuracy:
        alternatives = [
            name for name in event.affected_entities if name != entity
        ]
        if alternatives:
            entity = alternatives[0]
    return Attribution(
        domain=domain,
        entity=entity,
        evidence=(
            Evidence(
                kind="topology_correlation",
                detail=f"{event.root_cause.entity} is upstream of "
                f"{len(event.affected_entities)} affected entities",
            ),
            Evidence(
                kind="semantic_log_outlier" if event.novel_log_signature
                else "baseline_deviation",
                detail="log embedding distance exceeded the learned radius"
                if event.novel_log_signature
                else "signal exceeded its learned per-hour baseline",
            ),
        ),
    )


def _change_windows(labels: LabelFile) -> tuple[GroundTruthEvent, ...]:
    """Scheduled-change windows the stand-in is aware of."""
    return tuple(
        event
        for event in labels.events
        if event.kind in ("patching_window", "planned_deploy")
    )


def _in_change_window(
    prediction: Prediction, windows: Sequence[GroundTruthEvent]
) -> GroundTruthEvent | None:
    """The change window covering ``prediction``, if any."""
    for window in windows:
        entities = {window.root_cause.entity, *window.affected_entities}
        if (
            prediction.entity in entities
            and window.onset <= prediction.emitted_at <= window.end
        ):
            return window
    return None


def build_predictions(
    labels: LabelFile, profile: FixtureProfile, *, suppression: bool
) -> tuple[Prediction, ...]:
    """Build the stand-in's contract-C2 output for one seed.

    Args:
        labels: The scenario the stand-in is reacting to.
        profile: Stand-in behaviour.
        suppression: Whether change-window suppression is enabled. The two
            settings differ **only** in the suppression flag, which is what
            AC-12's A/B comparison requires.
    """
    if profile.never_fire:
        return ()
    generator = rng(labels.seed, "harness", "fixture", "predictions")
    predictions: list[Prediction] = []

    if profile.always_fire:
        predictions.extend(_always_fire_stream(labels))
    else:
        for event in labels.events:
            if event.is_genuine:
                predictions.extend(
                    _predictions_for_genuine(event, generator, profile)
                )
            elif event.kind in ("patching_window", "planned_deploy", "traffic_surge"):
                if generator.random() < profile.benign_false_positive_probability:
                    predictions.append(_false_positive(event, generator))

    if suppression and profile.suppress_change_window_alerts:
        windows = _change_windows(labels)
        suppressed: list[Prediction] = []
        for prediction in predictions:
            window = _in_change_window(prediction, windows)
            if window is None:
                suppressed.append(prediction)
            else:
                suppressed.append(
                    prediction.with_suppression(
                        f"explained by scheduled change {window.event_id} "
                        f"({window.kind})"
                    )
                )
        predictions = suppressed

    predictions.sort(key=lambda item: (item.emitted_at, item.prediction_id))
    return tuple(predictions)


def _predictions_for_genuine(
    event: GroundTruthEvent, generator, profile: FixtureProfile
) -> list[Prediction]:
    """Zero or one prediction for a genuine event, collapsed where applicable."""
    if generator.random() >= profile.detect_probability:
        return []
    lead = generator.randint(profile.lead_min_s, profile.lead_max_s)
    if profile.pre_horizon_only:
        emitted_at = event.onset - _dt.timedelta(seconds=7200 + lead)
        time_to_failure = 7200 + lead
    elif profile.reactive_only:
        emitted_at = event.onset + _dt.timedelta(seconds=60)
        time_to_failure = 0
    else:
        emitted_at = event.onset - _dt.timedelta(seconds=lead)
        time_to_failure = lead
    downstream = [
        name for name in event.affected_entities if name != event.root_cause.entity
    ]
    prediction_id = f"pred-{event.event_id}"
    collapsed_from: tuple[str, ...] = ()
    if profile.collapse and len(downstream) >= 2:
        collapsed_from = tuple(
            f"{prediction_id}-downstream-{name}" for name in sorted(downstream)
        )
    prediction = Prediction.from_parts(
        prediction_id=prediction_id,
        emitted_at=emitted_at,
        track=event.track,
        entity=event.root_cause.entity,
        time_to_failure_s=time_to_failure,
        confidence=round(0.6 + 0.35 * generator.random(), 4),
        signals=_signals_for(
            event, event.novel_log_signature and profile.flag_novel_signature
        ),
        root_cause=_attribution(event, generator, profile),
        recommended_remediation="Drain the affected pool and clear the "
        "upstream contention before failure.",
        incident_id=prediction_id,
        collapsed_from=collapsed_from,
    )
    result = [prediction]
    if not profile.collapse and downstream:
        # No collapse: one separate incident per downstream entity, which is
        # exactly the alert storm AC-13 forbids.
        for name in sorted(downstream):
            storm_id = f"{prediction_id}-downstream-{name}"
            result.append(
                replace(
                    prediction,
                    prediction_id=storm_id,
                    entity=name,
                    incident_id=storm_id,
                    collapsed_from=(),
                )
            )
    return result


def _false_positive(event: GroundTruthEvent, generator) -> Prediction:
    """One alert raised on a benign event."""
    offset = generator.randint(60, max(120, int(event.duration_s) - 60))
    emitted_at = event.onset + _dt.timedelta(seconds=offset)
    prediction_id = f"pred-fp-{event.event_id}"
    return Prediction.from_parts(
        prediction_id=prediction_id,
        emitted_at=emitted_at,
        track="unresponsiveness",
        entity=event.root_cause.entity,
        time_to_failure_s=900,
        confidence=0.51,
        signals=(
            Signal(
                name="cpu_saturation", value=0.95, baseline=0.42, contribution=1.0
            ),
        ),
        root_cause=Attribution(
            domain="application",
            entity=event.root_cause.entity,
            evidence=(Evidence(kind="baseline_deviation", detail="cpu above band"),),
        ),
        recommended_remediation="Investigate host saturation.",
        incident_id=prediction_id,
    )


def _always_fire_stream(labels: LabelFile) -> list[Prediction]:
    """A detector that fires on every entity every 10 minutes, forever.

    RD-01's ``fpr_strict`` and ``unmatched_alert_count`` exist to make this
    behaviour visible; the fixture provides it so those metrics are tested
    against the case they were written for.
    """
    predictions: list[Prediction] = []
    step = _dt.timedelta(minutes=10)
    for entity in labels.entities:
        moment = labels.window.start
        index = 0
        while moment < labels.window.end:
            prediction_id = f"pred-always-{entity.entity_id}-{index}"
            predictions.append(
                Prediction.from_parts(
                    prediction_id=prediction_id,
                    emitted_at=moment,
                    track="unresponsiveness",
                    entity=entity.entity_id,
                    time_to_failure_s=1800,
                    confidence=0.5,
                    signals=(
                        Signal(
                            name="always_on",
                            value=1.0,
                            baseline=0.0,
                            contribution=1.0,
                        ),
                    ),
                    root_cause=Attribution(
                        domain="application", entity=entity.entity_id
                    ),
                    recommended_remediation="n/a",
                    incident_id=prediction_id,
                )
            )
            moment += step
            index += 1
    return predictions


@dataclass(frozen=True, slots=True)
class FixtureAdapter:
    """Replay adapter over the synthetic stand-in.

    Attributes:
        profile: Stand-in behaviour.
        with_absence_proof: Whether to supply the AC-15 training-window absence
            proof the generator would normally produce.
        plan: Event plan; overridable so tests can inject a scenario with, for
            example, no genuine incidents at all.
    """

    profile: FixtureProfile = FixtureProfile()
    with_absence_proof: bool = True
    plan: tuple = _EVENT_PLAN
    name: str = "fixture"

    def scenario_set(self) -> str:
        """Identifier recorded in the results file."""
        return "fixture:synthetic-stand-in"

    def describe(self) -> str:
        """One line describing what is being scored."""
        return (
            "a synthetic stand-in for the ANO detectors (harness self-test "
            "article; NOT the system under test)"
        )

    def banner(self) -> str | None:
        """The loud warning printed around every fixture report."""
        return FIXTURE_BANNER

    def replay(self, seed: int) -> ReplayResult:
        """Build one seed's synthetic replay."""
        labels = build_labels(seed, plan=self.plan)
        predictions = build_predictions(labels, self.profile, suppression=True)
        off = build_predictions(labels, self.profile, suppression=False)
        proofs: tuple[str, ...] = ()
        if self.with_absence_proof:
            proofs = tuple(
                sorted(
                    event.event_id
                    for event in labels.events
                    if event.novel_log_signature
                )
            )
        return ReplayResult(
            seed=seed,
            labels=labels,
            predictions=predictions,
            predictions_suppression_off=off,
            metric_samples=build_samples(labels),
            novel_absence_proofs=proofs,
            training_window=(
                labels.window.start.isoformat().replace("+00:00", "Z"),
                (labels.window.start + _dt.timedelta(minutes=60))
                .isoformat()
                .replace("+00:00", "Z"),
            ),
            scenario_count=len(labels.events),
        )
