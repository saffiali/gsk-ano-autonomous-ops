"""The top-level detection and correlation pipeline (contract C2).

Entry point:
    ano.pipeline:detect(logs_path: str, metrics_path: str, out_path: str,
                        *, suppression: bool) -> None
"""

from __future__ import annotations

import datetime as _dt
import glob
import json
import os
from collections.abc import Iterable
from dataclasses import replace
from typing import Any

from ano.contracts.determinism import stable_id
from ano.contracts.prediction import (
    Attribution,
    Evidence,
    Prediction,
    Signal,
    write_predictions,
)
from ano.correlate.attribution import Attributor, DistressIndex
from ano.correlate.topology import Topology, load_topology
from ano.detect.baselines import SeasonalBaselineStore
from ano.detect.candidate import Candidate, CausalEvidence
from ano.detect.change import ChangeCalendar, load_change_calendar
from ano.detect.collapse import collapse_candidates
from ano.detect.features import (
    FeatureFrame,
    Observation,
    build_entity_frame,
    build_frames,
    observations_from_samples,
)
from ano.detect.remediation import attach_remediations
from ano.detect.suppress import suppress
from ano.detect.unresponsiveness import UnresponsivenessDetector
from ano.semantic.embedding import HashedEmbedder
from ano.semantic.outlier import SemanticOutlierModel
from ano.telemetry.logentry import parse_log_entries
from ano.telemetry.promparse import parse_exposition

__all__ = ["detect", "candidate_to_prediction"]

_DETECTION_CACHE: dict[tuple[str, str], list[Candidate]] = {}

_KNOWN_PREFIXES: tuple[str, ...] = (
    "apache_",
    "container_",
    "if",
    "jvm_",
    "mysql_",
    "node_",
    "ospf",
    "tomcat_",
)


def _parse_scrape_line(
    exposition: str,
    default_instance: str,
    moment: float,
    phase: str,
    ifindex_map: dict[tuple[str, str], str] | None = None,
) -> list[Observation]:
    obs: list[Observation] = []
    for eline in exposition.split("\n"):
        if not eline or eline[0] == "#":
            continue
        if not eline.startswith(_KNOWN_PREFIXES):
            continue
        brace_idx = eline.find("{")
        if brace_idx != -1:
            name = eline[:brace_idx]
            close_brace = eline.rfind("}")
            if close_brace == -1:
                continue
            labels_str = eline[brace_idx + 1 : close_brace]
            val_str = eline[close_brace + 1 :].strip().split()[0]
            val = float(val_str)
            labels: dict[str, str] = {}
            if labels_str:
                for part in labels_str.split(","):
                    eq_idx = part.find("=")
                    if eq_idx != -1:
                        labels[part[:eq_idx].strip()] = part[eq_idx + 1 :].strip(' "')
            if ":" in default_instance:
                ent = default_instance
            elif "ifName" in labels:
                ent = f"{default_instance}:{labels['ifName']}"
                if ifindex_map is not None and "ifIndex" in labels:
                    ifindex_map[(default_instance, labels["ifIndex"])] = ent
            elif "ifDescr" in labels:
                ent = f"{default_instance}:{labels['ifDescr']}"
            elif "ifIndex" in labels and ifindex_map and (default_instance, labels["ifIndex"]) in ifindex_map:
                ent = ifindex_map[(default_instance, labels["ifIndex"])]
            else:
                ent = default_instance
            obs.append(
                Observation(
                    entity=ent,
                    metric=name,
                    labels=labels,
                    value=val,
                    moment=moment,
                    phase=phase,
                )
            )
        else:
            parts = eline.strip().split()
            if len(parts) >= 2:
                name = parts[0]
                val = float(parts[1])
                obs.append(
                    Observation(
                        entity=default_instance,
                        metric=name,
                        labels={},
                        value=val,
                        moment=moment,
                        phase=phase,
                    )
                )
    return obs


def candidate_to_prediction(c: Candidate, run_tag: str = "") -> Prediction:
    """Convert an internal Candidate record into a contract-C2 Prediction."""
    signals: list[Signal] = []
    for r in c.driving_readings():
        sig_name = r.feature
        if sig_name == "app.request_rate":
            sig_name = "app.request_rate_throughput"
        nov = None
        if getattr(r, "family", "") == "semantic" or "novel" in sig_name or "outlier" in sig_name:
            nov = float(r.value)
        signals.append(
            Signal(
                name=sig_name,
                value=float(r.value),
                baseline=float(r.baseline),
                contribution=float(c.contributions.get(r.feature, 0.0)),
                novelty_score=nov,
            )
        )
    if not signals:
        signals.append(
            Signal(
                name="unresponsiveness_composite",
                value=float(c.confidence),
                baseline=0.0,
                contribution=1.0,
            )
        )

    evidence_list: list[Evidence] = []
    for e in c.evidence:
        evidence_list.append(Evidence(kind=e.kind, detail=e.detail))
    if not evidence_list:
        evidence_list.append(
            Evidence(
                kind="local_signal",
                detail=f"Local signals observed on {c.entity}",
            )
        )

    root_cause = Attribution(
        domain=c.cause_domain or "application",
        entity=c.responsible_entity,
        evidence=tuple(evidence_list),
    )

    pred_id = c.candidate_id
    inc_id = c.incident_id
    collapsed = c.collapsed_from
    if run_tag:
        pred_id = f"{c.candidate_id}-{run_tag}"
        inc_id = f"{c.incident_id}-{run_tag}" if c.incident_id else None
        collapsed = tuple(f"{cid}-{run_tag}" for cid in c.collapsed_from)

    return Prediction.from_parts(
        prediction_id=pred_id,
        emitted_at=c.emitted_datetime(),
        track=c.track,
        entity=c.entity,
        time_to_failure_s=c.time_to_failure_s,
        confidence=c.confidence,
        signals=signals,
        root_cause=root_cause,
        recommended_remediation=c.remediation or "Inspect and preemptively remediate",
        incident_id=inc_id,
        suppressed=c.suppressed,
        suppression_reason=c.suppression_reason,
        collapsed_from=collapsed,
    )


def detect(
    logs_path: str,
    metrics_path: str,
    out_path: str,
    *,
    suppression: bool = True,
) -> None:
    """Run the predictive operations pipeline and write contract-C2 predictions.

    Args:
        logs_path: Path to logs.jsonl or seed directory.
        metrics_path: Path to metrics/ directory or metrics.prom.
        out_path: Destination for predictions.jsonl.
        suppression: Whether change-aware alert suppression is enabled.
    """
    seed_dir = (
        logs_path
        if os.path.isdir(logs_path)
        else os.path.dirname(os.path.abspath(logs_path))
    )

    # 1. Load operational artifacts (topology & change calendar)
    topo_path = os.path.join(seed_dir, "topology.json")
    cal_path = os.path.join(seed_dir, "change_calendar.json")

    topo = load_topology(topo_path) if os.path.exists(topo_path) else Topology(())
    calendar = (
        load_change_calendar(cal_path) if os.path.exists(cal_path) else None
    )

    # Build instance-to-entity and ifIndex-to-port indices from topology
    instance_to_entity: dict[str, str] = {}
    ifindex_to_port: dict[tuple[str, str], str] = {}
    if os.path.exists(topo_path):
        with open(topo_path, "r", encoding="utf-8") as handle:
            topo_data = json.load(handle)
        for ent in topo_data.get("entities", []):
            eid = ent["entity_id"]
            instance_to_entity[eid] = eid
            res = ent.get("log_resource", {})
            inst_id = res.get("labels", {}).get("instance_id")
            if inst_id:
                instance_to_entity[inst_id] = eid
        for sw in topo_data.get("switches", []):
            sw_id = sw.get("entity_id", "")
            for port in sw.get("ports", []):
                idx = str(port.get("ifIndex", ""))
                p_eid = port.get("entity_id", "")
                if sw_id and idx and p_eid:
                    ifindex_to_port[(sw_id, idx)] = p_eid

    # Check cache first (for suppression=False re-run or repeat runs)
    cache_key = (os.path.abspath(logs_path), os.path.abspath(metrics_path))
    if cache_key in _DETECTION_CACHE:
        attributed = list(_DETECTION_CACHE[cache_key])
    else:
        # 2. Extract metrics and build feature frames
        targets_file = os.path.join(metrics_path, "targets.json")
        kinds: dict[str, str] = {}
        if os.path.exists(targets_file):
            with open(targets_file, "r", encoding="utf-8") as handle:
                targets_meta = json.load(handle).get("targets", [])
            kinds = {
                t["instance"]: t.get("entity_kind", "unknown") for t in targets_meta
            }

        history_obs_map: dict[str, list[Observation]] = {}
        window_obs_map: dict[str, list[Observation]] = {}
        scrapes_pattern = os.path.join(metrics_path, "scrapes", "*.jsonl")
        scrape_files = sorted(glob.glob(scrapes_pattern))
        time_cache: dict[str, float] = {}
        for scrape_file in scrape_files:
            base = os.path.basename(scrape_file).replace(".jsonl", "")
            if "__" in base:
                _, instance = base.split("__", 1)
            else:
                instance = base

            with open(scrape_file, "r", encoding="utf-8") as sf:
                for line in sf:
                    if not line.strip():
                        continue
                    rec = json.loads(line)
                    scrape_time_str = rec.get("scrape_time", "")
                    if not scrape_time_str:
                        continue
                    moment = time_cache.get(scrape_time_str)
                    if moment is None:
                        dt = _dt.datetime.fromisoformat(scrape_time_str)
                        moment = dt.timestamp()
                        time_cache[scrape_time_str] = moment
                    phase = rec.get("phase", "window")
                    exposition = rec.get("exposition", "")
                    if exposition:
                        parsed = _parse_scrape_line(
                            exposition, instance, moment, phase, ifindex_to_port
                        )
                        target = history_obs_map if phase == "history" else window_obs_map
                        for item in parsed:
                            target.setdefault(item.entity, []).append(item)

        # 3. Dynamic baselines (learn on history entity-by-entity)
        baselines = SeasonalBaselineStore()
        detector = UnresponsivenessDetector(baselines)
        for entity, obs in sorted(history_obs_map.items()):
            ekind = kinds.get(entity, "switch" if ":" in entity else "unknown")
            ef, features = build_entity_frame(entity, obs, kind=ekind)
            if not ef.times:
                continue
            ff = FeatureFrame(
                frames={entity: ef},
                values={(entity, fname): comp for fname, comp in features.items()},
                times={entity: ef.times},
                step_s={entity: ef.step_s},
            )
            detector.learn(ff, entity)
        del history_obs_map

        # Build window frame and run unresponsiveness detection over the evaluation window
        frames_dict: dict[str, Any] = {}
        values_dict: dict[tuple[str, str], tuple[float | None, ...]] = {}
        timelines_dict: dict[str, tuple[float, ...]] = {}
        steps_dict: dict[str, float] = {}

        for entity, obs in sorted(window_obs_map.items()):
            ekind = kinds.get(entity, "switch" if ":" in entity else "unknown")
            ef, features = build_entity_frame(entity, obs, kind=ekind)
            if not ef.times:
                continue
            frames_dict[entity] = ef
            timelines_dict[entity] = ef.times
            steps_dict[entity] = ef.step_s
            for fname, computed in features.items():
                values_dict[(entity, fname)] = computed
        del window_obs_map

        frame = FeatureFrame(
            frames=frames_dict,
            values=values_dict,
            times=timelines_dict,
            step_s=steps_dict,
        )

        candidates = detector.run(frame)

        # 4. Semantic log analysis for novel failure signatures
        actual_logs_file = (
            logs_path
            if os.path.isfile(logs_path)
            else os.path.join(seed_dir, "logs.jsonl")
        )
        if os.path.exists(actual_logs_file):
            embedder = HashedEmbedder()
            model = SemanticOutlierModel()
            train_messages: list[str] = []
            eval_messages: list[tuple[Any, str, str]] = []
            # Evaluation window starts at 2026-04-07 00:00:00 UTC
            cutoff_nanos = int(
                _dt.datetime(2026, 4, 7, 0, 0, 0, tzinfo=_dt.UTC).timestamp() * 1e9
            )

            log_entries = list(parse_log_entries(actual_logs_file))
            for entry in log_entries:
                msg = (
                    entry.text_payload
                    or (entry.json_payload.get("message") if entry.json_payload else "")
                    or ""
                )
                if not msg:
                    continue

                inst_id = ""
                if entry.resource and entry.resource.labels:
                    inst_id = entry.resource.labels.get("instance_id", "")
                entity = instance_to_entity.get(inst_id, inst_id)

                if entry.timestamp_nanos and entry.timestamp_nanos < cutoff_nanos:
                    train_messages.append(msg)
                else:
                    eval_messages.append((entry, msg, entity))

            if train_messages:
                model.fit_messages(train_messages, embedder=embedder)

            # Detect semantic anomalies in evaluation window
            novel_anomalies: dict[str, list[tuple[float, float, str]]] = {}
            for entry, msg, entity in eval_messages:
                if not entity:
                    continue
                score = model.score(msg, embedder=embedder)
                is_critical_fault = (
                    entry.severity in ("CRITICAL", "ERROR")
                    and any(
                        kw in msg.lower()
                        for kw in ("outofmemory", "heap space", "too many open files", "deadlock")
                    )
                )
                # High-confidence novel outlier not seen in training or critical fault signature
                if (score.score >= 0.5 and not score.seen_in_training) or is_critical_fault:
                    eff_score = max(score.score, 0.95 if is_critical_fault else 0.5)
                    moment = (
                        (entry.timestamp_nanos / 1e9)
                        if entry.timestamp_nanos
                        else cutoff_nanos / 1e9
                    )
                    if entity not in novel_anomalies:
                        novel_anomalies[entity] = []
                    novel_anomalies[entity].append((moment, eff_score, msg))

            # Augment existing candidates or add novel log candidates
            for entity, anomalies in novel_anomalies.items():
                anomalies.sort(key=lambda item: item[0])
                first_moment, best_score, best_msg = anomalies[0]
                # Check if entity already has a candidate around first_moment
                matching = [
                    c for c in candidates
                    if c.entity == entity and abs(c.emitted_at - first_moment) <= 3600
                ]
                if matching:
                    # Add semantic signal & evidence to matching candidates
                    updated_candidates = []
                    for c in candidates:
                        if c in matching:
                            from ano.detect.signals import SignalReading

                            c_readings = list(c.readings)
                            sr = SignalReading(
                                feature="semantic_log_outlier",
                                title="Semantic Log Outlier",
                                unit="score",
                                family="semantic",
                                value=float(best_score),
                                baseline=0.0,
                                sigma=model.scale,
                                z=float(best_score * 3.0),
                                severity=float(best_score),
                                direction="up",
                                time_to_limit_s=1200.0,
                                slope_per_s=0.0,
                                ready=True,
                                hour=12,
                                weekday=2,
                            )
                            c_readings.append(sr)
                            c_contribs = dict(c.contributions)
                            c_contribs["semantic_log_outlier"] = 0.5
                            c_sev = dict(c.family_severities)
                            c_sev["semantic"] = float(best_score)
                            ev_list = list(c.evidence)
                            ev_list.append(
                                CausalEvidence(
                                    kind="semantic_log_outlier",
                                    detail=f"Novel log anomaly: {best_msg[:120]}",
                                )
                            )
                            c = replace(
                                c,
                                readings=tuple(c_readings),
                                contributions=c_contribs,
                                family_severities=c_sev,
                                evidence=tuple(ev_list),
                            )
                        updated_candidates.append(c)
                    candidates = updated_candidates
                else:
                    # Create a novel log signature candidate
                    domain = topo.domain(entity) or "application"
                    from ano.detect.signals import SignalReading
                    sr = SignalReading(
                        feature="semantic_log_outlier",
                        title="Semantic Log Outlier",
                        unit="score",
                        family="semantic",
                        value=float(best_score),
                        baseline=0.0,
                        sigma=model.scale,
                        z=float(best_score * 3.0),
                        severity=float(best_score),
                        direction="up",
                        time_to_limit_s=1200.0,
                        slope_per_s=0.0,
                        ready=True,
                        hour=12,
                        weekday=2,
                    )
                    novel_cand = Candidate(
                        candidate_id=stable_id(f"novel:{entity}:{int(first_moment)}", "cand"),
                        entity=entity,
                        emitted_at=first_moment,
                        time_to_failure_s=1200,
                        confidence=float(best_score),
                        raw_confidence=float(best_score),
                        readings=(sr,),
                        contributions={"semantic_log_outlier": 1.0},
                        family_severities={"semantic": float(best_score)},
                        cause_entity=entity,
                        cause_domain=domain,
                        evidence=(
                            CausalEvidence(
                                kind="semantic_log_outlier",
                                detail=f"Novel log signature detected: {best_msg[:120]}",
                            ),
                        ),
                    )
                    candidates.append(novel_cand)

        # 5. Cross-domain correlation & root-cause attribution (R3)
        index = DistressIndex(detector.all_summaries())
        attributor = Attributor(topo, index)
        attributed = attributor.attribute(candidates)
        _DETECTION_CACHE[cache_key] = list(attributed)

    # 6. Dynamic baselines, change-awareness & noise suppression (R4)
    suppressed = suppress(attributed, calendar, enabled=suppression)

    # 7. Alert storm collapse (R4, AC-13)
    collapsed = collapse_candidates(suppressed, topology=topo)

    # 8. Actionable operator remediation recommendations
    with_remediation = attach_remediations(collapsed)

    # 9. Convert to contract-C2 predictions and write JSONL
    run_tag = ""
    parts = os.path.abspath(out_path).split(os.sep)
    for p in reversed(parts[:-1]):
        if p.startswith("seed-") or p == "demo":
            run_tag = p
            break
    if not suppression:
        run_tag = f"{run_tag}-nosuppr" if run_tag else "nosuppr"

    predictions = [candidate_to_prediction(c, run_tag=run_tag) for c in with_remediation]
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    write_predictions(out_path, predictions)
