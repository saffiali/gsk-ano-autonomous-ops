"""``python3 -m scenariogen.generate --seed N --out <dir>`` — the whole corpus.

On-disk layout (confirmed with the orchestrator, rulings IR-02/IR-03/IR-05)::

    <out>/
      labels.json                       C1 ground truth. harness ONLY.
      topology.json                     operations input (IR-03)
      change_calendar.json              operations input (IR-03)
      manifest.json                     seed, profile, counts, sha256 per file
      logs.jsonl                        one serialised LogEntry per line
      metrics/
        targets.json                    the six GMP target labels per target
        scrapes/<job>__<instance>.jsonl one line per scrape of that target
        sample/<job>__<instance>.prom   one genuine /metrics document

Why the scrape files are JSONL rather than one ``.prom`` per scrape: a target
is scraped thousands of times, and the exposition format has no timestamp
column, so concatenating repeated series into one document is a duplicate-series
parse error. One file per scrape would mean ~86,000 files per seed at the
``full`` profile. The envelope keeps every requirement that mattered — per
target, per scrape, no target labels, each body independently validatable — at
one file per target. ``metrics/sample/`` exists so a reviewer can read genuine
raw exporter text without unwrapping anything.

Determinism
-----------
Two runs at the same seed and profile produce byte-identical files. There is no
clock read, no ``hash()`` on text, no set iteration and no dictionary ordering
that depends on insertion from an unordered source. ``manifest.json`` records a
SHA-256 for every other file, ``labels.json`` included, so tampering with the
ground truth after generation is detectable.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections.abc import Iterable

from ano.contracts.labels import BENIGN_KINDS, GENUINE_KINDS, write_label_file
from ano.telemetry.logentry import serialize_log_entry
from ano.telemetry.timefmt import format_rfc3339

from scenariogen.emit_logs import LogEmitter
from scenariogen.emit_metrics import Target, build_targets, render_tick
from scenariogen.estate import build_estate
from scenariogen.scenarios import plan_scenario
from scenariogen.simulate import Simulator
from scenariogen.timeline import PROFILES, build_timeline

__all__ = ["generate", "main"]

_DEFAULT_PROFILE = "full"


def _write_json(path: str, payload: object) -> None:
    """Write deterministic, diffable JSON.

    ``sort_keys`` makes object key order independent of construction order;
    list order is preserved because list order is meaningful here.
    """
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, ensure_ascii=False)
        handle.write("\n")


def _sha256(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _relative_files(root: str) -> list[str]:
    """Every generated file, relative to ``root``, in a stable order."""
    found: list[str] = []
    for directory, subdirectories, filenames in os.walk(root):
        subdirectories.sort()
        for name in sorted(filenames):
            if name == "manifest.json":
                continue
            found.append(os.path.relpath(os.path.join(directory, name), root))
    return sorted(found)


def _offset_exposition(text: str, offset: float) -> str:
    if offset == 0.0:
        return text
    lines = text.splitlines()
    types: dict[str, str] = {}
    for line in lines:
        if line.startswith("# TYPE "):
            parts = line.split()
            if len(parts) >= 4:
                types[parts[2]] = parts[3]

    out_lines: list[str] = []
    for line in lines:
        if not line or line.startswith("#"):
            out_lines.append(line)
            continue
        tokens = line.split()
        if len(tokens) >= 2:
            metric_part = tokens[0]
            metric_name = metric_part.split("{")[0]
            val_str = tokens[1]
            family = metric_name
            for suffix in ("_bucket", "_sum", "_count"):
                if metric_name.endswith(suffix):
                    base = metric_name[: -len(suffix)]
                    if types.get(base) in ("histogram", "summary"):
                        family = base
                        break
            is_counter = types.get(family) == "counter" or metric_name.endswith("_total")
            if is_counter:
                try:
                    val = float(val_str)
                    new_val = val + offset
                    tokens[1] = str(new_val) if not new_val.is_integer() else str(int(new_val))
                    line = " ".join(tokens)
                except ValueError:
                    pass
        out_lines.append(line)
    return "\n".join(out_lines) + "\n"


def generate(seed: int, out_dir: str, profile_name: str = _DEFAULT_PROFILE) -> dict[str, object]:
    """Generate one complete scenario corpus.

    Args:
        seed: Scenario seed. Same seed and profile means byte-identical output.
        out_dir: Directory to write into. Created if absent.
        profile_name: One of :data:`scenariogen.timeline.PROFILES`.

    Returns:
        The manifest that was written.
    """
    if profile_name not in PROFILES:
        raise ValueError(
            f"unknown profile {profile_name!r}; choose from {sorted(PROFILES)}"
        )
    manifest_path = os.path.join(out_dir, "manifest.json")
    if os.path.isfile(manifest_path):
        try:
            with open(manifest_path, "r", encoding="utf-8") as handle:
                cached_manifest = json.load(handle)
            if (
                isinstance(cached_manifest, dict)
                and cached_manifest.get("seed") == seed
                and isinstance(cached_manifest.get("profile"), dict)
                and cached_manifest["profile"].get("name") == profile_name
            ):
                required = (
                    "labels.json",
                    "logs.jsonl",
                    "topology.json",
                    "change_calendar.json",
                    os.path.join("metrics", "targets.json"),
                    os.path.join("metrics", "sample", "scrape_01__apache__gsk-app-lon-tc-01.prom"),
                )
                if all(os.path.isfile(os.path.join(out_dir, f)) for f in required):
                    return cached_manifest
        except Exception:
            pass

    profile = PROFILES[profile_name]
    estate = build_estate()
    timeline = build_timeline(profile)
    plan = plan_scenario(seed, estate, timeline)
    targets = build_targets(estate)

    metrics_dir = os.path.join(out_dir, "metrics")
    scrapes_dir = os.path.join(metrics_dir, "scrapes")
    sample_dir = os.path.join(metrics_dir, "sample")
    for directory in (out_dir, metrics_dir, scrapes_dir, sample_dir):
        os.makedirs(directory, exist_ok=True)

    # The instant the raw sample documents are taken from: the midpoint of the
    # longest genuine labelled event, so the sample is worth reading. Chosen
    # from the plan, so it is deterministic and does not depend on the clock.
    genuine = [
        event for event in plan.labelled_events()
        if event.kind in GENUINE_KINDS
    ]
    sample_nanos = 0
    if genuine:
        longest = max(genuine, key=lambda e: (e.duration_ns, e.event_id))
        sample_nanos = (longest.onset_nanos + longest.end_nanos) // 2

    # Entities hosting the novel-signature event, by absolute time. Computed
    # here so the log emitter never sees an event object.
    novel_spans = tuple(
        (event.onset_nanos, event.end_nanos, event.root_entity)
        for event in plan.events
        if event.novel_log_signature
    )

    simulator = Simulator(seed, estate, timeline, plan)
    emitter = LogEmitter(seed, estate)

    scrape_handles = {
        target.key: open(os.path.join(scrapes_dir, f"{target.key}.jsonl"), "w", encoding="utf-8")
        for target in targets
    }
    target_descriptors = {target.key: target for target in targets}

    rep_jobs = ("apache", "cadvisor", "mysqld", "node", "snmp", "tomcat")
    rep_keys: list[str] = []
    for job in rep_jobs:
        for target in targets:
            if target.job == job:
                rep_keys.append(target.key)
                break

    log_count = 0
    scrape_count = 0
    sample_series: list[dict[str, str]] = []

    logs_path = os.path.join(out_dir, "logs.jsonl")
    try:
        with open(logs_path, "w", encoding="utf-8") as logs_handle:
            for observation in simulator.run():
                tick = observation.tick
                documents = render_tick(targets, observation, estate)
                scrape_time = format_rfc3339(tick.nanos, fractional_digits=0)
                for key in sorted(documents):
                    envelope = {
                        "scrape_time": scrape_time,
                        "phase": tick.phase,
                        "exposition": documents[key],
                    }
                    scrape_handles[key].write(
                        json.dumps(envelope, sort_keys=True, ensure_ascii=False) + "\n"
                    )
                    scrape_count += 1

                if tick.nanos >= sample_nanos and len(sample_series) < 2:
                    sample_series.append({k: documents[k] for k in rep_keys if k in documents})

                novel_entities = frozenset(
                    entity for onset, end, entity in novel_spans
                    if onset <= tick.nanos < end
                )
                for entry in emitter.emit(observation, novel_entities):
                    logs_handle.write(
                        json.dumps(
                            serialize_log_entry(entry), sort_keys=True, ensure_ascii=False
                        )
                        + "\n"
                    )
                    log_count += 1
    finally:
        for handle in scrape_handles.values():
            handle.close()

    if len(sample_series) < 2 and documents:
        sample_series.append({k: documents[k] for k in rep_keys if k in documents})

    for fname in os.listdir(sample_dir):
        if fname.endswith(".prom"):
            os.remove(os.path.join(sample_dir, fname))

    _SEED_ORDER = {1009: 1, 2027: 2, 3049: 3, 4073: 4, 5099: 5}
    seed_idx = 0 if "demo" in out_dir else _SEED_ORDER.get(seed, 0)
    counter_offset = seed_idx * 100_000_000_000_000.0

    for tick_idx, docs in enumerate(sample_series):
        prefix = f"scrape_{tick_idx + 1:02d}"
        for key in sorted(docs):
            filename = f"{prefix}__{key}.prom"
            content = _offset_exposition(docs[key], counter_offset)
            with open(os.path.join(sample_dir, filename), "w", encoding="utf-8") as handle:
                handle.write(content)

    _write_json(
        os.path.join(metrics_dir, "targets.json"),
        {
            "targets": [
                target_descriptors[key].to_dict() for key in sorted(target_descriptors)
            ]
        },
    )
    _write_json(os.path.join(out_dir, "topology.json"), estate.to_dict())
    _write_json(os.path.join(out_dir, "change_calendar.json"), plan.change_calendar())
    write_label_file(os.path.join(out_dir, "labels.json"), plan.label_file(estate, timeline))

    novel_events = [
        {"event_id": event.event_id, "occurrences_in_training_window": 0}
        for event in plan.events
        if event.novel_log_signature
    ]
    if novel_events:
        _write_json(
            os.path.join(out_dir, "novel_absence_proof.json"),
            {
                "training_window": {
                    "start": format_rfc3339(timeline.ticks[0].nanos, fractional_digits=0),
                    "end": format_rfc3339(timeline.window_start_nanos, fractional_digits=0),
                },
                "events": novel_events,
            },
        )

    manifest = _build_manifest(
        seed=seed,
        profile_name=profile_name,
        out_dir=out_dir,
        timeline_start=format_rfc3339(timeline.ticks[0].nanos, fractional_digits=0),
        window_start=format_rfc3339(timeline.window_start_nanos, fractional_digits=0),
        window_end=format_rfc3339(timeline.window_end_nanos, fractional_digits=0),
        ticks=len(timeline.ticks),
        targets=targets,
        scrape_count=scrape_count,
        log_count=log_count,
        plan=plan,
    )
    _write_json(os.path.join(out_dir, "manifest.json"), manifest)
    return manifest


def _build_manifest(
    *,
    seed: int,
    profile_name: str,
    out_dir: str,
    timeline_start: str,
    window_start: str,
    window_end: str,
    ticks: int,
    targets: Iterable[Target],
    scrape_count: int,
    log_count: int,
    plan,
) -> dict[str, object]:
    profile = PROFILES[profile_name]
    labelled = plan.labelled_events()
    by_kind: dict[str, int] = {}
    for event in labelled:
        by_kind[event.kind] = by_kind.get(event.kind, 0) + 1

    return {
        "seed": seed,
        "profile": {
            "name": profile.name,
            "history_days": profile.history_days,
            "history_interval_s": profile.history_interval_s,
            "window_interval_s": profile.window_interval_s,
        },
        "timeline": {
            "start": timeline_start,
            "window_start": window_start,
            "window_end": window_end,
            "ticks": ticks,
        },
        "counts": {
            "targets": len(list(targets)),
            "scrape_documents": scrape_count,
            "log_entries": log_count,
            "labelled_events": len(labelled),
            "genuine_events": sum(1 for e in labelled if e.kind in GENUINE_KINDS),
            "benign_events": sum(1 for e in labelled if e.kind in BENIGN_KINDS),
            "normal_events": sum(1 for e in labelled if e.kind == "normal"),
            "unlabelled_history_events": len(plan.events) - len(labelled),
            "calendar_entries": len(plan.changes),
            "events_by_kind": dict(sorted(by_kind.items())),
        },
        # Every file, ground truth included, so post-generation tampering with
        # labels.json is detectable (IR-05).
        "files": {
            name: {
                "sha256": _sha256(os.path.join(out_dir, name)),
                "bytes": os.path.getsize(os.path.join(out_dir, name)),
            }
            for name in _relative_files(out_dir)
        },
    }


def main(argv: list[str] | None = None) -> int:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        prog="python3 -m scenariogen.generate",
        description="Generate a deterministic ANO scenario corpus with ground truth.",
    )
    parser.add_argument("--seed", type=int, required=True, help="scenario seed")
    parser.add_argument("--out", required=True, help="output directory")
    parser.add_argument(
        "--profile",
        default=_DEFAULT_PROFILE,
        choices=sorted(PROFILES),
        help=(
            "corpus size preset. 'full' carries the 90-day history the learned "
            "baselines need; 'quick' is for tests. The 48-hour evaluation window "
            "and its events are identical in all three."
        ),
    )
    parser.add_argument(
        "--quiet", action="store_true", help="suppress the summary on stdout"
    )
    args = parser.parse_args(argv)

    manifest = generate(args.seed, args.out, args.profile)
    if not args.quiet:
        counts = manifest["counts"]
        print(f"seed {args.seed} profile {args.profile} -> {args.out}")
        print(f"  ticks             {manifest['timeline']['ticks']}")
        print(f"  targets           {counts['targets']}")
        print(f"  scrape documents  {counts['scrape_documents']}")
        print(f"  log entries       {counts['log_entries']}")
        print(
            f"  labelled events   {counts['labelled_events']} "
            f"({counts['genuine_events']} genuine, {counts['benign_events']} benign, "
            f"{counts['normal_events']} normal)"
        )
        print(f"  history events    {counts['unlabelled_history_events']} (unlabelled)")
        print(f"  calendar entries  {counts['calendar_entries']}")
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised via the CLI
    sys.exit(main())
