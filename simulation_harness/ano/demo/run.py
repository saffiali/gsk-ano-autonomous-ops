"""Operator-facing demo timeline CLI (Requirement R7, Feature 149-155).

Usage:
    python3 -m ano.demo.run --scenario <name> --seed <n>
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import sys
from typing import Any

SUPPORTED_SCENARIOS = frozenset(
    {
        "db_contention_cascade",
        "thread_pool_exhaustion",
        "ospf_route_flap",
        "socket_exhaustion",
        "memory_leak",
        "packet_loss",
        "patching_window",
        "planned_deploy",
        "traffic_surge",
        "all",
    }
)


def _safe_str(val: Any) -> str:
    return str(val) if val is not None else ""


def render_timeline(
    predictions: list[dict[str, Any]],
    topology_data: dict[str, Any] | None,
    calendar_data: dict[str, Any] | None,
    scenario: str,
    seed: int,
) -> None:
    """Render the operator-facing incident and timeline report."""
    print("=" * 80)
    print("  GSK Autonomous Network Operations (ANO) — Operator Investigation Surface")
    print(
        "  Execution mode: local / offline substitute (no cloud credentials or network required)"
    )
    print(f"  Target Estate: GSK Global Operations | Scenario: {scenario} | Seed: {seed}")
    print("=" * 80)
    print()

    # Estate overview
    entities = topology_data.get("entities", []) if topology_data else []
    print("--- 1. ESTATE TOPOLOGY CONTEXT ---")
    print(f"  Monitored Entities: {len(entities)}")
    by_kind: dict[str, int] = {}
    for ent in entities:
        k = ent.get("kind", "unknown")
        by_kind[k] = by_kind.get(k, 0) + 1
    for k, count in sorted(by_kind.items()):
        print(f"    - {k}: {count}")
    print()

    # Change calendar context
    changes = calendar_data.get("changes", []) if calendar_data else []
    print("--- 2. CHANGE CALENDAR & NOISE SUPPRESSION CONTEXT ---")
    print(f"  Scheduled Changes in Window: {len(changes)}")
    for ch in changes:
        cid = ch.get("change_id", "")
        ctype = ch.get("change_type", "")
        scope = ", ".join(ch.get("target_entities", []))
        start = ch.get("start", "")
        end = ch.get("end", "")
        print(f"    [{start} - {end}] {cid} ({ctype}) on {scope}")
    print()

    # Predictions summary
    total_preds = len(predictions)
    suppressed = [p for p in predictions if p.get("suppressed") is True]
    active = [p for p in predictions if not p.get("suppressed")]
    collapsed_count = sum(len(p.get("collapsed_from") or []) for p in predictions)

    print("--- 3. DETECTED INCIDENTS & PREDICTION TIMELINE ---")
    print(f"  Total Signals Detected:  {total_preds}")
    print(f"  Suppressed by Change:   {len(suppressed)} (Change-Aware Noise Elimination)")
    print(f"  Actionable Incidents:   {len(active)}")
    print(f"  Alert Storm Collapsed:  {collapsed_count} secondary symptoms collapsed into root cause")
    print()

    if not predictions:
        print("  No anomalies or incidents detected in the evaluated window.")
        return

    # Timeline view
    print(
        f"{'TIME':<24} | {'STATUS':<12} | {'LEAD TIME':<10} | {'ENTITY':<22} | {'ROOT CAUSE DOMAIN':<14} | {'RECOMMENDED REMEDIATION'}"
    )
    print("-" * 120)

    # Sort chronologically by emitted_at
    sorted_preds = sorted(predictions, key=lambda p: p.get("emitted_at", ""))
    for p in sorted_preds:
        emitted = p.get("emitted_at", "")[:19]
        is_supp = p.get("suppressed", False)
        status = "SUPPRESSED" if is_supp else "ACTIVE PRED"
        lead_s = p.get("time_to_failure_s", 0)
        lead_m = f"{lead_s // 60}m" if lead_s else "N/A"
        ent = p.get("entity", "")
        rc = p.get("root_cause") or {}
        domain = rc.get("domain", "unknown")
        remed = p.get("recommended_remediation", "Investigate entity")

        print(
            f"{emitted:<24} | {status:<12} | {lead_m:<10} | {ent:<22} | {domain:<14} | {remed[:50]}"
        )

        # Print details for active incidents
        if not is_supp:
            rc_ent = rc.get("entity", ent)
            evidences = rc.get("evidence", [])
            ev_summary = "; ".join(e.get("detail", "") for e in evidences[:2])
            print(f"    ↳ Attributed Root Entity: {rc_ent} (Domain: {domain})")
            if ev_summary:
                print(f"    ↳ Causal Evidence: {ev_summary}")
            collapsed_from = p.get("collapsed_from") or []
            if collapsed_from:
                print(
                    f"    ↳ Alert Storm Collapse: Absorbed {len(collapsed_from)} dependent alarms: {', '.join(collapsed_from[:4])}"
                )
            remed_full = p.get("recommended_remediation", "")
            if len(remed_full) > 50:
                print(f"    ↳ Prescriptive Action: {remed_full}")
            print()

    print("=" * 80)
    print("  GSK ANO Demo Execution Complete: Zero Unhandled Exceptions")
    print("=" * 80)


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for ano.demo.run."""
    if argv is None:
        argv = sys.argv[1:]

    parser = argparse.ArgumentParser(
        description="GSK Autonomous Network Operations (ANO) Demo Timeline CLI",
        add_help=True,
    )
    parser.add_argument("--scenario", default="db_contention_cascade")
    parser.add_argument("--seed", default="1234")
    parser.add_argument("--work-dir", default=None)
    parser.add_argument("--profile", default="full")

    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return exc.code

    scenario = (args.scenario or "").strip()
    if not scenario:
        sys.stderr.write("Error: Scenario name cannot be empty.\n")
        return 1

    if scenario not in SUPPORTED_SCENARIOS:
        sys.stderr.write(
            f"Error: Unknown scenario '{scenario}'. Choose from {sorted(SUPPORTED_SCENARIOS)}\n"
        )
        return 1

    seed_raw = args.seed
    try:
        seed = int(seed_raw)
    except (ValueError, TypeError):
        sys.stderr.write(f"Error: Seed must be an integer, got '{seed_raw}'.\n")
        return 1

    if seed <= 0:
        sys.stderr.write(f"Error: Seed must be a positive integer, got {seed}.\n")
        return 1

    # Check for shell metacharacters in work-dir or profile
    for val in (args.work_dir, args.profile):
        if val and any(char in val for char in (";", "&", "|", "`", "$")):
            sys.stderr.write("Error: Invalid characters in arguments.\n")
            return 1

    work_dir = args.work_dir or os.path.join("artifacts", "demo")
    os.makedirs(work_dir, exist_ok=True)

    # 1. Generate scenario if not already generated for this seed
    logs_path = os.path.join(work_dir, "logs.jsonl")
    metrics_path = os.path.join(work_dir, "metrics")
    predictions_path = os.path.join(work_dir, "predictions.jsonl")
    topo_path = os.path.join(work_dir, "topology.json")
    cal_path = os.path.join(work_dir, "change_calendar.json")
    seed_file = os.path.join(work_dir, "demo_seed.txt")

    current_seed: int | None = None
    if os.path.exists(seed_file):
        try:
            with open(seed_file, "r", encoding="utf-8") as sf:
                current_seed = int(sf.read().strip())
        except Exception:
            current_seed = None

    # If artifacts do not already exist or seed changed, invoke scenario generator
    if current_seed != seed or not (
        os.path.exists(logs_path)
        and os.path.exists(metrics_path)
        and os.path.exists(topo_path)
        and os.path.exists(cal_path)
    ):
        try:
            from scenariogen.api import generate

            generate(seed, work_dir, profile=args.profile)
            with open(seed_file, "w", encoding="utf-8") as sf:
                sf.write(str(seed))
            if os.path.exists(predictions_path):
                os.remove(predictions_path)
        except Exception as exc:
            sys.stderr.write(f"Error generating scenario: {exc}\n")
            return 1

    # 2. Run detection pipeline if predictions do not exist or are empty
    if not os.path.exists(predictions_path) or os.path.getsize(predictions_path) == 0:
        try:
            from ano.pipeline import detect

            detect(logs_path, metrics_path, predictions_path, suppression=True)
        except Exception as exc:
            sys.stderr.write(f"Error in detection pipeline: {exc}\n")
            return 1

    # 3. Load predictions (Contract C2)
    predictions: list[dict[str, Any]] = []
    if os.path.exists(predictions_path):
        with open(predictions_path, "r", encoding="utf-8") as pf:
            for line in pf:
                line = line.strip()
                if line:
                    predictions.append(json.loads(line))

    # 4. Load topology & change calendar for operator presentation
    topo_data = None
    if os.path.exists(topo_path):
        with open(topo_path, "r", encoding="utf-8") as tf:
            topo_data = json.load(tf)

    cal_data = None
    if os.path.exists(cal_path):
        with open(cal_path, "r", encoding="utf-8") as cf:
            cal_data = json.load(cf)

    # 5. Render operator timeline
    try:
        render_timeline(predictions, topo_data, cal_data, scenario, seed)
    except BrokenPipeError:
        try:
            sys.stdout.close()
        except Exception:
            pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
