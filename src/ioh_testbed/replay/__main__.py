"""``ioh-replay``: replay recorded cases as a live monitor feed, open-loop.

    uv run ioh-replay --workload configs/workload/standard_anaesthesia.yaml \\
        --scenario configs/scenarios/dwc_10s.yaml --cases 5 --duration 300 --sink kafka

    uv run ioh-replay ... --calibrate --sink null     # harness ceiling on this host

Exit status: 0 for an OK or DEGRADED run, 2 for INVALID, 3 if the source policy
refuses the environment/source combination.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
import uuid
from dataclasses import replace
from pathlib import Path

from ..benchmark import stamp
from .config import ENV_POLICIES, RunConfig, Scenario, Workload
from .pacer import NullSink, Pacer
from .reader import VitalDBSource
from .schedule import build_schedule

# Speed multipliers swept by --calibrate. Each step runs for about --calibrate-wall
# seconds of wall time; the sweep stops at the first step that is not OK.
CALIBRATION_SPEEDS = (1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000)
# Longest source window a calibration step may use, so every manifest case qualifies.
CALIBRATION_MAX_WINDOW_S = 3000


def parse_args(argv=None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(prog="ioh-replay", description=__doc__.split("\n\n")[0])
    ap.add_argument("--workload", required=True, type=Path)
    ap.add_argument("--scenario", required=True, type=Path, help="states packet_ms; there is no default")
    ap.add_argument("--cases", type=int, default=1, help="concurrent cases, all starting at t0")
    ap.add_argument("--duration", type=float, help="observation window in seconds (overrides workload)")
    ap.add_argument("--speed", type=float, help="replay rate multiplier (overrides workload); != 1 invalidates latency results")
    ap.add_argument("--sink", choices=["null", "kafka"], default="null")
    ap.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    ap.add_argument("--partitions", type=int, default=8, help="expected topic partitions (checked, not changed)")
    ap.add_argument("--env", choices=sorted(ENV_POLICIES), default="laptop")
    ap.add_argument("--source", default="vitaldb")
    ap.add_argument("--data-dir", type=Path, default=Path("data/vitaldb"))
    ap.add_argument("--manifest", type=Path, default=Path("src/ioh_testbed/replay/manifest.json"))
    ap.add_argument("--results", type=Path, default=Path("results"))
    ap.add_argument("--tolerance-ms", type=float, default=None,
                    help="p99 lateness above this is DEGRADED (default: 10%% of packet_ms)")
    ap.add_argument("--invalid-ms", type=float, default=None,
                    help="p99 lateness above this is INVALID (default: 100%% of packet_ms)")
    ap.add_argument("--spin-ms", type=float, default=2.0, help="busy-wait this close to each deadline; 0 disables")
    ap.add_argument("--calibrate", action="store_true", help="sweep speed to find this host's harness ceiling")
    ap.add_argument("--calibrate-wall", type=float, default=30.0, help="wall seconds per calibration step")
    return ap.parse_args(argv)


def make_sink(args):
    if args.sink == "kafka":
        from .kafka_sink import KafkaSink, ensure_topics

        topics = ensure_topics(args.bootstrap, partitions=args.partitions)
        for t, info in topics.items():
            if info["message.timestamp.type"] != "LogAppendTime":
                raise SystemExit(f"topic {t} is not LogAppendTime; ingress timestamps would be wrong")
            if info["partitions"] != args.partitions:
                print(f"warning: topic {t} has {info['partitions']} partitions, expected {args.partitions}",
                      file=sys.stderr)
        return KafkaSink(args.bootstrap), {"topics": topics}
    return NullSink(), {}


def thresholds_ms(args, packet_ms: int) -> tuple[float, float]:
    """Cadence-relative by default: harness lateness only bounds offered-load timing
    fidelity (pipeline latency is measured from LogAppendTime), so what matters is
    lateness as a fraction of the packet period, not an absolute number."""
    tol = args.tolerance_ms if args.tolerance_ms is not None else 0.10 * packet_ms
    inv = args.invalid_ms if args.invalid_ms is not None else 1.00 * packet_ms
    return tol, inv


def new_run_id() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + uuid.uuid4().hex[:6]


def run_once(args, cfg: RunConfig, manifest: list[dict]) -> int:
    t = time.perf_counter()
    schedule = build_schedule(cfg, VitalDBSource(args.data_dir), manifest, args.cases)
    build_s = time.perf_counter() - t
    print(
        f"scheduled {len(schedule.cases)} cases, {len(schedule.emitters)} streams, "
        f"window {schedule.window_s:g} s, packet {schedule.packet_ms} ms, built in {build_s:.1f} s",
        file=sys.stderr,
    )
    sink, sink_meta = make_sink(args)
    tol, inv = thresholds_ms(args, cfg.scenario.packet_ms)
    stats = asyncio.run(Pacer(sink, tolerance_ms=tol, invalid_ms=inv, spin_s=args.spin_ms / 1000.0).run(schedule))

    run_id = new_run_id()
    out = args.results / run_id
    stamp.write_run(
        out, run_id=run_id, config_paths=[args.workload, args.scenario],
        config=cfg.describe(), schedule=schedule.describe(), result=stats.summary(),
        lateness_s=stats.lateness_s,
        extra={"env": args.env, "source": args.source, "sink": args.sink,
               "schedule_build_s": round(build_s, 2), **sink_meta},
    )
    print(json.dumps({"run_id": run_id, **stats.summary()}, indent=2))
    print(f"wrote {out}/meta.json", file=sys.stderr)
    return 2 if stats.verdict == "INVALID" else 0


def calibrate(args, workload: Workload, scenario: Scenario, manifest: list[dict]) -> int:
    """Find the largest offered load at which the harness itself still keeps schedule.

    Speed is swept upward with a roughly constant wall time per step. The ceiling
    bounds what any experiment on this host can claim: an operating boundary found
    above it would be the instrument's, not the architecture's."""
    rows = []
    tol, inv = thresholds_ms(args, scenario.packet_ms)
    for speed in CALIBRATION_SPEEDS:
        window = int(min(args.calibrate_wall * speed, CALIBRATION_MAX_WINDOW_S))
        cfg = RunConfig(replace(workload, observation_window_s=window, speed=float(speed)), scenario)
        schedule = build_schedule(cfg, VitalDBSource(args.data_dir), manifest, args.cases)
        sink, _ = make_sink(args)
        stats = asyncio.run(Pacer(sink, tolerance_ms=tol, invalid_ms=inv, spin_s=args.spin_ms / 1000.0).run(schedule))
        s = stats.summary()
        wall = s["duration_s"] or 1e-9
        row = {
            "speed": speed,
            "window_s": window,
            "wall_s": s["duration_s"],
            "n_records": s["n_scheduled"],
            "offered_records_per_s": s["offered_records_per_s"],
            "bytes_per_s": round(stats.sink.get("bytes", 0) / wall),
            "lateness_p99_ms": s["lateness_ms"]["p99"],
            "lateness_max_ms": s["lateness_ms"]["max"],
            "n_backpressure": s["n_backpressure"],
            "n_undelivered": s["n_undelivered"],
            "verdict": s["verdict"],
        }
        rows.append(row)
        print(json.dumps(row), file=sys.stderr)
        if row["verdict"] != "OK":
            break

    ok = [r for r in rows if r["verdict"] == "OK"]
    ceiling = max(ok, key=lambda r: r["offered_records_per_s"]) if ok else None
    run_id = new_run_id()
    out = args.results / run_id
    out.mkdir(parents=True, exist_ok=True)
    report = {
        "run_id": run_id,
        "kind": "calibration",
        "sink": args.sink,
        "cases": args.cases,
        "workload": workload.name,
        "scenario": scenario.name,
        "packet_ms": scenario.packet_ms,
        "thresholds_ms": {"tolerance": tol, "invalid": inv},
        "spin_ms": args.spin_ms,
        "git": stamp.git_provenance(),
        "config_files": [stamp.file_digest(p) for p in (args.workload, args.scenario)],
        "environment": stamp.environment_descriptor(),
        "steps": rows,
        "ceiling": ceiling,
    }
    (out / "calibration.json").write_text(json.dumps(report, indent=2, default=str) + "\n")
    print(json.dumps({"run_id": run_id, "sink": args.sink, "ceiling": ceiling}, indent=2))
    print(f"wrote {out}/calibration.json", file=sys.stderr)
    return 0 if ceiling else 2


def main(argv=None) -> int:
    args = parse_args(argv)

    # Structural PHI guard: the environment decides what it may read, before any I/O.
    try:
        ENV_POLICIES[args.env].check(args.source)
    except PermissionError as e:
        print(f"refused: {e}", file=sys.stderr)
        return 3
    if args.source != "vitaldb":
        print(f"source {args.source!r} has no reader yet", file=sys.stderr)
        return 3

    workload = Workload.load(args.workload)
    scenario = Scenario.load(args.scenario)
    manifest = json.loads(args.manifest.read_text())["cases"]

    if args.calibrate:
        return calibrate(args, workload, scenario, manifest)

    if args.duration is not None or args.speed is not None:
        workload = replace(
            workload,
            observation_window_s=int(args.duration) if args.duration is not None else workload.observation_window_s,
            speed=args.speed if args.speed is not None else workload.speed,
        )
    return run_once(args, RunConfig(workload, scenario), manifest)


if __name__ == "__main__":
    raise SystemExit(main())
