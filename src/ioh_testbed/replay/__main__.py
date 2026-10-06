"""``ioh-replay``: replay recorded cases as a live monitor feed, open-loop.

    uv run ioh-replay --workload configs/workload/standard_anaesthesia.yaml \\
        --scenario configs/scenarios/dwc_10s.yaml --cases 5 --duration 300 --sink null

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

from .config import ENV_POLICIES, RunConfig, Scenario, Workload
from .pacer import NullSink, Pacer
from .reader import VitalDBSource
from .schedule import build_schedule


def parse_args(argv=None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(prog="ioh-replay", description=__doc__.split("\n\n")[0])
    ap.add_argument("--workload", required=True, type=Path)
    ap.add_argument("--scenario", required=True, type=Path, help="states packet_ms; there is no default")
    ap.add_argument("--cases", type=int, default=1, help="concurrent cases, all starting at t0")
    ap.add_argument("--duration", type=float, help="observation window in seconds (overrides workload)")
    ap.add_argument("--speed", type=float, help="replay rate multiplier (overrides workload); != 1 invalidates latency results")
    ap.add_argument("--sink", choices=["null", "kafka"], default="null")
    ap.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    ap.add_argument("--env", choices=sorted(ENV_POLICIES), default="laptop")
    ap.add_argument("--source", default="vitaldb")
    ap.add_argument("--data-dir", type=Path, default=Path("data/vitaldb"))
    ap.add_argument("--manifest", type=Path, default=Path("src/ioh_testbed/replay/manifest.json"))
    ap.add_argument("--results", type=Path, default=Path("results"))
    ap.add_argument("--tolerance-ms", type=float, default=10.0, help="p99 lateness above this is DEGRADED")
    ap.add_argument("--invalid-ms", type=float, default=1000.0, help="p99 lateness above this is INVALID")
    return ap.parse_args(argv)


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
    if args.duration is not None or args.speed is not None:
        workload = replace(
            workload,
            observation_window_s=int(args.duration) if args.duration is not None else workload.observation_window_s,
            speed=args.speed if args.speed is not None else workload.speed,
        )
    cfg = RunConfig(workload, Scenario.load(args.scenario))
    manifest = json.loads(args.manifest.read_text())["cases"]

    t = time.perf_counter()
    schedule = build_schedule(cfg, VitalDBSource(args.data_dir), manifest, args.cases)
    build_s = time.perf_counter() - t
    print(
        f"scheduled {len(schedule.cases)} cases, {len(schedule.emitters)} streams, "
        f"window {schedule.window_s:g} s, packet {schedule.packet_ms} ms, built in {build_s:.1f} s",
        file=sys.stderr,
    )

    if args.sink == "kafka":
        from .kafka_sink import KafkaSink  # optional dependency path; phase 3

        sink = KafkaSink(args.bootstrap)
    else:
        sink = NullSink()

    stats = asyncio.run(Pacer(sink, tolerance_ms=args.tolerance_ms, invalid_ms=args.invalid_ms).run(schedule))

    run_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + uuid.uuid4().hex[:6]
    out = args.results / run_id
    out.mkdir(parents=True, exist_ok=True)
    summary = {
        "run_id": run_id,
        "env": args.env,
        "source": args.source,
        "sink": args.sink,
        "config": cfg.describe(),
        "schedule": schedule.describe(),
        "schedule_build_s": round(build_s, 2),
        "result": stats.summary(),
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({"run_id": run_id, **stats.summary()}, indent=2))
    return 2 if stats.verdict == "INVALID" else 0


if __name__ == "__main__":
    raise SystemExit(main())
