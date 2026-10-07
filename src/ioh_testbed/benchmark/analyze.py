"""Offline analysis of one run folder: the headline metrics, their
decomposition, per-case progress-lag slopes, inference-stub statistics and a
summary of the resource poller. Writes ``summary.json`` next to the inputs.

    uv run python -m ioh_testbed.benchmark.analyze results/<run_id> [--slope-tolerance S]

Definitions are in ``metrics.py``. Partial windows (those starting before the
case's first event) are reported separately and excluded from the headline
percentiles. The stability slope tolerance is a parameter here, to be
calibrated in weeks 7-8; until then the slope is reported, not judged, unless
``--slope-tolerance`` is given.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np

from .metrics import Clock, percentiles, prediction_terms, progress_lag, slope

TERMS = ("t_pipeline", "t_pipeline_broker", "staleness", "window_wait", "queue_excess",
         "inference", "sink", "fetch", "stub_queue_wait", "stub_service")


def read_jsonl(p: Path) -> list[dict]:
    if not p.exists():
        return []
    with p.open() as f:
        return [json.loads(line) for line in f if line.strip()]


def analyze(run_dir: Path, slope_tolerance: float | None = None) -> dict:
    meta = json.loads((run_dir / "meta.json").read_text()) if (run_dir / "meta.json").exists() else {}
    clock = Clock(float(meta.get("clock", {}).get("offset_s", 0.0)))
    job = meta.get("job", {}).get("config", {})
    bound_ms = float(job.get("watermark_bound_ms", 0.0))
    preds = read_jsonl(run_dir / "predictions.jsonl")
    prog = read_jsonl(run_dir / "progress.jsonl")
    poll = read_jsonl(run_dir / "poller.jsonl")

    terms = [prediction_terms(p, clock, bound_ms) for p in preds]
    full = [t for t in terms if not t["partial"] and t["status"] == "ok"]
    by_status: dict[str, int] = defaultdict(int)
    for t in terms:
        by_status[str(t["status"])] += 1

    out: dict = {
        "run_id": meta.get("run_id", run_dir.name),
        "clock_offset_s": clock.offset_s,
        "watermark_bound_ms": bound_ms,
        "predictions": {
            "n": len(terms),
            "n_full_ok": len(full),
            "n_partial": sum(1 for t in terms if t["partial"]),
            "by_status": dict(by_status),
            "cases": sorted({t["case_id"] for t in terms}),
        },
        "latency_s": {name: percentiles([t[name] for t in full]) for name in TERMS},
        "latency_s_per_case": {
            c: {name: percentiles([t[name] for t in full if t["case_id"] == c]) for name in ("t_pipeline", "staleness")}
            for c in sorted({t["case_id"] for t in full})
        },
    }

    # consistency check of the clock offset: T_pipeline - fetch must equal the broker-only latency
    if full:
        diff = np.array([t["t_pipeline"] - t["fetch"] - t["t_pipeline_broker"] for t in full])
        out["clock_check_max_abs_s"] = float(np.abs(diff).max())

    # per-case progress lag and its slope over the observation window only: the
    # heartbeat keeps running through the drain period after the replay ends, when
    # lag grows by construction because nothing new is offered.
    t0 = meta.get("result", {}).get("t0_wall")
    window_s = meta.get("schedule", {}).get("window_s")
    series: dict[str, list[tuple[float, float]]] = defaultdict(list)
    n_outside = 0
    for h in prog:
        if h.get("type") != "progress":
            continue
        t = clock.host(h["tWallMs"])
        if t0 is not None and window_s is not None and not (t0 <= t <= t0 + window_s):
            n_outside += 1
            continue
        series[h["caseId"]].append((t, progress_lag(h, clock)))
    lag_out: dict = {}
    slopes = []
    for c, pts in sorted(series.items()):
        pts.sort()
        t = np.array([p[0] for p in pts])
        y = np.array([p[1] for p in pts])
        s = slope(t, y)
        lag_out[c] = {"lag_s": percentiles(y), **s}
        if s["n"] >= 3:
            slopes.append(s["slope"])
    out["progress_lag"] = {
        "window": {"t0_wall": t0, "window_s": window_s, "heartbeats_outside": n_outside},
        "per_case": lag_out,
        "slope_s_per_s": percentiles(slopes, qs=(50, 90, 100)) if slopes else {"N": 0},
    }
    if slope_tolerance is not None and slopes:
        worst = max(slopes)
        out["stability"] = {"tolerance_s_per_s": slope_tolerance, "worst_slope": worst,
                            "verdict": "stable" if worst <= slope_tolerance else "degrading"}

    if poll:
        out["poller"] = summarize_poller(poll)
    (run_dir / "summary.json").write_text(json.dumps(out, indent=2, default=_json_default) + "\n")
    return out


def summarize_poller(rows: list[dict]) -> dict:
    """Max back-pressure and busy time per vertex, max Kafka lag at the source
    (``pendingRecords`` or the consumer's ``records-lag-max``), and peak container
    memory, over the run."""
    bp: dict[str, float] = defaultdict(float)
    busy: dict[str, float] = defaultdict(float)
    pending = 0.0
    mem: dict[str, float] = defaultdict(float)
    cpu: dict[str, float] = defaultdict(float)
    for r in rows:
        for v in r.get("flink", {}).get("vertices", []):
            name = v.get("name", "?").split(" -> ")[0].replace("Source: ", "")
            bp[name] = max(bp[name], float(v.get("backPressuredTimeMsPerSecond", 0) or 0))
            busy[name] = max(busy[name], float(v.get("busyTimeMsPerSecond", 0) or 0))
            for key in ("pendingRecords", "records-lag-max"):
                if v.get(key) is not None:
                    pending = max(pending, float(v[key]))
        for name, st in r.get("docker", {}).items():
            mem[name] = max(mem[name], float(st.get("mem_bytes", 0) or 0))
            cpu[name] = max(cpu[name], float(st.get("cpu_pct", 0) or 0))
    return {
        "samples": len(rows),
        "max_backpressure_ms_per_s": dict(bp),
        "max_busy_ms_per_s": dict(busy),
        "max_pending_records": pending,
        "peak_mem_bytes": dict(mem),
        "peak_cpu_pct": dict(cpu),
    }


def _json_default(o):
    if isinstance(o, (np.floating, np.integer)):
        return o.item()
    raise TypeError(type(o))


def print_summary(s: dict) -> None:
    p = s["predictions"]
    print(f"run        {s['run_id']}  clock offset {s['clock_offset_s'] * 1000:.1f} ms  bound {s['watermark_bound_ms']:.0f} ms")
    print(f"predictions {p['n']} total, {p['n_full_ok']} full+ok, {p['n_partial']} partial, by status {p['by_status']}, "
          f"{len(p['cases'])} cases")
    if "clock_check_max_abs_s" in s:
        print(f"clock check max |T_pipeline - fetch - T_broker| = {s['clock_check_max_abs_s'] * 1000:.2f} ms")
    print(f"{'term':18s} {'N':>6s} {'p50':>9s} {'p90':>9s} {'p99':>9s} {'max':>9s}   (seconds)")
    for name, d in s["latency_s"].items():
        if d.get("N"):
            print(f"{name:18s} {d['N']:6d} {d['p50']:9.4f} {d['p90']:9.4f} {d['p99']:9.4f} {d['max']:9.4f}")
    pl = s["progress_lag"]
    if pl["per_case"]:
        print("progress lag per case (s): slope in s/s over the window")
        for c, d in pl["per_case"].items():
            lag = d["lag_s"]
            print(f"  case {c:>6s}  n={d['n']:5d}  p50={lag.get('p50', float('nan')):7.3f}  "
                  f"max={lag.get('max', float('nan')):7.3f}  slope={d['slope']:+.5f}  r2={d['r2']:.3f}")
    if "stability" in s:
        print(f"stability  {s['stability']['verdict']}  (worst slope {s['stability']['worst_slope']:+.5f} vs "
              f"tolerance {s['stability']['tolerance_s_per_s']})")
    if "poller" in s:
        po = s["poller"]
        print(f"poller     {po['samples']} samples; max backpressure ms/s {po['max_backpressure_ms_per_s']}; "
              f"max pending {po['max_pending_records']:.0f}; peak mem MB "
              + str({k: round(v / 1e6) for k, v in po['peak_mem_bytes'].items()}))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("run_dir", type=Path)
    ap.add_argument("--slope-tolerance", type=float, default=None, help="s/s; judge stability against it")
    args = ap.parse_args(argv)
    if not args.run_dir.exists():
        print(f"{args.run_dir} does not exist", file=sys.stderr)
        return 2
    print_summary(analyze(args.run_dir, args.slope_tolerance))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
