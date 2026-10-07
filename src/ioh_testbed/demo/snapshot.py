"""What the console shows, for a reader that is not looking at the page.

``take_snapshot`` gathers the shared settings, pipeline status and gauges and
every bed's statistics (no waveform or numeric series). ``History`` keeps the
last ten minutes of the series that matter when something goes wrong, at the
status loop's 2 s cadence. ``EventLog`` records plays, stops, applies, bed
exits and errors. ``render_text`` prints all of it as a report.

    uv run ioh-demo-snapshot            # text report from the running server
    uv run ioh-demo-snapshot --json     # the raw snapshot
    uv run ioh-demo-snapshot --file results/demo/snapshot.json   # after the server has stopped
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time
from collections import deque
from pathlib import Path

HISTORY_LEN = 300  # samples; 10 min at the 2 s status cadence


def take_snapshot(state: dict, bed_stats: list[dict]) -> dict:
    pipe = state["pipeline"]
    by_index = {s["bed"]: s for s in bed_stats}
    beds = []
    for b in state["beds"]:
        st = by_index.get(b["index"])
        beds.append({
            **b,
            "elapsed_patient_s": st["elapsed_patient_s"] if st else None,
            "ingress": st["ingress"] if st else None,
            "prediction_stats": st["prediction_stats"] if st else None,
            "progress_lag_s": st["progress_lag_s"] if st else None,
            "recent_predictions": st.get("recent_predictions", []) if st else [],
        })
    return {
        "t": time.time(),
        "shared": state["shared"],
        "pipeline": {k: v for k, v in pipe.items() if k != "job_config"},
        "job_config": pipe.get("job_config"),
        "beds": beds,
    }


class History:
    """Ring buffer of the diagnostic series, sampled from snapshots."""

    def __init__(self, maxlen: int = HISTORY_LEN):
        self.rows: deque = deque(maxlen=maxlen)
        self.lock = threading.Lock()

    def add(self, snap: dict) -> None:
        m = snap["pipeline"].get("metrics", {}) or {}
        stub = m.get("stub") or {}
        row = {
            "t": snap["t"],
            "backpressure_ms_s": m.get("source_backpressure_ms_s"),
            "features_busy_ms_s": m.get("features_busy_ms_s"),
            "kafka_lag_max": m.get("kafka_lag_max"),
            "stub_in_flight": stub.get("in_flight"),
            "stub_queued": stub.get("queued"),
            "stub_served": stub.get("served"),
            "beds": {},
        }
        for b in snap["beds"]:
            if b["status"] in ("running", "done", "error") and b.get("ingress"):
                ps = b["prediction_stats"] or {}
                row["beds"][str(b["index"])] = {
                    "status": b["status"],
                    "lag_s": b.get("progress_lag_s"),
                    "latency_p50": ps.get("latency_p50"),
                    "pred_completeness": ps.get("completeness"),
                    "pred_n": ps.get("n"),
                    "ingress_completeness": b["ingress"].get("completeness"),
                    "ingress_delay_p99": b["ingress"].get("delay_p99"),
                    "records_per_s": b["ingress"].get("records_per_s_30s"),
                }
        with self.lock:
            self.rows.append(row)

    def series(self) -> list[dict]:
        with self.lock:
            return list(self.rows)


class EventLog:
    def __init__(self, path: Path | None, maxlen: int = 500):
        self.path = path
        self.rows: deque = deque(maxlen=maxlen)
        self.lock = threading.Lock()
        if path is not None:
            path.parent.mkdir(parents=True, exist_ok=True)

    def add(self, kind: str, **fields) -> None:
        row = {"t": time.time(), "kind": kind, **fields}
        with self.lock:
            self.rows.append(row)
            if self.path is not None:
                try:
                    with self.path.open("a") as f:
                        f.write(json.dumps(row, default=str) + "\n")
                except OSError:
                    pass

    def recent(self, n: int = 60) -> list[dict]:
        with self.lock:
            return list(self.rows)[-n:]


# ---------------------------------------------------------------- text report

def _s(v, unit="s", digits=2) -> str:
    if v is None:
        return "--"
    if unit == "s":
        return f"{v * 1000:.0f} ms" if abs(v) < 1 else f"{v:.{digits}f} s"
    if unit == "%":
        return f"{v * 100:.1f}%"
    return f"{v}"


def _num(v, digits=1) -> str:
    if v is None or (isinstance(v, float) and (v != v)):
        return "--"
    return f"{v:.{digits}f}" if isinstance(v, float) else str(v)


def _clock(t: float) -> str:
    return time.strftime("%H:%M:%S", time.localtime(t))


def _trend(rows: list[dict], pick, n: int = 6) -> str:
    """A few evenly spaced values over the history, oldest first."""
    vals = [pick(r) for r in rows]
    vals = [v for v in vals if v is not None]
    if not vals:
        return "--"
    if len(vals) <= n:
        sample = vals
    else:
        step = (len(vals) - 1) / (n - 1)
        sample = [vals[round(i * step)] for i in range(n)]
    return " -> ".join(f"{v:.2f}" if isinstance(v, float) else str(v) for v in sample)


def render_text(snap: dict) -> str:
    out: list[str] = []
    sh, pipe = snap["shared"], snap["pipeline"]
    m = pipe.get("metrics", {}) or {}
    stub = m.get("stub") or {}
    out.append(f"IOH demo console snapshot at {_clock(snap['t'])}")
    out.append("")
    out.append("Shared settings")
    out.append(f"  speed {sh['speed']}x | window {sh['window_ms'] / 1000:g} s / slide {sh['slide_ms'] / 1000:g} s | "
               f"inference {sh['service_ms']:g} ms cv {sh['service_cv']:g} x {sh['workers']} workers | "
               f"failure style {sh['failure_style']}"
               + (f" (timeout {sh['shed_timeout_ms']} ms)" if sh['failure_style'] == 'shed' else "")
               + (f" (queue max {sh['reject_queue_max']})" if sh['failure_style'] == 'reject' else ""))
    out.append(f"  async capacity {sh['capacity']} x parallelism {sh['parallelism']} = {sh['capacity'] * sh['parallelism']} in flight | "
               f"watermark bound {sh['watermark_bound_ms']} ms | idleness {sh['idleness_ms']} ms")
    out.append("")
    out.append("Pipeline")
    lights = " ".join(f"{k}={'on' if pipe.get(k) else 'OFF'}" for k in ("kafka", "flink", "stub"))
    out.append(f"  {lights} | job {pipe.get('job_state')} {pipe.get('job_id') or ''} | clock offset {(pipe.get('clock_offset_s') or 0) * 1000:.1f} ms")
    out.append(f"  source back-pressure {_num(m.get('source_backpressure_ms_s'))} ms/s | window+inference busy {_num(m.get('features_busy_ms_s'))} ms/s | "
               f"source in {_num(m.get('source_records_in_s'))} rec/s | kafka lag {_num(m.get('kafka_lag_max'), 0)}")
    out.append(f"  stub: {stub.get('in_flight', '--')} in flight, {stub.get('queued', '--')} queued, {stub.get('served', '--')} served, "
               f"{stub.get('rejected', '--')} rejected, max queued {stub.get('max_queued', '--')}")
    if pipe.get("message"):
        out.append(f"  message: {pipe['message']}")
    out.append("")
    out.append("Beds")
    for b in snap["beds"]:
        head = f"  bed {b['index'] + 1}: {b['status']}"
        if b["status"] == "idle":
            out.append(head)
            continue
        head += f" | case {b['case']} | grain {b['grain']} | start at {b['start_at']:g} s"
        if b.get("elapsed_patient_s") is not None:
            head += f" | {b['elapsed_patient_s'] / 60:.1f} min patient time"
        out.append(head)
        if b.get("message"):
            out.append(f"    {b['message']}")
        ing, ps = b.get("ingress"), b.get("prediction_stats")
        if ing:
            out.append(f"    ingress: {ing['n_records']} records ({ing['n_wave']} wave, {ing['n_numeric']} numeric), "
                       f"{ing['records_per_s_30s']:.1f}/s | delay p50 {_s(ing['delay_p50'])} p99 {_s(ing['delay_p99'])} | "
                       f"complete {_s(ing['completeness'], '%')} | invalid {_s(ing['invalid_fraction'], '%')} | "
                       f"channels {len(ing['channels'])}/{len(ing['channels_expected'])}"
                       + (f" (missing {sorted(set(ing['channels_expected']) - set(ing['channels']))})" if len(ing['channels']) < len(ing['channels_expected']) else ""))
        if ps:
            out.append(f"    prediction: {ps['n']} total, {ps['n_full']} full of {ps['n_due']} due | complete {_s(ps['completeness'], '%')} | "
                       f"latency p50 {_s(ps['latency_p50'])} p99 {_s(ps['latency_p99'])} | staleness p50 {_s(ps['staleness_p50'])} | "
                       f"status {ps['by_status']} | last risk {ps['last_risk'] if ps['last_risk'] is None else round(ps['last_risk'] * 100)}")
        out.append(f"    progress lag {_s(b.get('progress_lag_s'))}")
        recent = b.get("recent_predictions") or []
        if recent:
            tail = recent[-6:]
            out.append("    last predictions: " + ", ".join(
                f"{p['status']}{'*' if p['partial'] else ''} {_s(p['t_pipeline'])}" for p in tail) + "  (* partial)")
    hist = snap.get("history") or []
    if len(hist) >= 2:
        span = hist[-1]["t"] - hist[0]["t"]
        out.append("")
        out.append(f"Trends over the last {span / 60:.1f} min (oldest -> newest)")
        out.append(f"  back-pressure ms/s: {_trend(hist, lambda r: r.get('backpressure_ms_s'))}")
        out.append(f"  stub queued:        {_trend(hist, lambda r: r.get('stub_queued'))}")
        for idx in sorted({k for r in hist for k in r['beds']}):
            out.append(f"  bed {int(idx) + 1} lag s:        {_trend(hist, lambda r, i=idx: r['beds'].get(i, {}).get('lag_s'))}")
            out.append(f"  bed {int(idx) + 1} latency p50:  {_trend(hist, lambda r, i=idx: r['beds'].get(i, {}).get('latency_p50'))}")
            out.append(f"  bed {int(idx) + 1} pred complete:{_trend(hist, lambda r, i=idx: r['beds'].get(i, {}).get('pred_completeness'))}")
    events = snap.get("events") or []
    if events:
        out.append("")
        out.append("Recent events")
        for e in events[-15:]:
            extra = {k: v for k, v in e.items() if k not in ("t", "kind", "job_config", "log_tail")}
            line = f"  {_clock(e['t'])} {e['kind']} {json.dumps(extra, default=str) if extra else ''}"
            out.append(line)
            if e.get("log_tail"):
                for l in e["log_tail"][-3:]:
                    out.append(f"      | {l}")
    return "\n".join(out)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="ioh-demo-snapshot", description=__doc__.split("\n\n")[0])
    ap.add_argument("--url", default="http://localhost:8080")
    ap.add_argument("--json", action="store_true", help="print the raw snapshot instead of the text report")
    ap.add_argument("--file", type=Path, default=None, help="read a saved snapshot.json instead of the server")
    args = ap.parse_args(argv)
    if args.file:
        snap = json.loads(args.file.read_text())
    else:
        import httpx
        try:
            snap = httpx.get(f"{args.url}/api/snapshot", timeout=10).json()
        except Exception as e:  # noqa: BLE001
            print(f"could not reach {args.url}: {e}; try --file results/demo/snapshot.json", file=sys.stderr)
            return 2
    print(json.dumps(snap, indent=1) if args.json else render_text(snap))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
