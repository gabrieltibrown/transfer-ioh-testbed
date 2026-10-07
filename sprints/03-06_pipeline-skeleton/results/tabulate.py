"""Print markdown tables for a list of run folders (results/<id>)."""
import json
import sys
from pathlib import Path

rows = []
for d in sys.argv[1:]:
    d = Path(d)
    meta = json.loads((d / "meta.json").read_text())
    s = json.loads((d / "summary.json").read_text())
    lat = s["latency_s"]
    p = s["predictions"]
    pl = s["progress_lag"]
    stub = meta["stub"]
    po = s.get("poller", {})
    slopes = [v["slope"] for v in pl["per_case"].values() if v["n"] >= 3]
    lagmax = max((v["lag_s"].get("max", 0) for v in pl["per_case"].values()), default=0)
    rows.append({
        "id": meta["run_id"], "label": meta.get("label", ""),
        "cases": meta["schedule"]["n_cases"], "window_s": meta["schedule"]["window_s"],
        "verdict": meta["result"]["verdict"], "offered": meta["result"]["offered_records_per_s"],
        "stub": f"{stub['config']['service_ms']:g} ms x{stub['config']['workers']}",
        "capacity": meta["job"]["config"]["inference_capacity_total"],
        "n": p["n"], "ok": p["n_full_ok"], "status": p["by_status"],
        "tp": lat["t_pipeline"], "st": lat["staleness"], "ww": lat["window_wait"], "qe": lat["queue_excess"],
        "inf": lat["inference"], "sq": lat["stub_queue_wait"], "sink": lat["sink"], "fetch": lat["fetch"],
        "slope_max": max(slopes) if slopes else float("nan"), "slope_p50": sorted(slopes)[len(slopes) // 2] if slopes else float("nan"),
        "lag_max": lagmax,
        "bp": po.get("max_backpressure_ms_per_s", {}), "pending": po.get("max_pending_records"),
        "mem": {k: round(v / 1e6) for k, v in po.get("peak_mem_bytes", {}).items()},
        "cpu": {k: round(v) for k, v in po.get("peak_cpu_pct", {}).items() if v},
        "stub_final": stub.get("final_stats"), "clock": meta["clock"]["offset_s"],
        "harness_p99": meta["result"]["lateness_ms"]["p99"],
    })

def pct(d, k="p50"):
    return f"{d[k]:.3f}" if d.get("N") else "-"

print("| Run | Cases | Stub | Capacity | Predictions (full ok / total) | T_pipeline p50 / p90 / p99 (s) | Staleness p50 (s) | Inference p50 / p99 (s) | Stub queue wait p99 (s) | Progress-lag slope max (s/s) | Lag max (s) |")
print("|---|---|---|---|---|---|---|---|---|---|---|")
for r in rows:
    print(f"| `{r['id']}` {r['label']} | {r['cases']} | {r['stub']} | {r['capacity']} | {r['ok']} / {r['n']} {r['status']} | "
          f"{pct(r['tp'])} / {pct(r['tp'],'p90')} / {pct(r['tp'],'p99')} | {pct(r['st'])} | {pct(r['inf'])} / {pct(r['inf'],'p99')} | "
          f"{pct(r['sq'],'p99')} | {r['slope_max']:+.5f} | {r['lag_max']:.2f} |")
print()
print("| Run | Harness verdict (p99 ms) | Offered rec/s | Clock offset (ms) | Max back-pressure ms/s | Max pending | Peak mem MB | Peak CPU % | Stub final |")
print("|---|---|---|---|---|---|---|---|---|")
for r in rows:
    bp = {k.split(" ")[0].replace(":", ""): round(v) for k, v in r["bp"].items()}
    print(f"| `{r['id']}` | {r['verdict']} ({r['harness_p99']}) | {r['offered']} | {r['clock']*1000:.1f} | {bp} | {r['pending']} | {r['mem']} | {r['cpu']} | "
          f"served {r['stub_final']['served']}, max queued {r['stub_final']['max_queued']}, max in flight {r['stub_final']['max_in_flight']}, rejected {r['stub_final']['rejected']} |")
print()
for r in rows:
    print(f"{r['id']}: window_wait p50 {pct(r['ww'])} queue_excess p50 {pct(r['qe'])} p99 {pct(r['qe'],'p99')}; sink p50 {pct(r['sink'])}; fetch p50 {pct(r['fetch'])}; slope p50 {r['slope_p50']:+.5f}")
