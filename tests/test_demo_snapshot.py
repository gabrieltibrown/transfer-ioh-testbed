import json

from ioh_testbed.demo.snapshot import EventLog, History, render_text, take_snapshot


def state(bp=0.0, queued=0):
    return {
        "beds": [{"index": i, "case": 1 if i == 0 else None, "grain": "dwc_10s", "start_at": 0.0,
                  "status": "running" if i == 0 else "idle", "key": "1-b0" if i == 0 else "", "started_wall": 0,
                  "run_id": "", "message": ""} for i in range(4)],
        "shared": {"speed": 1.0, "window_ms": 60000, "slide_ms": 20000, "service_ms": 50.0, "service_cv": 0.0,
                   "workers": 4, "failure_style": "wait", "shed_timeout_ms": 10000, "reject_queue_max": 4,
                   "capacity": 8, "idleness_ms": 1000, "watermark_bound_ms": 500, "parallelism": 2},
        "pipeline": {"kafka": True, "flink": True, "stub": True, "job_state": "RUNNING", "job_id": "abc",
                     "message": "", "clock_offset_s": -0.002, "event_origin": 0, "job_config": {"x": 1},
                     "metrics": {"source_backpressure_ms_s": bp, "features_busy_ms_s": 5.0,
                                 "stub": {"in_flight": 1, "queued": queued, "served": 10, "rejected": 0, "max_queued": 3}}},
        "grains": ["dei_256ms", "dwc_10s"], "failure_styles": ["wait", "shed", "reject"],
        "profile": {"name": "p", "waves": [], "numerics": []},
    }


def bed_stats(lag=0.3, latency=1.2):
    return [{
        "bed": 0, "key": "1-b0", "first_event": 0, "last_event": 100, "elapsed_patient_s": 100.0, "units": {},
        "ingress": {"n_records": 850, "n_wave": 50, "n_numeric": 800, "records_per_s_30s": 8.5, "delay_p50": 0.006,
                    "delay_p99": 0.012, "staleness_p50": 0.01, "completeness": 0.99, "invalid_fraction": 0.0,
                    "channels": ["ART", "HR"], "channels_expected": ["ART", "HR", "SPO2"]},
        "prediction_stats": {"n": 3, "n_full": 2, "n_due": 2, "completeness": 1.0, "by_status": {"ok": 3},
                             "latency_p50": latency, "latency_p99": latency + 0.1, "staleness_p50": latency + 0.02,
                             "last_risk": 0.42},
        "progress_lag_s": lag,
        "recent_predictions": [{"status": "ok", "partial": True, "t_pipeline": 1.1}, {"status": "ok", "partial": False, "t_pipeline": latency}],
    }]


def test_snapshot_merges_bed_stats_and_drops_job_config_from_pipeline():
    snap = take_snapshot(state(), bed_stats())
    assert snap["beds"][0]["ingress"]["n_records"] == 850 and snap["beds"][1]["ingress"] is None
    assert "job_config" not in snap["pipeline"] and snap["job_config"] == {"x": 1}
    assert snap["beds"][0]["recent_predictions"][-1]["t_pipeline"] == 1.2


def test_history_keeps_series_and_text_report_shows_trends(tmp_path):
    h = History(maxlen=10)
    for i in range(12):
        h.add(take_snapshot(state(bp=i * 100.0, queued=i), bed_stats(lag=0.3 + i, latency=1.2 + i)))
    rows = h.series()
    assert len(rows) == 10 and rows[-1]["backpressure_ms_s"] == 1100.0 and rows[0]["beds"]["0"]["lag_s"] == 2.3
    ev = EventLog(tmp_path / "events.jsonl")
    ev.add("play", bed=0, case=1)
    ev.add("bed_exit", bed=0, returncode=2, log_tail=["boom"])
    snap = {**take_snapshot(state(bp=1100.0, queued=11), bed_stats(lag=11.3, latency=12.2)), "history": rows, "events": ev.recent()}
    text = render_text(snap)
    assert "failure style wait" in text and "back-pressure 1100.0 ms/s" in text
    assert "bed 1: running | case 1" in text and "missing ['SPO2']" in text
    assert "progress lag 11.30 s" in text and "latency p50 12.20 s" in text
    assert "bed 1 lag s:" in text and "2.30 -> " in text
    assert "play" in text and "bed_exit" in text and "| boom" in text
    lines = (tmp_path / "events.jsonl").read_text().splitlines()
    assert len(lines) == 2 and json.loads(lines[1])["returncode"] == 2


def test_text_report_handles_idle_console():
    text = render_text({**take_snapshot(state(), []), "history": [], "events": []})
    assert "bed 1: running" in text and "bed 2: idle" in text
