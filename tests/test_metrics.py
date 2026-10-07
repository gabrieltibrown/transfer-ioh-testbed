import json

import numpy as np
import pytest

from ioh_testbed.benchmark.analyze import analyze
from ioh_testbed.benchmark.metrics import Clock, percentiles, prediction_terms, progress_lag, slope


def pred(case="1", end_ms=1_000_000, append_newest=990_500, fired=1_000_700, sent=1_000_710, ret=1_000_760,
         pred_append=1_000_765, receipt_s=1000.770, partial=False, status="ok", offset_s=0.0):
    # VM-stamped fields are shifted by the offset so that converting back yields the host values above
    o = int(offset_s * 1000)
    return {
        "caseId": case, "windowStartMs": end_ms - 60_000, "windowEndMs": end_ms, "partial": partial,
        "tEventNewestMs": end_ms - 1_000, "tAppendNewestMs": append_newest + o, "tWindowFiredMs": fired + o,
        "inference": {"status": status, "tSentMs": sent + o, "tReturnedMs": ret + o,
                      "stub": {"queue_wait_ms": 2.0, "service_ms": 45.0}},
        "t_pred_append_ms": pred_append + o, "t_receipt": receipt_s,
    }


def test_terms_without_offset():
    t = prediction_terms(pred(), Clock(0.0), bound_ms=500)
    assert t["t_pipeline"] == pytest.approx(1000.770 - 990.5)
    assert t["t_pipeline_broker"] == pytest.approx((1_000_765 - 990_500) / 1000)
    assert t["staleness"] == pytest.approx(1000.770 - 999.0)
    assert t["window_wait"] == pytest.approx(1000.700 - 990.5)
    assert t["queue_excess"] == pytest.approx(1000.700 - 1000.5)
    assert t["inference"] == pytest.approx(0.050)
    assert t["sink"] == pytest.approx(0.005)
    assert t["fetch"] == pytest.approx(0.005)
    assert t["stub_queue_wait"] == pytest.approx(0.002) and t["stub_service"] == pytest.approx(0.045)
    assert t["t_pipeline"] - t["fetch"] == pytest.approx(t["t_pipeline_broker"])


def test_offset_is_subtracted_from_every_cross_domain_term():
    off = 0.250  # VM clock 250 ms ahead of host
    a = prediction_terms(pred(), Clock(0.0), 500)
    b = prediction_terms(pred(offset_s=off), Clock(off), 500)
    for name in ("t_pipeline", "staleness", "window_wait", "queue_excess", "inference", "sink", "fetch", "t_pipeline_broker"):
        assert b[name] == pytest.approx(a[name]), name
    # ignoring the offset would corrupt every host-vs-VM term by exactly the offset
    c = prediction_terms(pred(offset_s=off), Clock(0.0), 500)
    assert c["t_pipeline"] == pytest.approx(a["t_pipeline"] - off)
    assert c["fetch"] == pytest.approx(a["fetch"] - off)
    assert c["t_pipeline_broker"] == pytest.approx(a["t_pipeline_broker"])  # broker-only: offset-free


def test_progress_lag_and_slope():
    h = {"tWallMs": 2_000_000, "tEventNewestSeenMs": 1_998_500}
    assert progress_lag(h, Clock(0.0)) == pytest.approx(1.5)
    assert progress_lag(h, Clock(0.1)) == pytest.approx(1.4)
    t = np.arange(0, 100, 1.0)
    y = 0.5 + 0.02 * t
    s = slope(t, y)
    assert s["slope"] == pytest.approx(0.02) and s["r2"] == pytest.approx(1.0) and s["fitted_change"] == pytest.approx(1.98)
    assert np.isnan(slope(t[:2], y[:2])["slope"])


def test_percentiles_drop_nan():
    d = percentiles([1.0, float("nan"), 3.0, 2.0])
    assert d["N"] == 3 and d["p50"] == 2.0 and d["max"] == 3.0
    assert percentiles([]) == {"N": 0}


def test_analyze_run_folder(tmp_path):
    meta = {"run_id": "r", "clock": {"offset_s": 0.0}, "job": {"config": {"watermark_bound_ms": 500}}}
    (tmp_path / "meta.json").write_text(json.dumps(meta))
    preds = [pred(case="1", end_ms=1_000_000 + 20_000 * k, append_newest=990_500 + 20_000 * k,
                  fired=1_000_700 + 20_000 * k, sent=1_000_710 + 20_000 * k, ret=1_000_760 + 20_000 * k,
                  pred_append=1_000_765 + 20_000 * k, receipt_s=1000.770 + 20.0 * k, partial=(k == 0))
             for k in range(5)]
    preds.append(pred(case="2", status="timeout"))
    (tmp_path / "predictions.jsonl").write_text("\n".join(json.dumps(p) for p in preds) + "\n")
    prog = [{"type": "progress", "caseId": "1", "tWallMs": 1_000_000 + 1000 * i, "tEventNewestSeenMs": 999_000 + 1000 * i - 10 * i}
            for i in range(30)]
    (tmp_path / "progress.jsonl").write_text("\n".join(json.dumps(h) for h in prog) + "\n")
    s = analyze(tmp_path, slope_tolerance=0.005)
    assert s["predictions"] == {"n": 6, "n_full_ok": 4, "n_partial": 1, "by_status": {"ok": 5, "timeout": 1}, "cases": ["1", "2"]}
    assert s["latency_s"]["t_pipeline"]["N"] == 4
    assert s["latency_s"]["t_pipeline"]["p50"] == pytest.approx(10.27)
    assert s["clock_check_max_abs_s"] < 1e-9
    case1 = s["progress_lag"]["per_case"]["1"]
    assert case1["slope"] == pytest.approx(0.010, abs=1e-6)  # lag grows 10 ms per second
    assert s["stability"]["verdict"] == "degrading"
    assert (tmp_path / "summary.json").exists()
