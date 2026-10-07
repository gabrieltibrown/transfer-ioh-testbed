import numpy as np
import pytest

from ioh_testbed.demo.tap import BedState, ingress_terms, percentile, reduce_wave, windows_due
from ioh_testbed.replay.packetize import WavePacket
from ioh_testbed.replay.reader import SampleStream
from ioh_testbed.replay.records import numeric_record, wave_record

GAIN, BIAS = 0.9874570312500001, 31861.366


def wave(hz=500.0, n=5000, t0=1.7e9, invalid=(), unavailable=(), seq=1, packet_ms=10000):
    vals = (np.arange(n, dtype=float) % 100) - 32000.0
    for i in invalid:
        vals[i] = np.nan
    pk = WavePacket("SNUADC/ART", 0.0, hz, vals, np.array(invalid, dtype=int), np.array(unavailable, dtype=int))
    s = SampleStream("SNUADC/ART", hz, GAIN, BIAS, "mmHg", [])
    return wave_record(pk, s, case_id="1", label="ART", seq=seq, event_ts=t0, packet_ms=packet_ms,
                       t_sched=t0 + packet_ms / 1000, t_produce=t0 + packet_ms / 1000 + 0.0005)


def test_reduce_wave_buckets_and_decodes():
    rec = wave(hz=500.0, n=5000)
    red = reduce_wave(rec)
    assert red["label"] == "ART" and red["hz"] == 500.0 and red["n"] == 5000
    assert len(red["points"]) == 1000  # 10 s at 100 buckets/s
    t, lo, hi = red["points"][0]
    assert t == pytest.approx(1.7e9, abs=1e-3)
    raw = np.arange(5, dtype=float) - 32000.0
    assert lo == pytest.approx((raw * GAIN + BIAS).min(), rel=1e-9)
    assert hi == pytest.approx((raw * GAIN + BIAS).max(), rel=1e-9)
    assert red["t_last"] == pytest.approx(1.7e9 + 4999 / 500.0)


def test_reduce_wave_passes_low_rates_through_and_marks_gaps():
    rec = wave(hz=62.5, n=625, invalid=[3], unavailable=[623, 624], packet_ms=10000)
    red = reduce_wave(rec)
    assert len(red["points"]) == 625  # one sample per bucket at 62.5 Hz
    assert red["points"][3] == [pytest.approx(1.7e9 + 3 / 62.5, abs=1e-3), None, None]
    assert red["points"][624][1] is None
    assert red["n_invalid"] == 1 and red["n_unavailable"] == 2
    ok = red["points"][0]
    assert ok[1] == ok[2]


def test_ingress_terms():
    rec = wave()
    t_append = rec["_t_produce"] + 0.002
    terms = ingress_terms(rec, t_append)
    assert terms["delay"] == pytest.approx(0.0025, abs=1e-6)
    assert terms["staleness"] == pytest.approx(t_append - rec["_event_ts_last"])


def test_windows_due_on_epoch_grid():
    # first event at 1005 on a 20 s slide: first full window [1020, 1080) fires once event time >= 1080
    assert windows_due(1070, 1005, 60, 20) == 0
    assert windows_due(1080, 1005, 60, 20) == 1
    assert windows_due(1119, 1005, 60, 20) == 2
    assert windows_due(1200, 1005, 60, 20) == 7


def test_bed_state_frame_counts_completeness_and_deltas():
    bed = BedState("1-b0", channel_rates={"ART": 0.1, "HR": 1.0, "SPO2": 1.0})
    t0 = 1.7e9
    bed.add_wave(wave(t0=t0), t0 + 10.0 + 0.003)
    for i in range(10):
        bed.add_numeric(numeric_record(case_id="1", label="HR", seq=i + 1, event_ts=t0 + i, value=70.0 + i,
                                       unit="/min", t_sched=t0 + i, t_produce=t0 + i + 0.0004), t0 + i + 0.002)
    f = bed.frame(60.0, 20.0, now=t0 + 11)
    assert f["ingress"]["n_records"] == 11 and f["ingress"]["n_wave"] == 1 and f["ingress"]["n_numeric"] == 10
    assert f["elapsed_patient_s"] == pytest.approx(9.998)
    # due per channel from its own first record: ART 0.1/s and HR 1/s over 9.998 s; SPO2 never appeared
    assert f["ingress"]["completeness"] == pytest.approx(min(1.0, 11 / (9.998 * 1.1)))
    assert f["ingress"]["channels_expected"] == ["ART", "HR", "SPO2"]
    assert f["ingress"]["delay_p50"] == pytest.approx(0.002, abs=2e-3)
    assert f["ingress"]["channels"] == ["ART", "HR"]
    assert len(f["waves"]) == 1 and len(f["numerics"]) == 10
    # deltas are consumed by the frame
    f2 = bed.frame(60.0, 20.0, now=t0 + 11)
    assert f2["waves"] == [] and f2["numerics"] == []
    assert f2["prediction_stats"]["n"] == 0 and f2["prediction_stats"]["completeness"] is None
    # a full snapshot still has everything
    f3 = bed.frame(60.0, 20.0, now=t0 + 11, delta=False)
    assert len(f3["waves"]) == 1 and len(f3["numerics"]) == 10
    # a channel that connects late is counted from when it did
    bed.add_numeric(numeric_record(case_id="1", label="SPO2", seq=1, event_ts=t0 + 8, value=98.0, unit="%",
                                   t_sched=t0 + 8, t_produce=t0 + 8), t0 + 8)
    f_late = bed.frame(60.0, 20.0, now=t0 + 11)
    assert f_late["ingress"]["completeness"] == pytest.approx(min(1.0, 12 / (9.998 * 1.1 + 1.998 * 1.0)))


def test_bed_state_predictions_and_progress():
    bed = BedState("1-b0", channel_rates={"HR": 1.0})
    t0 = 1.7e9
    bed.add_numeric(numeric_record(case_id="1", label="HR", seq=1, event_ts=t0, value=70.0, unit="/min",
                                   t_sched=t0, t_produce=t0), t0)
    bed.add_numeric(numeric_record(case_id="1", label="HR", seq=2, event_ts=t0 + 100, value=70.0, unit="/min",
                                   t_sched=t0 + 100, t_produce=t0 + 100), t0 + 100)
    rec = {"windowStartMs": int((t0 + 20) * 1000), "windowEndMs": int((t0 + 80) * 1000), "partial": False,
           "tAppendNewestMs": int((t0 + 80.5) * 1000), "tEventNewestMs": int((t0 + 80) * 1000), "nRecords": 500,
           "inference": {"status": "ok", "risk": 0.42, "tSentMs": 1, "tReturnedMs": 61}}
    bed.add_prediction(rec, t_receipt=t0 + 81.7, t_pred_append_s=t0 + 81.69, offset_s=0.0)
    bed.add_progress({"tWallMs": int((t0 + 100) * 1000), "tEventNewestSeenMs": int((t0 + 99.5) * 1000)}, offset_s=0.0)
    f = bed.frame(60.0, 20.0, now=t0 + 101)
    ps = f["prediction_stats"]
    assert ps["n"] == 1 and ps["by_status"] == {"ok": 1} and ps["last_risk"] == 0.42
    assert ps["latency_p50"] == pytest.approx(1.2)
    assert ps["staleness_p50"] == pytest.approx(1.7)
    assert ps["n_due"] == windows_due(t0 + 100, t0, 60, 20, 2.0) and ps["completeness"] == pytest.approx(1 / ps["n_due"])
    assert windows_due(1080, 1005, 60, 20, grace_s=2.0) == 0 and windows_due(1082, 1005, 60, 20, grace_s=2.0) == 1
    assert f["predictions"][0]["inference_s"] == pytest.approx(0.06)
    assert f["progress_lag_s"] == pytest.approx(0.5)


def test_empty_statistics_are_none_not_nan():
    assert percentile([], 50) is None
    assert percentile([float("nan")], 50) is None
    bed = BedState("1-b0", channel_rates={"HR": 1.0})
    f = bed.frame(60.0, 20.0, now=1.7e9)
    assert f["ingress"]["delay_p50"] is None and f["prediction_stats"]["latency_p50"] is None
