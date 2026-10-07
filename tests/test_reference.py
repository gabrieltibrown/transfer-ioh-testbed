import json

import numpy as np
import pytest

from ioh_testbed.benchmark.reference import (
    compare, dwc_ms, expected_fired, physical, reference_windows, window_starts,
)
from ioh_testbed.replay.config import RunConfig, Scenario, Workload
from ioh_testbed.replay.reader import VitalDBSource
from ioh_testbed.replay.schedule import build_schedule
from tests.conftest import blocks

ART = {"srate": 500, "gain": 0.9874570312500001, "bias": 31861.366, "unit": "mmHg"}


def workload(tmp_path, window=120):
    p = tmp_path / "wl.yaml"
    p.write_text(json.dumps({"name": "t", "observation_window_s": window, "profiles": [{"name": "p", "tracks": [
        {"source": "SNUADC/ART", "label": "ART", "kind": "wave", "target_hz": 125},
        {"source": "Solar8000/HR", "label": "HR", "kind": "numeric", "target_hz": 1},
    ]}]}))
    return Workload.load(p)


def test_window_assignment_matches_flink_semantics():
    # size 60 s, slide 20 s, offset 0: a timestamp belongs to the three windows whose
    # start is the slide-aligned floor and the two before it.
    assert window_starts(1_000_000, 60_000, 20_000) == [1_000_000, 980_000, 960_000]
    assert window_starts(1_019_999, 60_000, 20_000) == [1_000_000, 980_000, 960_000]
    assert window_starts(1_020_000, 60_000, 20_000) == [1_020_000, 1_000_000, 980_000]


def test_dwc_ms_truncates_like_the_wire_format():
    assert dwc_ms(1791366600.1234) == 1791366600123
    assert dwc_ms(1791366600.9999) == 1791366600999


def test_physical_matches_gain_bias():
    raw = np.array([-32768.0, -32000.0, 0.0, 32767.0])
    assert np.allclose(physical(raw, ART["gain"], ART["bias"]), raw * ART["gain"] + ART["bias"], rtol=1e-12)


def test_reference_windows_counts_and_partial_flags(tmp_path, vitaldb_case_factory):
    vitaldb_case_factory("1.parquet",
        waves={"SNUADC/ART": {**ART, "blocks": blocks(1000.0, 500, 120)}},
        numerics={"Solar8000/HR": {"unit": "/min", "points": [(1000.0 + 2 * i, 60 + i) for i in range(60)]}})
    cfg = RunConfig(workload(tmp_path), Scenario("s", 10000))
    sched = build_schedule(cfg, VitalDBSource(tmp_path), [{"caseid": 1, "duration_s": 120}], 1)
    t0_wall = 1_700_000_010.0  # 10 s past a 20 s boundary, so the first windows are partial
    wins = reference_windows(sched, t0_wall, 60_000, 20_000)
    keys = sorted(wins)
    assert all(k[0] == "1" for k in keys)
    first = min(k[1] for k in keys)
    assert first == 1_700_000_010_000 - 10_000 - 40_000  # ts 1_700_000_010_000 -> starts 1_700_000_000_000, -20 s, -40 s
    w0 = wins[("1", 1_700_000_000_000, 1_700_000_060_000)]
    assert w0.partial  # starts before the case's first event
    # numerics: 1 Hz from t0; the window [1_700_000_000, +60) holds HR at 10..59 s -> 50 values
    assert w0.labels["HR"].count == 50
    assert w0.labels["HR"].last == 60 + 24  # ZOH of 0.5 Hz input: value at 59 s is observation 24 (t = 48 s)
    # waves: 10 s packets stamped at their last sample: 10 + 9.992 s and 20 + 9.992 s ... -> 19.992, 29.992, 39.992, 49.992, 59.992 -> 5 packets
    assert w0.labels["ART"].records == 5 and w0.n_wave == 5
    assert w0.labels["ART"].count == 5 * 1250 and w0.labels["ART"].invalid == 0
    full = wins[("1", 1_700_000_020_000, 1_700_000_080_000)]
    assert not full.partial
    assert full.labels["HR"].count == 60
    assert full.labels["ART"].mean == pytest.approx(1000 * ART["gain"] + ART["bias"])


def test_expected_fired_respects_watermark_bound(tmp_path, vitaldb_case_factory):
    vitaldb_case_factory("1.parquet",
        waves={"SNUADC/ART": {**ART, "blocks": blocks(1000.0, 500, 120)}},
        numerics={"Solar8000/HR": {"unit": "/min", "points": [(1000.0 + 2 * i, 60) for i in range(60)]}})
    cfg = RunConfig(workload(tmp_path), Scenario("s", 10000))
    sched = build_schedule(cfg, VitalDBSource(tmp_path), [{"caseid": 1, "duration_s": 120}], 1)
    wins = reference_windows(sched, 1_700_000_000.0, 60_000, 20_000)
    max_event = max(w.t_event_newest_ms for w in wins.values())
    fired = expected_fired(wins, bound_ms=500)
    assert fired and all(w.end_ms <= max_event - 500 for w in fired.values())
    assert any(w.end_ms > max_event - 500 for w in wins.values())  # the tail never fires


def test_compare_detects_mismatches(tmp_path, vitaldb_case_factory):
    vitaldb_case_factory("1.parquet",
        numerics={"Solar8000/HR": {"unit": "/min", "points": [(1000.0 + 2 * i, 60 + i) for i in range(60)]}})
    p = tmp_path / "wl.yaml"
    p.write_text(json.dumps({"name": "t", "observation_window_s": 120, "profiles": [{"name": "p", "tracks": [
        {"source": "Solar8000/HR", "label": "HR", "kind": "numeric", "target_hz": 1}]}]}))
    cfg = RunConfig(Workload.load(p), Scenario("s", 10000))
    sched = build_schedule(cfg, VitalDBSource(tmp_path), [{"caseid": 1, "duration_s": 120}], 1)
    wins = reference_windows(sched, 1_700_000_000.0, 60_000, 20_000)
    ref = wins[("1", 1_700_000_000_000, 1_700_000_060_000)]
    ok = {"nRecords": ref.n_records, "nWave": 0, "nNumeric": ref.n_numeric, "partial": False,
          "tEventOldestMs": ref.t_event_oldest_ms, "tEventNewestMs": ref.t_event_newest_ms,
          "features": {"HR": {"kind": "numeric", "mean": ref.labels["HR"].mean, "min": ref.labels["HR"].min,
                              "max": ref.labels["HR"].max, "count": ref.labels["HR"].count,
                              "records": ref.labels["HR"].records, "last": ref.labels["HR"].last}}}
    assert compare(ok, ref) == []
    bad = json.loads(json.dumps(ok))
    bad["features"]["HR"]["mean"] += 0.01
    bad["nRecords"] += 1
    errs = compare(bad, ref)
    assert len(errs) == 2 and any("mean" in e for e in errs) and any("nRecords" in e for e in errs)
