import json

import pytest

from ioh_testbed.replay.config import RunConfig, Scenario, Workload
from ioh_testbed.replay.reader import VitalDBSource
from ioh_testbed.replay.schedule import build_schedule, select_cases
from tests.conftest import REPO, blocks

ART = {"srate": 500, "gain": 0.98, "bias": 31861.0, "unit": "mmHg"}


def small_workload(tmp_path, window=30, tracks=None):
    tracks = tracks or [
        {"source": "SNUADC/ART", "label": "ART", "kind": "wave", "target_hz": 125},
        {"source": "SNUADC/ECG_II", "label": "ECG_II", "kind": "wave", "target_hz": 500},
        {"source": "Solar8000/HR", "label": "HR", "kind": "numeric", "target_hz": 1},
    ]
    p = tmp_path / "wl.yaml"
    p.write_text(json.dumps({"name": "t", "observation_window_s": window,
                             "profiles": [{"name": "p", "tracks": tracks}]}))
    return Workload.load(p)


def scenario(ms):
    return Scenario("s", ms)


def test_all_streams_of_a_case_share_one_origin(tmp_path, vitaldb_case_factory):
    # ART starts at 1000, ECG at 1003, HR observations begin at 1005: all rebased from 1000.
    vitaldb_case_factory("1.parquet",
        waves={"SNUADC/ART": {**ART, "blocks": blocks(1000.0, 500, 40)},
               "SNUADC/ECG_II": {**ART, "blocks": blocks(1003.0, 500, 37)}},
        numerics={"Solar8000/HR": {"unit": "/min", "points": [(1005.0 + 2 * i, 80) for i in range(20)]}})
    cfg = RunConfig(small_workload(tmp_path), scenario(256))
    sched = build_schedule(cfg, VitalDBSource(tmp_path),
                           [{"caseid": 1, "duration_s": 40}], n_cases=1)
    case = sched.cases[0]
    assert case.t_start == 1000.0
    by_label = {e.label: e for e in case.emitters}
    for e in by_label.values():
        assert e.advance()
    assert by_label["ART"].current.t_event_rel == pytest.approx(0.0)
    assert by_label["ECG_II"].current.t_event_rel == pytest.approx(3.0)
    assert by_label["HR"].current.t_event_rel == pytest.approx(5.0)


def test_wave_packets_are_emitted_when_complete(tmp_path, vitaldb_case_factory):
    # A packet whose first sample is at t can only exist at t + packet_ms.
    vitaldb_case_factory("1.parquet", waves={"SNUADC/ART": {**ART, "blocks": blocks(1000.0, 500, 40)}},
                         numerics={"Solar8000/HR": {"unit": "/min", "points": [(1000.0, 80)]}})
    cfg = RunConfig(small_workload(tmp_path), scenario(10000))
    case = build_schedule(cfg, VitalDBSource(tmp_path), [{"caseid": 1, "duration_s": 40}], 1).cases[0]
    by_label = {e.label: e for e in case.emitters}
    for e in by_label.values():
        assert e.advance()
    assert by_label["ART"].current.t_event_rel == pytest.approx(0.0)
    assert by_label["ART"].current.t_rel == pytest.approx(10.0)
    assert by_label["HR"].current.t_rel == by_label["HR"].current.t_event_rel == pytest.approx(0.0)


def test_window_truncates_emissions(tmp_path, vitaldb_case_factory):
    vitaldb_case_factory("1.parquet", waves={"SNUADC/ECG_II": {**ART, "blocks": blocks(1000.0, 500, 100)}})
    tracks = [{"source": "SNUADC/ECG_II", "label": "ECG_II", "kind": "wave", "target_hz": 500}]
    cfg = RunConfig(small_workload(tmp_path, window=30, tracks=tracks), scenario(256))
    sched = build_schedule(cfg, VitalDBSource(tmp_path), [{"caseid": 1, "duration_s": 100}], 1)
    em = sched.cases[0].emitters[0]
    n = 0
    while em.advance():
        assert em.current.t_rel < 30
        n += 1
    assert n == 117  # emitted at (k + 1) * 0.256 < 30  ->  k <= 116


def test_window_crop_still_yields_full_packets_at_10s(tmp_path, vitaldb_case_factory):
    vitaldb_case_factory("1.parquet", waves={"SNUADC/ECG_II": {**ART, "blocks": blocks(1000.0, 500, 100)}})
    tracks = [{"source": "SNUADC/ECG_II", "label": "ECG_II", "kind": "wave", "target_hz": 500}]
    cfg = RunConfig(small_workload(tmp_path, window=30, tracks=tracks), scenario(10000))
    em = build_schedule(cfg, VitalDBSource(tmp_path), [{"caseid": 1, "duration_s": 100}], 1).cases[0].emitters[0]
    pk = []
    while em.advance():
        pk.append(em.current.payload)
    assert len(pk) == 2 and all(p.n == 5000 and p.unavailable.size == 0 for p in pk)  # complete by 10 s, 20 s


def test_missing_source_is_recorded_and_other_streams_proceed(tmp_path, vitaldb_case_factory):
    vitaldb_case_factory("1.parquet", waves={"SNUADC/ART": {**ART, "blocks": blocks(1000.0, 500, 40)}})
    cfg = RunConfig(small_workload(tmp_path), scenario(256))
    case = build_schedule(cfg, VitalDBSource(tmp_path), [{"caseid": 1, "duration_s": 40}], 1).cases[0]
    assert case.missing == ("SNUADC/ECG_II", "Solar8000/HR")
    assert [e.label for e in case.emitters] == ["ART"]


def test_select_cases_requires_window_length():
    cases = [{"caseid": 1, "duration_s": 100}, {"caseid": 2, "duration_s": 20}, {"caseid": 3, "duration_s": 60}]
    assert [c["caseid"] for c in select_cases(cases, 2, 30)] == [1, 3]
    with pytest.raises(ValueError, match="only 2"):
        select_cases(cases, 3, 30)


def test_round_robin_profiles_across_cases(tmp_path, vitaldb_case_factory):
    for cid in (1, 2, 3):
        vitaldb_case_factory(f"{cid}.parquet", waves={"SNUADC/ECG_II": {**ART, "blocks": blocks(1000.0, 500, 40)}})
    wl = Workload.load(REPO / "configs" / "workload" / "mixed.yaml")
    wl = Workload(wl.name, wl.profiles, observation_window_s=30)
    sched = build_schedule(RunConfig(wl, scenario(10000)), VitalDBSource(tmp_path),
                           [{"caseid": c, "duration_s": 40} for c in (1, 2, 3)], 3)
    assert [c.profile for c in sched.cases] == ["standard_anaesthesia", "invasive_cardiac", "minimal_monitoring"]
    assert sched.describe()["n_cases"] == 3


@pytest.mark.vitaldb
def test_real_cases_schedule_quickly_with_window_crop():
    import time
    manifest = json.loads((REPO / "src/ioh_testbed/replay/manifest.json").read_text())["cases"]
    wl = Workload.load(REPO / "configs/workload/standard_anaesthesia.yaml")
    wl = Workload(wl.name, wl.profiles, observation_window_s=60)
    t = time.perf_counter()
    sched = build_schedule(RunConfig(wl, Scenario.load(REPO / "configs/scenarios/dwc_10s.yaml")),
                           VitalDBSource(REPO / "data/vitaldb"), manifest, n_cases=3)
    assert time.perf_counter() - t < 20
    assert len(sched.cases) == 3
    assert all(len(c.emitters) >= 10 for c in sched.cases)


def test_explicit_case_ids_in_given_order_and_key_suffix(tmp_path, vitaldb_case_factory):
    for cid in (3, 7):
        vitaldb_case_factory(f"{cid}.parquet", waves={"SNUADC/ART": {**ART, "blocks": blocks(1000.0, 500, 40)}})
    tracks = [{"source": "SNUADC/ART", "label": "ART", "kind": "wave", "target_hz": 125}]
    cfg = RunConfig(small_workload(tmp_path, tracks=tracks), scenario(10000))
    manifest = [{"caseid": 3, "duration_s": 40}, {"caseid": 5, "duration_s": 40}, {"caseid": 7, "duration_s": 40}]
    sched = build_schedule(cfg, VitalDBSource(tmp_path), manifest, 1, case_ids=[7, 3], key_suffix="-b2")
    assert [c.case_id for c in sched.cases] == ["7-b2", "3-b2"]
    assert all(e.case_id == "7-b2" for e in sched.cases[0].emitters)
    with pytest.raises(ValueError, match="not in the manifest"):
        select_cases(manifest, 1, 30, case_ids=[9])
    with pytest.raises(ValueError, match="shorter"):
        select_cases(manifest, 1, 30, case_ids=[3], start_s=20)


def test_start_at_rebases_every_stream_from_the_offset(tmp_path, vitaldb_case_factory):
    # 100 s of ART from 1000, HR every 2 s; start 30 s in with a 30 s window.
    vitaldb_case_factory("1.parquet",
        waves={"SNUADC/ART": {**ART, "blocks": blocks(1000.0, 500, 100)}},
        numerics={"Solar8000/HR": {"unit": "/min", "points": [(1000.0 + 2 * i, 60 + i) for i in range(50)]}})
    cfg = RunConfig(small_workload(tmp_path, window=30, tracks=[
        {"source": "SNUADC/ART", "label": "ART", "kind": "wave", "target_hz": 125},
        {"source": "Solar8000/HR", "label": "HR", "kind": "numeric", "target_hz": 1}]), scenario(10000))
    sched = build_schedule(cfg, VitalDBSource(tmp_path), [{"caseid": 1, "duration_s": 100}], 1, start_s=30)
    case = sched.cases[0]
    assert case.t_start == 1030.0
    by = {e.label: e for e in case.emitters}
    pk = []
    while by["ART"].advance():
        pk.append(by["ART"].current)
    # packets complete at 40, 50 (and 60 is outside the 30 s window): emitted at t_rel 10 and 20
    assert [round(e.t_rel) for e in pk] == [10, 20] and [round(e.t_event_rel) for e in pk] == [0, 10]
    hr = []
    while by["HR"].advance():
        hr.append(by["HR"].current)
    assert len(hr) == 30 and hr[0].t_rel == 0.0 and hr[0].payload == 60 + 15  # observation at 1030 is index 15
