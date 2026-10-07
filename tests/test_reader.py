import numpy as np
import pytest

from ioh_testbed.replay.reader import read_vitaldb_case
from tests.conftest import blocks

ART = {"srate": 500, "gain": 0.9874570312500001, "bias": 31861.366, "unit": "mmHg"}


def test_contiguous_blocks_become_one_segment(vitaldb_case_factory):
    p = vitaldb_case_factory(waves={"SNUADC/ART": {**ART, "blocks": blocks(1000.0, 500, 5)}})
    case = read_vitaldb_case(p, ["SNUADC/ART"])
    s = case.waves["SNUADC/ART"]
    assert len(s.segments) == 1
    assert s.n_samples == 2500
    assert s.segments[0].t0 == 1000.0
    assert s.srate == 500 and s.gain == pytest.approx(ART["gain"]) and s.bias == pytest.approx(ART["bias"])


def test_gap_between_blocks_starts_new_segment_not_spliced(vitaldb_case_factory):
    b = blocks(1000.0, 500, 3) + blocks(1007.0, 500, 2)  # 4 s hole after t=1003
    p = vitaldb_case_factory(waves={"SNUADC/ART": {**ART, "blocks": b}})
    s = read_vitaldb_case(p, ["SNUADC/ART"]).waves["SNUADC/ART"]
    assert [(seg.t0, seg.n) for seg in s.segments] == [(1000.0, 1500), (1007.0, 1000)]


def test_null_block_is_a_gap(vitaldb_case_factory):
    b = blocks(1000.0, 500, 2) + [(1002.0, None)] + blocks(1003.0, 500, 2)
    p = vitaldb_case_factory(waves={"SNUADC/ART": {**ART, "blocks": b}})
    s = read_vitaldb_case(p, ["SNUADC/ART"]).waves["SNUADC/ART"]
    assert [(seg.t0, seg.n) for seg in s.segments] == [(1000.0, 1000), (1003.0, 1000)]


def test_null_samples_become_nan_and_are_preserved(vitaldb_case_factory):
    samples = [1000] * 500
    samples[10] = None
    samples[499] = None
    p = vitaldb_case_factory(waves={"SNUADC/ART": {**ART, "blocks": [(1000.0, samples), (1001.0, [1000] * 500)]}})
    s = read_vitaldb_case(p, ["SNUADC/ART"]).waves["SNUADC/ART"]
    v = s.segments[0].values
    assert v.size == 1000
    assert np.isnan(v).sum() == 2
    assert np.isnan(v[10]) and np.isnan(v[499]) and not np.isnan(v[500])


def test_62_5hz_alternating_62_63_blocks_stay_contiguous(vitaldb_case_factory):
    b = [(1000.0 + i, [500] * (62 if i % 2 == 0 else 63)) for i in range(10)]
    p = vitaldb_case_factory(waves={"Primus/CO2": {"srate": 62.5, "gain": 0.075, "bias": 2458.0, "blocks": b}})
    s = read_vitaldb_case(p, ["Primus/CO2"]).waves["Primus/CO2"]
    assert len(s.segments) == 1
    assert s.n_samples == 5 * 62 + 5 * 63


def test_float_track_without_gain_reads_fvals(vitaldb_case_factory):
    b = [(1000.0 + i, [0.5] * 128) for i in range(3)]
    p = vitaldb_case_factory(waves={"BIS/EEG1_WAV": {"srate": 128, "blocks": b}})
    s = read_vitaldb_case(p, ["BIS/EEG1_WAV"]).waves["BIS/EEG1_WAV"]
    assert s.gain == 1.0 and s.bias == 0.0
    assert s.n_samples == 384 and s.segments[0].values[0] == pytest.approx(0.5)


def test_physical_decode(vitaldb_case_factory):
    raw = -32280
    p = vitaldb_case_factory(waves={"SNUADC/ART": {**ART, "blocks": [(1000.0, [raw] * 500)]}})
    s = read_vitaldb_case(p, ["SNUADC/ART"]).waves["SNUADC/ART"]
    assert s.physical(np.array([raw]))[0] == pytest.approx(raw * ART["gain"] + ART["bias"])


def test_numeric_stream(vitaldb_case_factory):
    pts = [(1000.0 + 2 * i, 80 + i) for i in range(5)]
    p = vitaldb_case_factory(numerics={"Solar8000/HR": {"unit": "/min", "points": pts}})
    case = read_vitaldb_case(p, ["Solar8000/HR"])
    n = case.numerics["Solar8000/HR"]
    assert n.t.tolist() == [1000.0, 1002.0, 1004.0, 1006.0, 1008.0]
    assert n.values.tolist() == [80, 81, 82, 83, 84]
    assert n.unit == "/min"


def test_missing_wanted_source_is_reported_not_fatal(vitaldb_case_factory):
    p = vitaldb_case_factory(waves={"SNUADC/ART": {**ART, "blocks": blocks(1000.0, 500, 1)}})
    case = read_vitaldb_case(p, ["SNUADC/ART", "SNUADC/CVP", "Solar8000/CVP"])
    assert case.missing == ("SNUADC/CVP", "Solar8000/CVP")
    assert "SNUADC/ART" in case.waves


def test_track_starting_after_the_window_is_late_start_not_missing(vitaldb_case_factory):
    # HR from case start; the arterial line is connected 400 s in. A 300 s window must
    # report ART as late_start (it exists), not missing (it does not), and must not
    # report it at all when reading the whole recording.
    p = vitaldb_case_factory(
        waves={"SNUADC/ART": {**ART, "blocks": blocks(1400.0, 500, 10)}},
        numerics={"Solar8000/HR": {"points": [(1000.0 + 2 * i, 70) for i in range(300)]}},
    )
    cropped = read_vitaldb_case(p, ["SNUADC/ART", "Solar8000/HR", "SNUADC/CVP"], window_s=300)
    assert cropped.missing == ("SNUADC/CVP",)
    assert cropped.late_start == ("SNUADC/ART",)
    assert "SNUADC/ART" not in cropped.waves and "Solar8000/HR" in cropped.numerics
    full = read_vitaldb_case(p, ["SNUADC/ART", "Solar8000/HR", "SNUADC/CVP"])
    assert full.missing == ("SNUADC/CVP",) and full.late_start == ()
    assert full.waves["SNUADC/ART"].n_samples == 5000


def test_case_span_covers_unwanted_tracks_too(vitaldb_case_factory):
    p = vitaldb_case_factory(
        waves={"SNUADC/ART": {**ART, "blocks": blocks(1000.0, 500, 2)}},
        numerics={"Solar8000/HR": {"points": [(990.0, 70), (1100.0, 71)]}},
    )
    case = read_vitaldb_case(p, ["SNUADC/ART"])  # HR not wanted
    assert case.t_start == 990.0 and case.t_end == 1100.0


@pytest.mark.vitaldb
def test_real_case_1_art(real_case_path):
    case = read_vitaldb_case(real_case_path, ["SNUADC/ART", "Solar8000/ART_MBP"], caseid=1)
    s = case.waves["SNUADC/ART"]
    assert s.srate == 500.0
    assert len(s.segments) == 1  # VitalDB block cadence is exactly 1.0 s, no gaps
    assert s.n_samples == 5_770_549
    nan_frac = np.isnan(np.concatenate([g.values for g in s.segments])).mean()
    assert 0 < nan_frac < 0.001
    assert case.numerics["Solar8000/ART_MBP"].values.size == 5449
    assert case.missing == ()
