import json
import re

import numpy as np
import pytest

from ioh_testbed.replay.packetize import packetize_wave
from ioh_testbed.replay.reader import SampleStream, Segment
from ioh_testbed.replay.records import (
    SequenceCounter,
    decode_dwc,
    format_dwc_ts,
    numeric_record,
    parse_dwc_ts,
    to_json_bytes,
    wave_record,
)

GAIN, BIAS = 0.9874570312500001, 31861.366


def art_stream(values, srate=500.0):
    return SampleStream("SNUADC/ART", srate, GAIN, BIAS, "mmHg",
                        [Segment(1000.0, np.asarray(values, dtype=float))])


def first_packet(values, packet_ms=256, hz=500, srate=500.0):
    s = art_stream(values, srate)
    return s, next(packetize_wave(s, packet_ms, hz))


def test_wave_record_shape_and_cadence_fields():
    s, pkt = first_packet(np.arange(2500.0))
    rec = wave_record(pkt, s, case_id="7", label="ART", seq=1, event_ts=1.7e9, packet_ms=256)
    assert rec["p_patnr"] == "7" and rec["_case_id"] == "7"
    assert rec["c_label"] == "ART" and rec["c_sequence_number"] == 1
    assert rec["c_n_samples"] == 128 == len(rec["c_value"])
    assert rec["c_hz"] == 500.0 and rec["c_sample_period"] == 2
    assert rec["_packet_ms"] == 256 and rec["_source"] == "ioh-testbed-replay"
    assert all(isinstance(v, int) for v in rec["c_value"])


def test_c_hz_is_float_so_62_5_is_not_truncated():
    s, pkt = first_packet(np.arange(625.0), packet_ms=256, hz=62.5, srate=62.5)
    rec = wave_record(pkt, s, case_id="1", label="CO2", seq=1, event_ts=1.7e9, packet_ms=256)
    assert rec["c_hz"] == 62.5
    assert rec["c_sample_period"] == 16


@pytest.mark.parametrize("raw", [-32768, -32280, 0, 31861, 32767])
def test_scale_fields_round_trip_the_decode(raw):
    s, pkt = first_packet(np.arange(500.0))
    rec = wave_record(pkt, s, case_id="1", label="ART", seq=1, event_ts=1.7e9, packet_ms=256)
    assert decode_dwc(raw, rec) == pytest.approx(raw * GAIN + BIAS, rel=1e-12)
    assert decode_dwc(raw, rec) == pytest.approx(s.physical(np.array([raw]))[0], rel=1e-12)


def test_invalid_and_unavailable_indices_carried_and_values_substituted():
    vals = np.arange(2500.0)
    vals[2499] = np.nan
    s = art_stream(vals)
    last = list(packetize_wave(s, 256, 500))[-1]
    rec = wave_record(last, s, case_id="1", label="ART", seq=20, event_ts=1.7e9, packet_ms=256)
    assert rec["c_invalid_samples"] == [67]
    assert rec["c_unavailable_samples"] == list(range(68, 128))
    assert rec["c_value"][67] == 0 and all(v == 0 for v in rec["c_value"][68:])
    assert rec["c_value"][66] == 2498


def test_dwc_timestamp_format_and_round_trip():
    ts = 1_727_394_113.480
    s = format_dwc_ts(ts)
    assert re.fullmatch(r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{3} \+00:00", s), s
    assert s == "2024-09-26 23:41:53.480 +00:00"
    assert parse_dwc_ts(s) == pytest.approx(ts, abs=1e-3)
    # LIVIA's real-world example with a non-UTC offset parses too
    assert parse_dwc_ts("2024-09-26 23:41:53.480 +02:00") == pytest.approx(ts - 7200, abs=1e-3)


def test_sequence_counter_is_per_case_label_and_gapless():
    c = SequenceCounter()
    assert [c.next("1", "ART") for _ in range(3)] == [1, 2, 3]
    assert c.next("1", "ECG_II") == 1
    assert c.next("2", "ART") == 1
    assert c.next("1", "ART") == 4


def test_numeric_record():
    rec = numeric_record(case_id="3", label="HR", seq=5, event_ts=1.7e9, value=81.0, unit="/min",
                         t_sched=1.7e9 + 0.001, t_produce=1.7e9 + 0.002)
    assert rec["p_patnr"] == "3" and rec["c_label"] == "HR" and rec["c_value"] == 81.0
    assert rec["c_sequence_number"] == 5 and rec["c_unit_label"] == "/min"
    assert rec["_t_produce"] > rec["_t_sched"] > rec["_event_ts"]


def test_numeric_nan_is_rejected():
    with pytest.raises(ValueError, match="finite"):
        numeric_record(case_id="3", label="HR", seq=1, event_ts=1.7e9, value=float("nan"), unit="")


def test_json_encoding_is_compact_and_refuses_nan():
    s, pkt = first_packet(np.arange(500.0))
    rec = wave_record(pkt, s, case_id="1", label="ART", seq=1, event_ts=1.7e9, packet_ms=256)
    b = to_json_bytes(rec)
    assert b" " not in b.split(b'"c_time_stamp_wave_sample"')[0]  # compact separators
    assert json.loads(b)["c_n_samples"] == 128
    rec["c_calibration_abs_lower"] = float("nan")
    with pytest.raises(ValueError):
        to_json_bytes(rec)


def test_json_size_inflation_is_as_documented():
    # stream-model 1.8: JSON is roughly 2.5x the raw int16 payload. Bound it so a
    # change in encoding shows up in tests rather than silently in load figures.
    s, pkt = first_packet(np.random.default_rng(0).integers(-32768, 32767, 5000).astype(float),
                          packet_ms=10000)
    b = to_json_bytes(wave_record(pkt, s, case_id="1", label="ART", seq=1, event_ts=1.7e9, packet_ms=10000))
    ratio = len(b) / (5000 * 2)
    assert 2.0 < ratio < 4.0, ratio
