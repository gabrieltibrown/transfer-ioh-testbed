"""Packet shape, conservation, decimation, gap and validity assertions from
docs/stream-model.md section 4, parametric in packet_ms."""

import numpy as np
import pytest

from ioh_testbed.replay.packetize import packetize_wave, resample_numeric_zoh, samples_per_packet
from ioh_testbed.replay.reader import NumericStream, SampleStream, Segment


def stream(values, srate=500.0, t0=1000.0, source="SNUADC/ART"):
    return SampleStream(source=source, srate=srate, gain=1.0, bias=0.0, unit="",
                        segments=[Segment(t0, np.asarray(values, dtype=float))])


@pytest.mark.parametrize("packet_ms,hz,expected", [
    (10000, 500, 5000), (10000, 125, 1250), (10000, 62.5, 625),
    (256, 500, 128), (256, 125, 32), (256, 62.5, 16),
])
def test_samples_per_packet_matches_stream_model(packet_ms, hz, expected):
    assert samples_per_packet(packet_ms, hz) == expected


def test_non_integer_packet_size_rejected():
    with pytest.raises(ValueError, match="positive integer"):
        samples_per_packet(100, 62.5)  # 6.25 samples


def test_non_integer_stride_rejected():
    with pytest.raises(ValueError, match="stride"):
        list(packetize_wave(stream([0.0] * 500), 256, 200))  # 500/200 = 2.5


@pytest.mark.parametrize("packet_ms", [10000, 256])
def test_every_packet_is_exactly_full_size(packet_ms):
    n = samples_per_packet(packet_ms, 500)
    pk = list(packetize_wave(stream(np.arange(2500.0)), packet_ms, 500))
    assert all(p.n == n for p in pk)


def test_exact_division_no_padding_at_10s():
    pk = list(packetize_wave(stream(np.arange(5000.0)), 10000, 500))
    assert len(pk) == 1 and pk[0].unavailable.size == 0 and pk[0].invalid.size == 0


def test_trailing_partial_packet_is_padded_and_marked_unavailable():
    pk = list(packetize_wave(stream(np.arange(2500.0)), 256, 500))  # 2500 / 128 = 19 r 68
    assert len(pk) == 20
    assert all(p.unavailable.size == 0 for p in pk[:19])
    last = pk[-1]
    assert last.n == 128 and last.n_acquired == 68
    assert last.unavailable.tolist() == list(range(68, 128))
    assert np.isnan(last.values[68:]).all() and not np.isnan(last.values[:68]).any()


@pytest.mark.parametrize("packet_ms", [10000, 256])
def test_conservation_acquired_samples_in_equals_out(packet_ms):
    vals = np.arange(2500.0)
    pk = list(packetize_wave(stream(vals), packet_ms, 500))
    out = np.concatenate([p.values[: p.n_acquired] for p in pk])
    assert out.size == vals.size and np.array_equal(out, vals)


def test_carry_across_source_block_edge_is_seamless():
    # Reader concatenated 5 one-second blocks; a 128-sample packet straddles the
    # 500/501 edge (packet 3 covers indices 384..511) with no seam.
    vals = np.arange(2500.0)
    pk = list(packetize_wave(stream(vals), 256, 500))
    assert pk[3].values[115] == 499.0 and pk[3].values[116] == 500.0
    assert pk[3].t0 == pytest.approx(1000.0 + 3 * 0.256)


def test_packet_timing_follows_cadence():
    pk = list(packetize_wave(stream(np.arange(5000.0)), 256, 500))
    t0s = np.array([p.t0 for p in pk])
    assert np.allclose(np.diff(t0s), 0.256)


def test_decimation_500_to_125_by_stride():
    vals = np.arange(2500.0)
    pk = list(packetize_wave(stream(vals), 256, 125))  # 625 decimated / 32 = 19 r 17
    assert len(pk) == 20 and pk[0].srate == 125.0
    assert pk[0].values[:4].tolist() == [0.0, 4.0, 8.0, 12.0]
    out = np.concatenate([p.values[: p.n_acquired] for p in pk])
    assert np.array_equal(out, vals[::4])
    assert pk[-1].n_acquired == 17


def test_invalid_indices_are_computed_after_decimation():
    vals = np.arange(2500.0)
    vals[8] = np.nan  # survives stride 4 -> decimated index 2
    vals[10] = np.nan  # decimated away: not a multiple of 4
    pk = list(packetize_wave(stream(vals), 256, 125))
    assert pk[0].invalid.tolist() == [2]
    assert sum(p.invalid.size for p in pk) == 1


def test_invalid_indices_local_to_packet_and_distinct_from_padding():
    vals = np.arange(2500.0)
    vals[499] = np.nan
    vals[500] = np.nan
    vals[2499] = np.nan  # last acquired sample of the padded trailing packet
    pk = list(packetize_wave(stream(vals), 256, 500))
    assert pk[3].invalid.tolist() == [115, 116]
    last = pk[-1]
    assert last.invalid.tolist() == [67]
    assert 67 not in last.unavailable


def test_gap_between_segments_is_not_spanned():
    s = SampleStream("SNUADC/ART", 500.0, 1.0, 0.0, "", [
        Segment(1000.0, np.arange(1500.0)),  # 3 s
        Segment(1007.0, np.arange(1000.0)),  # 2 s after a 4 s hole
    ])
    pk = list(packetize_wave(s, 256, 500))
    seg1 = [p for p in pk if p.t0 < 1007.0]
    seg2 = [p for p in pk if p.t0 >= 1007.0]
    assert len(seg1) == 12 and seg1[-1].n_acquired == 1500 - 11 * 128  # 92
    assert seg2[0].t0 == 1007.0 and len(seg2) == 8 and seg2[-1].n_acquired == 1000 - 7 * 128  # 104
    assert sum(p.n_acquired for p in pk) == 2500


def test_zoh_half_hz_to_one_hz():
    n = NumericStream("Solar8000/HR", "/min", np.array([0.0, 2.0, 4.0]), np.array([80.0, 81.0, 82.0]))
    t, v = resample_numeric_zoh(n, 1.0)
    assert t.tolist() == [0.0, 1.0, 2.0, 3.0, 4.0]
    assert v.tolist() == [80.0, 80.0, 81.0, 81.0, 82.0]


def test_zoh_holds_to_t_end_and_handles_unsorted_input():
    n = NumericStream("x", "", np.array([4.0, 0.0, 2.0]), np.array([82.0, 80.0, 81.0]))
    t, v = resample_numeric_zoh(n, 1.0, t_end=6.0)
    assert t.tolist() == [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
    assert v.tolist() == [80.0, 80.0, 81.0, 81.0, 82.0, 82.0, 82.0]


def test_zoh_empty_stream():
    t, v = resample_numeric_zoh(NumericStream("x", "", np.empty(0), np.empty(0)), 1.0)
    assert t.size == 0 and v.size == 0
