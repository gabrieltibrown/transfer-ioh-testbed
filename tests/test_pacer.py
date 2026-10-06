"""The open-loop property, in simulated time so it runs instantly."""

import asyncio
import json

import numpy as np
import pytest

from ioh_testbed.replay.config import WAVE
from ioh_testbed.replay.packetize import WavePacket
from ioh_testbed.replay.pacer import TOPIC_NUMERIC, TOPIC_WAVE, NullSink, Pacer, RunStats
from ioh_testbed.replay.reader import SampleStream
from ioh_testbed.replay.schedule import CasePlan, Emission, Schedule, StreamEmitter


class SimClock:
    """Sleeping advances time by the requested amount plus a fixed overshoot, which
    is what a real event loop does to every sleep."""

    def __init__(self, overshoot_s=0.0):
        self.t = 1000.0
        self.overshoot = overshoot_s

    def now(self):
        return self.t

    def wall(self):
        return 1.7e9 + (self.t - 1000.0)

    async def sleep(self, d):
        self.t += max(d, 0.0) + self.overshoot


class CapturingSink(NullSink):
    """Refusal is keyed on the record, not the call, so the pacer's single retry
    is refused too: that is the persistent-backpressure case being tested."""

    def __init__(self, refuse_seq_multiple=0):
        super().__init__()
        self.records = []
        self.refuse_seq_multiple = refuse_seq_multiple

    def emit(self, topic, key, value):
        rec = json.loads(value)
        if self.refuse_seq_multiple and rec["c_sequence_number"] % self.refuse_seq_multiple == 0:
            raise BufferError("full")
        super().emit(topic, key, value)
        self.records.append((topic, key, rec))


def wave_stream():
    return SampleStream("SNUADC/ART", 500.0, 1.0, 0.0, "mmHg", [])


def wave_emitter(case_id, label, n, cadence_s, start=0.0):
    pk = WavePacket("SNUADC/ART", 0.0, 500.0, np.zeros(128), np.empty(0, int), np.empty(0, int))
    items = (Emission(start + i * cadence_s, pk) for i in range(n))
    return StreamEmitter(case_id, label, WAVE, "mmHg", wave_stream(), items)


def numeric_emitter(case_id, label, n, cadence_s=1.0):
    items = (Emission(i * cadence_s, 80.0 + i) for i in range(n))
    return StreamEmitter(case_id, label, "numeric", "/min", None, items)


def schedule(emitters, speed=1.0, packet_ms=256):
    return Schedule([CasePlan("1", "p", 1000.0, emitters, ())], 30.0, speed, packet_ms)


def run(sched, sink, clock, **kw):
    pacer = Pacer(sink, clock=clock.now, wall=clock.wall, sleep=clock.sleep, **kw)
    return asyncio.run(pacer.run(sched))


def test_lateness_does_not_accumulate_with_overshoot():
    clock = SimClock(overshoot_s=0.001)
    emitters = [wave_emitter("1", f"W{i}", 2000, 0.256) for i in range(3)]
    stats = run(schedule(emitters), NullSink(), clock)
    late_ms = stats.lateness_s * 1000
    assert stats.n_scheduled == 6000
    # Bounded by the single-sleep overshoot, and flat: tick 5000 is no later than tick 5.
    assert late_ms.max() <= 1.0 + 1e-6
    assert late_ms[5000] == pytest.approx(late_ms[5], abs=1e-6)
    assert stats.verdict == "OK"


def test_zero_overshoot_zero_lateness_and_exact_duration():
    clock = SimClock()
    stats = run(schedule([wave_emitter("1", "W", 100, 0.256)]), NullSink(), clock)
    assert stats.lateness_s.max() == 0.0
    assert stats.duration_s == pytest.approx(99 * 0.256)


def test_sequence_numbers_gapless_per_stream_and_topics_routed():
    sink = CapturingSink()
    stats = run(schedule([wave_emitter("7", "ART", 5, 0.256), numeric_emitter("7", "HR", 3)]), sink, SimClock())
    waves = [r for t, k, r in sink.records if t == TOPIC_WAVE]
    nums = [r for t, k, r in sink.records if t == TOPIC_NUMERIC]
    assert [r["c_sequence_number"] for r in waves] == [1, 2, 3, 4, 5]
    assert [r["c_sequence_number"] for r in nums] == [1, 2, 3]
    assert all(k == b"7" for _, k, _ in sink.records)
    assert stats.per_kind == {WAVE: 5, "numeric": 3} and stats.per_case == {"7": 8}


def test_record_timing_fields_are_consistent():
    clock = SimClock(overshoot_s=0.002)
    sink = CapturingSink()
    run(schedule([wave_emitter("1", "W", 10, 0.256)]), sink, clock)
    first, *rest = [r for _, _, r in sink.records]
    # The first emission is due at t0 itself: no sleep, so no overshoot.
    assert first["_t_produce"] == pytest.approx(first["_t_sched"])
    for r in rest:
        assert r["_t_produce"] >= r["_t_sched"]
        assert r["_t_produce"] - r["_t_sched"] == pytest.approx(0.002, abs=1e-6)
    for r in [first, *rest]:
        assert r["_event_ts"] == pytest.approx(r["_t_sched"])  # speed 1: event time == schedule time


def test_speed_compresses_deadlines_and_event_time_together():
    clock = SimClock()
    sink = CapturingSink()
    stats = run(schedule([wave_emitter("1", "W", 11, 1.0)], speed=2.0), sink, clock)
    assert stats.duration_s == pytest.approx(5.0)  # 10 s of source time in 5 s
    ev = [r["_event_ts"] for _, _, r in sink.records]
    assert np.allclose(np.diff(ev), 0.5)
    assert stats.summary()["latency_results_valid"] is False


def test_heap_merges_streams_in_deadline_order():
    sink = CapturingSink()
    run(schedule([wave_emitter("1", "A", 4, 1.0, start=0.0), wave_emitter("1", "B", 4, 1.0, start=0.5)]),
        sink, SimClock())
    assert [r["c_label"] for _, _, r in sink.records] == ["A", "B"] * 4


def test_backpressure_is_counted_not_dropped_silently_and_schedule_unchanged():
    sink = CapturingSink(refuse_seq_multiple=10)
    stats = run(schedule([wave_emitter("1", "W", 100, 0.1)]), sink, SimClock())
    assert stats.n_scheduled == 100
    assert stats.n_backpressure == 10 and stats.n_emitted == 90
    assert stats.n_emitted + stats.n_backpressure == 100
    assert stats.verdict == "INVALID"
    assert stats.duration_s == pytest.approx(99 * 0.1)  # refusal did not slow the schedule


def test_verdict_thresholds():
    def stats(p99_ms, bp=0, und=0):
        return RunStats(10, 10 - bp, bp, und, np.full(100, p99_ms / 1000), 1.0, 1.0, 10.0, 1000.0)
    assert stats(1.0).verdict == "OK"
    assert stats(50.0).verdict == "DEGRADED"
    assert stats(5000.0).verdict == "INVALID"
    assert stats(1.0, bp=1).verdict == "INVALID"
    assert stats(1.0, und=3).verdict == "INVALID"


def test_real_clock_smoke_run_is_ok_within_tolerance():
    # Two streams, 20 ticks at 5 ms: ~100 ms of real time. Generous tolerance for CI.
    emitters = [wave_emitter("1", "A", 20, 0.005), numeric_emitter("1", "HR", 20, 0.005)]
    stats = asyncio.run(Pacer(NullSink(), tolerance_ms=50.0).run(schedule(emitters)))
    assert stats.n_scheduled == 40 and stats.verdict == "OK", stats.summary()
