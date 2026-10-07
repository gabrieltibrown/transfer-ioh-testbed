"""Open-loop pacer: emit every scheduled record at its absolute deadline.

The offered workload must never depend on how the pipeline behaves (CLAUDE.md).
Two properties enforce that here and are tested in simulated time:

* **Absolute deadlines.** Each emission's deadline is ``t0 + t_rel / speed``,
  fixed at schedule time. Sleeping until a deadline cannot accumulate error;
  sleeping a relative gap does (LIVIA: 1.48 ms/packet, 62 s over a 3 h case).
* **Never drop, never block.** A sink must be non-blocking. If it cannot accept
  a record it raises ``BufferError``; the pacer polls it once and retries, and
  if still refused it counts a backpressure event and the run is INVALID. The
  offered schedule is unchanged either way, and the loss is visible in the
  verdict rather than hidden in the data.

Scheduling uses the monotonic clock. Wall-clock equivalents for the record
fields ``_t_sched`` / ``_t_produce`` come from one ``(t0_mono, t0_wall)`` pair
so they are comparable with Kafka's ``LogAppendTime``. At ``speed != 1`` event
time is compressed by the same factor as the deadlines; latency results from
such runs are not valid and ``RunStats`` says so.

**What lateness means, and how the verdict is thresholded.** Harness lateness
(``t_produce - t_sched``) never enters pipeline latency, which is measured from
the broker's ``LogAppendTime``. It bounds only how faithfully the offered load's
timing tracks the schedule. The CLI therefore sets the verdict thresholds
relative to the packet cadence: DEGRADED when p99 lateness exceeds 10% of
``packet_ms``, INVALID when it exceeds 100% (the harness fell a whole packet
behind) or any record was refused. On macOS roughly 1% of long sleeps wake
15-20 ms late from timer coalescing, which the hybrid wait cannot recover; that
is immaterial against a 256 ms or 10 s cadence and is ~1 ms on Linux.
"""

from __future__ import annotations

import asyncio
import heapq
import itertools
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol

import numpy as np

from .config import WAVE
from .records import SequenceCounter, numeric_record, to_json_bytes, wave_record
from .schedule import Schedule, StreamEmitter

TOPIC_WAVE = "dwc-waveform"
TOPIC_NUMERIC = "dwc-numeric"


class Sink(Protocol):
    def emit(self, topic: str, key: bytes, value: bytes) -> None:
        """Non-blocking. Raise BufferError if the record cannot be accepted now."""

    def poll(self) -> None: ...

    def flush(self, timeout_s: float) -> int:
        """Block until delivered or timeout; return the number still outstanding."""

    def stats(self) -> dict: ...


class NullSink:
    """Counts and discards. The harness-only ceiling is measured against this."""

    def __init__(self) -> None:
        self.n = 0
        self.bytes = 0

    def emit(self, topic: str, key: bytes, value: bytes) -> None:
        self.n += 1
        self.bytes += len(value)

    def poll(self) -> None:
        pass

    def flush(self, timeout_s: float) -> int:
        return 0

    def stats(self) -> dict:
        return {"sink": "null", "n": self.n, "bytes": self.bytes}


@dataclass
class RunStats:
    n_scheduled: int
    n_emitted: int
    n_backpressure: int
    n_undelivered: int  # left in the sink after flush
    lateness_s: np.ndarray
    duration_s: float
    speed: float
    tolerance_ms: float
    invalid_ms: float
    spin_ms: float = 0.0
    t0_wall: float = 0.0  # run origin on the host clock; window alignment downstream depends on it
    per_case: dict[str, int] = field(default_factory=dict)
    per_kind: dict[str, int] = field(default_factory=dict)
    sink: dict = field(default_factory=dict)

    @property
    def verdict(self) -> str:
        if self.n_backpressure or self.n_undelivered or self.p99_ms > self.invalid_ms:
            return "INVALID"
        if self.p99_ms > self.tolerance_ms:
            return "DEGRADED"
        return "OK"

    def _pct(self, q: float) -> float:
        return float(np.percentile(self.lateness_s, q) * 1000) if self.lateness_s.size else 0.0

    @property
    def p99_ms(self) -> float:
        return self._pct(99)

    def summary(self) -> dict:
        n = int(self.lateness_s.size)
        return {
            "verdict": self.verdict,
            "latency_results_valid": self.verdict == "OK" and self.speed == 1.0,
            "n_scheduled": self.n_scheduled,
            "n_emitted": self.n_emitted,
            "n_backpressure": self.n_backpressure,
            "n_undelivered": self.n_undelivered,
            "duration_s": round(self.duration_s, 3),
            "offered_records_per_s": round(self.n_scheduled / self.duration_s, 2) if self.duration_s else 0.0,
            "lateness_ms": {
                "N": n,
                "p50": round(self._pct(50), 3),
                "p90": round(self._pct(90), 3),
                "p99": round(self.p99_ms, 3),
                "max": round(float(self.lateness_s.max() * 1000), 3) if n else 0.0,
                "n_over_tolerance": int((self.lateness_s * 1000 > self.tolerance_ms).sum()) if n else 0,
            },
            "thresholds_ms": {"tolerance": self.tolerance_ms, "invalid": self.invalid_ms},
            "spin_ms": self.spin_ms,
            "speed": self.speed,
            "t0_wall": self.t0_wall,
            "per_case": self.per_case,
            "per_kind": self.per_kind,
            "sink": self.sink,
        }


class Pacer:
    def __init__(
        self,
        sink: Sink,
        *,
        tolerance_ms: float = 10.0,
        invalid_ms: float = 1000.0,
        spin_s: float = 0.002,
        clock: Callable[[], float] = time.monotonic,
        wall: Callable[[], float] = time.time,
        sleep: Callable[[float], "asyncio.Future"] = asyncio.sleep,
        spin: Callable[[float], None] | None = None,
        flush_timeout_s: float = 30.0,
    ):
        """``spin_s``: hybrid wait. Sleep until this close to the deadline, then
        busy-wait the remainder. OS timers wake late by 1 ms (Linux) to 10-20 ms
        (macOS, timer coalescing); spinning the last couple of milliseconds buys
        sub-millisecond lateness for a few percent of one core. 0 disables it."""
        self.sink = sink
        self.tolerance_ms = tolerance_ms
        self.invalid_ms = invalid_ms
        self.spin_s = spin_s
        self.clock = clock
        self.wall = wall
        self.sleep = sleep
        self.spin = spin or self._busy_wait
        self.flush_timeout_s = flush_timeout_s

    def _busy_wait(self, deadline: float) -> None:
        while self.clock() < deadline:
            pass

    async def run(self, schedule: Schedule) -> RunStats:
        speed = schedule.speed
        seqs = SequenceCounter()
        lateness: list[float] = []
        per_case: dict[str, int] = {}
        per_kind: dict[str, int] = {WAVE: 0, "numeric": 0}
        n_scheduled = n_emitted = n_backpressure = 0

        t0_mono, t0_wall = self.clock(), self.wall()
        tiebreak = itertools.count()
        heap: list[tuple[float, int, StreamEmitter]] = []
        for em in schedule.emitters:
            if em.advance():
                heapq.heappush(heap, (em.current.t_rel / speed, next(tiebreak), em))

        while heap:
            d_rel, _, em = heapq.heappop(heap)
            deadline = t0_mono + d_rel
            now = self.clock()
            remaining = deadline - now
            if remaining > self.spin_s:
                await self.sleep(remaining - self.spin_s)
                now = self.clock()
            if now < deadline:
                self.spin(deadline)
                now = self.clock()
            late = now - deadline
            n_scheduled += 1

            t_sched_wall = t0_wall + d_rel
            t_produce_wall = t0_wall + (now - t0_mono)
            event_ts = t0_wall + em.current.t_event_rel / speed
            seq = seqs.next(em.case_id, em.label)
            if em.kind == WAVE:
                topic = TOPIC_WAVE
                rec = wave_record(
                    em.current.payload, em.stream, case_id=em.case_id, label=em.label, seq=seq,
                    event_ts=event_ts, packet_ms=schedule.packet_ms,
                    t_sched=t_sched_wall, t_produce=t_produce_wall,
                )
            else:
                topic = TOPIC_NUMERIC
                rec = numeric_record(
                    case_id=em.case_id, label=em.label, seq=seq, event_ts=event_ts,
                    value=em.current.payload, unit=em.unit,
                    t_sched=t_sched_wall, t_produce=t_produce_wall,
                )

            payload = to_json_bytes(rec)
            key = em.case_id.encode()
            try:
                self.sink.emit(topic, key, payload)
            except BufferError:
                self.sink.poll()
                try:
                    self.sink.emit(topic, key, payload)
                except BufferError:
                    n_backpressure += 1
                else:
                    n_emitted += 1
            else:
                n_emitted += 1
            self.sink.poll()

            lateness.append(late)
            per_case[em.case_id] = per_case.get(em.case_id, 0) + 1
            per_kind[em.kind] = per_kind.get(em.kind, 0) + 1
            em.n_emitted += 1
            if em.advance():
                heapq.heappush(heap, (em.current.t_rel / speed, next(tiebreak), em))

        undelivered = self.sink.flush(self.flush_timeout_s)
        return RunStats(
            n_scheduled=n_scheduled,
            n_emitted=n_emitted,
            n_backpressure=n_backpressure,
            n_undelivered=int(undelivered),
            lateness_s=np.asarray(lateness, dtype=float),
            duration_s=self.clock() - t0_mono,
            speed=speed,
            tolerance_ms=self.tolerance_ms,
            invalid_ms=self.invalid_ms,
            spin_ms=self.spin_s * 1000,
            t0_wall=t0_wall,
            per_case=per_case,
            per_kind=per_kind,
            sink=self.sink.stats(),
        )
