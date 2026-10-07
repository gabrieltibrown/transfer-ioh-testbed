"""Offline reference for the Flink job's windowed features.

Proposal 6: "outputs are checked against a reference implementation". The
reference reuses the harness's own schedule (so packetisation, decimation and
event-time rebasing are identical to what was replayed) and re-implements only
what the Flink job adds on top:

- the record timestamp the job assigns: the DWC string timestamp (millisecond
  truncation, as ``format_dwc_ts`` writes it) plus ``(n - 1) * sample period``
  for a wave packet, so a whole packet is assigned to windows by its last sample
- Flink's epoch-aligned sliding window assignment,
  ``SlidingEventTimeWindows(window_ms, slide_ms)`` with offset 0
- per-label statistics with the DWC ``ScaleRangeSpec16`` decode and invalid or
  unavailable samples excluded
- the ``partial`` flag: a window that starts before the case's first event

Everything in milliseconds, integer, like the job.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np

from ..replay.config import WAVE
from ..replay.records import SCALE_LOWER, SCALE_UPPER, format_dwc_ts, parse_dwc_ts
from ..replay.schedule import Schedule, StreamEmitter


def dwc_ms(epoch_s: float) -> int:
    """Epoch ms exactly as a consumer recovers it from the DWC string timestamp."""
    return round(parse_dwc_ts(format_dwc_ts(epoch_s)) * 1000)


@dataclass
class RefRecord:
    case_id: str
    label: str
    kind: str
    t_first_ms: int
    t_ms: int  # record timestamp: last sample for waves
    values: np.ndarray  # physical values that count (numeric: one value)
    n_invalid: int = 0


@dataclass
class RefLabel:
    kind: str
    sum: float = 0.0
    min: float = float("inf")
    max: float = float("-inf")
    count: int = 0
    invalid: int = 0
    records: int = 0
    last: float = float("nan")
    last_t_ms: int = -(2**62)

    def add(self, r: RefRecord) -> None:
        self.records += 1
        self.invalid += r.n_invalid
        if r.values.size:
            self.sum += float(r.values.sum())
            self.min = min(self.min, float(r.values.min()))
            self.max = max(self.max, float(r.values.max()))
            self.count += int(r.values.size)
        if r.kind != WAVE and r.t_ms >= self.last_t_ms:
            self.last_t_ms = r.t_ms
            self.last = float(r.values[0])

    @property
    def mean(self) -> float:
        return self.sum / self.count if self.count else float("nan")


@dataclass
class RefWindow:
    case_id: str
    start_ms: int
    end_ms: int
    partial: bool = False
    n_records: int = 0
    n_wave: int = 0
    n_numeric: int = 0
    t_event_oldest_ms: int = 2**62
    t_event_newest_ms: int = -(2**62)
    labels: dict[str, RefLabel] = field(default_factory=dict)

    @property
    def key(self) -> tuple[str, int, int]:
        return (self.case_id, self.start_ms, self.end_ms)

    def add(self, r: RefRecord) -> None:
        lab = self.labels.setdefault(r.label, RefLabel(r.kind))
        lab.add(r)
        self.n_records += 1
        if r.kind == WAVE:
            self.n_wave += 1
        else:
            self.n_numeric += 1
        self.t_event_oldest_ms = min(self.t_event_oldest_ms, r.t_first_ms)
        self.t_event_newest_ms = max(self.t_event_newest_ms, r.t_ms)


def physical(raw: np.ndarray, gain: float, bias: float) -> np.ndarray:
    """The consumer-side linear map from c_scale_* / c_calibration_abs_*, in the
    same floating-point order the Java job uses."""
    lo_a = SCALE_LOWER * gain + bias
    hi_a = SCALE_UPPER * gain + bias
    return lo_a + (raw - float(SCALE_LOWER)) * (hi_a - lo_a) / (float(SCALE_UPPER) - SCALE_LOWER)


def emitter_records(em: StreamEmitter, case_t_start: float, t0_wall: float, speed: float) -> list[RefRecord]:
    out: list[RefRecord] = []
    while em.advance():
        e = em.current
        event_ts = t0_wall + e.t_event_rel / speed
        t_first = dwc_ms(event_ts)
        if em.kind == WAVE:
            p = e.payload
            raw = np.where(np.isnan(p.values), 0, p.values).astype(np.int64)
            skip = np.zeros(p.n, dtype=bool)
            skip[p.invalid] = True
            skip[p.unavailable] = True
            vals = physical(raw[~skip].astype(float), em.stream.gain, em.stream.bias)
            t_last = t_first + round((p.n - 1) * 1000.0 / p.srate)
            out.append(RefRecord(em.case_id, em.label, WAVE, t_first, t_last, vals, int(skip.sum())))
        else:
            out.append(RefRecord(em.case_id, em.label, "numeric", t_first, t_first, np.array([float(e.payload)])))
    return out


def window_starts(t_ms: int, window_ms: int, slide_ms: int) -> list[int]:
    """Flink's SlidingEventTimeWindows.assignWindows with offset 0."""
    last_start = t_ms - ((t_ms + slide_ms) % slide_ms)
    starts = []
    s = last_start
    while s > t_ms - window_ms:
        starts.append(s)
        s -= slide_ms
    return starts


def reference_windows(
    schedule: Schedule, t0_wall: float, window_ms: int, slide_ms: int
) -> dict[tuple[str, int, int], RefWindow]:
    """All windows that contain at least one record, keyed by (case, start, end).
    Which of them the job actually fires depends on how far the watermark got;
    see ``expected_fired``."""
    windows: dict[tuple[str, int, int], RefWindow] = {}
    first_event: dict[str, int] = {}
    per_case_records: dict[str, list[RefRecord]] = {}
    for case in schedule.cases:
        recs: list[RefRecord] = []
        for em in case.emitters:
            recs += emitter_records(em, case.t_start, t0_wall, schedule.speed)
        per_case_records[case.case_id] = recs
        if recs:
            first_event[case.case_id] = min(r.t_first_ms for r in recs)
    for case_id, recs in per_case_records.items():
        for r in recs:
            for s in window_starts(r.t_ms, window_ms, slide_ms):
                key = (case_id, s, s + window_ms)
                w = windows.get(key)
                if w is None:
                    w = windows[key] = RefWindow(case_id, s, s + window_ms, partial=s < first_event[case_id])
                w.add(r)
    return windows


def expected_fired(windows: dict, bound_ms: int) -> dict:
    """Windows the job must have fired given the per-case maximum event time:
    the watermark reaches ``max_event - bound - 1`` and a window fires once the
    watermark is at or past ``end - 1``."""
    max_event: dict[str, int] = {}
    for w in windows.values():
        max_event[w.case_id] = max(max_event.get(w.case_id, -(2**62)), w.t_event_newest_ms)
    return {k: w for k, w in windows.items() if w.end_ms <= max_event[w.case_id] - bound_ms}


def compare(prediction: dict, ref: RefWindow, rel: float = 1e-6, abs_: float = 1e-6) -> list[str]:
    """Mismatches between one Flink prediction record and its reference window; empty if none."""
    errs: list[str] = []
    if prediction["nRecords"] != ref.n_records:
        errs.append(f"nRecords {prediction['nRecords']} != {ref.n_records}")
    if prediction["nWave"] != ref.n_wave or prediction["nNumeric"] != ref.n_numeric:
        errs.append(f"nWave/nNumeric {prediction['nWave']}/{prediction['nNumeric']} != {ref.n_wave}/{ref.n_numeric}")
    if bool(prediction.get("partial")) != ref.partial:
        errs.append(f"partial {prediction.get('partial')} != {ref.partial}")
    if prediction["tEventOldestMs"] != ref.t_event_oldest_ms or prediction["tEventNewestMs"] != ref.t_event_newest_ms:
        errs.append("event-time bounds differ")
    feats = prediction["features"]
    if set(feats) != set(ref.labels):
        errs.append(f"labels {sorted(feats)} != {sorted(ref.labels)}")
    for label, lab in ref.labels.items():
        f = feats.get(label)
        if f is None:
            continue
        if f["count"] != lab.count or f["records"] != lab.records:
            errs.append(f"{label}: count/records {f['count']}/{f['records']} != {lab.count}/{lab.records}")
        for name, want in (("mean", lab.mean), ("min", lab.min), ("max", lab.max)):
            got = f[name]
            if not np.isclose(got, want, rtol=rel, atol=abs_, equal_nan=True):
                errs.append(f"{label}: {name} {got} != {want}")
        if lab.kind == WAVE:
            if f.get("invalid") != lab.invalid:
                errs.append(f"{label}: invalid {f.get('invalid')} != {lab.invalid}")
        elif not np.isclose(f["last"], lab.last, rtol=rel, atol=abs_, equal_nan=True):
            errs.append(f"{label}: last {f['last']} != {lab.last}")
    return errs
