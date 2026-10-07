"""Build the emission schedule for one run.

A run replays ``n_cases`` recorded cases concurrently for one observation
window. Every case starts at the run's ``t0``, so concurrent case count is a
controlled independent variable. Each case is rebased by a single offset,
``t_rel = t_source - case.t_start``, shared by **all** of its streams, which is
the property LIVIA's per-label ``recording_start`` breaks (T11).

Streams are lazy: an emitter holds only its current item, and the pacer merges
emitters through a min-heap of deadlines. Precomputing every packet for 50 cases
would be well over a gigabyte at either cadence.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from .config import NUMERIC, WAVE, RunConfig
from .packetize import packetize_wave, resample_numeric_zoh
from .reader import SampleStream, Source


@dataclass(frozen=True)
class Emission:
    t_rel: float  # seconds since case start, in source time
    payload: Any  # WavePacket for waves, float for numerics


class StreamEmitter:
    """One (case, label) stream. ``current`` is the next emission due."""

    __slots__ = ("case_id", "label", "kind", "unit", "stream", "_items", "current", "n_emitted")

    def __init__(
        self,
        case_id: str,
        label: str,
        kind: str,
        unit: str,
        stream: SampleStream | None,
        items: Iterator[Emission],
    ):
        self.case_id = case_id
        self.label = label
        self.kind = kind
        self.unit = unit
        self.stream = stream  # waves only: scale fields for the wire record
        self._items = items
        self.current: Emission | None = None
        self.n_emitted = 0

    def advance(self) -> bool:
        self.current = next(self._items, None)
        return self.current is not None


@dataclass
class CasePlan:
    case_id: str
    profile: str
    t_start: float
    emitters: list[StreamEmitter]
    missing: tuple[str, ...]  # profile sources absent from the recording; a sensor never attached
    late_start: tuple[str, ...] = ()  # present, but first data falls after the observation window


@dataclass
class Schedule:
    cases: list[CasePlan]
    window_s: float
    speed: float
    packet_ms: int

    @property
    def emitters(self) -> list[StreamEmitter]:
        return [e for c in self.cases for e in c.emitters]

    def describe(self) -> dict:
        return {
            "n_cases": len(self.cases),
            "window_s": self.window_s,
            "speed": self.speed,
            "packet_ms": self.packet_ms,
            "cases": [
                {
                    "case_id": c.case_id,
                    "profile": c.profile,
                    "n_streams": len(c.emitters),
                    "missing_sources": list(c.missing),
                    "late_start_sources": list(c.late_start),
                }
                for c in self.cases
            ],
        }


def select_cases(manifest_cases: list[dict], n: int, window_s: float) -> list[dict]:
    """The first ``n`` manifest cases, in manifest order, that are at least one
    observation window long. Deterministic, so a run is reproducible from its config."""
    eligible = [c for c in manifest_cases if float(c.get("duration_s", 0.0)) >= window_s]
    if len(eligible) < n:
        raise ValueError(
            f"{n} cases requested but only {len(eligible)} in the manifest are >= {window_s:g} s long"
        )
    return eligible[:n]


def build_schedule(
    cfg: RunConfig, source: Source, manifest_cases: list[dict], n_cases: int
) -> Schedule:
    window = float(cfg.workload.observation_window_s)
    packet_ms = cfg.scenario.packet_ms
    plans: list[CasePlan] = []
    for rank, c in enumerate(select_cases(manifest_cases, n_cases, window)):
        profile = cfg.workload.profile_for(rank)
        case_id = str(c["caseid"])
        data = source.read_case(c["caseid"], [t.source for t in profile.tracks], window_s=window)
        emitters: list[StreamEmitter] = []
        for t in profile.tracks:
            if t.kind == WAVE and t.source in data.waves:
                s = data.waves[t.source]
                items = (
                    Emission(p.t0 - data.t_start, p)
                    for p in packetize_wave(s, packet_ms, t.target_hz)
                    if p.t0 - data.t_start < window
                )
                emitters.append(StreamEmitter(case_id, t.label, WAVE, s.unit, s, items))
            elif t.kind == NUMERIC and t.source in data.numerics:
                ns = data.numerics[t.source]
                grid, vals = resample_numeric_zoh(ns, t.target_hz, t_end=data.t_start + window)
                items = (
                    Emission(g - data.t_start, float(v))
                    for g, v in zip(grid.tolist(), vals.tolist())
                    if 0.0 <= g - data.t_start < window
                )
                emitters.append(StreamEmitter(case_id, t.label, NUMERIC, ns.unit, None, items))
        plans.append(CasePlan(case_id, profile.name, data.t_start, emitters, data.missing, data.late_start))
    return Schedule(plans, window, float(cfg.workload.speed), packet_ms)
