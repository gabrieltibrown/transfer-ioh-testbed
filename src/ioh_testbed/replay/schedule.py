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
    """``t_rel`` is when the record is emitted, ``t_event_rel`` the event time it
    carries (its first sample), both in seconds since case start in source time.
    They differ for wave packets: a monitor can only export a packet once its last
    sample exists, so a packet is emitted ``packet_ms`` after its first sample."""

    t_rel: float
    payload: Any  # WavePacket for waves, float for numerics
    t_event_rel: float | None = None

    def __post_init__(self) -> None:
        if self.t_event_rel is None:
            object.__setattr__(self, "t_event_rel", self.t_rel)


class StreamEmitter:
    """One (case, label) stream. ``current`` is the next emission due."""

    __slots__ = ("_items", "case_id", "current", "kind", "label", "n_emitted", "stream", "unit")

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


def select_cases(
    manifest_cases: list[dict], n: int, window_s: float, case_ids: list[int] | None = None,
    start_s: float = 0.0,
) -> list[dict]:
    """The first ``n`` manifest cases, in manifest order, that are at least one
    observation window long. Deterministic, so a run is reproducible from its config.
    With ``case_ids`` those cases are taken instead, in the given order."""
    need = start_s + window_s
    if case_ids is not None:
        by_id = {int(c["caseid"]): c for c in manifest_cases}
        chosen = []
        for cid in case_ids:
            if cid not in by_id:
                raise ValueError(f"case {cid} is not in the manifest")
            if float(by_id[cid].get("duration_s", 0.0)) < need:
                raise ValueError(f"case {cid} is shorter than start {start_s:g} s + window {window_s:g} s")
            chosen.append(by_id[cid])
        return chosen
    eligible = [c for c in manifest_cases if float(c.get("duration_s", 0.0)) >= need]
    if len(eligible) < n:
        raise ValueError(
            f"{n} cases requested but only {len(eligible)} in the manifest are >= {need:g} s long"
        )
    return eligible[:n]


def build_schedule(
    cfg: RunConfig,
    source: Source,
    manifest_cases: list[dict],
    n_cases: int,
    case_ids: list[int] | None = None,
    key_suffix: str = "",
    start_s: float = 0.0,
) -> Schedule:
    """``key_suffix`` is appended to the case id used as Kafka key and ``p_patnr``,
    so one recording can replay on two beds without sharing keyed state downstream.
    ``start_s`` begins the replay that many seconds into each recording; every
    stream is rebased from that point, keeping the shared-origin property."""
    window = float(cfg.workload.observation_window_s)
    packet_ms = cfg.scenario.packet_ms
    plans: list[CasePlan] = []
    for rank, c in enumerate(select_cases(manifest_cases, n_cases, window, case_ids, start_s)):
        profile = cfg.workload.profile_for(rank)
        case_id = f"{c['caseid']}{key_suffix}"
        data = source.read_case(c["caseid"], [t.source for t in profile.tracks], window_s=window, start_s=start_s)
        origin = data.t_start + start_s
        emitters: list[StreamEmitter] = []
        for t in profile.tracks:
            if t.kind == WAVE and t.source in data.waves:
                s = data.waves[t.source]
                # Emitted at the end of the packet's span; cropped by emission time, so
                # the last packet of a window is the one that completes inside it.
                span = packet_ms / 1000.0
                items = (
                    Emission(p.t0 + span - origin, p, p.t0 - origin)
                    for p in packetize_wave(s, packet_ms, t.target_hz)
                    if p.t0 >= origin and p.t0 + span - origin < window  # samples from the origin on
                )
                emitters.append(StreamEmitter(case_id, t.label, WAVE, s.unit, s, items))
            elif t.kind == NUMERIC and t.source in data.numerics:
                ns = data.numerics[t.source]
                grid, vals = resample_numeric_zoh(ns, t.target_hz, t_end=origin + window)
                items = (
                    Emission(g - origin, float(v))
                    for g, v in zip(grid.tolist(), vals.tolist())
                    if 0.0 <= g - origin < window
                )
                emitters.append(StreamEmitter(case_id, t.label, NUMERIC, ns.unit, None, items))
        plans.append(CasePlan(case_id, profile.name, origin, emitters, data.missing, data.late_start))
    return Schedule(plans, window, float(cfg.workload.speed), packet_ms)
