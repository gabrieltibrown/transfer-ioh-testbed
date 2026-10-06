"""Read one recorded case into per-track streams.

The source format is VitalDB's row-per-block Parquet: a waveform row carries a
block start ``dt``, an ``srate``, ``gain``/``bias`` and an array of ~1 s of
scaled int16 samples (``ivals``), or float samples (``fvals``) for tracks such
as BIS that have no gain. Missing samples are nulls inside the array. Numeric
rows carry one ``nval`` per row.

Waveform blocks are concatenated into contiguous **segments**; a discontinuity
between consecutive blocks starts a new segment rather than being spliced
(gap preservation, docs/stream-model.md T1). The packetizer consumes segments.

Everything is behind a small ``Source`` protocol so a DWC reader can replace the
VitalDB one without touching the rest of the harness.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


@dataclass
class Segment:
    """A contiguous run of samples. ``values`` are raw scaled values as float64,
    NaN where the source had a null."""

    t0: float
    values: np.ndarray

    @property
    def n(self) -> int:
        return int(self.values.size)


@dataclass
class SampleStream:
    source: str
    srate: float
    gain: float
    bias: float
    unit: str
    segments: list[Segment]

    @property
    def n_samples(self) -> int:
        return sum(s.n for s in self.segments)

    def physical(self, raw: np.ndarray) -> np.ndarray:
        """Decode scaled values to physical units. Verified against VitalDB's own
        ART_MBP numerics: physical = raw * gain + bias."""
        return raw * self.gain + self.bias


@dataclass
class NumericStream:
    source: str
    unit: str
    t: np.ndarray
    values: np.ndarray


@dataclass
class CaseData:
    caseid: int | str
    t_start: float  # earliest dt in the whole file, used as the case's rebasing origin (T11)
    t_end: float
    waves: dict[str, SampleStream]
    numerics: dict[str, NumericStream]
    missing: tuple[str, ...]  # wanted sources absent from this case; a sensor not attached


class Source(Protocol):
    def read_case(
        self, caseid: int | str, wanted: Iterable[str], window_s: float | None = None
    ) -> CaseData: ...


class VitalDBSource:
    def __init__(self, data_dir: Path):
        self.data_dir = Path(data_dir)

    def read_case(
        self, caseid: int | str, wanted: Iterable[str], window_s: float | None = None
    ) -> CaseData:
        return read_vitaldb_case(
            self.data_dir / f"{caseid}.parquet", wanted, caseid=caseid, window_s=window_s
        )


_COLUMNS = ["dt", "dname", "tname", "unit", "ivals", "fvals", "nval", "srate", "gain", "bias"]

# Slack past the window when cropping, so the block that straddles the window end
# is read in full. Source blocks are ~1 s.
_CROP_SLACK_S = 2.0


def read_vitaldb_case(
    path: Path,
    wanted: Iterable[str],
    caseid: int | str | None = None,
    window_s: float | None = None,
) -> CaseData:
    """``window_s`` crops reading to the first ``window_s`` seconds of the case, since
    a run only replays its observation window. ``t_start``/``t_end`` still describe
    the whole recording."""
    wanted = set(wanted)
    caseid = caseid if caseid is not None else Path(path).stem

    # Case span from every row, not just the wanted tracks, so all of a case's
    # tracks share one rebasing origin.
    span = pq.read_table(path, columns=["dt"])["dt"]
    t_start, t_end = pc.min(span).as_py(), pc.max(span).as_py()

    table = pq.read_table(path, columns=_COLUMNS)
    key = pc.binary_join_element_wise(
        pc.cast(table["dname"], pa.string()), pc.cast(table["tname"], pa.string()), "/"
    )
    mask = pc.is_in(key, value_set=pa.array(sorted(wanted), pa.string()))
    if window_s is not None:
        mask = pc.and_(mask, pc.less_equal(table["dt"], t_start + window_s + _CROP_SLACK_S))
    table = table.filter(mask)
    key = key.filter(mask)
    df = table.to_pandas()
    df["key"] = key.to_pandas().to_numpy()

    waves: dict[str, SampleStream] = {}
    numerics: dict[str, NumericStream] = {}
    for k, g in df.groupby("key", sort=False):
        is_wave = g["srate"].notna()
        if is_wave.any():
            gw = g[is_wave].sort_values("dt")
            srate = float(gw["srate"].iloc[0])
            gain = gw["gain"].iloc[0]
            if gain is None or np.isnan(gain):  # float tracks (BIS): no scaling
                arrays, gain, bias = gw["fvals"], 1.0, 0.0
            else:
                arrays, gain, bias = gw["ivals"], float(gain), float(gw["bias"].iloc[0])
            unit = gw["unit"].iloc[0]
            waves[k] = SampleStream(
                source=k, srate=srate, gain=gain, bias=bias,
                unit=str(unit) if unit is not None else "",
                segments=_segments(gw["dt"].to_numpy(), arrays, srate),
            )
        gn = g[g["nval"].notna() & g["srate"].isna()].sort_values("dt")
        if len(gn):
            unit = gn["unit"].iloc[0]
            numerics[k] = NumericStream(
                source=k, unit=str(unit) if unit is not None else "",
                t=gn["dt"].to_numpy(dtype=float), values=gn["nval"].to_numpy(dtype=float),
            )

    found = set(waves) | set(numerics)
    return CaseData(
        caseid=caseid, t_start=float(t_start), t_end=float(t_end),
        waves=waves, numerics=numerics, missing=tuple(sorted(wanted - found)),
    )


def _segments(dts: np.ndarray, arrays, srate: float) -> list[Segment]:
    """Concatenate consecutive blocks while they are contiguous; start a new
    segment at any discontinuity. Tolerance is one sample period, which absorbs
    the 62/63-sample alternation of 62.5 Hz blocks without treating it as a gap.
    A null or empty block is a gap."""
    tol = 1.0 / srate
    segments: list[Segment] = []
    buf: list[np.ndarray] = []
    seg_t0 = None
    expected = None
    for dt, arr in zip(dts, arrays):
        if arr is None or len(arr) == 0:
            if buf:
                segments.append(Segment(seg_t0, np.concatenate(buf)))
                buf = []
            expected = None
            continue
        # None elements become NaN. float32 holds the full int16 range exactly and
        # halves the footprint of 50 concurrent cases.
        a = np.asarray(arr, dtype=np.float32)
        if expected is not None and abs(dt - expected) <= tol:
            buf.append(a)
        else:
            if buf:
                segments.append(Segment(seg_t0, np.concatenate(buf)))
            seg_t0, buf = float(dt), [a]
        expected = dt + a.size / srate
    if buf:
        segments.append(Segment(seg_t0, np.concatenate(buf)))
    return segments
