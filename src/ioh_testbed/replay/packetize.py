"""Turn continuous per-track streams into fixed-cadence packets.

Waveforms: each contiguous segment from the reader is decimated to the target
rate by integer stride (no anti-alias filter; load fidelity, not spectral
fidelity, see docs/stream-model.md T2) and cut into packets of exactly
``packet_ms`` worth of samples. Packets are aligned to the segment start, so
carry across source-block edges is automatic: the reader already concatenated
contiguous blocks. A segment's trailing partial packet is **padded to full
size** and the padded positions are reported as ``unavailable``; samples that
were null in the source are reported as ``invalid``. That is the monitor's own
model (packets are always ``array_size``; missing data is flagged per sample)
and it maps onto DWC's ``c_unavailable_samples`` / ``c_invalid_samples``.

Numerics: zero-order hold onto a regular grid at the target rate (T4), the
semantics a monitor already has for a value that has not been refreshed.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass

import numpy as np

from .reader import NumericStream, SampleStream


@dataclass
class WavePacket:
    source: str
    t0: float  # event time of the first sample, source time
    srate: float  # emitted sample rate
    values: np.ndarray  # raw scaled values, float64, NaN at invalid and unavailable positions
    invalid: np.ndarray  # indices null in the source: acquired, but bad
    unavailable: np.ndarray  # indices padded past the end of acquired data

    @property
    def n(self) -> int:
        return int(self.values.size)

    @property
    def n_acquired(self) -> int:
        return self.n - int(self.unavailable.size)


def _integer_ratio(num: float, den: float, what: str) -> int:
    r = num / den
    n = int(round(r))
    if n < 1 or abs(r - n) > 1e-9:
        raise ValueError(f"{what} must be a positive integer, got {r:g}")
    return n


def samples_per_packet(packet_ms: int, target_hz: float) -> int:
    return _integer_ratio(packet_ms * target_hz, 1000.0, f"samples per packet at {packet_ms} ms, {target_hz} Hz")


def packetize_wave(stream: SampleStream, packet_ms: int, target_hz: float) -> Iterator[WavePacket]:
    stride = _integer_ratio(stream.srate, target_hz, f"decimation stride {stream.srate:g}/{target_hz:g}")
    n = samples_per_packet(packet_ms, target_hz)
    packet_s = packet_ms / 1000.0
    for seg in stream.segments:
        decimated = seg.values[::stride]
        for k, start in enumerate(range(0, decimated.size, n)):
            chunk = decimated[start : start + n]
            acquired = chunk.size
            pad = n - acquired
            if pad:
                chunk = np.concatenate([chunk, np.full(pad, np.nan)])
            nan_idx = np.flatnonzero(np.isnan(chunk))
            yield WavePacket(
                source=stream.source,
                t0=seg.t0 + k * packet_s,
                srate=float(target_hz),
                values=chunk,
                invalid=nan_idx[nan_idx < acquired],
                unavailable=np.arange(acquired, n) if pad else np.empty(0, dtype=np.intp),
            )


def resample_numeric_zoh(
    stream: NumericStream, target_hz: float, t_end: float | None = None
) -> tuple[np.ndarray, np.ndarray]:
    """Regular grid from the first observation, stepping ``1/target_hz``, each point
    holding the most recent observation at or before it. Nothing is emitted before
    the first observation: there is no value to hold yet. ``t_end`` extends the hold
    past the last observation, as a monitor keeps displaying a stale numeric."""
    if stream.t.size == 0:
        return np.empty(0), np.empty(0)
    order = np.argsort(stream.t, kind="stable")
    t, v = stream.t[order], stream.values[order]
    period = 1.0 / target_hz
    end = t[-1] if t_end is None else max(float(t_end), float(t[0]))
    steps = int(np.floor((end - t[0]) / period + 1e-9))
    grid = t[0] + np.arange(steps + 1) * period
    idx = np.searchsorted(t, grid, side="right") - 1
    return grid, v[idx]
