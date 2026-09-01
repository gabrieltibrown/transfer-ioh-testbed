"""Workload profiles: which tracks to replay, and at what record granularity.

The workload profile is deliberately independent of the data source, so a VitalDB
case can be driven at the DWC channel profile from proposal 2.3 and the two remain
comparable. See the granularity note below.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

# Granularity: mimic the Philips PIC iX / IntelliVue live wave stream.
#
# The testbed replays recorded DWC Parquet in order to mock a real-time PIC iX
# waveform stream, so the packet shape should be the monitor's, not the storage
# format's. Philips Data Export Interface Programming Guide (X2/MP/MX/FM Rel. L.0,
# 4535 645 88011), "Interpreting Wave Data":
#
#   wave type              sample period   array size      update period
#   500 sps (ECG)          2 ms            128 samples     256 ms
#   250 sps (compound)     4 ms            3 x 64 samples  256 ms
#   125 sps                8 ms            32 samples      256 ms
#   62.5 sps               16 ms           16 samples      256 ms
#
#   "For wave data export, the Computer Client needs to be able to receive
#    observed values with 256 ms of wave data in one message."
#
# So the realistic default is chunk:256. Note the source format does not match:
# VitalDB stores 1 s blocks, and 1000/256 is not an integer, so re-packetizing
# treats each track as a continuous sample stream, emits fixed-size packets on
# 256 ms boundaries carrying the remainder across source blocks, and preserves
# genuine gaps rather than splicing them out.
#
#   block        forward the stored source block unchanged (VitalDB: ~1 s).
#                Useful as a contrast, not realistic for PIC iX.
#   chunk:<ms>   re-packetize at a fixed cadence. chunk:256 is the monitor's.
#   sample       one record per sample. Not what any real component emits;
#                retained only to bound the measurable range.
#
# Open question for when DWC access lands: does te_wave preserve the 256 ms
# packet or aggregate it? c_n_samples answers it. If it is 128/32/16 then DWC is
# packet-faithful and chunk:256 is a true mock of the live stream.
PIC_IX_UPDATE_PERIOD_MS = 256

# Array size the monitor uses per 256 ms packet, by wave sample rate (Hz).
PIC_IX_ARRAY_SIZE = {500.0: 128, 250.0: 64, 125.0: 32, 62.5: 16}

GRANULARITY_RE = re.compile(r"^(block|sample|chunk:(?P<ms>\d+))$")


@dataclass(frozen=True)
class Granularity:
    kind: str  # "block" | "chunk" | "sample"
    chunk_ms: int | None = None

    @classmethod
    def parse(cls, spec: str) -> "Granularity":
        m = GRANULARITY_RE.match(spec.strip())
        if not m:
            raise ValueError(
                f"bad granularity {spec!r}; expected 'block', 'sample' or 'chunk:<ms>'"
            )
        if m.group("ms"):
            ms = int(m.group("ms"))
            if ms <= 0:
                raise ValueError("chunk duration must be positive")
            return cls("chunk", ms)
        return cls(m.group(1))

    def __str__(self) -> str:
        return f"chunk:{self.chunk_ms}" if self.kind == "chunk" else self.kind


@dataclass(frozen=True)
class WorkloadProfile:
    name: str
    waveform_tracks: tuple[str, ...]
    numeric_tracks: tuple[str, ...]
    granularity: Granularity
    speed: float = 1.0  # replay rate multiplier; 1.0 is real time
    notes: str = ""

    @property
    def tracks(self) -> tuple[str, ...]:
        return self.waveform_tracks + self.numeric_tracks

    @classmethod
    def load(cls, path: str | Path) -> "WorkloadProfile":
        d = yaml.safe_load(Path(path).read_text())
        return cls(
            name=d["name"],
            waveform_tracks=tuple(d["waveform_tracks"]),
            numeric_tracks=tuple(d["numeric_tracks"]),
            granularity=Granularity.parse(str(d.get("granularity", "chunk:256"))),
            speed=float(d.get("speed", 1.0)),
            notes=d.get("notes", ""),
        )

    def describe(self) -> dict:
        """Flat dict for the run metadata, so a result is traceable to its workload."""
        return {
            "name": self.name,
            "waveform_tracks": list(self.waveform_tracks),
            "numeric_tracks": list(self.numeric_tracks),
            "granularity": str(self.granularity),
            "speed": self.speed,
        }


@dataclass(frozen=True)
class SourcePolicy:
    """Which data sources an execution environment is permitted to read.

    The cloud profile must be structurally incapable of reading Charite DWC data,
    not merely trusted not to. See the data governance rules in CLAUDE.md.
    """

    env: str
    allowed_sources: frozenset[str] = field(default_factory=frozenset)

    def check(self, source: str) -> None:
        if source not in self.allowed_sources:
            raise PermissionError(
                f"environment {self.env!r} may not read source {source!r}; "
                f"allowed: {sorted(self.allowed_sources)}"
            )


ENV_POLICIES = {
    "laptop": SourcePolicy("laptop", frozenset({"vitaldb", "synthetic"})),
    "charite": SourcePolicy("charite", frozenset({"vitaldb", "synthetic", "dwc"})),
    # PHI must never reach a cloud VM.
    "cloud": SourcePolicy("cloud", frozenset({"vitaldb", "synthetic"})),
}
