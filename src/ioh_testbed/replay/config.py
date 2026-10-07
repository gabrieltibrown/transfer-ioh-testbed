"""Run configuration: what to replay, how it is packetised, and who may read what.

A run is a *workload* (sensor profiles, how they are assigned to cases, the
observation window) combined with a *scenario* (the packet cadence). They are
separate files because the cadence is the single most uncertain input to the
whole testbed and must be stated explicitly on every run; see
docs/stream-model.md section 1.2.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

WAVE = "wave"
NUMERIC = "numeric"

# Philips IntelliVue wave budget per monitor (DEI guide, "Interpreting Wave Data"):
# up to 3 ECG waves at 500 sps, up to 8 non-ECG waves at 125 or 62.5 sps.
MAX_ECG_WAVES = 3
MAX_NON_ECG_WAVES = 8
WAVE_RATES_HZ = (500.0, 250.0, 125.0, 62.5)


@dataclass(frozen=True)
class TrackSpec:
    """One emitted channel: where it comes from in the source, and what it looks like on the wire."""

    source: str  # source key, e.g. VitalDB "SNUADC/ART"
    label: str  # DWC-style label emitted, e.g. "ART"
    kind: str  # WAVE or NUMERIC
    target_hz: float  # wave sample rate, or numeric update rate, as emitted

    def __post_init__(self) -> None:
        if self.kind not in (WAVE, NUMERIC):
            raise ValueError(f"{self.label}: kind must be {WAVE!r} or {NUMERIC!r}, got {self.kind!r}")
        if self.target_hz <= 0:
            raise ValueError(f"{self.label}: target_hz must be positive")
        if self.kind == WAVE and self.target_hz not in WAVE_RATES_HZ:
            raise ValueError(
                f"{self.label}: wave target_hz {self.target_hz} is not a Philips rate {WAVE_RATES_HZ}"
            )

    @property
    def is_ecg(self) -> bool:
        return self.kind == WAVE and self.label.upper().startswith("ECG")


@dataclass(frozen=True)
class SensorProfile:
    """The channels one monitor emits for one case. Composition is sensor-dependent
    in production, so a run uses several profiles across its cases (T9)."""

    name: str
    tracks: tuple[TrackSpec, ...]

    def __post_init__(self) -> None:
        ecg = sum(1 for t in self.tracks if t.is_ecg)
        non_ecg = sum(1 for t in self.tracks if t.kind == WAVE and not t.is_ecg)
        if ecg > MAX_ECG_WAVES:
            raise ValueError(f"{self.name}: {ecg} ECG waves exceeds monitor budget of {MAX_ECG_WAVES}")
        if non_ecg > MAX_NON_ECG_WAVES:
            raise ValueError(
                f"{self.name}: {non_ecg} non-ECG waves exceeds monitor budget of {MAX_NON_ECG_WAVES}"
            )
        labels = [t.label for t in self.tracks]
        if len(set(labels)) != len(labels):
            raise ValueError(f"{self.name}: duplicate labels {labels}")

    @property
    def waves(self) -> tuple[TrackSpec, ...]:
        return tuple(t for t in self.tracks if t.kind == WAVE)

    @property
    def numerics(self) -> tuple[TrackSpec, ...]:
        return tuple(t for t in self.tracks if t.kind == NUMERIC)

    def records_per_second(self, packet_ms: int) -> float:
        """Expected emitted records/s for one case on this profile (stream-model 1.8)."""
        return len(self.waves) * (1000.0 / packet_ms) + sum(t.target_hz for t in self.numerics)


@dataclass(frozen=True)
class Scenario:
    """The packet cadence hypothesis a run is conditioned on."""

    name: str
    packet_ms: int
    notes: str = ""

    def __post_init__(self) -> None:
        if self.packet_ms <= 0:
            raise ValueError("packet_ms must be positive")

    @classmethod
    def load(cls, path: str | Path) -> Scenario:
        d = yaml.safe_load(Path(path).read_text())
        if "packet_ms" not in d:
            raise ValueError(f"{path}: scenario must state packet_ms explicitly; there is no default")
        return cls(name=d["name"], packet_ms=int(d["packet_ms"]), notes=d.get("notes", ""))


@dataclass(frozen=True)
class Workload:
    name: str
    profiles: tuple[SensorProfile, ...]
    assignment: str = "round_robin"
    observation_window_s: int = 1800
    speed: float = 1.0  # replay rate multiplier. Latency results at speed != 1 are not valid
    notes: str = ""

    def __post_init__(self) -> None:
        if not self.profiles:
            raise ValueError(f"{self.name}: at least one profile required")
        if self.assignment != "round_robin":
            raise ValueError(f"{self.name}: unknown assignment {self.assignment!r}")
        if self.observation_window_s <= 0 or self.speed <= 0:
            raise ValueError(f"{self.name}: observation_window_s and speed must be positive")

    def profile_for(self, case_rank: int) -> SensorProfile:
        """Deterministic profile for the case at position ``case_rank`` in the selected,
        ascending case list. Recorded in run metadata so a result is reproducible."""
        return self.profiles[case_rank % len(self.profiles)]

    @classmethod
    def load(cls, path: str | Path) -> Workload:
        d = yaml.safe_load(Path(path).read_text())
        profiles = tuple(
            SensorProfile(
                name=p["name"],
                tracks=tuple(TrackSpec(**t) for t in p["tracks"]),
            )
            for p in d["profiles"]
        )
        return cls(
            name=d["name"],
            profiles=profiles,
            assignment=d.get("assignment", "round_robin"),
            observation_window_s=int(d.get("observation_window_s", 1800)),
            speed=float(d.get("speed", 1.0)),
            notes=d.get("notes", ""),
        )


@dataclass(frozen=True)
class RunConfig:
    workload: Workload
    scenario: Scenario

    def describe(self) -> dict:
        """Flat dict for results/<run_id>/meta.json, so any number is traceable."""
        return {
            "workload": self.workload.name,
            "scenario": self.scenario.name,
            "packet_ms": self.scenario.packet_ms,
            "observation_window_s": self.workload.observation_window_s,
            "speed": self.workload.speed,
            "assignment": self.workload.assignment,
            "profiles": [
                {
                    "name": p.name,
                    "tracks": [t.__dict__ for t in p.tracks],
                    "records_per_second": round(p.records_per_second(self.scenario.packet_ms), 3),
                }
                for p in self.workload.profiles
            ],
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
