"""Config layer: the shipped configs load and respect the monitor budget, the cadence
cannot be left implicit, and the governance policy refuses what it must."""

from pathlib import Path

import pytest

from ioh_testbed.replay.config import (
    ENV_POLICIES,
    RunConfig,
    Scenario,
    SensorProfile,
    TrackSpec,
    Workload,
)

REPO = Path(__file__).resolve().parents[1]
WORKLOADS = sorted((REPO / "configs" / "workload").glob("*.yaml"))
SCENARIOS = sorted((REPO / "configs" / "scenarios").glob("*.yaml"))


@pytest.mark.parametrize("path", WORKLOADS, ids=lambda p: p.stem)
def test_shipped_workloads_load_and_fit_budget(path):
    wl = Workload.load(path)
    assert wl.profiles
    for p in wl.profiles:
        assert sum(t.is_ecg for t in p.tracks) <= 3
        assert sum(1 for t in p.waves if not t.is_ecg) <= 8


@pytest.mark.parametrize("path", SCENARIOS, ids=lambda p: p.stem)
def test_shipped_scenarios_state_cadence(path):
    assert Scenario.load(path).packet_ms in (10000, 256)


def test_scenario_without_packet_ms_is_rejected(tmp_path):
    f = tmp_path / "s.yaml"
    f.write_text("name: oops\nnotes: forgot the cadence\n")
    with pytest.raises(ValueError, match="packet_ms"):
        Scenario.load(f)


def test_wave_rate_must_be_philips_rate():
    with pytest.raises(ValueError, match="Philips rate"):
        TrackSpec(source="x", label="ART", kind="wave", target_hz=100)


def test_ecg_budget_enforced():
    ecg = [TrackSpec(f"s{i}", f"ECG_{i}", "wave", 500) for i in range(4)]
    with pytest.raises(ValueError, match="ECG waves exceeds"):
        SensorProfile("too_many_ecg", tuple(ecg))


def test_non_ecg_budget_enforced():
    waves = [TrackSpec(f"s{i}", f"W{i}", "wave", 125) for i in range(9)]
    with pytest.raises(ValueError, match="non-ECG waves exceeds"):
        SensorProfile("too_many_waves", tuple(waves))


def test_round_robin_assignment_is_deterministic():
    wl = Workload.load(REPO / "configs" / "workload" / "mixed.yaml")
    names = [wl.profile_for(i).name for i in range(7)]
    assert names == [
        "standard_anaesthesia", "invasive_cardiac", "minimal_monitoring",
        "standard_anaesthesia", "invasive_cardiac", "minimal_monitoring",
        "standard_anaesthesia",
    ]


@pytest.mark.parametrize(
    "packet_ms, expected",
    [(10000, 5 * 0.1 + 8), (256, 5 * (1000 / 256) + 8)],
)
def test_records_per_second_matches_stream_model_1_8(packet_ms, expected):
    wl = Workload.load(REPO / "configs" / "workload" / "standard_anaesthesia.yaml")
    assert wl.profiles[0].records_per_second(packet_ms) == pytest.approx(expected)


def test_describe_carries_the_cadence():
    rc = RunConfig(
        Workload.load(REPO / "configs" / "workload" / "standard_anaesthesia.yaml"),
        Scenario.load(REPO / "configs" / "scenarios" / "dwc_10s.yaml"),
    )
    d = rc.describe()
    assert d["packet_ms"] == 10000
    assert d["profiles"][0]["records_per_second"] == pytest.approx(8.5)


def test_cloud_policy_refuses_dwc():
    with pytest.raises(PermissionError):
        ENV_POLICIES["cloud"].check("dwc")
    ENV_POLICIES["charite"].check("dwc")
    ENV_POLICIES["cloud"].check("vitaldb")
