import subprocess
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from ioh_testbed.demo import server as srv
from ioh_testbed.demo.server import Demo, Shared, dumps, make_app


class FakeProc:
    def __init__(self):
        self.returncode = None
        self.signals = []

    def poll(self):
        return self.returncode

    def send_signal(self, s):
        self.signals.append(s)
        self.returncode = -15

    def wait(self, timeout=None):
        return self.returncode

    def kill(self):
        self.returncode = -9


@pytest.fixture
def demo(tmp_path, monkeypatch):
    # The TestClient is used without its context manager, so startup (clock probe,
    # tap, pipeline apply) does not run; bed processes are faked.
    monkeypatch.setattr(srv.pl, "start", lambda cmd, log: FakeProc())
    d = Demo(srv.REPO / "configs/pipeline/laptop.yaml", "localhost:9092", 8000, tmp_path / "results")
    return d


def test_cases_and_state(demo):
    c = TestClient(make_app(demo))
    cases = c.get("/api/cases").json()
    assert cases and {"caseid", "duration_s", "n_wave_tracks"} <= set(cases[0])
    s = c.get("/api/state").json()
    assert len(s["beds"]) == 4 and all(b["status"] == "idle" for b in s["beds"])
    assert s["shared"]["window_ms"] == 60000 and s["shared"]["failure_style"] == "wait"
    assert s["grains"] == ["dei_256ms", "dwc_10s"]
    assert s["profile"]["waves"] == ["ECG_II", "ART", "PLETH", "AWP", "CO2"]


def test_play_validates_and_spawns(demo):
    c = TestClient(make_app(demo))
    first = demo.manifest[0]["caseid"]
    assert c.post("/api/beds/9/play", json={"case": first}).status_code == 404
    assert c.post("/api/beds/0/play", json={"case": 999999}).status_code == 400
    assert c.post("/api/beds/0/play", json={"case": first, "grain": "nope"}).status_code == 400
    r = c.post("/api/beds/0/play", json={"case": first, "grain": "dei_256ms", "start_at": 30})
    assert r.status_code == 200
    b = r.json()
    assert b["status"] == "running" and b["key"].startswith(f"{first}-b0s") and b["grain"] == "dei_256ms"
    key = b["key"]
    assert key in demo.tap_beds
    rates = demo.tap_beds[key].channel_rates
    assert rates["ECG_II"] == pytest.approx(1000 / 256) and rates["HR"] == 1.0 and len(rates) == 13
    r = c.post("/api/beds/0/stop")
    assert r.json()["status"] == "idle" and key not in demo.tap_beds
    # a second play of the same case on the same bed gets a fresh key, so no Flink window state carries over
    r2 = c.post("/api/beds/0/play", json={"case": first, "grain": "dwc_10s"})
    assert r2.json()["key"] != key and r2.json()["key"].startswith(f"{first}-b0s")


def test_shared_validation_and_failure_style_mapping():
    s = Shared(failure_style="shed", shed_timeout_ms=5000)
    s.validate()
    assert s.inference_timeout_ms() == 5000 and s.stub_config(8000)["queue_max"] == 100_000
    r = Shared(failure_style="reject", reject_queue_max=3)
    assert r.inference_timeout_ms() == 900_000 and r.stub_config(8000)["queue_max"] == 3
    with pytest.raises(ValueError):
        Shared(window_ms=50_000, slide_ms=20_000).validate()
    with pytest.raises(ValueError):
        Shared(failure_style="drop").validate()


def test_config_endpoint_rejects_bad_body(demo):
    c = TestClient(make_app(demo))
    assert c.post("/api/config", json={"failure_style": "drop"}).status_code == 400
    assert c.post("/api/config", json={"unknown_key": 1}).status_code == 400


def test_websocket_messages_never_contain_nan_tokens():
    import json as _json
    raw = dumps({"a": float("nan"), "b": [1.0, float("inf")], "c": {"d": float("-inf"), "e": 2}})
    assert "NaN" not in raw and "Infinity" not in raw
    assert _json.loads(raw) == {"a": None, "b": [1.0, None], "c": {"d": None, "e": 2}}
