import asyncio
import statistics
import time

import httpx
import pytest

from ioh_testbed.inference.stub import ServiceTime, StubConfig, make_app, risk_from_features


def client(cfg: StubConfig) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=make_app(cfg)), base_url="http://stub")


async def predict(c: httpx.AsyncClient, features=None):
    return await c.post("/predict", json={"features": features or {}})


def test_service_time_mean_and_cv():
    st = ServiceTime(100.0, 0.5, seed=1)
    xs = [st.draw_ms() for _ in range(20000)]
    assert statistics.mean(xs) == pytest.approx(100.0, rel=0.03)
    assert statistics.pstdev(xs) / statistics.mean(xs) == pytest.approx(0.5, rel=0.05)
    assert ServiceTime(100.0, 0.0, seed=1).draw_ms() == 100.0


def test_service_time_is_deterministic_under_seed():
    a = [ServiceTime(50.0, 0.3, seed=7).draw_ms() for _ in range(5)]
    b = [ServiceTime(50.0, 0.3, seed=7).draw_ms() for _ in range(5)]
    assert a == b


def test_risk_is_a_logistic_in_map():
    assert risk_from_features({}) == 0.5
    assert risk_from_features({"ART_MBP": {"mean": 65.0}}) == pytest.approx(0.5)
    assert risk_from_features({"ART_MBP": {"mean": 50.0}}) > 0.9
    assert risk_from_features({"ART_MBP": {"mean": 90.0}}) < 0.01


def test_single_request_reports_timestamps_and_service_time():
    async def go():
        async with client(StubConfig(service_ms=20.0, workers=1)) as c:
            t = time.time()
            r = await predict(c, {"ART_MBP": {"mean": 55.0}})
            elapsed = time.time() - t
        return r, elapsed

    r, elapsed = asyncio.run(go())
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok" and 0.5 < body["risk"] < 1.0
    assert body["t_recv"] <= body["t_start"] <= body["t_done"]
    assert body["service_ms"] == 20.0
    assert body["t_done"] - body["t_start"] >= 0.019
    assert body["queue_wait_ms"] < 10.0
    assert elapsed >= 0.019


def test_capacity_queues_the_excess():
    # 2 workers, 4 concurrent requests of 100 ms: two are served at once, two wait
    # about one service time. Total wall time is ~2 service times, not 4 (parallel)
    # and not 1 (unbounded).
    async def go():
        async with client(StubConfig(service_ms=100.0, workers=2)) as c:
            t = time.time()
            rs = await asyncio.gather(*(predict(c) for _ in range(4)))
            wall = time.time() - t
            stats = (await c.get("/stats")).json()
        return [r.json() for r in rs], wall, stats

    bodies, wall, stats = asyncio.run(go())
    waits = sorted(b["queue_wait_ms"] for b in bodies)
    assert waits[0] < 20 and waits[1] < 20
    assert 80 < waits[2] < 160 and 80 < waits[3] < 160
    assert 0.18 < wall < 0.35
    assert stats["served"] == 4 and stats["rejected"] == 0
    assert stats["max_in_flight"] == 2 and stats["max_queued"] >= 2
    assert stats["in_flight"] == 0 and stats["queued"] == 0


def test_queue_max_rejects_with_503():
    async def go():
        async with client(StubConfig(service_ms=100.0, workers=1, queue_max=1)) as c:
            rs = await asyncio.gather(*(predict(c) for _ in range(4)))
            stats = (await c.get("/stats")).json()
        return rs, stats

    rs, stats = asyncio.run(go())
    codes = sorted(r.status_code for r in rs)
    # one in service, one queued, the rest rejected
    assert codes == [200, 200, 503, 503]
    rejected = [r.json() for r in rs if r.status_code == 503]
    assert all(b["status"] == "rejected" for b in rejected)
    assert stats["requests"] == 4 and stats["served"] == 2 and stats["rejected"] == 2


def test_config_endpoint_echoes_configuration():
    async def go():
        async with client(StubConfig(service_ms=7.0, service_cv=0.2, workers=3, queue_max=9, seed=4)) as c:
            return (await c.get("/config")).json()

    assert asyncio.run(go()) == {"service_ms": 7.0, "service_cv": 0.2, "workers": 3, "queue_max": 9, "seed": 4}


def test_invalid_config_rejected():
    with pytest.raises(ValueError):
        StubConfig(workers=0)
