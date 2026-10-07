"""Configurable inference stub: the pipeline's model placeholder.

The TRANSFER model is not available, so inference demand is an experimental
variable rather than a measurement (proposal 5). The stub exposes the three
knobs the proposal names and nothing else:

    service time   ``--service-ms``   mean time one prediction occupies a worker
    variability    ``--service-cv``   coefficient of variation of a lognormal
                                      service time; 0 is deterministic
    capacity       ``--workers``      predictions served concurrently; further
                                      requests queue, and beyond ``--queue-max``
                                      they are rejected with 503

Service time is modelled by sleeping, not by burning CPU, so the stub does not
compete with Kafka and Flink for cores on a shared host. That is a stated
limitation: a real model's CPU or GPU contention is not represented.

Every response carries the stub's own timestamps and queue wait so the
benchmark can separate queueing from service time at the inference boundary.
"""

from __future__ import annotations

import argparse
import asyncio
import math
import random
import time
from dataclasses import asdict, dataclass

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


@dataclass(frozen=True)
class StubConfig:
    service_ms: float = 50.0
    service_cv: float = 0.0
    workers: int = 4
    queue_max: int = 1000
    seed: int = 0

    def __post_init__(self) -> None:
        if self.service_ms < 0 or self.service_cv < 0 or self.workers < 1 or self.queue_max < 0:
            raise ValueError(f"invalid stub config: {self}")


class ServiceTime:
    """Lognormal with the configured mean and coefficient of variation."""

    def __init__(self, mean_ms: float, cv: float, seed: int):
        self.mean_ms = mean_ms
        self.cv = cv
        self._rng = random.Random(seed)
        if cv > 0:
            self._sigma = math.sqrt(math.log(1.0 + cv * cv))
            self._mu = math.log(mean_ms) - self._sigma**2 / 2.0 if mean_ms > 0 else 0.0

    def draw_ms(self) -> float:
        if self.cv == 0 or self.mean_ms == 0:
            return self.mean_ms
        return self._rng.lognormvariate(self._mu, self._sigma)


def risk_from_features(features: dict) -> float:
    """A deterministic placeholder score so predictions are not constant: a
    logistic in the window-mean arterial pressure, if present."""
    try:
        map_mean = float(features["ART_MBP"]["mean"])
    except (KeyError, TypeError, ValueError):
        return 0.5
    return 1.0 / (1.0 + math.exp((map_mean - 65.0) / 5.0))


class Stats:
    def __init__(self) -> None:
        self.requests = 0
        self.served = 0
        self.rejected = 0
        self.in_flight = 0
        self.queued = 0
        self.max_queued = 0
        self.max_in_flight = 0
        self.service_ms_sum = 0.0
        self.queue_wait_ms_sum = 0.0
        self.queue_wait_ms_max = 0.0

    def snapshot(self) -> dict:
        return {
            "requests": self.requests,
            "served": self.served,
            "rejected": self.rejected,
            "in_flight": self.in_flight,
            "queued": self.queued,
            "max_queued": self.max_queued,
            "max_in_flight": self.max_in_flight,
            "service_ms_mean": self.service_ms_sum / self.served if self.served else 0.0,
            "queue_wait_ms_mean": self.queue_wait_ms_sum / self.served if self.served else 0.0,
            "queue_wait_ms_max": self.queue_wait_ms_max,
        }


def make_app(cfg: StubConfig) -> FastAPI:
    app = FastAPI(title="ioh inference stub")
    sem = asyncio.Semaphore(cfg.workers)
    svc = ServiceTime(cfg.service_ms, cfg.service_cv, cfg.seed)
    stats = Stats()
    app.state.config = cfg
    app.state.stats = stats

    @app.post("/predict")
    async def predict(request: Request):
        t_recv = time.time()
        stats.requests += 1
        if stats.queued >= cfg.queue_max:
            stats.rejected += 1
            return JSONResponse(
                status_code=503,
                content={"status": "rejected", "t_recv": t_recv, "queued": stats.queued, "workers": cfg.workers},
            )
        try:
            body = await request.json()
        except Exception:
            body = {}
        features = body.get("features", body) if isinstance(body, dict) else {}

        stats.queued += 1
        stats.max_queued = max(stats.max_queued, stats.queued)
        async with sem:
            stats.queued -= 1
            stats.in_flight += 1
            stats.max_in_flight = max(stats.max_in_flight, stats.in_flight)
            t_start = time.time()
            service_ms = svc.draw_ms()
            if service_ms > 0:
                await asyncio.sleep(service_ms / 1000.0)
            t_done = time.time()
            stats.in_flight -= 1

        queue_wait_ms = (t_start - t_recv) * 1000.0
        stats.served += 1
        stats.service_ms_sum += service_ms
        stats.queue_wait_ms_sum += queue_wait_ms
        stats.queue_wait_ms_max = max(stats.queue_wait_ms_max, queue_wait_ms)
        return {
            "status": "ok",
            "risk": risk_from_features(features),
            "t_recv": t_recv,
            "t_start": t_start,
            "t_done": t_done,
            "queue_wait_ms": queue_wait_ms,
            "service_ms": service_ms,
            "workers": cfg.workers,
        }

    @app.get("/config")
    async def config():
        return asdict(cfg)

    @app.get("/stats")
    async def get_stats():
        return stats.snapshot()

    @app.get("/healthz")
    async def healthz():
        return {"ok": True}

    return app


def parse_args(argv=None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(prog="ioh-stub", description=__doc__.split("\n\n")[0])
    ap.add_argument("--service-ms", type=float, default=50.0, help="mean service time per prediction")
    ap.add_argument("--service-cv", type=float, default=0.0, help="lognormal coefficient of variation; 0 = fixed")
    ap.add_argument("--workers", type=int, default=4, help="concurrent predictions; more wait in the queue")
    ap.add_argument("--queue-max", type=int, default=1000, help="queued requests beyond this are rejected (503)")
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=8000)
    return ap.parse_args(argv)


def main(argv=None) -> int:
    import uvicorn

    args = parse_args(argv)
    cfg = StubConfig(args.service_ms, args.service_cv, args.workers, args.queue_max, args.seed)
    uvicorn.run(make_app(cfg), host=args.host, port=args.port, log_level="warning", access_log=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
