"""``ioh-demo``: the demo console back end.

Four beds, each an ``ioh-replay`` process replaying one VitalDB case into the
real Kafka + Flink + stub pipeline; a tap that turns the pipeline's records into
drawable frames; a websocket that pushes them at 10 Hz; REST endpoints for the
case list, bed control and the shared pipeline settings. Serves the built
front end from ``web/dist`` when present.

    uv run ioh-demo [--port 8080] [--pipeline configs/pipeline/laptop.yaml]

Requires the compose stack (Kafka, Flink) to be up. Applying shared settings
restarts the stub and resubmits the Flink job, which stops all beds.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import sys
import threading
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

import httpx
import yaml
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from ..benchmark import pipeline as pl
from ..benchmark.poller import FlinkMetrics
from ..replay.config import Scenario, Workload
from .tap import BedState, Tap

REPO = Path(__file__).resolve().parents[3]
WEB_DIST = Path(__file__).resolve().parent / "web" / "dist"
GRAINS = {"dwc_10s": REPO / "configs/scenarios/dwc_10s.yaml", "dei_256ms": REPO / "configs/scenarios/dei_256ms.yaml"}
WORKLOAD = REPO / "configs/workload/standard_anaesthesia.yaml"
MANIFEST = REPO / "src/ioh_testbed/replay/manifest.json"
N_BEDS = 4
FAILURE_STYLES = ("wait", "shed", "reject")


@dataclass
class Shared:
    """Settings for the one pipeline all beds feed."""

    speed: float = 1.0
    window_ms: int = 60_000
    slide_ms: int = 20_000
    service_ms: float = 50.0
    service_cv: float = 0.0
    workers: int = 4
    failure_style: str = "wait"
    shed_timeout_ms: int = 10_000
    reject_queue_max: int = 4
    capacity: int = 8
    idleness_ms: int = 1_000
    watermark_bound_ms: int = 500
    parallelism: int = 2

    def validate(self) -> None:
        if self.speed <= 0 or self.window_ms <= 0 or self.slide_ms <= 0 or self.window_ms % self.slide_ms:
            raise ValueError("speed must be > 0 and window a positive multiple of slide")
        if self.failure_style not in FAILURE_STYLES:
            raise ValueError(f"failure_style must be one of {FAILURE_STYLES}")
        if self.workers < 1 or self.capacity < 1 or self.service_ms < 0 or self.service_cv < 0:
            raise ValueError("workers and capacity >= 1, service time and cv >= 0")

    def stub_config(self, port: int) -> dict:
        return {
            "service_ms": self.service_ms, "service_cv": self.service_cv, "workers": self.workers,
            "queue_max": self.reject_queue_max if self.failure_style == "reject" else 100_000,
            "seed": 0, "port": port,
        }

    def inference_timeout_ms(self) -> int:
        return self.shed_timeout_ms if self.failure_style == "shed" else 900_000


@dataclass
class Bed:
    index: int
    case: int | None = None
    grain: str = "dwc_10s"
    start_at: float = 0.0
    status: str = "idle"  # idle | running | done | error
    key: str = ""
    started_wall: float = 0.0
    run_id: str = ""
    proc: subprocess.Popen | None = field(default=None, repr=False)
    state: BedState | None = field(default=None, repr=False)
    message: str = ""

    def public(self) -> dict:
        return {
            "index": self.index, "case": self.case, "grain": self.grain, "start_at": self.start_at,
            "status": self.status, "key": self.key, "started_wall": self.started_wall, "run_id": self.run_id,
            "message": self.message,
        }


class Demo:
    def __init__(self, pipeline_cfg: Path, bootstrap: str, stub_port: int, results: Path):
        self.pipe = yaml.safe_load(pipeline_cfg.read_text())
        self.fl = self.pipe["flink"]
        self.bootstrap = bootstrap
        self.stub_port = stub_port
        self.results = results
        self.shared = Shared(
            window_ms=int(self.fl["window_ms"]), slide_ms=int(self.fl["slide_ms"]),
            capacity=int(self.fl["inference_capacity"]), idleness_ms=int(self.fl["idleness_ms"]),
            watermark_bound_ms=int(self.fl["watermark_bound_ms"]), parallelism=int(self.fl.get("parallelism", 2)),
            service_ms=float(self.pipe["stub"]["service_ms"]), workers=int(self.pipe["stub"]["workers"]),
        )
        self.workload = Workload.load(WORKLOAD)
        self.profile = self.workload.profile_for(0)
        self.manifest = json.loads(MANIFEST.read_text())["cases"]
        self.beds = [Bed(i) for i in range(N_BEDS)]
        self.event_origin = time.time()
        self.offset_s = 0.0
        self.tap_beds: dict[str, BedState] = {}
        self.tap: Tap | None = None
        self.flink = pl.Flink(self.fl["rest"])
        self.job_id: str | None = None
        self.job_config: dict = {}
        self.stub_proc: subprocess.Popen | None = None
        self.pipeline_status: dict = {"kafka": False, "flink": False, "stub": False, "job_state": None, "message": ""}
        self.metrics: dict = {}
        self.lock = threading.Lock()
        self.logs = results / "logs"
        self.logs.mkdir(parents=True, exist_ok=True)
        self._stop = threading.Event()

    # ------------------------------------------------------------ lifecycle

    def start(self) -> None:
        try:
            probe = pl.clock_probe(self.bootstrap, 10)
            self.offset_s = probe["min_s"]
            self.pipeline_status["kafka"] = True
        except Exception as e:  # noqa: BLE001
            self.pipeline_status["message"] = f"kafka: {e}"
        self.tap = Tap(self.bootstrap, self.tap_beds, self.offset_s)
        self.tap.start()
        threading.Thread(target=self._status_loop, daemon=True, name="status").start()
        if self.pipeline_status["kafka"]:
            threading.Thread(target=self._apply_safely, daemon=True, name="apply").start()

    def shutdown(self) -> None:
        self._stop.set()
        for b in self.beds:
            self._stop_bed(b)
        pl.stop(self.stub_proc, "stub")
        if self.job_id:
            try:
                self.flink.cancel(self.job_id)
            except Exception:  # noqa: BLE001
                pass
        if self.tap:
            self.tap.stop()

    def _apply_safely(self) -> None:
        try:
            self.apply(self.shared)
        except Exception as e:  # noqa: BLE001
            self.pipeline_status["message"] = f"apply failed: {e}"

    # ------------------------------------------------------------ shared settings

    def apply(self, shared: Shared) -> None:
        """Stop all beds, restart the stub and resubmit the Flink job with ``shared``."""
        shared.validate()
        with self.lock:
            for b in self.beds:
                self._stop_bed(b)
            self.shared = shared
            self.event_origin = time.time()
            self.pipeline_status["message"] = "applying settings"
            pl.stop(self.stub_proc, "stub")
            self.stub_proc = pl.start(pl.stub_command(shared.stub_config(self.stub_port)), self.logs / "stub.log")
            pl.wait_http(f"http://localhost:{self.stub_port}/healthz")
            self.pipeline_status["stub"] = True
            run_id = "demo-" + time.strftime("%H%M%S")
            args, cfg = pl.job_arguments(
                self.fl, run_id=run_id, capacity=shared.capacity, parallelism=shared.parallelism,
                idleness_ms=shared.idleness_ms, bound_ms=shared.watermark_bound_ms,
                inference_timeout_ms=shared.inference_timeout_ms(), window_ms=shared.window_ms,
                slide_ms=shared.slide_ms,
            )
            self.job_id = pl.submit_job(self.flink, REPO / self.fl["jar"], args, shared.parallelism)
            self.job_config = cfg
            self.pipeline_status.update({"flink": True, "job_state": "RUNNING", "message": ""})
            self.metrics_reader = FlinkMetrics(self.fl["rest"], self.job_id)

    # ------------------------------------------------------------ beds

    def play(self, index: int, case: int, grain: str, start_at: float) -> Bed:
        if grain not in GRAINS:
            raise ValueError(f"grain must be one of {sorted(GRAINS)}")
        info = next((c for c in self.manifest if int(c["caseid"]) == int(case)), None)
        if info is None:
            raise ValueError(f"case {case} is not in the manifest")
        remaining = float(info["duration_s"]) - start_at
        if remaining < 120:
            raise ValueError("less than 120 s of recording left after the start offset")
        with self.lock:
            bed = self.beds[index]
            self._stop_bed(bed)
            packet_ms = Scenario.load(GRAINS[grain]).packet_ms
            bed.case, bed.grain, bed.start_at = int(case), grain, float(start_at)
            bed.key = f"{case}-b{index}"
            bed.run_id = f"bed{index}-{time.strftime('%H%M%S')}"
            bed.state = BedState(bed.key, self.profile.records_per_second(packet_ms), self.shared.speed)
            bed.started_wall = time.time()
            self.tap_beds[bed.key] = bed.state
            duration = min(remaining - 5.0, 4 * 3600.0)
            cmd = [sys.executable, "-m", "ioh_testbed.replay",
                   "--workload", str(WORKLOAD), "--scenario", str(GRAINS[grain]),
                   "--case-id", str(case), "--key-suffix", f"-b{index}", "--start-at", str(start_at),
                   "--duration", str(int(duration)), "--speed", str(self.shared.speed),
                   "--event-origin", repr(self.event_origin),
                   "--sink", "kafka", "--bootstrap", self.bootstrap,
                   "--results", str(self.results), "--run-id", bed.run_id,
                   "--tolerance-ms", "1000000", "--invalid-ms", "1000000"]
            bed.proc = pl.start(cmd, self.logs / f"bed{index}.log")
            bed.status, bed.message = "running", ""
            return bed

    def stop(self, index: int) -> Bed:
        with self.lock:
            bed = self.beds[index]
            self._stop_bed(bed)
            return bed

    def _stop_bed(self, bed: Bed) -> None:
        if bed.proc is not None and bed.proc.poll() is None:
            pl.stop(bed.proc, f"bed{bed.index}", timeout=5)
        if bed.status == "running":
            bed.status = "idle"
        bed.proc = None
        if bed.key in self.tap_beds:
            del self.tap_beds[bed.key]

    def _poll_beds(self) -> None:
        for b in self.beds:
            if b.status == "running" and b.proc is not None and b.proc.poll() is not None:
                rc = b.proc.returncode
                b.status = "done" if rc == 0 else "error"
                b.message = "" if rc == 0 else f"replay exited with {rc}, see logs/bed{b.index}.log"

    # ------------------------------------------------------------ status

    def _status_loop(self) -> None:
        stub = httpx.Client(timeout=1.0)
        while not self._stop.is_set():
            try:
                self._poll_beds()
                if self.job_id:
                    j = self.flink.job(self.job_id)
                    self.pipeline_status["job_state"] = j.get("state")
                    self.pipeline_status["flink"] = j.get("state") == "RUNNING"
                    try:
                        sample = self.metrics_reader.sample()
                        verts = {v["name"].split(" ")[0].replace("Source:", "source").replace(":", ""): v
                                 for v in sample.get("vertices", [])}
                        src = verts.get("source", {})
                        feat = verts.get("features", {})
                        self.metrics = {
                            "source_backpressure_ms_s": src.get("backPressuredTimeMsPerSecond"),
                            "source_busy_ms_s": src.get("busyTimeMsPerSecond"),
                            "source_records_in_s": src.get("numRecordsInPerSecond"),
                            "kafka_lag_max": src.get("records-lag-max"),
                            "features_busy_ms_s": feat.get("busyTimeMsPerSecond"),
                            "features_backpressure_ms_s": feat.get("backPressuredTimeMsPerSecond"),
                            "checkpoints": sample.get("checkpoints"),
                        }
                    except Exception:  # noqa: BLE001
                        pass
                try:
                    r = stub.get(f"http://localhost:{self.stub_port}/stats")
                    self.pipeline_status["stub"] = r.status_code == 200
                    self.metrics["stub"] = r.json()
                except Exception:  # noqa: BLE001
                    self.pipeline_status["stub"] = False
            except Exception as e:  # noqa: BLE001
                self.pipeline_status["message"] = str(e)
            self._stop.wait(2.0)

    def state(self) -> dict:
        return {
            "beds": [b.public() for b in self.beds],
            "shared": asdict(self.shared),
            "pipeline": {**self.pipeline_status, "job_id": self.job_id, "job_config": self.job_config,
                         "clock_offset_s": self.offset_s, "event_origin": self.event_origin,
                         "metrics": self.metrics},
            "grains": sorted(GRAINS),
            "failure_styles": list(FAILURE_STYLES),
            "profile": {"name": self.profile.name,
                        "waves": [t.label for t in self.profile.waves],
                        "numerics": [t.label for t in self.profile.numerics]},
        }

    def cases(self) -> list[dict]:
        return [{"caseid": int(c["caseid"]), "duration_s": float(c["duration_s"]),
                 "n_wave_tracks": c.get("n_wave_tracks"), "n_numeric_tracks": c.get("n_numeric_tracks"),
                 "wave_tracks": c.get("wave_tracks", [])} for c in self.manifest]

    def frames(self, delta: bool = True) -> list[dict]:
        out = []
        if self.tap is None:
            return out
        with self.tap.lock:
            for b in self.beds:
                if b.state is not None and b.status in ("running", "done"):
                    f = b.state.frame(self.shared.window_ms / 1000.0, self.shared.slide_ms / 1000.0, delta=delta)
                    f["bed"] = b.index
                    f["status"] = b.status
                    f["speed"] = self.shared.speed
                    out.append(f)
        return out


# ---------------------------------------------------------------- app

def make_app(demo: Demo) -> FastAPI:
    app = FastAPI(title="ioh demo console")

    @app.on_event("startup")
    def _startup():
        demo.start()

    @app.on_event("shutdown")
    def _shutdown():
        demo.shutdown()

    @app.get("/api/cases")
    def cases():
        return demo.cases()

    @app.get("/api/state")
    def state():
        return demo.state()

    @app.post("/api/config")
    async def config(body: dict):
        try:
            shared = Shared(**{**asdict(demo.shared), **body})
            await asyncio.get_running_loop().run_in_executor(None, demo.apply, shared)
        except (TypeError, ValueError) as e:
            raise HTTPException(400, str(e)) from e
        except Exception as e:  # noqa: BLE001
            raise HTTPException(500, f"apply failed: {e}") from e
        return demo.state()

    @app.post("/api/beds/{index}/play")
    def play(index: int, body: dict):
        if not 0 <= index < N_BEDS:
            raise HTTPException(404, "no such bed")
        try:
            bed = demo.play(index, int(body["case"]), body.get("grain", "dwc_10s"), float(body.get("start_at", 0.0)))
        except (KeyError, ValueError) as e:
            raise HTTPException(400, str(e)) from e
        return bed.public()

    @app.post("/api/beds/{index}/stop")
    def stop(index: int):
        if not 0 <= index < N_BEDS:
            raise HTTPException(404, "no such bed")
        return demo.stop(index).public()

    @app.websocket("/ws")
    async def ws(sock: WebSocket):
        await sock.accept()
        try:
            await sock.send_text(json.dumps({"type": "snapshot", "state": demo.state(), "frames": demo.frames(delta=False)},
                                            default=_json_default))
            while True:
                await asyncio.sleep(0.1)
                msg = {"type": "frame", "t": time.time(), "frames": demo.frames(delta=True),
                       "pipeline": demo.state()["pipeline"], "beds": [b.public() for b in demo.beds]}
                await sock.send_text(json.dumps(msg, default=_json_default))
        except WebSocketDisconnect:
            return
        except Exception:  # noqa: BLE001
            return

    if WEB_DIST.exists():
        app.mount("/assets", StaticFiles(directory=WEB_DIST / "assets"), name="assets")

        @app.get("/")
        def index():
            return FileResponse(WEB_DIST / "index.html")
    else:
        @app.get("/")
        def index_missing():
            return JSONResponse({"message": "front end not built: cd src/ioh_testbed/demo/web && npm install && npm run build"})

    return app


def _json_default(o):
    if isinstance(o, float) and o != o:
        return None
    if isinstance(o, set):
        return sorted(o)
    raise TypeError(type(o))


def main(argv=None) -> int:
    import uvicorn

    ap = argparse.ArgumentParser(prog="ioh-demo", description=__doc__.split("\n\n")[0])
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--pipeline", type=Path, default=REPO / "configs/pipeline/laptop.yaml")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--stub-port", type=int, default=8000)
    ap.add_argument("--results", type=Path, default=REPO / "results/demo")
    args = ap.parse_args(argv)
    demo = Demo(args.pipeline, args.bootstrap, args.stub_port, args.results)
    uvicorn.run(make_app(demo), host="127.0.0.1", port=args.port, log_level="warning")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
