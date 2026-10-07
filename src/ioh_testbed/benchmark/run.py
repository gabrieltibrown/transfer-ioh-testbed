"""``ioh-run``: one end-to-end experiment run, fully recorded.

    uv run ioh-run --workload configs/workload/standard_anaesthesia.yaml \\
        --scenario configs/scenarios/dwc_10s.yaml --pipeline configs/pipeline/laptop.yaml \\
        --cases 5 --duration 600

Sequence: clock probe, start the inference stub, (re)submit the Flink job with
this run's parameters and wait until it is running, start the poller and the
interface consumer, run the replay harness open-loop into Kafka, drain, stop
everything, cancel the job, collect container logs, probe the clock again,
write the combined ``meta.json`` and print the analysis.

The Kafka broker and the Flink cluster must already be up (src/compose). The
stub and the consumer run on the host so the Docker VM holds only Kafka and
Flink.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx
import yaml

from . import stamp

PROBE_TOPIC = "clock-probe"


def parse_args(argv=None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(prog="ioh-run", description=__doc__.split("\n\n")[0])
    ap.add_argument("--workload", required=True, type=Path)
    ap.add_argument("--scenario", required=True, type=Path)
    ap.add_argument("--pipeline", required=True, type=Path)
    ap.add_argument("--cases", type=int, default=1)
    ap.add_argument("--duration", type=float, required=True, help="observation window in seconds")
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--results", type=Path, default=Path("results"))
    ap.add_argument("--bootstrap", default="localhost:9092", help="broker as seen from the host")
    ap.add_argument("--stub-service-ms", type=float, default=None)
    ap.add_argument("--stub-cv", type=float, default=None)
    ap.add_argument("--stub-workers", type=int, default=None)
    ap.add_argument("--stub-queue-max", type=int, default=None)
    ap.add_argument("--inference-capacity", type=int, default=None, help="override pipeline.flink.inference_capacity")
    ap.add_argument("--parallelism", type=int, default=None, help="override pipeline.flink.parallelism")
    ap.add_argument("--drain", type=float, default=None, help="seconds to wait after the replay (default: pipeline.run.drain_s)")
    ap.add_argument("--label", default="", help="free-text note stored in meta.json")
    return ap.parse_args(argv)


def new_run_id() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + uuid.uuid4().hex[:6]


def log(msg: str) -> None:
    print(f"[ioh-run {time.strftime('%H:%M:%S')}] {msg}", file=sys.stderr, flush=True)


# ---------------------------------------------------------------- clock probe

def clock_probe(bootstrap: str, n: int) -> dict:
    """Host-to-broker clock offset: produce n tiny messages, compare each
    LogAppendTime with the host time at produce. Each sample includes the
    one-way transit, so the minimum is the best estimate and an upper bound."""
    from confluent_kafka import Producer

    samples: list[float] = []

    def cb(err, msg):
        if err is None:
            _, ts_ms = msg.timestamp()
            samples.append(ts_ms / 1000.0 - float(msg.key().decode()))

    p = Producer({"bootstrap.servers": bootstrap, "linger.ms": 0, "acks": "all"})
    for _ in range(n):
        t = time.time()
        p.produce(PROBE_TOPIC, key=str(t).encode(), value=b"{}", callback=cb)
        p.flush(5)
    if not samples:
        raise RuntimeError("clock probe produced no samples; is the clock-probe topic present?")
    s = sorted(samples)
    return {"n": len(s), "min_s": s[0], "median_s": s[len(s) // 2], "max_s": s[-1]}


# ---------------------------------------------------------------- flink REST

class Flink:
    def __init__(self, rest: str):
        self.rest = rest.rstrip("/")
        self.c = httpx.Client(timeout=30.0)

    def overview(self) -> dict:
        r = self.c.get(f"{self.rest}/overview")
        r.raise_for_status()
        return r.json()

    def config(self) -> dict:
        return self.c.get(f"{self.rest}/config").json()

    def cancel_all_running(self) -> list[str]:
        cancelled = []
        for j in self.c.get(f"{self.rest}/jobs").json().get("jobs", []):
            if j["status"] in ("RUNNING", "CREATED", "RESTARTING", "INITIALIZING"):
                self.c.patch(f"{self.rest}/jobs/{j['id']}", params={"mode": "cancel"})
                cancelled.append(j["id"])
        for jid in cancelled:
            self.wait_state(jid, {"CANCELED", "FAILED", "FINISHED"}, timeout=60)
        return cancelled

    def upload(self, jar: Path) -> str:
        with jar.open("rb") as f:
            r = self.c.post(f"{self.rest}/jars/upload", files={"jarfile": (jar.name, f, "application/x-java-archive")})
        r.raise_for_status()
        return r.json()["filename"].rsplit("/", 1)[-1]

    def run(self, jar_id: str, args: list[str], parallelism: int | None) -> str:
        body: dict = {"programArgsList": args}
        if parallelism and parallelism > 0:
            body["parallelism"] = parallelism
        r = self.c.post(f"{self.rest}/jars/{jar_id}/run", json=body)
        r.raise_for_status()
        return r.json()["jobid"]

    def job(self, jid: str) -> dict:
        return self.c.get(f"{self.rest}/jobs/{jid}").json()

    def wait_state(self, jid: str, states: set[str], timeout: float) -> dict:
        t_end = time.time() + timeout
        while time.time() < t_end:
            j = self.job(jid)
            if j.get("state") in states:
                return j
            time.sleep(0.5)
        raise TimeoutError(f"job {jid} did not reach {states} within {timeout}s (state {j.get('state')})")

    def wait_running(self, jid: str, timeout: float = 120) -> dict:
        """RUNNING with every vertex RUNNING, i.e. source splits assigned."""
        t_end = time.time() + timeout
        while time.time() < t_end:
            j = self.job(jid)
            if j.get("state") == "RUNNING" and j.get("vertices") and all(v["status"] == "RUNNING" for v in j["vertices"]):
                return j
            if j.get("state") in ("FAILED", "CANCELED", "FINISHED"):
                raise RuntimeError(f"job {jid} ended with {j.get('state')}")
            time.sleep(0.5)
        raise TimeoutError(f"job {jid} not fully running within {timeout}s")

    def cancel(self, jid: str) -> None:
        self.c.patch(f"{self.rest}/jobs/{jid}", params={"mode": "cancel"})
        try:
            self.wait_state(jid, {"CANCELED", "FAILED", "FINISHED"}, timeout=60)
        except TimeoutError as e:
            log(str(e))


# ---------------------------------------------------------------- processes

def start(cmd: list[str], log_path: Path) -> subprocess.Popen:
    f = log_path.open("ab")
    return subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT, env={**os.environ, "PYTHONUNBUFFERED": "1"})


def stop(p: subprocess.Popen | None, name: str, timeout: float = 15) -> None:
    if p is None or p.poll() is not None:
        return
    p.send_signal(signal.SIGTERM)
    try:
        p.wait(timeout)
    except subprocess.TimeoutExpired:
        log(f"{name} did not exit on SIGTERM; killing")
        p.kill()
        p.wait(5)


def wait_http(url: str, timeout: float = 30) -> None:
    t_end = time.time() + timeout
    while time.time() < t_end:
        try:
            if httpx.get(url, timeout=1.0).status_code == 200:
                return
        except Exception:  # noqa: BLE001
            pass
        time.sleep(0.25)
    raise TimeoutError(f"{url} not reachable within {timeout}s")


def wait_file(p: Path, timeout: float = 30) -> None:
    t_end = time.time() + timeout
    while time.time() < t_end:
        if p.exists():
            return
        time.sleep(0.1)
    raise TimeoutError(f"{p} not created within {timeout}s")


def docker_logs(container: str, dest: Path) -> None:
    try:
        with dest.open("wb") as f:
            subprocess.run(["docker", "logs", container], stdout=f, stderr=subprocess.STDOUT, timeout=60, check=False)
    except Exception as e:  # noqa: BLE001
        log(f"could not collect logs of {container}: {e}")


def wipe_checkpoints() -> None:
    subprocess.run(["docker", "exec", "ioh-flink-jm", "sh", "-c", "rm -rf /flink-checkpoints/* 2>/dev/null || true"],
                   check=False, timeout=30, capture_output=True)


# ---------------------------------------------------------------- main

def main(argv=None) -> int:
    args = parse_args(argv)
    pipe = yaml.safe_load(args.pipeline.read_text())
    fl, st, rn = pipe["flink"], pipe["stub"], pipe.get("run", {})
    run_id = args.run_id or new_run_id()
    out = args.results / run_id
    out.mkdir(parents=True, exist_ok=True)
    logs = out / "logs"
    logs.mkdir(exist_ok=True)
    log(f"run {run_id} -> {out}")

    stub_cfg = {
        "service_ms": args.stub_service_ms if args.stub_service_ms is not None else st["service_ms"],
        "service_cv": args.stub_cv if args.stub_cv is not None else st.get("service_cv", 0.0),
        "workers": args.stub_workers if args.stub_workers is not None else st["workers"],
        "queue_max": args.stub_queue_max if args.stub_queue_max is not None else st.get("queue_max", 1000),
        "seed": st.get("seed", 0),
        "port": st.get("port", 8000),
    }
    capacity = args.inference_capacity if args.inference_capacity is not None else fl["inference_capacity"]
    parallelism = args.parallelism if args.parallelism is not None else fl.get("parallelism", -1)
    drain = args.drain if args.drain is not None else float(rn.get("drain_s", 30))
    job_args = [
        "--bootstrap", fl["bootstrap_in_cluster"],
        "--group-id", f"ioh-flink-{run_id}",
        "--window-ms", str(fl["window_ms"]), "--slide-ms", str(fl["slide_ms"]),
        "--watermark-bound-ms", str(fl["watermark_bound_ms"]), "--idleness-ms", str(fl["idleness_ms"]),
        "--progress-interval-ms", str(fl.get("progress_interval_ms", 1000)),
        "--inference-url", fl["inference_url"], "--inference-capacity", str(capacity),
        "--inference-timeout-ms", str(fl["inference_timeout_ms"]), "--checkpoint-ms", str(fl["checkpoint_ms"]),
        "--parallelism", str(parallelism), "--run-id", run_id,
    ]
    job_cfg = {
        "bootstrap": fl["bootstrap_in_cluster"], "group_id": f"ioh-flink-{run_id}",
        "window_ms": fl["window_ms"], "slide_ms": fl["slide_ms"], "watermark_bound_ms": fl["watermark_bound_ms"],
        "idleness_ms": fl["idleness_ms"], "progress_interval_ms": fl.get("progress_interval_ms", 1000),
        "inference_url": fl["inference_url"], "inference_capacity_per_subtask": capacity,
        "inference_capacity_total": capacity * max(parallelism, 1),
        "inference_timeout_ms": fl["inference_timeout_ms"], "checkpoint_ms": fl["checkpoint_ms"],
        "parallelism": parallelism,
    }

    flink = Flink(fl["rest"])
    stub_proc = poller_proc = iface_proc = None
    job_id = None
    harness_rc = None
    meta_extra: dict = {}
    try:
        ov = flink.overview()
        log(f"flink {ov.get('flink-version')} slots {ov.get('slots-available')}/{ov.get('slots-total')}")
        log("clock probe (start)")
        probe_start = clock_probe(args.bootstrap, int(rn.get("clock_probes", 20)))
        log(f"  offset min {probe_start['min_s'] * 1000:.2f} ms, median {probe_start['median_s'] * 1000:.2f} ms")

        log(f"stub: service {stub_cfg['service_ms']} ms cv {stub_cfg['service_cv']} workers {stub_cfg['workers']}")
        stub_proc = start([sys.executable, "-m", "ioh_testbed.inference.stub",
                           "--service-ms", str(stub_cfg["service_ms"]), "--service-cv", str(stub_cfg["service_cv"]),
                           "--workers", str(stub_cfg["workers"]), "--queue-max", str(stub_cfg["queue_max"]),
                           "--seed", str(stub_cfg["seed"]), "--port", str(stub_cfg["port"])], logs / "stub.log")
        wait_http(f"http://localhost:{stub_cfg['port']}/healthz")

        cancelled = flink.cancel_all_running()
        if cancelled:
            log(f"cancelled stale jobs {cancelled}")
        wipe_checkpoints()
        jar = Path(fl["jar"])
        jar_id = flink.upload(jar)
        job_id = flink.run(jar_id, job_args, parallelism if parallelism > 0 else None)
        log(f"job {job_id} submitted; waiting for RUNNING")
        flink.wait_running(job_id)
        log("job running")

        poller_proc = start([sys.executable, "-m", "ioh_testbed.benchmark.poller", "--out", str(out / "poller.jsonl"),
                             "--flink-rest", fl["rest"], "--job-id", job_id,
                             "--stub-url", f"http://localhost:{stub_cfg['port']}"], logs / "poller.log")
        ready = out / ".interface-ready"
        iface_proc = start([sys.executable, "-m", "ioh_testbed.interface.consumer", "--bootstrap", args.bootstrap,
                            "--out", str(out), "--ready-file", str(ready)], logs / "interface.log")
        wait_file(ready)
        ready.unlink(missing_ok=True)
        time.sleep(2.0)  # let the source's split assignment settle before offering load

        harness = [sys.executable, "-m", "ioh_testbed.replay", "--workload", str(args.workload),
                   "--scenario", str(args.scenario), "--cases", str(args.cases), "--duration", str(args.duration),
                   "--sink", "kafka", "--bootstrap", args.bootstrap, "--results", str(args.results), "--run-id", run_id]
        log(f"replay: {args.cases} cases x {args.duration:g} s")
        t_h = time.time()
        with (logs / "harness.log").open("ab") as f:
            harness_rc = subprocess.run(harness, stdout=f, stderr=subprocess.STDOUT).returncode
        log(f"replay exit {harness_rc} after {time.time() - t_h:.1f} s; draining {drain:g} s")
        time.sleep(drain)
    except Exception as e:  # noqa: BLE001
        log(f"ERROR {e}")
        meta_extra["error"] = str(e)
    finally:
        stop(iface_proc, "interface")
        stop(poller_proc, "poller")
        stub_stats = None
        try:
            stub_stats = httpx.get(f"http://localhost:{stub_cfg['port']}/stats", timeout=2).json()
        except Exception:  # noqa: BLE001
            pass
        stop(stub_proc, "stub")
        if job_id:
            flink.cancel(job_id)
        docker_logs("ioh-flink-jm", logs / "flink-jobmanager.log")
        docker_logs("ioh-flink-tm", logs / "flink-taskmanager.log")
        try:
            probe_end = clock_probe(args.bootstrap, int(rn.get("clock_probes", 20)))
        except Exception as e:  # noqa: BLE001
            probe_end = {"error": str(e)}

    meta_path = out / "meta.json"
    meta = json.loads(meta_path.read_text()) if meta_path.exists() else {"run_id": run_id, "git": stamp.git_provenance(),
                                                                          "environment": stamp.environment_descriptor()}
    meta.update({
        "pipeline_config": stamp.file_digest(args.pipeline),
        "job": {"id": job_id, "jar": str(fl["jar"]), "args": job_args, "config": job_cfg,
                "flink_version": (flink.overview().get("flink-version") if job_id else None)},
        "stub": {"config": stub_cfg, "final_stats": stub_stats},
        "clock": {"start": probe_start if "probe_start" in locals() else None, "end": probe_end,
                  "offset_s": (probe_start["min_s"] if "probe_start" in locals() else 0.0),
                  "note": "vm = host + offset_s; min of LogAppendTime - t_produce over probes, includes one-way transit"},
        "drain_s": drain,
        "harness_exit": harness_rc,
        "label": args.label,
        **meta_extra,
    })
    meta_path.write_text(json.dumps(meta, indent=2, default=str) + "\n")
    log(f"wrote {meta_path}")

    from .analyze import analyze, print_summary
    try:
        print_summary(analyze(out))
    except Exception as e:  # noqa: BLE001
        log(f"analysis failed: {e}")
        return 1
    return 0 if harness_rc == 0 and "error" not in meta_extra else 1


if __name__ == "__main__":
    raise SystemExit(main())
