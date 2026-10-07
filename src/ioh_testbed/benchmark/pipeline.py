"""Pipeline control shared by ``ioh-run`` and the demo console: clock probe,
Flink REST client, subprocess helpers, job argument builder.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import httpx

PROBE_TOPIC = "clock-probe"


def log(msg: str) -> None:
    print(f"[ioh {time.strftime('%H:%M:%S')}] {msg}", file=sys.stderr, flush=True)


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
        if r.status_code >= 400:
            errs = r.json().get("errors", [r.text]) if r.headers.get("content-type", "").startswith("application/json") else [r.text]
            causes = [line.strip() for line in "\n".join(errs).splitlines() if "Caused by" in line]
            raise RuntimeError(f"job submission failed ({r.status_code}): " + (causes[-1] if causes else errs[0][:500]))
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


# ---------------------------------------------------------------- job config

def job_arguments(fl: dict, *, run_id: str, capacity: int, parallelism: int, idleness_ms: int,
                  bound_ms: int, inference_timeout_ms: int, window_ms: int | None = None,
                  slide_ms: int | None = None) -> tuple[list[str], dict]:
    """Program arguments for the Flink job and the matching description for
    ``meta.json``, from a pipeline config's ``flink`` section plus overrides."""
    window_ms = int(window_ms if window_ms is not None else fl["window_ms"])
    slide_ms = int(slide_ms if slide_ms is not None else fl["slide_ms"])
    group = f"ioh-flink-{run_id}"
    args = [
        "--bootstrap", fl["bootstrap_in_cluster"],
        "--group-id", group,
        "--window-ms", str(window_ms), "--slide-ms", str(slide_ms),
        "--watermark-bound-ms", str(bound_ms), "--idleness-ms", str(idleness_ms),
        "--progress-interval-ms", str(fl.get("progress_interval_ms", 1000)),
        "--inference-url", fl["inference_url"], "--inference-capacity", str(capacity),
        "--inference-timeout-ms", str(inference_timeout_ms), "--checkpoint-ms", str(fl["checkpoint_ms"]),
        "--parallelism", str(parallelism), "--run-id", run_id,
    ]
    cfg = {
        "bootstrap": fl["bootstrap_in_cluster"], "group_id": group,
        "window_ms": window_ms, "slide_ms": slide_ms, "watermark_bound_ms": int(bound_ms),
        "idleness_ms": int(idleness_ms), "progress_interval_ms": fl.get("progress_interval_ms", 1000),
        "inference_url": fl["inference_url"], "inference_capacity_per_subtask": int(capacity),
        "inference_capacity_total": int(capacity) * max(int(parallelism), 1),
        "inference_timeout_ms": int(inference_timeout_ms), "checkpoint_ms": fl["checkpoint_ms"],
        "parallelism": int(parallelism),
    }
    return args, cfg


def stub_command(stub_cfg: dict) -> list[str]:
    return [sys.executable, "-m", "ioh_testbed.inference.stub",
            "--service-ms", str(stub_cfg["service_ms"]), "--service-cv", str(stub_cfg["service_cv"]),
            "--workers", str(stub_cfg["workers"]), "--queue-max", str(stub_cfg["queue_max"]),
            "--seed", str(stub_cfg.get("seed", 0)), "--port", str(stub_cfg["port"])]


def submit_job(flink: Flink, jar: Path, args: list[str], parallelism: int) -> str:
    """Cancel anything running, wipe checkpoints, upload, run, wait until every
    vertex is RUNNING. Returns the job id."""
    cancelled = flink.cancel_all_running()
    if cancelled:
        log(f"cancelled stale jobs {cancelled}")
    wipe_checkpoints()
    jar_id = flink.upload(jar)
    job_id = flink.run(jar_id, args, parallelism if parallelism > 0 else None)
    log(f"job {job_id} submitted; waiting for RUNNING")
    flink.wait_running(job_id)
    return job_id
