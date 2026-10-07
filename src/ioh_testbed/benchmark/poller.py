"""Resource poller: one JSON line per second with Flink vertex metrics, checkpoint
counters, container memory and CPU from the Docker API, and the inference stub's
queue state. The proposal asks for Kafka/Flink lag and utilisation to localise
bottlenecks; this is the collector, ``analyze.py`` summarises it.

Kafka lag is read as the source's ``pendingRecords`` metric rather than from
consumer-group offsets, which Flink commits only at checkpoints.
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from pathlib import Path

import httpx

VERTEX_METRICS = (
    "backPressuredTimeMsPerSecond", "busyTimeMsPerSecond", "idleTimeMsPerSecond",
    "numRecordsInPerSecond", "numRecordsOutPerSecond", "pendingRecords",
    "currentInputWatermark", "numLateRecordsDropped",
)
CONTAINERS = ("ioh-kafka", "ioh-flink-jm", "ioh-flink-tm")


class FlinkMetrics:
    def __init__(self, rest: str, job_id: str):
        self.rest = rest.rstrip("/")
        self.job_id = job_id
        self.client = httpx.Client(timeout=3.0)
        self.vertices: list[tuple[str, str]] = []

    def discover(self) -> None:
        r = self.client.get(f"{self.rest}/jobs/{self.job_id}")
        r.raise_for_status()
        self.vertices = [(v["id"], v["name"]) for v in r.json().get("vertices", [])]

    def sample(self) -> dict:
        if not self.vertices:
            self.discover()
        out = {"vertices": []}
        for vid, name in self.vertices:
            try:
                r = self.client.get(
                    f"{self.rest}/jobs/{self.job_id}/vertices/{vid}/subtasks/metrics",
                    params={"get": ",".join(VERTEX_METRICS), "agg": "max,sum"},
                )
                row = {"id": vid, "name": name}
                for m in r.json():
                    row[m["id"]] = m.get("max")
                    if m["id"] in ("numRecordsInPerSecond", "numRecordsOutPerSecond", "pendingRecords", "numLateRecordsDropped"):
                        row[m["id"]] = m.get("sum")
                out["vertices"].append(row)
            except Exception as e:  # noqa: BLE001
                out["vertices"].append({"id": vid, "name": name, "error": str(e)})
        try:
            c = self.client.get(f"{self.rest}/jobs/{self.job_id}/checkpoints").json()
            counts = c.get("counts", {})
            latest = (c.get("latest") or {}).get("completed") or {}
            out["checkpoints"] = {"completed": counts.get("completed"), "failed": counts.get("failed"),
                                  "in_progress": counts.get("in_progress"),
                                  "latest_duration_ms": latest.get("end_to_end_duration"),
                                  "latest_size_bytes": latest.get("state_size")}
        except Exception as e:  # noqa: BLE001
            out["checkpoints"] = {"error": str(e)}
        return out


class DockerStats:
    """One-shot container stats over the Docker socket; CPU percent from our own deltas."""

    def __init__(self, socket: str = "/var/run/docker.sock"):
        self.client = httpx.Client(transport=httpx.HTTPTransport(uds=socket), base_url="http://docker", timeout=3.0)
        self.prev: dict[str, tuple[float, float]] = {}

    def sample(self) -> dict:
        out = {}
        for name in CONTAINERS:
            try:
                s = self.client.get(f"/containers/{name}/stats", params={"stream": "false", "one-shot": "true"}).json()
                mem = s.get("memory_stats", {})
                usage = float(mem.get("usage", 0)) - float(mem.get("stats", {}).get("inactive_file", 0))
                cpu = s.get("cpu_stats", {})
                total = float(cpu.get("cpu_usage", {}).get("total_usage", 0))
                system = float(cpu.get("system_cpu_usage", 0))
                ncpu = float(cpu.get("online_cpus", 1) or 1)
                pct = None
                if name in self.prev:
                    pt, ps = self.prev[name]
                    if system > ps:
                        pct = (total - pt) / (system - ps) * ncpu * 100.0
                self.prev[name] = (total, system)
                out[name] = {"mem_bytes": usage, "mem_limit": mem.get("limit"), "cpu_pct": pct}
            except Exception as e:  # noqa: BLE001
                out[name] = {"error": str(e)}
        return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--flink-rest", default="http://localhost:8081")
    ap.add_argument("--job-id", required=True)
    ap.add_argument("--stub-url", default="http://localhost:8000")
    ap.add_argument("--interval", type=float, default=1.0)
    ap.add_argument("--docker-socket", default="/var/run/docker.sock")
    args = ap.parse_args(argv)

    stop = {"flag": False}
    signal.signal(signal.SIGTERM, lambda *_: stop.__setitem__("flag", True))
    signal.signal(signal.SIGINT, lambda *_: stop.__setitem__("flag", True))

    flink = FlinkMetrics(args.flink_rest, args.job_id)
    docker = DockerStats(args.docker_socket)
    stub = httpx.Client(timeout=1.0)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with args.out.open("a") as f:
        next_t = time.time()
        while not stop["flag"]:
            row = {"t": time.time()}
            try:
                row["flink"] = flink.sample()
            except Exception as e:  # noqa: BLE001
                row["flink"] = {"error": str(e)}
            row["docker"] = docker.sample()
            try:
                row["stub"] = stub.get(f"{args.stub_url}/stats").json()
            except Exception as e:  # noqa: BLE001
                row["stub"] = {"error": str(e)}
            f.write(json.dumps(row, separators=(",", ":")) + "\n")
            f.flush()
            n += 1
            next_t += args.interval
            delay = next_t - time.time()
            if delay > 0:
                time.sleep(delay)
            else:
                next_t = time.time()
    print(f"[poller] {n} samples", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
