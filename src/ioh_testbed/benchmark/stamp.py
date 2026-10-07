"""Run provenance: everything needed to reproduce a number, written next to it.

CLAUDE.md requires every result to record the git commit and the config that
produced it. This adds the environment, because the same config on the laptop,
the Charite server and a cloud VM are different experiments, and the run's
adherence verdict, because a number from a run whose harness fell behind is not
a measurement.

Written per run to ``results/<run_id>/``:

    meta.json        provenance + environment + config + schedule + result summary
    lateness_s.npy   raw per-record harness lateness, so percentiles can be recomputed

    uv run python -m ioh_testbed.benchmark.stamp results/<run_id>   # print a run
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path

import numpy as np


def _run(cmd: list[str], timeout: float = 10.0) -> str | None:
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, check=False).stdout.strip() or None
    except (OSError, subprocess.TimeoutExpired):
        return None


def git_provenance(repo: Path | None = None) -> dict:
    cwd = str(repo) if repo else None
    def g(*a):
        try:
            return subprocess.run(["git", *a], cwd=cwd, capture_output=True, text=True, timeout=10, check=False).stdout.strip()
        except (OSError, subprocess.TimeoutExpired):
            return ""
    commit = g("rev-parse", "HEAD")
    return {
        "commit": commit or None,
        "describe": g("describe", "--tags", "--always", "--dirty") or None,
        "branch": g("rev-parse", "--abbrev-ref", "HEAD") or None,
        "dirty": bool(g("status", "--porcelain")),
    }


def file_digest(path: Path) -> dict:
    data = Path(path).read_bytes()
    return {"path": str(path), "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}


def _host_mem_bytes() -> int | None:
    if sys.platform == "darwin":
        v = _run(["sysctl", "-n", "hw.memsize"])
        return int(v) if v and v.isdigit() else None
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal:"):
                return int(line.split()[1]) * 1024
    except OSError:
        pass
    return None


def _docker() -> dict:
    if not shutil.which("docker"):
        return {"available": False}
    raw = _run(["docker", "info", "--format", "{{json .}}"], timeout=20)
    if not raw:
        return {"available": False}
    try:
        info = json.loads(raw)
    except json.JSONDecodeError:
        return {"available": False}
    out = {
        "available": True,
        "server_version": info.get("ServerVersion"),
        "vm_cpus": info.get("NCPU"),
        "vm_mem_bytes": info.get("MemTotal"),
        "containers": {},
    }
    names = _run(["docker", "ps", "--filter", "name=ioh-", "--format", "{{.Names}}"], timeout=20)
    for name in (names or "").split():
        insp = _run(["docker", "inspect", "--format",
                     "{{.Config.Image}}|{{.Image}}|{{.HostConfig.Memory}}|{{.HostConfig.NanoCpus}}", name], timeout=20)
        if insp:
            image, image_id, mem, nanocpus = insp.split("|")
            digest = _run(["docker", "image", "inspect", "--format", "{{index .RepoDigests 0}}", image], timeout=20)
            out["containers"][name] = {
                "image": image,
                "image_digest": digest,
                "image_id": image_id,
                "mem_limit_bytes": int(mem) if mem.isdigit() else None,
                "cpus": int(nanocpus) / 1e9 if nanocpus.isdigit() and int(nanocpus) else None,
            }
    return out


def environment_descriptor() -> dict:
    return {
        "hostname": platform.node(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "host_cpus": os.cpu_count(),
        "host_mem_bytes": _host_mem_bytes(),
        "python": sys.version.split()[0],
        "uv": _run(["uv", "--version"]),
        "docker": _docker(),
        # Harness and (sprint 2) sink run on one host in sprint 1, so all timestamps
        # share a clock. Cross-host runs must measure and record the offset here.
        "clock_domain": "single-host",
    }


def write_run(
    out_dir: Path,
    *,
    run_id: str,
    config_paths: list[Path],
    config: dict,
    schedule: dict,
    result: dict,
    lateness_s: np.ndarray,
    extra: dict | None = None,
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    meta = {
        "run_id": run_id,
        "git": git_provenance(),
        "config_files": [file_digest(p) for p in config_paths],
        "environment": environment_descriptor(),
        "config": config,
        "schedule": schedule,
        "result": result,
        **(extra or {}),
    }
    (out_dir / "meta.json").write_text(json.dumps(meta, indent=2, default=str) + "\n")
    np.save(out_dir / "lateness_s.npy", lateness_s.astype(np.float32))
    return out_dir / "meta.json"


def main(argv=None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    if len(args) != 1:
        print("usage: python -m ioh_testbed.benchmark.stamp results/<run_id>", file=sys.stderr)
        return 2
    d = Path(args[0])
    meta = json.loads((d / "meta.json").read_text())
    r = meta["result"]
    env = meta["environment"]
    print(f"run       {meta['run_id']}")
    print(f"commit    {meta['git']['describe']}  dirty={meta['git']['dirty']}")
    print("configs   " + ", ".join(f"{c['path']}@{c['sha256'][:10]}" for c in meta["config_files"]))
    print(f"cadence   packet_ms={meta['config']['packet_ms']}  scenario={meta['config']['scenario']}  "
          f"workload={meta['config']['workload']}  speed={meta['config']['speed']}")
    print(f"cases     {meta['schedule']['n_cases']} x {meta['schedule']['window_s']:g} s")
    print(f"host      {env['platform']}  cpus={env['host_cpus']}  mem={env['host_mem_bytes']}  "
          f"docker_vm_mem={env['docker'].get('vm_mem_bytes')}")
    lat = r["lateness_ms"]
    print(f"verdict   {r['verdict']}  latency_results_valid={r['latency_results_valid']}")
    print(f"lateness  N={lat['N']}  p50={lat['p50']} ms  p90={lat['p90']} ms  p99={lat['p99']} ms  max={lat['max']} ms")
    print(f"offered   {r['offered_records_per_s']} records/s  backpressure={r['n_backpressure']}  "
          f"undelivered={r['n_undelivered']}")
    lp = d / "lateness_s.npy"
    if lp.exists():
        arr = np.load(lp)
        print(f"raw       {arr.size} samples in {lp.name}; recomputed p99 = {np.percentile(arr, 99) * 1000:.3f} ms")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
