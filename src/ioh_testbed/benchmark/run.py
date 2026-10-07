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
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx
import yaml

from . import stamp
from .pipeline import (
    Flink,
    clock_probe,
    docker_logs,
    job_arguments,
    log,
    start,
    stop,
    stub_command,
    submit_job,
    wait_file,
    wait_http,
)


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
    ap.add_argument("--idleness-ms", type=int, default=None, help="override pipeline.flink.idleness_ms")
    ap.add_argument("--watermark-bound-ms", type=int, default=None, help="override pipeline.flink.watermark_bound_ms")
    ap.add_argument("--inference-timeout-ms", type=int, default=None, help="override pipeline.flink.inference_timeout_ms")
    ap.add_argument("--drain", type=float, default=None, help="seconds to wait after the replay (default: pipeline.run.drain_s)")
    ap.add_argument("--label", default="", help="free-text note stored in meta.json")
    return ap.parse_args(argv)


def new_run_id() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + uuid.uuid4().hex[:6]


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
    idleness = args.idleness_ms if args.idleness_ms is not None else fl["idleness_ms"]
    bound = args.watermark_bound_ms if args.watermark_bound_ms is not None else fl["watermark_bound_ms"]
    inf_timeout = args.inference_timeout_ms if args.inference_timeout_ms is not None else fl["inference_timeout_ms"]
    drain = args.drain if args.drain is not None else float(rn.get("drain_s", 30))
    job_args, job_cfg = job_arguments(
        fl, run_id=run_id, capacity=capacity, parallelism=parallelism, idleness_ms=idleness,
        bound_ms=bound, inference_timeout_ms=inf_timeout,
    )

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
        stub_proc = start(stub_command(stub_cfg), logs / "stub.log")
        wait_http(f"http://localhost:{stub_cfg['port']}/healthz")

        job_id = submit_job(flink, Path(fl["jar"]), job_args, parallelism)
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
