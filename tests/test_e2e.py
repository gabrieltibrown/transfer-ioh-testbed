"""End-to-end correctness: the Flink job's fired windows for a completed run must
match the offline reference built from the same recordings and configuration.

    IOH_RUN_DIR=results/<run_id> uv run pytest -m e2e

Needs a finished ``ioh-run`` folder (predictions.jsonl, meta.json) and the cached
VitalDB data the run used. Skipped otherwise.
"""

import json
import os
from pathlib import Path

import pytest

from ioh_testbed.benchmark.reference import compare, expected_fired, reference_windows
from ioh_testbed.replay.config import RunConfig, Scenario, Workload
from ioh_testbed.replay.reader import VitalDBSource
from ioh_testbed.replay.schedule import build_schedule
from tests.conftest import REPO

pytestmark = pytest.mark.e2e


@pytest.fixture(scope="module")
def run_dir() -> Path:
    env = os.environ.get("IOH_RUN_DIR")
    if env:
        d = Path(env)
    else:
        runs = sorted((REPO / "results").glob("*/predictions.jsonl"))
        if not runs:
            pytest.skip("no run folder with predictions.jsonl; set IOH_RUN_DIR")
        d = runs[-1].parent
    if not (d / "meta.json").exists() or not (d / "predictions.jsonl").exists():
        pytest.skip(f"{d} is not a finished ioh-run folder")
    return d


def test_fired_windows_match_reference(run_dir):
    meta = json.loads((run_dir / "meta.json").read_text())
    job = meta["job"]["config"]
    paths = [REPO / c["path"] for c in meta["config_files"]]
    workload = Workload.load(paths[0])
    scenario = Scenario.load(paths[1])
    cfg = RunConfig(workload, scenario)
    if meta["schedule"]["window_s"] != workload.observation_window_s:
        workload = Workload(**{**workload.__dict__, "observation_window_s": meta["schedule"]["window_s"]})
        cfg = RunConfig(workload, scenario)
    manifest = json.loads((REPO / "src/ioh_testbed/replay/manifest.json").read_text())["cases"]
    data_dir = REPO / "data/vitaldb"
    if not data_dir.exists():
        pytest.skip("cached VitalDB data not present")
    sched = build_schedule(cfg, VitalDBSource(data_dir), manifest, meta["schedule"]["n_cases"])
    t0_wall = float(meta["result"]["t0_wall"])
    assert sched.speed == 1.0

    ref = reference_windows(sched, t0_wall, int(job["window_ms"]), int(job["slide_ms"]))
    fired = expected_fired(ref, int(job["watermark_bound_ms"]))

    preds = [json.loads(line) for line in (run_dir / "predictions.jsonl").read_text().splitlines() if line.strip()]
    got = {(p["caseId"], p["windowStartMs"], p["windowEndMs"]): p for p in preds}
    assert got, "no predictions in the run"
    unexpected = sorted(set(got) - set(ref))
    assert not unexpected, f"windows fired that no reference window explains: {unexpected[:5]}"
    missing = sorted(set(fired) - set(got))
    assert not missing, f"{len(missing)} reference windows should have fired but did not: {missing[:5]}"

    mismatches = {k: errs for k, p in got.items() if (errs := compare(p, ref[k]))}
    assert not mismatches, json.dumps({str(k): v for k, v in list(mismatches.items())[:5]}, indent=1)
    assert all(p["appendTimeAuthoritative"] for p in preds)
    assert all(p["inference"]["status"] == "ok" for p in preds), {p["inference"]["status"] for p in preds}
