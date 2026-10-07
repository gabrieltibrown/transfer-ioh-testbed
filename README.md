# Operating Under Pressure

**Scaling a Stream-Processing Pipeline for Real-Time Intraoperative Hypotension
Prediction**

Gabriel Brown — M.Sc. Data Science, Freie Universität Berlin
TRANSFER project, Charité – Universitätsmedizin Berlin

## Summary

An instrumented replay-based testbed that measures the end-to-end latency and
scaling behaviour of a real-time intraoperative hypotension (IOH) prediction
pipeline. Recorded perioperative data is replayed as a live monitor feed through
a Kafka/Flink path to a configurable inference stub and a clinician-facing
interface, while a benchmark wrapped around the pipeline records where delay
accumulates. The goal is to locate the architecture's operating boundary across
two dimensions — concurrent surgical cases and inference demand — and to
interpret the measured latency against the clinical warning-time budget that a
prediction must fit inside to be actionable. See `proposal.txt` for the research
questions and the full evaluation plan.

## Architecture

```
replay harness ──► Kafka ──► Flink job ──► inference stub ──► interface consumer
  ioh-replay     compose    src/flink      ioh-stub           ioh-interface
        └──────────── ioh-run: clock probe, poller, meta.json, analysis ────────────┘
```

All five stages exist as of sprint 03-06. The Flink job keys by case, computes a
trailing 60 s feature window every 20 s of event time, calls the inference stub
asynchronously with bounded capacity, and writes predictions and per-case
progress heartbeats back to Kafka. The interface consumer stamps receipt time;
`ioh-run` orchestrates one fully recorded run and `benchmark/analyze.py` turns it
into the three headline metrics.

The replay harness is **open-loop**: it emits every record at an absolute
deadline regardless of how the pipeline behaves, so pipeline slowdown shows up
as queueing delay rather than being hidden by a throttled source. Kafka's
`LogAppendTime` is the testbed's ingress timestamp. Cases are keyed by surgical
case so concurrent operations stay separately observable.

The feed is modelled on the Philips PIC iX / Data Warehouse Connect path
(`docs/stream-model.md`), replaying public VitalDB recordings re-packetised to
the production cadence. Cadence is a required parameter of every run because it
is the single most uncertain property of the real feed.

## Running the testbed

All commands run from the repo root. Python dependencies are managed with `uv`;
infrastructure runs in Docker Desktop.

### One-time setup

```bash
uv sync                                                        # .venv from uv.lock
uv run python src/ioh_testbed/replay/fetch_vitaldb.py -n 50    # ~0.9 GB into data/ (gitignored)
uv run pytest                                                  # no data or broker needed
uv run pytest -m vitaldb                                       # reader against the cached cases
```

The fetch is idempotent and skips files already present. It selects cases on
Parquet content, not the VitalDB catalogue, and writes
`src/ioh_testbed/replay/manifest.json` (committed), which fixes exactly which
cases back every run.

### Infrastructure

```bash
docker compose --env-file src/compose/profiles/laptop.env -f src/compose/docker-compose.yml up -d --wait
docker ps                 # ioh-kafka running; ioh-kafka-init exits after creating the topics
uv run pytest -m kafka    # round trip against the broker
```

`laptop.env` holds Kafka to 1.5 GB, the Flink JobManager to 1 GB and the
TaskManager to 2 GB; Docker Desktop's VM must be set to at least 5 GB
(Settings > Resources > Memory). Other hosts get their own profile with the same
keys. Topics `dwc-waveform`, `dwc-numeric`, `predictions`, `progress` and
`clock-probe` are created by `kafka-init` with `LogAppendTime` and
`KAFKA_PARTITIONS` partitions; auto-creation is disabled. The Flink UI is at
http://localhost:8081.

The Flink job is built in a Maven container (no local JVM needed); the jar lands
in `src/flink/target/` and `ioh-run` uploads it through the REST API:

```bash
docker compose --env-file src/compose/profiles/laptop.env -f src/compose/docker-compose.yml --profile build run --rm flink-build
```

Stop the broker when not in use; add `-v` to wipe its data, which is advisable
after calibration sweeps (they write gigabytes):

```bash
docker compose --env-file src/compose/profiles/laptop.env -f src/compose/docker-compose.yml down [-v]
```

### A pipeline run

```bash
uv run ioh-run \
  --workload configs/workload/standard_anaesthesia.yaml \
  --scenario configs/scenarios/dwc_10s.yaml \
  --pipeline configs/pipeline/laptop.yaml \
  --cases 5 --duration 600 [--stub-service-ms 200 --stub-workers 2 --inference-capacity 8]
uv run python -m ioh_testbed.benchmark.analyze results/<run_id>   # re-print or re-run the analysis
```

A run is one workload, one scenario and one pipeline config. `ioh-run` probes
the host-to-broker clock offset, starts the inference stub, resubmits the Flink
job with this run's parameters (fresh state, fresh consumer group), starts the
resource poller and the interface consumer, runs the replay harness open-loop,
drains, stops everything, cancels the job, collects container logs and writes
`results/<run_id>/`:

| File | Content |
|---|---|
| `meta.json` | harness provenance and verdict, job config and id, stub config and final stats, clock offsets, image digests |
| `predictions.jsonl` | one line per fired window after inference, with receipt time and broker append time |
| `progress.jsonl` | per-case heartbeats (newest event time seen, watermark, records seen) |
| `poller.jsonl` | 1 s samples: Flink vertex metrics, checkpoints, container memory and CPU, stub queue |
| `summary.json` | the analysis: percentiles of every latency term, per-case progress-lag slopes |
| `logs/` | harness, stub, interface, poller, JobManager and TaskManager logs |

Metric definitions are in `src/ioh_testbed/benchmark/metrics.py` and must not
drift: `T_pipeline = t_receipt - tAppendNewest`, `staleness = t_receipt -
tEventNewest`, `progress_lag = tWall - tEventNewestSeen`, each with the measured
clock offset removed. The latency floor includes the watermark bound by
construction: `T_pipeline >= bound + emission granularity + watermark interval`.

### A harness-only run

```bash
uv run ioh-replay \
  --workload configs/workload/standard_anaesthesia.yaml \
  --scenario configs/scenarios/dwc_10s.yaml \
  --cases 5 --duration 300 --sink kafka
uv run python -m ioh_testbed.benchmark.stamp results/<run_id>    # provenance, verdict, percentiles
```

Offers load into Kafka without the pipeline, for harness calibration and broker
checks. It writes `results/<run_id>/meta.json` (git commit and dirty flag, config
SHA-256s, host and Docker descriptor, schedule, verdict, lateness percentiles)
and `lateness_s.npy` (every lateness sample).

| Flag | Meaning |
|---|---|
| `--cases N` | concurrent cases, all starting at `t0`; the concurrency axis. Cases shorter than the window are ineligible |
| `--duration S` | observation window in seconds (workload default 1800) |
| `--scenario` | packet cadence; `dwc_10s.yaml` is the baseline, `dei_256ms.yaml` the sensitivity run. Required, no default |
| `--workload` | sensor profile mix and assignment rule; `standard_anaesthesia.yaml` or `mixed.yaml` |
| `--sink null\|kafka` | `null` dry-runs the schedule without a broker |
| `--speed X` | time compression. Anything but 1 sets `latency_results_valid: false`; smoke tests and calibration only |
| `--partitions N` | expected topic partition count, checked against the broker |
| `--env`, `--source` | data governance; `--env cloud --source dwc` is refused |
| `--tolerance-ms`, `--invalid-ms` | verdict thresholds; defaults are 10% and 100% of `packet_ms` |
| `--spin-ms` | busy-wait window before each deadline (default 2); 0 disables |

Exit codes: 0 for `OK` or `DEGRADED`, 2 for `INVALID`, 3 for a policy refusal.

### Harness calibration

```bash
uv run ioh-replay --workload configs/workload/standard_anaesthesia.yaml \
  --scenario configs/scenarios/dwc_10s.yaml --cases 10 --sink kafka \
  --calibrate --calibrate-wall 20
```

Sweeps `--speed` upward at a fixed wall time per step and stops at the first
step whose verdict leaves `OK`; the ceiling is the last `OK` step. Output is
`results/<run_id>/calibration.json`. Run it per sink and per scenario, and rerun
whenever the host changes: the ceiling is a property of the machine and bounds
what any experiment on it may claim.

### Recording a result

Copy the run folder into the current sprint's `results/` and the configs it
used into the sprint's `configs/`, as in `sprints/01-02_replay-and-skeleton/`.
With the sprint tag and the SHA-256s in `meta.json`, the number is reproducible.
`results/` at the repo root is gitignored scratch.

### Rules for valid numbers

- Measure serially, with nothing else heavy on the host. A concurrent sweep was
  enough to make the Kafka round-trip test fail once.
- Check `verdict` and `latency_results_valid` before using a result. `DEGRADED`
  means the harness itself was late by more than 10% of the cadence; `INVALID`
  means a record was refused or lateness exceeded one cadence.
- Never `git add` anything under `data/`. `git check-ignore -v data/vitaldb/1.parquet`
  must print a match.

## Repo layout

- `src/ioh_testbed/` — the single evolving codebase. `replay/` (reader,
  packetizer, wire records, schedule, pacer, Kafka sink, CLI), `inference/`
  (the configurable stub), `interface/` (the minimal consumer), `benchmark/`
  (metrics, orchestrator, poller, analysis, offline reference, provenance). Git
  history is the source of truth; the pipeline is never forked into per-week
  copies.
- `src/flink/` — the Flink job (Java 17, Maven), built in a container.
- `src/compose/` — infrastructure, with explicit resource limits per host profile.
- `configs/` — `workload/` (sensor profiles, observation window), `scenarios/`
  (packet cadence) and `pipeline/` (window, watermark bound, inference capacity,
  stub defaults). A run is one of each.
- `docs/` — `stream-model.md` (production feed, VitalDB gap, bridge, test
  assertions) and `philips-data-egress.md` (every documented way to get data out
  of the Philips ecosystem, with citations).
- `tests/` — synthetic fixtures; `-m vitaldb`, `-m kafka` and `-m e2e` opt into
  cached data, a running broker, and a finished run folder respectively. Java
  unit tests run inside the Maven build.
- `sprints/NN-MM_topic/` — per-sprint deliverables (plan, findings, report,
  frozen configs, results). `NN-MM` is a thesis **week range**. Append-only,
  because `src/` is rewritten continuously: a week-9 result is unreproducible
  from week-14 `src/` unless the config that produced it is frozen alongside it.
- `proposal.txt` — the approved thesis proposal, verbatim text.
- `data/` — recorded data, gitignored. Never committed.

## Status

**Sprint 03-06 in progress.** The full path replay, Kafka, Flink, inference
stub, interface exists and is measured end to end; see
`sprints/03-06_pipeline-skeleton/`. Stability-criterion calibration and the
concurrency and inference sweeps are weeks 7-12.
