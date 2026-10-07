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
replay harness ──► Kafka ──► Flink ──► inference stub ──► interface
   [sprint 1]    [sprint 1]  [sprint 2]    [sprint 2]       [sprint 2]
        └──────────────── end-to-end latency benchmark ────────────────┘
```

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

`laptop.env` holds the broker to 1.5 GB and 2 CPUs because the dev machine's
Docker VM is 3.83 GB. Other hosts get their own profile with the same keys.
Topics `dwc-waveform` and `dwc-numeric` are created by `kafka-init` with
`LogAppendTime` and `KAFKA_PARTITIONS` partitions; auto-creation is disabled.

Stop the broker when not in use; add `-v` to wipe its data, which is advisable
after calibration sweeps (they write gigabytes):

```bash
docker compose --env-file src/compose/profiles/laptop.env -f src/compose/docker-compose.yml down [-v]
```

### A run

```bash
uv run ioh-replay \
  --workload configs/workload/standard_anaesthesia.yaml \
  --scenario configs/scenarios/dwc_10s.yaml \
  --cases 5 --duration 300 --sink kafka
uv run python -m ioh_testbed.benchmark.stamp results/<run_id>    # provenance, verdict, percentiles
```

A run is one workload and one scenario. It prints a JSON summary and writes
`results/<run_id>/meta.json` (git commit and dirty flag, config SHA-256s, host
and Docker descriptor, schedule, verdict, lateness percentiles) and
`lateness_s.npy` (every lateness sample).

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
  packetizer, wire records, schedule, pacer, Kafka sink, CLI), `benchmark/`
  (run provenance). Git history is the source of truth; the pipeline is never
  forked into per-week copies.
- `src/compose/` — infrastructure, with explicit resource limits per host profile.
- `configs/` — `workload/` (sensor profiles, observation window) and
  `scenarios/` (packet cadence). A run is one of each.
- `docs/` — `stream-model.md` (production feed, VitalDB gap, bridge, test
  assertions) and `philips-data-egress.md` (every documented way to get data out
  of the Philips ecosystem, with citations).
- `tests/` — synthetic fixtures; `-m vitaldb` and `-m kafka` opt into cached data
  and a running broker.
- `sprints/NN-MM_topic/` — per-sprint deliverables (plan, findings, report,
  frozen configs, results). `NN-MM` is a thesis **week range**. Append-only,
  because `src/` is rewritten continuously: a week-9 result is unreproducible
  from week-14 `src/` unless the config that produced it is frozen alongside it.
- `proposal.txt` — the approved thesis proposal, verbatim text.
- `data/` — recorded data, gitignored. Never committed.

## Status

**Sprint 01-02 in progress.** Replay harness, Kafka, provenance and calibration
are built and tested; see `sprints/01-02_replay-and-skeleton/`. Flink job,
inference stub and interface sink are sprint 2.
