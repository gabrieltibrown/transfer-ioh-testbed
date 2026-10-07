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

## Quickstart

```bash
uv sync
uv run python src/ioh_testbed/replay/fetch_vitaldb.py -n 50       # ~0.9 GB into data/ (gitignored)
uv run pytest                                                     # no data or broker needed
docker compose --env-file src/compose/profiles/laptop.env -f src/compose/docker-compose.yml up -d --wait
uv run ioh-replay --workload configs/workload/standard_anaesthesia.yaml \
    --scenario configs/scenarios/dwc_10s.yaml --cases 5 --duration 300 --sink kafka
uv run python -m ioh_testbed.benchmark.stamp results/<run_id>    # provenance + verdict
uv run ioh-replay ... --calibrate --sink null                     # this host's harness ceiling
```

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
