# Sprint 03-06 report: pipeline skeleton

Proposal weeks 3-6. Companion files: `plan.md` (frozen, as approved on
2026-10-07), `findings.md` (facts and measured numbers), `configs/` (frozen
inputs), `results/` (run artefacts with `meta.json`, `summary.json`, raw
jsonl and logs).

## What was built

The path downstream of Kafka, so the whole proposal architecture now exists
and is measured end to end.

| Component | Where | What it settles |
|---|---|---|
| Flink job | `src/flink/` (Java 17, Flink 2.2.1, Maven in a container) | DWC-shaped records keyed by case; per-case trailing 60 s window every 20 s of event time; incremental features; bounded-capacity async inference; progress heartbeats; predictions and progress back to Kafka with `LogAppendTime` |
| Inference stub | `inference/stub.py`, `ioh-stub` | the proposal's three inference variables: service time, variability, worker capacity; queue wait and service time in every response |
| Interface consumer | `interface/consumer.py`, `ioh-interface` | the prediction-receipt boundary: `t_receipt` and the prediction's broker append time, nothing computed online |
| Metric definitions | `benchmark/metrics.py` | T_pipeline, staleness, progress lag and the decomposition, as pure functions with the clock offset applied |
| Orchestrator | `benchmark/run.py`, `ioh-run` | one fully recorded run: clock probe, stub, job resubmission with fresh state, poller, consumer, harness, drain, logs, combined `meta.json`, analysis |
| Poller | `benchmark/poller.py` | 1 s samples of Flink vertex metrics (back-pressure, busy, pending records), checkpoints, container memory and CPU, stub queue |
| Analysis | `benchmark/analyze.py` | percentiles of every term, per-case progress-lag slope over the observation window, `summary.json` |
| Reference | `benchmark/reference.py`, `tests/test_e2e.py` | offline recomputation of every fired window from the same recordings and configuration; the correctness authority for the job |
| Harness fix | `replay/schedule.py` (H1) | wave packets emitted when complete; `_event_ts_last` carried |
| Infrastructure | `src/compose/` | Flink session cluster with explicit limits, in-cluster Kafka listener, output topics, checkpoint volume, Maven build service |

103 Python tests (3 opt-in markers), 8 Java tests in the Maven build.

## Definitions pinned

- **T_pipeline** `= t_receipt - tAppendNewest`: wall-clock from the ingress of
  the newest record contributing to a window to receipt of its prediction at
  the interface (processing-time latency of a window result, Karimov et al.
  2018).
- **Event-time staleness** `= t_receipt - tEventNewest`: age of the newest
  sample in the window at delivery.
- **Per-case progress lag** `= tWall - tEventNewestSeen`, sampled by a 1 s
  heartbeat per case before the window operator; its least-squares slope over
  the observation window is the Theodolite stability statistic. The slope
  tolerance is a parameter, to be calibrated in weeks 7-8.
- **Decomposition**: window wait (ingress to window firing, including the
  watermark wait), queue excess (window wait beyond `windowEnd + bound`),
  inference round trip, sink (Flink exit to broker append), fetch (broker to
  interface). The offset-free check `tPredAppend - tAppendNewest` must equal
  `T_pipeline - fetch`.
- **Clock domains**: harness and interface on the host, Kafka and Flink on the
  Docker VM. The offset is probed per run and subtracted from every
  cross-domain term.
- **Prediction** = trailing 60 s window, every 20 s, per case; the first full
  window of a case fires 60 s after its first event; earlier windows are
  flagged `partial` and excluded from the headline percentiles.
- **Latency floor by construction**: `T_pipeline >= bound + emission
  granularity + watermark interval`, here 0.5 s + up to 1 s + 50 ms.

## Results

RESULTS_PLACEHOLDER

## What sprint 03-06 leaves ready for weeks 7-8

- One command per run with every parameter recorded; a results folder that
  `analyze.py` turns into the headline metrics and the decomposition.
- The two experimental axes exposed as flags: `--cases` and the stub's
  `--stub-service-ms`, `--stub-cv`, `--stub-workers`, plus Flink's
  `--inference-capacity` and `--parallelism`.
- The reference check as a regression test for any change to the job.
- Per-case progress-lag slopes, ready for the stability tolerance to be
  calibrated against.

## Limitations recorded

- The stub sleeps; it does not burn CPU. A real model's contention with Flink
  on a shared host is not represented.
- The laptop's Docker VM is 5 GB; results here bound what the laptop can show,
  not what the Charité server will.
- Features are summary statistics chosen for verifiability, not clinical
  relevance; the window and cadence are HPI-like placeholders until the
  TRANSFER model fixes them.
- The watermark bound dominates the measured floor at the 10 s cadence with
  1 Hz numerics. That is a property of event-time windowing on this feed, and
  the report states it rather than tuning it away.
