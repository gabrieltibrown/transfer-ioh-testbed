# Sprint 03-06 findings

Facts established during the sprint that are not derivable from the code.
Measured 2026-10-07 on the development laptop unless stated.

## Environment

| | |
|---|---|
| Host | macOS, 8 cores, 8 GB RAM (unchanged) |
| Docker Desktop VM | raised from 3.83 GB to **5 GB** (5,157,793,792 bytes reported) on 2026-10-07 |
| Container limits | Kafka 1.5 GB, JobManager 1 GB, TaskManager 2 GB: 4.5 GB of 5 GB |
| Flink | **2.2.1**, `flink:2.2.1-java17`, digest `sha256:140c3909f06cbea741be78fc630e809fae9dadcbfa492a684acf39efbbf68946` |
| Kafka connector | `flink-connector-kafka` **5.0.0-2.2** (the only 5.x line; 4.0.x pairs with Flink 2.0 only) |
| Build | `maven:3.9-eclipse-temurin-17`, digest `sha256:1a352420f7aba21f5ad08df31bab55f74c013fb491f1ae8ab1dd7ff9ed698584`; Java 17 target; no JVM on the host |
| Kafka | unchanged, `apache/kafka:4.0.0` |

Idle memory with the job not running (`docker stats`): TaskManager 515 MB,
JobManager 400 MB, Kafka 553 MB. Loaded figures are in the results below.

## Flink 2.x facts that cost time

Recorded so nobody rediscovers them.

- `RichFunction.open(Configuration)` no longer overrides anything; the signature
  is `open(OpenContext)`.
- `DeliveryGuarantee` lives in `flink-connector-base`, which is a separate
  provided-scope artifact, not part of `flink-streaming-java`.
- `KafkaRecordSerializationSchema.builder()` with lambda serializers fails at
  submission with "types of the interface SerializationSchema could not be
  inferred": the 2.2 builder wraps the value schema for lineage facets and runs
  the type extractor on it. Concrete serializer classes work.
- The checkpoint directory on a named volume is root-owned; Flink runs as uid
  9999 and the job fails at submission with "Failed to create directory for
  shared state". A one-shot `chown` service fixes it (`flink-init` in compose).
- `execution.checkpointing.dir` is the 2.x key (was `state.checkpoints.dir`).
- Flink commits Kafka consumer offsets only at checkpoints, so consumer-group
  lag from the admin API moves in checkpoint-interval steps. The source's
  `pendingRecords` metric is the live lag.

## Harness correction H1

Sprint 1 emitted a wave packet at its first-sample time. A monitor can only
export a packet once its last sample exists, so emission is now at the end of
the packet span (`t0 + packet_ms`), with `_event_ts` still the first sample and
a new `_event_ts_last`. Consequence for the window crop: the last packet of an
observation window is the one that completes inside it, so a 300 s window at
10 s cadence yields 29 wave packets per channel, not 30. The event-time
staleness floor for waves is therefore 0 at emission for the newest sample and
`packet_ms` for the oldest, as it would be on the real feed.

## Clock domains

The harness and the interface consumer run on the host; Kafka and Flink on the
Docker VM. The probe (20 tiny messages, `LogAppendTime - t_produce`, minimum
taken) measured the VM clock **1.2 to 3.4 ms behind** the host across runs on
2026-10-07. The analysis subtracts the measured offset from every cross-domain
term; the broker-only latency `tPredAppend - tAppendNewest` agreed with
`T_pipeline - fetch` to within 0.01 ms in every run, which validates the probe.
Docker Desktop's VM clock is known to drift after macOS sleep: restart Docker if
the probe reports more than a few ms.

## Latency floor, by construction

With a watermark bound of 500 ms, 1 Hz numerics and a 50 ms watermark interval,
a window ending at `E` cannot fire before a record with event time at least
`E + 500 ms` has been processed. Numerics are the densest event-time source, so
that record arrives at the first whole second at or after `E + 0.5 s`: between
0.5 and 1.5 s after `E`, mean about 1.0 s. The smoke run measured
`window_wait` p50 1.18 s and `queue_excess` (above `E + bound`) p50 0.52 s,
matching the arithmetic. **About 1.2 s of the measured 1.27 s T_pipeline is the
watermark mechanism, not processing.** RQ1's floor must be reported with the
bound and the source granularity stated, and the bound is a parameter in
`configs/pipeline/`.

## Results

Filled from `results/` (see the per-run `summary.json` and `meta.json`).

All runs: `standard_anaesthesia` workload, `dwc_10s` scenario, `laptop` pipeline
config (60 s window, 20 s slide, bound 500 ms, capacity 8 x parallelism 2 = 16,
checkpoints every 10 s), Flink 2.2.1, measured serially on 2026-10-07. Harness
verdict was `OK` in every run (p99 lateness 10 to 22 ms against a 10 s cadence),
so the offered load was delivered as configured. The first five runs were made
at commits `5a97ecb`/`ae50eea` with untracked documentation in the tree (the
`dirty` flag); no pipeline code differed from the committed state. Artefacts are
in `results/<run_id>/`; `results/tabulate.py` regenerates the tables.

### The six runs

| Run | Cases | Stub | Idleness | Operator timeout | Predictions full ok / total (status) | T_pipeline p50 / p90 / p99 (s) | Staleness p50 (s) | Inference p50 / p99 (s) | Lag slope max (s/s) |
|---|---|---|---|---|---|---|---|---|---|
| `3ea09b` correctness, 600 s | 1 | 50 ms x4 | 5 s | 10 s | 27 / 30 (ok 30) | 1.23 / 1.29 / 1.33 | 1.24 | 0.074 / 0.090 | -0.0001 |
| `3dc337` baseline, 600 s | 5 | 50 ms x4 | 5 s | 10 s | 135 / 150 (ok 150) | 4.98 / 5.32 / 5.37 | 4.98 | 0.072 / 0.130 | +0.0000 |
| `f40e9e` idleness 1 s, 300 s | 5 | 50 ms x4 | **1 s** | 10 s | 60 / 75 (ok 75) | **2.25 / 2.61 / 2.66** | 2.26 | 0.079 / 0.137 | -0.0000 |
| `389b67` async stable, 600 s | 20 | 200 ms x2 | 5 s | 10 s | 537 / 600 (ok 597, error 3) | 7.34 / 8.33 / 8.65 | 7.39 | 1.05 / 1.69 | +0.0010 |
| `7cda9f` async saturated, shedding, 600 s | 20 | 2000 ms x1 | 5 s | 10 s | 0 / 600 (ok 4, timeout 400, error 196) | n/a | n/a | n/a | +0.0027 |
| `b239c1` async saturated, no shedding, 600 s | 20 | 2000 ms x1 | 5 s | **900 s** | 247 / 307 (ok 307) | **200 / 299 / 326** | 200 | **28.0 / 32.0** | **+0.33** |

Decomposition of T_pipeline (p50, seconds) and resources:

| Run | Window wait | Queue excess | Inference | Stub queue wait p99 | Sink | Fetch | Source back-pressure max (ms/s) | Peak mem Kafka / JM / TM (MB) | Peak TM CPU (%) |
|---|---|---|---|---|---|---|---|---|---|
| `3ea09b` | 1.15 | 0.33 | 0.074 | 0.001 | 0.004 | 0.002 | 0 | 934 / 696 / 1074 | 354 |
| `3dc337` | 4.89 | 3.91 | 0.072 | 0.054 | 0.003 | 0.002 | 0 | 962 / 688 / 1231 | 59 |
| `f40e9e` | 2.16 | 1.14 | 0.079 | 0.055 | 0.003 | 0.002 | 0 | 994 / 721 / 1486 | 49 |
| `389b67` | 6.21 | 5.28 | 1.05 | 1.42 | 0.001 | 0.001 | 0 | 999 / 703 / 1435 | 73 |
| `7cda9f` | n/a | n/a | n/a | n/a | n/a | n/a | 0 (poller metrics unavailable, see below) | 1016 / 735 / 1472 | 106 |
| `b239c1` | 168 | 167 | 28.0 | 30.0 | 0.003 | 0.002 | **1000** | 1027 / 740 / **1658** | 331 |

The clock check (`T_pipeline - fetch` against the broker-only latency) agreed
to 0.00 ms in every run. Sink and fetch together are under 10 ms everywhere:
the Kafka hop out of Flink and the consumer are not where time goes.

### Correctness

Run `3ea09b` (1 case, 600 s) fired 30 windows; `tests/test_e2e.py` matched every
one against the offline reference: same window set, same record counts, same
per-label counts and statistics within 1e-6, same partial flags, every record
stamped with `LogAppendTime`. The 180 s smoke run before it passed the same
check. The job's windowing and features are therefore verified against an
independent implementation, which is the proposal's week 7-8 "reference
implementation" requirement brought forward.

### The latency floor, and what dominates it

At one case, T_pipeline p50 is 1.23 s, of which 1.15 s is window wait: the
watermark bound (0.5 s) plus the wait for the next 1 Hz numeric record that
carries event time past `windowEnd + bound` (0.33 s at p50 here). Inference,
sink and fetch add about 80 ms. **The floor is a property of event-time
windowing on this feed, not of processing cost.**

At five and twenty cases the window wait rose to 4.9 s and 6.2 s with no
back-pressure, no queueing at the stub and TaskManager CPU under 75%. The cause
is the waveform stream: packets of all channels of a case are emitted together
every 10 s, so a waveform partition's watermark advances once per burst and is
then held for the idleness period (5 s) before the source stops counting it.
Any window whose end falls inside that 5 s waits for it. Which windows do
depends on where each case's packet grid lands relative to the 20 s epoch grid:
run `3ea09b` happened to land in the idle gap (queue excess 0.33 s), the 5- and
20-case runs did not (3.9 s and 5.3 s). Run `f40e9e` repeated the 5-case
baseline with idleness 1 s: queue excess fell to 1.14 s and T_pipeline p50 to
2.25 s, confirming the mechanism. The pipeline default is now 1 s. The term
belongs in the floor relation:

    T_pipeline >= bound + numeric granularity + min(idleness, wave packet period) + watermark interval

and it is a direct consequence of the DWC-style 10 s packet cadence: with
256 ms packets it would be negligible. This is the first measured interaction
between the source's cadence (stream-model 1.2) and pipeline latency.

### Inference demand

**Stable** (`389b67`: 20 cases, mean demand 1 prediction/s, stub capacity
10/s): all windows fired, inference p50 1.05 s rather than 0.2 s because the
20 cases' windows fire in bursts (shared t0, shared epoch grid) and the stub's
two workers drain a burst of 20 in about 2 s. Progress-lag slope stayed below
0.001 s/s. Three of 600 calls failed with an HTTP connect timeout under the
burst, which the analysis counts and excludes.

**Saturated, shedding** (`7cda9f`: capacity 0.5/s against demand 1/s, operator
timeout 10 s): the async operator timed out 400 windows and 196 failed with an
HTTP request timeout; 4 succeeded. Flink never filled its 16 slots because
each entry left after 10 s; the operator shed load instead of back-pressuring,
the stub's queue grew by 0.5/s to 304 (the abandoned requests were still
queued there; the job now cancels the HTTP call on operator timeout) and
progress lag stayed almost flat. A timeout shorter than the queueing delay
turns saturation into prediction loss, not latency; that is a design choice
the thesis must state.

**Saturated, no shedding** (`b239c1`: operator timeout 900 s): the operator
filled to 16 in flight within two minutes, the source was back-pressured at
1000 ms/s for the rest of the run (busy time of the features vertex also at
1000 ms/s), inference round trips settled at 28 s (16 in flight at 2 s each),
only 307 of 600 windows fired, T_pipeline p50 reached 200 s and progress lag
grew at 0.22 s/s (median case) to 0.33 s/s (worst). Final per-case lags
clustered at 123 s, 163 s and 199 s: cases keyed to different partitions and
subtasks fell behind by different amounts under the same offered load, which is
exactly the cross-case interference the per-case progress lag exists to
expose and aggregate latency cannot. TaskManager memory peaked at 1.66 GB of
its 2 GB limit under back-pressure.

### Memory on the 5 GB VM

Peak container memory across the runs: Kafka 1.03 GB (limit 1.5), JobManager
740 MB (limit 1), TaskManager 1.66 GB (limit 2, reached only under sustained
back-pressure). The stack fits with about 0.5 GB of VM headroom; 20 concurrent
cases at the 10 s cadence are not a memory problem on the laptop.

### Instrument notes

- The poller's Flink vertex metrics were empty in the first five runs: the REST
  endpoint returns nothing if any requested metric name is unknown, and
  `pendingRecords` is not exposed by this source. Fixed before `b239c1` (metric
  names discovered per vertex; Kafka lag from `records-lag-max`). Container
  memory, CPU and stub statistics were collected in every run.
- The harness reconciled in every run: records emitted equal records delivered,
  zero refused.
- Runs are start-time sensitive through the epoch-aligned windows and the shared
  case start. For the sweeps, either randomise case start offsets within the
  packet period or report the alignment; the former is a one-line harness change.

## Still open, not answerable internally

Unchanged from sprint 01-02: DWC wave write cadence and bedside-to-row delay;
CDC or streaming subscription on DWC; Capsule MDIP licence; the `c_n_samples`
distribution in `te_wave`. New: the serving characteristics of the TRANSFER
model, which decide where on the inference-demand axis the real system sits.
