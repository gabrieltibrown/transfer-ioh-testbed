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

RESULTS_PLACEHOLDER

## Still open, not answerable internally

Unchanged from sprint 01-02: DWC wave write cadence and bedside-to-row delay;
CDC or streaming subscription on DWC; Capsule MDIP licence; the `c_n_samples`
distribution in `te_wave`. New: the serving characteristics of the TRANSFER
model, which decide where on the inference-demand axis the real system sits.
