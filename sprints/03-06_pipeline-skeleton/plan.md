# Sprint 03-06 plan: pipeline skeleton (Flink, inference stub, interface, benchmark)

## Context

Sprint 01-02 (merged, tag `sprint/01-02`) delivered the replay harness, Kafka
with `LogAppendTime` ingress, provenance stamping and the harness ceilings. The
pipeline downstream of Kafka does not exist. Proposal weeks 3-6 are "build and
instrument the Kafka/Flink, inference and interface path". The outcome of this
sprint is a walking skeleton that produces the three headline metrics
(pipeline latency, event-time staleness, per-case progress lag) for a real
replay, verified against a reference implementation on one case, with the
inference stub's async path shown to saturate and back-pressure as designed.
Experiments themselves are weeks 7-12 and are out of scope here.

Decisions already taken by the user (2026-10-07):

- Flink job in Java, built with Maven in a container (no local JVM)
- Prediction = trailing **60 s** event-time window, fired every **20 s**, per
  case (HPI-like cadence)
- Docker Desktop memory raised to **~5.5 GB** by the user; the inference stub
  and interface consumer run on the host via `uv`
- Interface = the proposal's **minimal consumer**; the TRANSFER interface is not
  integrated this sprint

Branch `sprint/03-06-pipeline-skeleton`, sprint folder
`sprints/03-06_pipeline-skeleton/`, tag `sprint/03-06`, PR to `main`.

## Definition of done

1. `uv run ioh-run --workload ... --scenario ... --cases 5 --duration 600`
   starts the stub, resubmits the Flink job, runs the replay into Kafka, collects
   predictions and progress at the interface consumer, and writes
   `results/<run_id>/` with `meta.json`, `predictions.jsonl`, `progress.jsonl`,
   `poller.jsonl`. `uv run python -m ioh_testbed.benchmark.analyze results/<run_id>`
   prints T_pipeline / staleness percentiles and per-case progress-lag slopes.
2. **Single-case correctness**: for a 1-case 600 s run, the Flink window
   outputs (boundaries, record counts, feature values) match a Python reference
   computed offline from the same Parquet, within float tolerance.
3. **Async I/O verified**: with stub capacity below offered demand, Flink's
   async operator fills to capacity, back-pressure is visible in the REST
   metrics, and per-case progress lag grows; with capacity above demand it stays
   flat. One recorded demonstration run of each.
4. All containers in compose with explicit memory limits; idle and loaded
   memory measured and recorded against the raised Docker VM.
5. `uv run pytest` passes without data or a broker; Java unit tests pass in the
   Maven container; `-m e2e` opts into the full path.
6. `plan.md`, `findings.md`, `report.md`, frozen `configs/`, `results/` in the
   sprint folder; tag; PR.

## Design decisions to pin

| Decision | Choice | Why |
|---|---|---|
| Flink version | The 2.x minor that `flink-connector-kafka` 4.0.x-2.0 documents (2.0/2.2/2.3); pick at first build, pin the official image by digest, Java 17 target (java21 image if no java17 tag) | The connector, not "latest", decides the pairing. 1.x is end-of-life for new work |
| Topology | Session cluster: 1 JobManager, 1 TaskManager, `FLINK_SLOTS` slots, job parallelism a config input | Production topology at reduced scale; parallelism and slots are later interference variables |
| Event time | Derived from the **DWC fields** in the deserializer: waves `c_time_stamp_wave_sample + (c_n_samples - 1) * c_sample_period`, numerics `c_time_stamp`; `_event_ts` only as a test cross-check. Bounded out-of-orderness `WATERMARK_BOUND_MS` (default 500), `pipeline.auto-watermark-interval` 50 ms, idleness 5 s, per-partition watermarks | The job must stay DWC-compatible, so the swap to DWC must not touch it. Within-partition disorder is harness jitter (~20 ms); the bound is an additive term in the latency floor (`T_pipeline >= bound + emission granularity + watermark interval`), so it is a recorded config input, not a constant. Idleness must be short: a 1-case run leaves most splits idle |
| Ingress carried through | Kafka record timestamp (`LogAppendTime`) captured in the deserializer as `tAppendMs` on every record and propagated as max/min through the window | `T_pipeline` is measured from ingress; the job must carry it, not the consumer guess it |
| Windowing | Built-in `SlidingEventTimeWindows(60 s, 20 s)` keyed by case, with an incremental `AggregateFunction` (no raw packets in window state) and a `ProcessWindowFunction` that attaches `windowStart`, `windowEnd`, `watermarkAtFire`, `tWindowFired` and a `partial` flag for the epoch-aligned windows at case start | Standard Flink semantics, easy to replicate in the Python reference, small state. Allowed lateness 0; late records counted in a metric. Whole wave packets are assigned by record timestamp, so a 10 s packet may straddle a boundary; the reference does the same |
| Features | Per numeric label: mean/min/max/last/count. Per wave label: mean/min/max of decoded physical values, sample count, invalid count. Plus `nRecords`, `tEventNewest`, `tEventOldest`, `tAppendNewest`, `tAppendOldest` | Cheap, deterministic, enough to validate against a reference and to carry the metadata the metrics need |
| Inference call | `AsyncDataStream.unorderedWait`, capacity `INFERENCE_ASYNC_CAPACITY` **per subtask** (in-flight total = capacity x parallelism, both recorded), timeout `INFERENCE_TIMEOUT_MS`, `java.net.http.HttpClient.sendAsync`; timeouts produce a prediction with `status=timeout`, never a job failure | The proposal's mechanism (Horchidan et al.). Unordered: the analysis must not assume per-case prediction order. Capacity is the knob that saturates |
| Inference stub | FastAPI on the host: `POST /predict`; `--service-ms`, `--service-cv` (lognormal), `--workers` (asyncio semaphore), `--queue-max` (503 beyond), `--seed`; response carries `queue_wait_ms`, `service_ms`, timestamps | Service time, variability and worker capacity are exactly the proposal's three inference variables. Sleep-based, not CPU-bound; stated |
| Output topics | `predictions` and `progress`, keyed by case, `LogAppendTime`, created by `kafka-init`; prediction `KafkaSink` with `DeliveryGuarantee.NONE` and `linger.ms=0` | `LogAppendTime` on `predictions` separates Flink exit from consumer fetch; no sink batching or checkpoint flushing inside the decomposition |
| Progress heartbeat | `KeyedProcessFunction` before the window: a 1 s processing-time timer per key emits `{caseId, tWall, tEventNewestSeen, watermark, recordsSeen}` to `progress` | Per-case progress lag = `tWall - tEventNewestSeen`; Theodolite-style lag series per key, independent of prediction cadence |
| Metric definitions | `T_pipeline = t_receipt - tAppendNewest` (processing-time latency of a window result, Karimov et al. 2018); `staleness = t_receipt - tEventNewest`; `progress_lag(case, t) = tWall - tEventNewestSeen`; decomposition: `tWindowFired - tAppendNewest` (with `tWindowFired - (windowEnd + bound)` as the queueing part net of watermark wait), inference round trip, `tPredAppend - tInferenceReturned`, `t_receipt - tPredAppend`. Broker-clock-only check: `tPredAppend - tAppendNewest` | Pinned once, in code and in the report; must not drift. The broker-only variant lives in one clock domain and validates the offset probe |
| Clock domains | Flink and Kafka run on the Docker VM clock, the harness and consumer on the host clock. A probe at run start and end measures host-to-broker offset; recorded in `meta.json`; the analysis **subtracts** it from every cross-domain term (T_pipeline, staleness, progress lag, `tWindowFired` terms), and warns above 5 ms | Docker Desktop's VM clock drifts after macOS sleep. Sprint 1's "single-host" assumption no longer holds |
| Per-run isolation | `ioh-run` cancels and resubmits the Flink job per run via the REST API with the run's parameters, waits for RUNNING with splits assigned before starting the replay, wipes the checkpoint path first; Kafka source at `latest` with consumer group `ioh-flink-<run_id>`; interface consumer at `latest`; JobManager and TaskManager logs copied into the run folder on exit | Fresh state, no bleed between runs, run parameters in the job's own config, logs where the first saturation run will need them |
| Checkpointing | Enabled, interval `FLINK_CHECKPOINT_MS` (default 10 s), hashmap state backend with filesystem checkpoints in the container, `taskmanager.memory.managed.size: 0m`; 0 disables | Production setting; its cost is a load characteristic. Managed memory only serves RocksDB and would waste 40% of TM memory |
| Harness fix H1 | Wave packets are emitted at the **end** of their span; `Emission` gains a separate event-time offset so `_event_ts` stays the first-sample time (the pacer currently derives it from the emission deadline); `_event_ts_last = _event_ts + (n-1)/srate`; schedule cropped by emission time | A monitor cannot export a packet before its last sample exists. Sprint 1 emitted at the start, which would make staleness negative by up to `packet_ms` |
| Resource metrics | One Python poller: Flink REST (back-pressure, busy time, records in/out per vertex, source `pendingRecords` as Kafka lag, checkpoints), `docker stats` per container; 1 s cadence to `poller.jsonl` | Proposal asks for Kafka/Flink lag and utilisation to localise bottlenecks. Admin-API lag would move only at checkpoints. No Prometheus/Grafana: VM memory is scarce and jsonl is reproducible |

## Build order

### Phase 0: housekeeping and the harness fix

0.1 Branch, `sprints/03-06_pipeline-skeleton/plan.md` (this plan, frozen).
0.2 User raises Docker Desktop memory; record the new VM size in `findings.md`
and in `src/compose/profiles/laptop.env` comments; README gets the instruction.
0.3 H1: `Emission(t_rel, t_event_rel, payload)` in `schedule.py`, pacer derives
`event_ts` from `t_event_rel`, `records.py` adds `_event_ts_last`; update
`docs/stream-model.md` T1 and §4 "packet inter-arrival"; fix
`tests/test_schedule.py`, `tests/test_records.py`, `tests/test_pacer.py` and
`tests/test_kafka_sink.py::small_schedule`, which build `Emission` directly.
0.4 `ioh-replay --run-id` so the orchestrator owns the results folder
(`src/ioh_testbed/replay/__main__.py`).

### Phase 1: inference stub (Python, independent of Flink)

1.1 `src/ioh_testbed/inference/stub.py` + `ioh-stub` script in `pyproject.toml`.
1.2 `tests/test_stub.py` with FastAPI's `TestClient` (add `httpx` to dev deps):
service time distribution, capacity queueing (2 workers, 4 concurrent requests,
two wait about one service time), 503 when the queue is full, deterministic
under `--seed`.

### Phase 2: infrastructure

2.1 `src/compose/docker-compose.yml`: Kafka gains an in-cluster listener
`kafka:29092` beside `localhost:9092`; `kafka-init` also creates `predictions`
and `progress`; services `flink-jobmanager` (REST 8081 to host),
`flink-taskmanager` with `jobmanager.memory.process.size`,
`taskmanager.memory.process.size`, managed size 0, slots,
`extra_hosts: ["host.docker.internal:host-gateway"]` so the stub URL also works
on the Linux server; `flink-build` (Maven image, compose profile `build`, `.m2`
volume, writes `src/flink/target/*.jar` on the host; the jar reaches the
cluster only via REST upload, no shared volume).
2.2 `laptop.env`: `FLINK_IMAGE`, `FLINK_JM_MEM=768m`, `FLINK_TM_MEM=2048m`,
`FLINK_SLOTS=4`, `FLINK_PARALLELISM=2`, `FLINK_CHECKPOINT_MS=10000`,
`WATERMARK_BOUND_MS=500`, `INFERENCE_URL=http://host.docker.internal:8000/predict`.
2.3 Pull, pin digests, bring up, record idle memory per container.

### Phase 3: Flink job (`src/flink/`, Maven, Java 17)

3.1 Skeleton: `WaveRecord`/`NumericRecord` POJOs (`int[]` samples, no boxing),
Jackson deserializer that captures the Kafka timestamp and derives event time
from the DWC timestamp fields, one `KafkaSource` over both DWC topics, watermark
strategy, `keyBy(caseId)`, progress heartbeat function, `KafkaSink` to
`progress`. Milestone: `ioh-replay --cases 1` produces a progress stream the
consumer can read. First end-to-end signal.
3.2 Sliding window with `FeatureAggregate` (incremental) and
`FeatureWindowFunction` (timing metadata, `tWindowFired` from the VM clock).
3.3 `InferenceAsyncFunction` and `Prediction` POJO; `KafkaSink` to `predictions`.
3.4 Job config from program args (bootstrap, topics, window, slide, inference
URL, capacity, timeout, checkpoint interval); `JobConfig.describe()` logged and
echoed into the first `progress` record so the run's metadata captures it.
3.5 JUnit 5: deserializer against a fixture record generated by `records.py`
and committed under `src/test/resources/` (including the event-time derivation
against `_event_ts`); `FeatureAggregate` arithmetic. No MiniCluster test: the
Python reference check is the correctness authority.

### Phase 4: interface consumer, analysis, reference

4.1 `src/ioh_testbed/interface/consumer.py` + `ioh-interface` script: consumes
`predictions` and `progress` from `latest`, stamps `t_receipt`, records the
prediction message's `LogAppendTime`, appends jsonl, prints a line every 10 s.
Reuses the consumer pattern in `tests/test_kafka_sink.py`.
4.2 `src/ioh_testbed/benchmark/analyze.py`: pure functions over the jsonl
files; `summary.json`; percentiles overall and per case for T_pipeline,
staleness and each decomposition term; per-case progress-lag slope by least
squares over the observation window; stub statistics; the stability slope
tolerance is a parameter, not yet calibrated (weeks 7-8).
4.3 `src/ioh_testbed/benchmark/reference.py`: offline sliding-window features
for one case from `reader.py` + `packetize.py` with the same decode and stats,
epoch-aligned window boundaries, whole packets assigned by record timestamp,
partial start windows flagged. `tests/test_reference.py` on synthetic cases;
`tests/test_e2e.py -m e2e` compares a real 1-case run's `predictions.jsonl`
against it.

### Phase 5: orchestration and poller

5.1 `src/ioh_testbed/benchmark/run.py` + `ioh-run`: run id; clock probe; start
stub (subprocess, params from CLI); wipe checkpoint path; resubmit Flink job via
REST (`/jars/upload` once, `/jars/:id/run` per run, poll until RUNNING with
splits assigned, cancel at end); start poller and interface consumer; run
`ioh-replay --sink kafka --run-id`; wait; stop; probe again; copy JM/TM logs;
write combined `meta.json` (harness meta, job config incl. bound, interval,
capacity x parallelism, stub config, Flink job id, image digests, clock
offsets). Extends `benchmark/stamp.py` rather than
duplicating it.
5.2 `src/ioh_testbed/benchmark/poller.py`: 1 s loop to `poller.jsonl`.

### Phase 6: verification runs and close

6.1 Correctness: 1 case, 600 s, reference check passes; record.
6.2 Baseline: 5 cases, 600 s, `dwc_10s`, stub `--service-ms 50 --workers 4`,
capacity 8. Report percentiles and decomposition; this is the first measured
latency floor on the laptop, labelled as such.
6.3 Async demonstration: 20 cases at demand 1 prediction/s; (a) stub 200 ms,
2 workers, stable; (b) stub 2000 ms, 1 worker, saturated: back-pressure in REST
metrics, async operator at capacity, progress-lag slope positive. Record both.
6.4 Memory: idle and at 20 cases, per container, against the VM size.
6.5 `findings.md`, `report.md`, freeze configs and results, tag, PR.

## Files

New: `src/flink/` (pom.xml, `src/main/java/de/charite/ioh/...`, tests),
`src/ioh_testbed/inference/stub.py`, `src/ioh_testbed/interface/consumer.py`,
`src/ioh_testbed/benchmark/{run,poller,analyze,reference}.py`,
`configs/pipeline/laptop.yaml` (window, slide, capacity, stub defaults),
`tests/test_{stub,analyze,reference,e2e}.py`, `sprints/03-06_pipeline-skeleton/`.
Modified: `src/compose/docker-compose.yml`, `src/compose/profiles/laptop.env`,
`src/ioh_testbed/replay/{schedule,records,__main__}.py`,
`src/ioh_testbed/benchmark/stamp.py`, `docs/stream-model.md`, `pyproject.toml`,
`README.md`, `CLAUDE.md` (architecture status line).

## Verification

```bash
uv run pytest                                              # python, no data/broker
docker compose --env-file src/compose/profiles/laptop.env -f src/compose/docker-compose.yml --profile build run --rm flink-build   # mvn package + tests
docker compose --env-file src/compose/profiles/laptop.env -f src/compose/docker-compose.yml up -d --wait
uv run ioh-run --workload configs/workload/standard_anaesthesia.yaml --scenario configs/scenarios/dwc_10s.yaml \
    --pipeline configs/pipeline/laptop.yaml --cases 1 --duration 600
uv run pytest -m e2e                                       # reference check on that run
uv run python -m ioh_testbed.benchmark.analyze results/<run_id>
docker stats --no-stream                                   # memory per container
```

## Out of scope (later sprints)

CORR case-level enrichment (no data yet), impairment injection (T10), cloud
provisioning, DWC reader, TRANSFER interface integration, Prometheus/Grafana,
mid-case channel changes, stability-tolerance calibration (weeks 7-8), the
concurrency and inference sweeps (weeks 9-12).

## Risks

- Flink 2.x and connector version pairing; Java 17 image availability. Resolve
  at first build, pin, record.
- Docker VM at 5.5 GB on an 8 GB host leaves macOS ~2.5 GB; if the host swaps,
  measurements are invalid. Watch `memory_pressure`; fall back to TM 1.5 GB.
- VM clock drift after sleep. The probe catches it; the fix is restarting Docker.
- JSON parsing of 5000-int arrays in Flink at 50 cases is ~250 KB/s, cheap, but
  the deserializer must not box per sample (use `int[]`).
- The 60 s / 20 s window makes the first full prediction per case 60 s after
  start; runs shorter than ~180 s give too few predictions to report percentiles.
- The latency floor includes the watermark bound by construction. The report
  must state `T_pipeline >= bound + emission granularity + watermark interval`
  so RQ1's floor is not misread as a pure processing cost.
