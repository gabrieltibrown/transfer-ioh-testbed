# Sprint 01-02 build plan (rescoped): replay harness + Kafka + calibration

## Context

Sprint 1 was consumed, legitimately, by getting the source model right. The
research produced thesis-relevant findings (DEI is not the PIC iX path; DWC is
the only continuous-wave egress and likely writes ~10 s blocks; LIVIA's pacing
drifts 62 s over a 3 h case) but zero pipeline code. The user approved
rescoping sprint 1 to **replay harness + Kafka + ingress stamping + tests +
harness ceiling calibration**. Flink job, inference stub, sink and analysis move
to sprint 2 (proposal weeks 3-6).

Branch: `sprint/01-02-replay-and-skeleton`, 9 commits ahead of `main`, no PR yet.

**Definition of done for sprint 1:**
1. `uv run ioh-replay --config configs/workload/standard_anaesthesia.yaml --cases 5 --duration 300`
   emits to a local Kafka with schedule adherence `OK`, and the records can be
   consumed back and verified
2. `uv run pytest` passes with no dependency on `data/` (synthetic fixtures)
3. The harness's own ceiling on this laptop is measured and recorded
4. `sprints/01-02_replay-and-skeleton/` holds plan, findings, report; tag `sprint/01-02`; PR opened

## Design decisions to pin now (they shape every file below)

| Decision | Choice | Why |
|---|---|---|
| Packet cadence | `packet_ms` is **required** in every workload config, no default. Default *scenario* is 10000 (DWC estimate); `256` is the named Capsule/DEI sensitivity scenario | Most uncertain input; must appear in run metadata every time. Fixed default + one sensitivity run, not a swept axis |
| Granularity model | Single `packet_ms: int`. Delete `block` / `chunk` / `sample` | One code path; it is the physical quantity the thesis discusses |
| Per-track spec | `{source, label, kind: wave\|numeric, target_hz}` | Needed for T2 (500→125 decimation), T5 (numeric ZOH to 1 Hz), T9 (heterogeneous cases) |
| Sensor profiles | 2-3 YAML profiles within the 3 ECG + 8 non-ECG budget, assigned per case by a deterministic rule (e.g. `caseid % n`) | Heterogeneous per-case load is the §2.2 uneven-workload mechanism. Numeric count is the dominant knob under 10 s cadence (55-107 available per case) |
| Event-time rebasing | One `t0` per run. Per-case offset `= dt - case_dt0`. **All tracks of a case share the offset** | LIVIA's per-label `recording_start` bug erases intra-patient alignment |
| Concurrency control | All selected cases start at `t0`; fixed observation window (default 1800 s); cases shorter than the window are ineligible | "Concurrent case count" must be a controlled independent variable. Shortest cached case is 0.93 h |
| Pacer architecture | One scheduler loop over a min-heap of `(deadline, stream)`; **absolute deadlines** from `t0`; sleep until deadline, emit, push next | Non-accumulating error (measured 0.007 ms/packet vs LIVIA's 1.48 ms); scales to thousands of streams without per-stream coroutines |
| Three timestamps | `t_sched` (deadline) and `t_produce` (wall time at `produce()`) **in the record**; `t_append` from Kafka `LogAppendTime` on the topic | Separates harness lateness (`t_produce - t_sched`) from producer/broker delay (`t_append - t_produce`) from pipeline latency (later: `t_receipt - t_append`). `t_append` is the authoritative **testbed ingress** |
| Never drop, never block the loop | `confluent_kafka.Producer.produce()` raises `BufferError` when full. On it: `poll(0)`, retry once within the tick; if still full, **count a backpressure event**, keep the record in a bounded overflow, degrade the adherence verdict | Dropping silently changes offered load (LIVIA defect). Blocking the loop stalls every stream (LIVIA defect) |
| Adherence verdict | Per run: lateness p50/p99/max, late count, backpressure count. `OK` if p99 lateness < tolerance and zero backpressure, else `DEGRADED`, else `INVALID` | A run whose harness fell behind must say so in its metadata |
| Topics | `dwc-waveform`, `dwc-numeric`, keyed by `case_id`, `message.timestamp.type=LogAppendTime`, partition count a config input | Mirrors DWC's wave/numeric table split; partitions are a §2.2 interference variable later |
| Wire format | JSON, DWC `te_wave` field set for waves (LIVIA's schema); an analogous minimal record for numerics (our design, marked as such) | Fidelity to the production wire path; Java POJO downstream in sprint 2 |
| Tests | pytest; **synthetic Parquet fixtures built in-test with pyarrow**; VitalDB-backed tests opt-in via marker | `data/` is gitignored, tests must run without it |
| Signal fidelity | Decimate without anti-alias filter; numeric ZOH | Load fidelity only; inference is a stub. Stated as a limitation |

## Build order

Pure functions first (fast feedback, hardest correctness), then scheduling
against a null sink, then Kafka, then calibration. Each step is one or two
small commits with its tests.

### Phase 0: housekeeping (make the repo tell the truth)

0.1 `docs/stream-model.md` Revision 3: propagate the parametric-cadence
conclusion through §1.8, §2, §3.1, §3.4, §4; recompute the load table for 10 s
and 256 ms side by side (10 s is numerics-dominated by record count, 94%);
replace §1.1b's acquisition menu with a pointer to `philips-data-egress.md`;
attribute the DWC→Mirth→Kafka description to the proposal's reading of Joachim
et al. Target ~300 lines.

0.2 `fetch_vitaldb.py`: select on Parquet **content** (wave rows with `srate`
for ART and ECG_II, numeric rows for ART_MBP), not the `/trks` catalogue. Record
per-case `channels`, `duration_s`, `n_wave_tracks`, `n_numeric_tracks` in the
manifest. Replace cases 3, 14, 32 with the next eligible ids.

0.3 `sprints/01-02_replay-and-skeleton/plan.md` (this plan, frozen) and
`findings.md` (environment: 8 cores / 8 GB host, **Docker VM 3.83 GB**, no JVM,
uv 0.8.13; VitalDB: 50 cases / 881 MB, 3 catalogue-only cases, duration
distribution, track counts; the rescope decision and why).

### Phase 1: pure core, no I/O

1.1 `replay/config.py` replaces `profiles.py`: `TrackSpec`, `SensorProfile`,
`WorkloadConfig` (`packet_ms` required, `observation_window_s`, `speed`,
`profiles`, `assignment`), YAML loading, `describe()` for metadata. Keep
`SourcePolicy` / `ENV_POLICIES` unchanged.
`configs/workload/standard_anaesthesia.yaml`, `invasive_cardiac.yaml`,
`minimal_monitoring.yaml`; `configs/scenarios/dwc_10s.yaml`, `dei_256ms.yaml`.

1.2 `replay/reader.py`: VitalDB Parquet → per-track `SampleStream`
(`t_start`, `srate`, `values` as float with NaN for null, `gain`, `bias`,
`unit`) and `NumericStream` (`t`, `value`). Decode `physical = raw*gain + bias`
(verified). Behind a `Source` protocol so DWC can slot in.

1.3 `replay/packetize.py`: `SampleStream` → `WavePacket`s of exactly
`packet_ms`: continuous-stream reconstruction from block `dt + i/srate`,
**remainder carry** across source blocks, **gap preservation** (a gap longer
than one sample period starts a new packet run rather than being spliced),
decimation to `target_hz` by integer stride, NaN positions → `invalid_samples`
index list computed **after** decimation and re-packetisation. `NumericStream` →
ZOH resample to `target_hz`.

1.4 `replay/records.py`: `WavePacket` → DWC `te_wave`-shaped dict (`p_patnr`,
`c_label`, `c_sequence_number`, `c_time_stamp_wave_sample`, `c_sample_period`,
`c_hz`, `c_value`, `c_n_samples`, `c_scale_lower/upper`,
`c_calibration_abs_lower/upper` derived from gain/bias, `c_invalid_samples`,
`_ingest_ts` renamed to explicit `t_sched`/`t_produce`). Per-`(case,label)`
monotonic sequence. Numeric record analogue. Note `c_hz` as float so 62.5 is
not truncated to 62.

Tests (phase 1): packet size exact for 500/125/62.5 at both cadences; no short
packets at block boundaries; samples in == samples out ÷ stride; a synthetic gap
is preserved not spliced; invalid index list correct after decimation
(off-by-one trap); `raw*gain+bias` round-trips through `c_scale_*` /
`c_calibration_abs_*`; sequence strictly increasing with no gaps; ZOH numeric at
1 Hz from 0.5 Hz input.

### Phase 2: scheduling against a null sink

2.1 `replay/schedule.py`: case selection (eligible, ≥ window), per-case offset,
per-stream deadline generator honouring `speed`; min-heap merge of all streams.

2.2 `replay/pacer.py`: asyncio loop over the heap; `Sink` protocol with
`NullSink`; lateness histogram; backpressure counter; adherence verdict.
`replay/__main__.py` CLI (`ioh-replay`) with `--sink null|kafka`.

Tests (phase 2): drift does not accumulate over a long synthetic run (assert
max lateness bounded, not growing with tick index); an artificially stalling
sink does not change the number of emissions scheduled per second; all tracks of
one case share one offset; verdict transitions `OK → DEGRADED → INVALID` under
injected lateness.

### Phase 3: Kafka

3.1 `src/compose/docker-compose.yml`: `apache/kafka` pinned by digest, KRaft
single broker, explicit `mem_limit`/`cpus`, topic init with `LogAppendTime` and
configurable partitions; `profiles/laptop.env`. Record the image digest in
findings.

3.2 `replay/kafka_sink.py`: `confluent_kafka.Producer`; `produce()` with
delivery callback; `poll(0)` each tick; `BufferError` handling per the decision
table; flush at end with timeout; emit counts. Governance check: `SourcePolicy`
enforced at CLI start.

3.3 End-to-end smoke (opt-in, needs Docker): 1 case, 60 s, consume back with a
throwaway consumer; assert record count = scheduled count, keys = case ids,
`LogAppendTime` present and ≥ `t_produce`.

### Phase 4: instrument calibration + run metadata

4.1 `benchmark/stamp.py`: `results/<run_id>/meta.json` with git commit + dirty
flag, config path + sha256, `packet_ms`, scenario, profiles, case ids, env
descriptor (host CPU/mem, Docker VM mem, image digests, Python/uv versions),
adherence verdict and lateness percentiles with N and window.

4.2 Ceiling mode: `ioh-replay --calibrate` sweeps `speed` (or case count) with
`NullSink` then `KafkaSink`, 5-min runs, until verdict leaves `OK`. Output the
max offered records/s per sink. Record both laptop ceilings in `findings.md`.
These numbers bound what any later experiment on this machine can claim.

### Phase 5: close

5.1 `sprints/01-02_replay-and-skeleton/report.md`: what was built, definitions
pinned, measured numbers with run ids, what moved to sprint 2 and what sprint 1
leaves ready for it (topics, `LogAppendTime`, record schema, `t_*` semantics).

5.2 Tag `sprint/01-02`. Open PR to `main` (short title, no AI attribution, no
em dashes, per global conventions).

## Verification

```bash
uv run pytest                                        # phases 1-2, no data needed
uv run pytest -m vitaldb                             # reader against cached cases
docker compose -f src/compose/docker-compose.yml --env-file src/compose/profiles/laptop.env up -d
uv run ioh-replay --config configs/workload/standard_anaesthesia.yaml \
    --scenario configs/scenarios/dwc_10s.yaml --cases 5 --duration 300 --sink kafka
uv run python -m ioh_testbed.benchmark.stamp results/<run_id>   # verdict OK, N, p50/p99
uv run ioh-replay --calibrate --sink null ; uv run ioh-replay --calibrate --sink kafka
git check-ignore -v data/vitaldb/1.parquet           # still ignored
uv run ioh-replay --env cloud --source dwc            # must refuse
```

## Out of scope (sprint 2)

Flink job (Java, Maven-in-Docker, async I/O verification), inference stub,
interface sink, latency/staleness analysis, mid-case dynamic channel changes,
impairment injection (T10) beyond a stub flag, cloud provisioning, DWC reader.

## Risks

- Docker VM is 3.83 GB. Kafka alone needs ~1 GB heap; fine for sprint 1, tight
  for sprint 2. Measure, record.
- JSON of 5000-int arrays is ~2.5x the binary size. Faithful to the production
  wire path; note it as a load characteristic.
- If the single-loop pacer ceiling is too low on the laptop, shard cases across
  processes. Design the heap scheduler so a case set can be partitioned.
- `confluent-kafka` wheels for macOS arm64 exist; if the build fails, pin a
  version with wheels rather than compiling librdkafka.
