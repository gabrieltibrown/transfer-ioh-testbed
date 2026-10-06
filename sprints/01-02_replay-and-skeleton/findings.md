# Sprint 01-02 findings

Facts established during the sprint that are not derivable from the code.
Measured on 2026-09 and 2026-10; see git log for exact dates.

## Environment

| | |
|---|---|
| Host | macOS, 8 cores, 8 GB RAM |
| Docker Desktop VM | **3.83 GB** (4,109,217,792 bytes) allocated to containers, not 8 GB |
| Docker / Compose | 29.1.3 / v2.40.3 |
| Java | none installed. Flink job will build in a Maven container |
| Python | 3.13.5 via Homebrew; `uv` 0.8.13 |

The 3.83 GB Docker ceiling is the binding local constraint. Kafka alone wants
~1 GB heap. Sprint 1 (Kafka only) fits; sprint 2 (adding Flink JM + TM + stub)
will be tight and must be measured, not assumed.

## VitalDB cache

| | |
|---|---|
| Source | `https://api.vitaldb.net/<caseid>.parquet`, one Parquet per case |
| Cached | 50 cases, 898 MB, under `data/vitaldb/` (gitignored) |
| Eligible by catalogue | 3,643 of 6,388 cases list `SNUADC/ART`, `Solar8000/ART_MBP`, `SNUADC/ECG_II` |
| **Catalogue is unreliable** | 4 of the first 54 catalogue-eligible cases (3, 14, 32, 91) list ART and ART_MBP but their Parquet files contain neither. Selection validates on content; rejected ids are recorded in the manifest |
| Duration | min 0.93 h, p25 2.98 h, median 3.98 h, p75 5.49 h, max 8.67 h |
| Waveform tracks per case | min 4, median 8, max 11 |
| Numeric tracks per case | min 55, median 71, max 107 |

Waveforms are stored as ~1 s blocks (500 samples at 500 Hz; `dt`, `srate`,
`gain`, `bias`, `ivals`). Decode is `physical = raw * gain + bias`, verified
against `ART_MBP`. Missing samples are nulls inside `ivals`, ~0.01% of samples,
with identical counts across tracks from the same device. Block cadence is
exactly 1.0000 s with zero gaps in every case checked.

VitalDB rates: SNUADC waveforms 500 Hz (ART, ECG_II, ECG_V5, PLETH), Primus
62.5 Hz (AWP, CO2), BIS 128 Hz (EEG). Numerics: Solar8000 0.5 Hz, BIS 1 Hz,
Primus 0.157 Hz. No native 125 Hz.

## Source model

See `docs/stream-model.md` and `docs/philips-data-egress.md`. Headline facts:

- The IntelliVue Data Export Interface is **not** the PIC iX path; it is
  unavailable over LAN when monitors are on the Philips LAN
- Data Warehouse Connect is the only PIC iX export carrying continuous
  waveforms; HL7 outbound is numerics only
- DWC wave rows are estimated at **~10 s blocks** (3-20 s range), derived from
  Malunjkar et al. 2021 row counts, two methods agreeing. Not measured
- Numerics are 1 Hz real-time in both DEI and DWC
- LIVIA's replay job accumulates **62 s of drift over a 3 h case** under
  relative-sleep pacing (measured, `docs/pacing_drift_benchmark.py`)

## Infrastructure

| | |
|---|---|
| Kafka image | `apache/kafka:4.0.0`, digest `sha256:3f7b939115cd4872e9cee9369d80bd69712fde55f9902f46d793f64848dedc75` |
| Broker | single node, KRaft, 1 GB heap, 1.5 GB container limit, 2 CPUs (`src/compose/profiles/laptop.env`) |
| Topics | `dwc-waveform`, `dwc-numeric`, 8 partitions, `message.timestamp.type=LogAppendTime`, auto-create disabled |
| Producer | confluent-kafka 2.15 (librdkafka), `acks=all`, `linger.ms=5`, no compression |

## Harness timing on this host

Pacing is absolute-deadline; the open-loop property holds (tests in
`tests/test_pacer.py`, in simulated time). Two host-specific facts:

- **macOS timer slop.** With pure sleep-to-deadline at realistic rates
  (20 cases, speed 1, ~100 records/s, i.e. ~10 ms between emissions) p99 lateness
  was 18 ms and p50 1.2 ms. Roughly 1% of long `asyncio.sleep` calls wake
  15-20 ms late from timer coalescing. A 2 ms hybrid spin wait brings p50 to
  **0.1 ms** and p90 to 0.6 ms but cannot recover the 1% tail (p99 ~17 ms).
  Spinning 20 ms before every deadline would pin a core and perturb Kafka and
  Flink on the same laptop, so it was not done. Expect ~1 ms on Linux.
- **Consequence for the verdict.** Harness lateness never enters `T_pipeline`,
  which is measured from the broker's `LogAppendTime`; it bounds only
  offered-load timing fidelity. Thresholds are therefore cadence-relative:
  DEGRADED above 10% of `packet_ms`, INVALID above 100% or on any refused
  record. A fixed 10 ms tolerance made every realistic-speed run on this
  laptop DEGRADED for a reason that does not matter to the experiment.
- Under high offered load the pacer rarely sleeps, so lateness *falls*: the
  indicative null-sink sweep (5 cases, 4 s steps) reached speed 500,
  **18,800 records/s, 18 MB/s of JSON, p99 3 ms**, without leaving OK. The
  definitive ceilings are in the Results section below.
- Measurements are CPU-sensitive. The Kafka round-trip test failed once while a
  calibration sweep ran concurrently and passed cleanly alone. Calibration and
  real runs must be serial and uncontended.

## Rescope

Sprint 1 was planned as weeks 1-2 findings plus a walking skeleton through
Flink. The source-model research consumed the sprint. On 2026-10-06 the sprint
was rescoped to **replay harness + Kafka + ingress stamping + tests + harness
ceiling calibration**. Flink job, inference stub, interface sink and analysis
move to sprint 2. Rationale: the Flink job carries the most unresolved decisions
and bolting it onto a spent sprint risks the rushed-instrumentation failure the
plan warned against. See `plan.md`.

## Still open, not answerable internally

For Philips or the integration partner: DWC wave write cadence and
bedside-to-row delay; whether DWC supports CDC or a streaming subscription;
whether Capsule MDIP is licensed at the site; the `c_n_samples` distribution in
`te_wave`.
