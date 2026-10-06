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
