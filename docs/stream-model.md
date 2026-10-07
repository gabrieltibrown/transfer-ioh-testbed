# Stream model: the production feed, VitalDB, and the bridge between them

Status: Revision 3, sprint 01-02. Living document.

Revision history: Rev 1 treated the IntelliVue Data Export Interface (DEI) as
the production feed and fixed the packet cadence at 256 ms. Rev 2 established
that DEI is not the PIC iX path and downgraded 256 ms to a hypothesis. Rev 3
makes cadence a required parameter with a DWC-derived default, and propagates
that through the load model, the gap analysis, the bridge and the tests.

The testbed replays recorded Parquet in order to mock the live waveform feed
the production pipeline will consume. Every number the thesis reports is
conditioned on how faithful that mock is. This document states what the feed
looks like, where VitalDB differs, and which transformations bridge the
difference, so the fidelity claim is auditable rather than assumed.

| Source | What it is | Authority for our path |
|---|---|---|
| [PIIC iX Release B.01 Technical Data Sheet](https://www.biyomar.com/uploads/katalog/monitor-cihazlari/piic-ix.pdf) | Lists PIIC iX's export interfaces | Primary for *which* interface carries waves |
| [Malunjkar et al. 2021, arXiv:2106.03965](https://arxiv.org/abs/2106.03965) | 500-bed PIC iX + DWC deployment, published row counts | Primary for DWC behaviour and the cadence estimate |
| Philips *Data Export Interface Programming Guide*, [part 453564588011](https://www.documents.philips.com/doclib/enc/fetch/2000/4504/577242/577243/577247/582636/582882/X2%2C_MP%2C_MX_&_FM_Series_Rel._L.0_Data_Export_Interface_Program._Guide_4535_645_88011_(ENG).pdf) | The monitor's *own* export interface, **not** our path | Rate ladder and wave object model only. **Not** cadence |
| LIVIA `real_waveform_producer.py` | Charite code emitting DWC `te_wave` rows to Kafka | Secondary, shows the DWC field set in practice |
| 50 cached VitalDB cases, `data/vitaldb/` | This repo | Primary for the VitalDB side |

No public Philips document specifying DWC's `te_wave` schema or its delivery
cadence was found. The full survey of acquisition paths and what each carries
is in [`philips-data-egress.md`](philips-data-egress.md); this document takes
its conclusions as given.

## 1. What the production feed emits

### 1.1 Which interface

**Data Warehouse Connect (DWC) is the only PIIC iX export that carries
continuous waveforms.** HL7 outbound is numerics only; Wave Strip, Holter and
12-lead exports are episodic. The monitor's DEI delivers genuine 256 ms packets
but is unavailable over LAN when monitors are on the Philips LAN, which is the
Charite deployment. Capsule MDIP is Philips' live-streaming platform and the
only plausible sub-second route at department scale, but is undocumented
publicly and separately licensed. Details and citations in the egress document.

DWC is a continuously written SQL database that is part of PIC iX, not a
discharge-time dump, so near real time is reachable by tailing it. The
proposal's description of the Joachim et al. reference platform (2.3), DWC
through Mirth Connect into Kafka, is consistent with exactly that.

What carries over from the DEI guide, because it describes the monitor's
measurement modules rather than the transport and is independently corroborated
by the DWC field set: the sample-rate ladder 500 / 250 / 125 / 62.5 sps
(`c_sample_period` in {2, 4, 8, 16} ms; LIVIA's `c_hz = 1000 // c_sample_period`
only makes sense against this set) and the wave object model in 1.5 to 1.7.

### 1.2 Cadence is a required parameter, not a constant

Two candidate values, and which one applies depends on the acquisition path,
which cannot be settled internally:

| Scenario | `packet_ms` | Basis | Status |
|---|---|---|---|
| `dwc_10s` | 10000 | Derived from Malunjkar et al. daily row counts, two methods agreeing (below) | **Baseline.** Best current estimate for the likely path |
| `dei_256ms` | 256 | Philips DEI guide, "Interpreting Wave Data" | Sensitivity scenario. Applies if the path is DEI or (plausibly) Capsule |

**Derivation of the DWC estimate.** Malunjkar et al. report ~120 M numeric
rows/day and ~10 M wave-sample rows/day across ~400 active monitors. Numerics
first, as a check on the method: 120 M / 86400 s / 400 = 3.47 rows/s/bed, i.e.
about 3.5 channels at **1 Hz**, which matches their stated "1 second vital
numerics". Applying the same arithmetic to waves gives **0.29 rows/s/bed**; at
3 to 6 wave channels per bed that is one row every 10 to 21 s. A second
estimate from data volume agrees: 220 GB/day total, numerics and enumerations
at ~100 B/row account for only ~18 GB, so waves dominate at roughly 8 to 15
KB/row, which is 4000 to 7500 int16 samples, i.e. 8 to 15 s of 500 Hz data. The
`te_wave` schema (one `c_label` and one `c_value` array per row) supports the
one-row-per-channel-per-block assumption both estimates rest on. A 256 ms
packet would be ~80x denser than observed.

This is a derived estimate from another hospital's aggregates, not a
measurement. The `c_n_samples` distribution in `te_wave` settles it in one
query when DWC access lands.

**Design consequence.** `packet_ms` is **required** in every workload config,
with no default in code, so the assumption appears in every run's metadata.
The thesis runs the `dwc_10s` baseline and one `dei_256ms` sensitivity run. It
is not a swept axis; RQ2's axes are concurrent case count and inference demand.

**Thesis consequence.** Event-time staleness has a floor of about one block
duration at the source, before Kafka, Flink or inference contribute. Under
`dwc_10s` that is ~10 s against the 60 to 170 s budget in proposal 4, 6 to 17%
consumed at acquisition, plausibly the largest fixed term in `T_pipeline`.
Quantifying it is an RQ1 result in itself.

### 1.3 Channel composition is sensor-dependent

There is no fixed channel list. The DEI guide declares that object counts "may
change dynamically" and that the client discovers available waves from the
dynamic context; channels appear and disappear as sensors are attached or
removed. The budget is up to **3 ECG waves at 500 sps** (or one compound
3-channel at 250 sps) plus up to **8 non-ECG waves at 125 or 62.5 sps**.
Per-case composition is therefore a subset of what is plugged in, and modelling
every case identically understates the uneven-load mechanism in proposal 2.2.

### 1.4 Numerics: 1 Hz real-time

Both sources agree: DEI `POLL_EXT_PERIOD_NU_1SEC` selects "1 sec Real-time
Numerics", and Malunjkar et al. report "1 second vital numerics" from DWC. The
1 to 5 Hz in proposal 2.3 is a population observation across recordings, not an
interface property.

### 1.5 Validity is explicit, at two levels

Per observation, `MeasurementState` is a bit field (`INVALID 0x8000`,
`QUESTIONABLE 0x4000`, `UNAVAILABLE 0x2000`, `CALIBRATION_ONGOING 0x1000`, ...);
"the measurement is valid if the first octet of the state is all 0". Per
sample, `SA_FIX_*` masks mark individual samples invalid, paced, defib, saturated
or QRS. Invalid data is part of the normal stream, not an error path. DWC
carries this as `c_invalid_samples` and `c_unavailable_samples` index lists.

### 1.6 Scaling, and the timestamp precision problem

Raw samples are scaled integers. `ScaleRangeSpec16` gives a linear map between
`lower/upper_scaled_value` (u16) and `lower/upper_absolute_value` (float); DWC
carries these as `c_scale_*` and `c_calibration_abs_*`, the latter typed STRING
and arriving empty rather than null.

Timing matters more: DEI `RelativeTime` is set "with a precision of 2 ms", but
wall-clock absolute time is derived "with a precision of about 1 s". Each poll
reply carries a `sequence_no` that increments per result. So **source event time
is only good to about 1 s in wall-clock terms**, which is almost certainly why
LIVIA reports "duplicates/non-monotonic jumps/clock-reset artifacts" and sorts
by `c_sequence_number`. It bounds how precisely staleness can be attributed to
the pipeline, as proposal 7.1 anticipates.

### 1.7 The DWC `te_wave` field set

From LIVIA, with the DEI attribute each field corresponds to:

| `te_wave` column | DEI origin | Note |
|---|---|---|
| `p_patnr` | patient/bed identity | Kafka key. Patient, not surgical case |
| `c_label` | `NOM_ATTR_ID_LABEL_STRING` | wave label |
| `c_sequence_number` | poll `sequence_no` | **ordering authority** |
| `c_time_stamp_wave_sample` | `abs_time_stamp` | event time, ~1 s precision |
| `c_sample_period` | `NOM_ATTR_TIME_PD_SAMP` | ms: 2 / 4 / 8 / 16 |
| `c_hz` | derived | `1000 // c_sample_period`; LIVIA truncates 62.5 to 62 |
| `c_value` | `SaObsValue.array` | the sample array |
| `c_n_samples` | `SaSpec.array_size` | 128/64/32/16 if packet-faithful; ~5000 at 500 Hz if ~10 s blocks |
| `c_scale_lower/upper`, `c_calibration_abs_lower/upper` | `ScaleRangeSpec16` | |
| `c_unavailable_samples`, `c_invalid_samples` | `SA_FIX_*` / `MeasurementState` | nested index lists |
| `c_physio_id`, `c_base_physio_id`, `c_is_slow_wave`, `c_is_derived`, `c_channel` | context attributes | |

### 1.8 Per-case load under both scenarios

Standard anaesthesia profile: ECG_II at 500 sps; ART and PLETH at 125; AWP and
CO2 at 62.5; 8 numerics at 1 Hz. Bytes are raw int16 before JSON encoding.

| | `dei_256ms` | `dwc_10s` |
|---|---|---|
| wave packets/s per channel | 3.906 | 0.1 |
| wave records/s per case (5 waves) | 19.5 | 0.5 |
| numeric records/s per case | 8 | 8 |
| **records/s per case** | **27.5** | **8.5** |
| numerics' share of record count | 29% | **94%** |
| wave packet size, 500 / 125 / 62.5 sps | 128 / 32 / 16 samples | 5000 / 1250 / 625 samples (10 / 2.5 / 1.25 KB) |
| wave bytes/s per case | 1.75 KB | 1.75 KB |
| **50 cases, records/s** | **1,375** | **425** |
| 50 cases, wave bytes/s | ~87 KB | ~87 KB |

Three things follow. Bandwidth is negligible either way; load is record count
and per-record processing, which is RQ2's regime. The cadence assumption **flips
which stream dominates record rate**: waves under 256 ms, numerics under 10 s.
And under 10 s, **how many numerics a profile includes is the dominant load
knob**: VitalDB offers 55 to 107 per case, so a profile with 20 numerics would
be 20.5 records/s and 98% numerics. Sensor profiles must pin this explicitly.
JSON encoding inflates bytes roughly 2.5x; that is faithful to the production
wire path and is a load characteristic to state, not hide.

## 2. The gap: VitalDB versus the production feed

Measured on the 50 cached cases.

| Dimension | Production feed | VitalDB | Gap |
|---|---|---|---|
| Packet cadence | `packet_ms`: ~10 s (DWC est.) or 256 ms (DEI) | **1000 ms**, all wave types | wrong in either direction; re-packetise |
| Array size | `packet_ms * srate / 1000` | 500 / 62 / 128 (1 s at native rate) | follows from cadence |
| ECG rate | 500 sps | 500 sps (`SNUADC/ECG_II`, `ECG_V5`) | **match** |
| Other waves | 125 or 62.5 sps | **500** (`ART`, `PLETH`), 62.5 (`Primus/AWP`, `CO2`), 128 (`BIS/EEG`) | no native 125; ART and PLETH 4x too fast |
| Numerics | 1 Hz | **0.5 Hz** (Solar8000), 1 Hz (BIS), **0.157 Hz** (Primus) | 2x to 6x too slow, device-dependent |
| Numeric count | profile-defined, ~8 to 14 | 55 to 107 per case | must select a subset |
| Scaling | `ScaleRangeSpec16` pairs | `gain`/`bias`, verified `physical = raw*gain + bias` | equivalent linear map |
| Per-sample validity | `SA_FIX_*` masks, index lists | nulls inside `ivals`, 0.01% of samples, identical counts across one device's tracks | same concept, different encoding |
| Validity scope | per-observation `MeasurementState` too | absent | |
| Dropouts | expected; client must detect missing samples | **none**: 1.0000 s cadence, zero gaps, every case checked | VitalDB is unrealistically clean |
| Ordering authority | `c_sequence_number`; timestamps unreliable | `dt` only, monotonic and regular | no disorder to handle |
| Event-time precision | ~1 s wall clock | exact by construction | no timestamp noise |
| Channel composition | dynamic, 3 ECG + 8 non-ECG budget | fixed per recording | no mid-case change |
| Keying | `p_patnr`, patient/bed | `caseid`, surgical case | mapping needed for DWC only |
| Catalogue reliability | n/a | 4 of 54 catalogue-eligible cases lack the listed tracks | select on content |

Two entries matter most. **VitalDB is too clean**: no dropouts, exact
timestamps, no reordering, so a pipeline validated only against it never
exercises late data, watermark behaviour under disorder, or gap handling, which
are the mechanisms proposal 2.2 identifies as coupling cases together. And
**waves are too fast while numerics are too slow**, so naive replay gets
per-case load wrong in both directions at once.

## 3. The bridge

Design rule: **emulate the load and timing characteristics of the production
feed, not the clinical signal content.** Defensible because inference is a
configurable stub, so sample values never influence a measurement. It stops
being defensible the moment a real model consumes these streams; that
limitation is recorded here and in the sprint report.

### 3.1 Transformations

| # | Gap | Transformation |
|---|---|---|
| T1 | 1 s source blocks vs `packet_ms` | Treat each track as a continuous sample stream (block `dt` plus `i/srate`), re-packetise on fixed `packet_ms` boundaries, carry the remainder across source-block edges. Mandatory for 256 ms (1000/256 is not integral) and for any gap; exact for 10000 |
| T2 | ART, PLETH at 500 sps vs 125 | Decimate 4:1 by integer stride. **No anti-alias filter**; load fidelity is the goal. Recorded as a limitation |
| T3 | ECG at 500; AWP, CO2 at 62.5 | Pass through |
| T4 | Numerics at 0.5 / 0.157 Hz vs 1 Hz | Zero-order hold to 1 Hz, the semantics a monitor already has for an unrefreshed value |
| T5 | 55 to 107 numerics vs a realistic subset | Sensor profile selects which numerics, explicitly, since this is the dominant load knob under `dwc_10s` |
| T6 | `gain`/`bias` vs `ScaleRangeSpec16` | Keep raw scaled ints; emit `c_scale_*` and `c_calibration_abs_*` derived from gain and bias, so consumers use the DWC contract |
| T7 | Nulls in `ivals` vs `SA_FIX_*` | Null positions become `c_invalid_samples` index lists, computed **after** decimation and re-packetisation |
| T8 | No sequence number | Per `(case, label)` monotonic `c_sequence_number` |
| T9 | Fixed composition | Assign a **sensor profile** per case from a small set within the budget, deterministically, so per-case load is heterogeneous |
| T10 | No dropouts, exact timestamps | **Injectable impairments**, default off: packet loss, timestamp jitter, reordering, module disconnect. Baseline runs stay clean; enabled to test watermark behaviour. Stub flag only in sprint 1 |
| T11 | Per-case `dt` origins differ | One replay `t0` per run; per-case offset `dt - case_dt0`; **all tracks of a case share it** |

### 3.2 Deliberately not emulated in this sprint

Mid-case dynamic channel creation and deletion (varies offered load mid-run,
confounds the baseline). Compound 250 sps ECG. Clinical signal fidelity.
`p_patnr` to surgical case mapping, irrelevant for VitalDB. Real impairment
injection beyond a flag.

### 3.3 Open items

Listed once, in `sprints/01-02_replay-and-skeleton/findings.md` under "Still
open". The one that changes this document most is the `c_n_samples`
distribution in `te_wave`.

### 3.4 What LIVIA's replay job tells us, and where it must not be copied

`real_waveform_producer.py` replays exported DWC Parquet into `dwc-waveform`.
It is a useful schema reference and a useful warning.

**Evidence it provides.** It sets no rate; pacing comes entirely from
`c_time_stamp_wave_sample` spacing. `c_hz = 1000 // c_sample_period` only makes
sense against {2, 4, 8, 16} ms, the Philips ladder, and truncates 62.5 to 62.
`MAX_BURST_PER_SEC = 20` per (patient, label) implies steady-state packet rate
well below 20/s per signal: consistent with any block-based cadence, and it
rules out per-sample delivery.

**Measured defect: relative-sleep pacing accumulates drift.** 400 concurrent
streams at 256 ms cadence, serialising a 128-sample payload per tick
(`pacing_drift_benchmark.py`):

| Strategy | Overshoot per packet | Drift over a 3 h case |
|---|---|---|
| `await asyncio.sleep(gap)` | 1.480 ms | 62.4 s |
| absolute deadline schedule | 0.007 ms | 0.3 s |

62 s of harness drift sits inside the 60 to 170 s clinical budget and is
indistinguishable from the pipeline falling behind.

**Other properties to avoid:** dropping past `MAX_BURST_PER_SEC` with `seq`
advanced only on send, so the loss is invisible downstream; `producer.send()`
synchronous inside a coroutine, which stalls every stream when the buffer fills;
`_ingest_ts` stamped before the asynchronous send; `recording_start` taken per
(patient, label), which erases real offsets between a patient's signals (T11
inverts this).

**Properties to keep:** the wire schema, ordering by `c_sequence_number`,
rebasing event time to now, preserving genuine gaps, and the defensive null
handling, which documents real DWC data quality.

## 4. What the playback tests must assert

Derived from sections 1 and 3 so the fidelity claim is executable. Every
assertion is parametric in `packet_ms` and runs at both 10000 and 256.

**Packet shape**
- every wave packet carries exactly `packet_ms * target_hz / 1000` samples:
  5000 / 1250 / 625 at 10 s, 128 / 32 / 16 at 256 ms
- packet inter-arrival in event time is `packet_ms`, per track
- remainder carry: no short packets at source-block boundaries, no sample
  duplicated or dropped across a boundary
- total samples emitted equals total samples read divided by stride, exactly
- a synthetic gap in the source is preserved as a gap, not spliced

**Rates**
- per case, wave packets at `1000 / packet_ms` per track, numerics at 1.0/s
- a `standard_anaesthesia` case yields `5 * 1000/packet_ms + 8` records/s

**Content**
- decoded value equals `raw * gain + bias` within float tolerance
- `c_scale_*` and `c_calibration_abs_*` round-trip to the same physical value
- null input samples appear in `c_invalid_samples`, and the index list is
  correct after decimation and re-packetisation
- `c_sequence_number` strictly increases per `(case, label)` with no gaps
- `c_hz` is 62.5, not 62
- all tracks of one case share one event-time offset

**Timing, the open-loop property**
- emission follows absolute deadlines from `t0`; schedule error is bounded, not
  growing with tick index
- under an artificially stalling sink, emissions scheduled per second are
  unchanged: no dropping, no slowing
- adherence verdict transitions `OK`, `DEGRADED`, `INVALID` under injected
  lateness and backpressure, and is recorded in run metadata

**Governance**
- the `cloud` source policy refuses a `dwc` source
- no test depends on anything under `data/`
