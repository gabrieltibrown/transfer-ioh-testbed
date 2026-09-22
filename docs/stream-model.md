# Stream model: PIC iX production feed, VitalDB, and the bridge between them

Status: draft, sprint 01-02. Living document, revise when DWC or PIC iX access lands.

The testbed replays recorded Parquet in order to mock a live Philips waveform
stream. Every number the thesis reports is therefore conditioned on how faithful
that mock is. This document states what the production feed looks like, where
VitalDB differs, and exactly which transformations bridge the difference, so the
fidelity claim is auditable rather than assumed.

Three sources are used, and they are not equally authoritative:

| Source | What it is | Authority |
|---|---|---|
| Philips *Data Export Interface Programming Guide*, X2/MP/MX/FM Rel. L.0, 4535 645 88011 | Vendor spec for the IntelliVue monitor wave/numeric export | Primary, quoted |
| LIVIA `waveform_producer` replay code | Charite code emitting DWC `te_wave` rows to Kafka | Secondary, shows the DWC field set in practice |
| Measurements on 50 cached VitalDB cases | This repo, `data/vitaldb/` | Primary for the VitalDB side |

## 1. What the production feed emits

### 1.1 Caveat on which interface

The quantitative detail below is authoritative for the **IntelliVue Data Export
Interface**, the monitor's own export interface. The production path at Charite
is PIC iX, the central station, and its egress may be HL7, Capsule MDIP, or the
Data Warehouse Connect (DWC) warehouse export rather than DEI directly.

What supports carrying DEI numbers over to DWC: the DWC `te_wave` column set that
LIVIA emits is close to a field-for-field persistence of the DEI wave object
model, as section 1.7 shows. That is strong evidence the same underlying wave
objects are being recorded. It is not proof that PIC iX re-emits them at the same
cadence.

**Open question, cheap to settle:** the distribution of `c_n_samples` in
`te_wave`. If it is 128/64/32/16, DWC is packet-faithful to the monitor and the
model below transfers directly. Anything else means PIC iX or DWC re-aggregates,
and section 3 needs revisiting. Ask the Charite integration team which egress
feeds the pipeline, and run one query when access lands.

### 1.2 Wave delivery: 256 ms packets

The core fact. From "Interpreting Wave Data":

| Wave type | Sample period | Sample size | Array size | Update period | Bandwidth |
|---|---|---|---|---|---|
| 500 samples/s (ECG) | 2 ms | 16 bits | 128 samples | 256 ms | 1064 bytes/s |
| 250 samples/s (compound ECG) | 4 ms | 16 bits | 3 x 64 samples | 256 ms | 1640 bytes/s |
| 125 samples/s | 8 ms | 16 bits | 32 samples | 256 ms | 296 bytes/s |
| 62.5 samples/s | 16 ms | 16 bits | 16 samples | 256 ms | 168 bytes/s |

> "For wave data export, the Computer Client needs to be able to receive observed
> values with 256 ms of wave data in one message."

Every wave type updates on the same 256 ms cadence, so packet **rate** is
constant at 3.906 packets/s per wave regardless of sample rate; only array size
changes. Static and dynamic context attributes arrive separately, one object per
1024 ms.

### 1.3 Channel composition is sensor-dependent and dynamic

There is no fixed channel list. The monitor declares
`P_OPT_DYN_CREATE_OBJECTS` and `P_OPT_DYN_DELETE_OBJECTS` to indicate that "the
number of internal objects (e.g. the number of Numerics) may change
dynamically", and the client polls the dynamic context to discover what exists.
Channels appear and disappear as sensors are attached, switched off or unplugged.
"Entries in the Wave object priority list are ignored if the label does not exist
or the object is not available."

The client selects what it wants via the Wave object priority list, within a hard
budget:

- up to **3 ECG waves at 500 sps**, or one compound 3-channel wave at 250 sps
- up to **8 non-ECG waves** at 125 or 62.5 sps
- bandwidth restrictions apply on top

So per-case channel composition is a negotiated subset of whatever is plugged in.
Modelling every case identically is therefore unrealistic, and specifically
understates the uneven-load mechanism in proposal 2.2.

### 1.4 Numerics: 1 Hz real-time

`POLL_EXT_PERIOD_NU_1SEC` selects "1 sec Real-time Numerics". Averaged variants
exist at 12 s, 60 s and 300 s. So the realistic real-time numeric cadence is
**1 Hz**, not the 1-5 Hz quoted in proposal 2.3, which is a population
observation across recordings rather than an interface property.

`NuObsValue` carries `physio_id`, `state`, `unit_code`, `value` (float).

### 1.5 Validity is explicit, at two levels

Per observation, `MeasurementState` is a bit field:

```
INVALID 0x8000   QUESTIONABLE 0x4000   UNAVAILABLE 0x2000
CALIBRATION_ONGOING 0x1000   TEST_DATA 0x0800   DEMO_DATA 0x0400
VALIDATED_DATA 0x0080   EARLY_INDICATION 0x0040   MSMT_ONGOING 0x0020
```

"The measurement is valid if the first octet of the state is all 0."

Per sample, the Sample Array Fixed Value mechanism marks individual samples:
`SA_FIX_INVALID_MASK`, `SA_FIX_PACER_MASK`, `SA_FIX_DEFIB_MARKER_MASK`,
`SA_FIX_SATURATION`, `SA_FIX_QRS_MASK`.

Invalid data is part of the normal stream, not an error path.

### 1.6 Scaling, and the timestamp precision problem

Raw samples are scaled integers. `ScaleRangeSpec16` gives the linear map:

```c
typedef struct {
    FLOATType lower_absolute_value;  FLOATType upper_absolute_value;
    u_16      lower_scaled_value;    u_16      upper_scaled_value;
} ScaleRangeSpec16;
```

`SaSpec` gives `array_size`, and `SampleType{sample_size, significant_bits}`
where non-significant bits must be masked.

Timing is the part with real consequences for this thesis:

- `RelativeTime` is u_32 at 1/8 ms (125 us) resolution, "the monitor sets the
  Relative Time with a precision of 2 ms"
- **wall clock is far coarser**: the client "can calculate the absolute time
  (wall clock) from a known relation between Absolute Time and Relative Time
  with a precision of about 1 s"
- each poll reply carries `poll_number`, `sequence_no`, `rel_time_stamp` and
  `abs_time_stamp`; `sequence_no` increments per periodic result so the client
  can verify ordering

So **source event time is only good to about 1 second in wall-clock terms**,
while relative time is good to 2 ms. This is almost certainly why LIVIA reports
"duplicates/non-monotonic jumps/clock-reset artifacts" and sorts by
`c_sequence_number` rather than timestamp. It bounds how precisely event-time
staleness can ever be attributed to the pipeline, and it is exactly what
proposal 7.1 anticipates when it says receipt timestamps must be checked before
implementation.

### 1.7 How it reaches the pipeline: the DWC `te_wave` field set

From LIVIA, with the DEI attribute each field corresponds to:

| `te_wave` column | DEI origin | Note |
|---|---|---|
| `p_patnr` | patient/bed identity | Kafka key. Patient, not surgical case |
| `c_label` | `NOM_ATTR_ID_LABEL_STRING` | wave label, e.g. ECG lead, ART |
| `c_sequence_number` | poll `sequence_no` | **ordering authority** |
| `c_time_stamp_wave_sample` | `abs_time_stamp` | event time, ~1 s precision |
| `c_sample_period` | `NOM_ATTR_TIME_PD_SAMP` | ms: 2 / 4 / 8 / 16 |
| `c_hz` | derived | `1000 // c_sample_period` |
| `c_value` | `SaObsValue.array` | the sample array |
| `c_n_samples` | `SaSpec.array_size` | expect 128 / 64 / 32 / 16 |
| `c_scale_lower`, `c_scale_upper` | `ScaleRangeSpec16` scaled | |
| `c_calibration_abs_lower/upper` | `ScaleRangeSpec16` absolute | typed STRING, arrives empty not null |
| `c_unavailable_samples`, `c_invalid_samples` | `SA_FIX_*` / `MeasurementState` | index lists, nested |
| `c_physio_id`, `c_base_physio_id` | `physio_id` | SCADA partition code |
| `c_is_slow_wave`, `c_is_derived`, `c_channel` | context attributes | |

### 1.8 Resulting per-case load

For a plausible standard anaesthesia configuration (1 ECG at 500 sps; ART and
PLETH at 125; AWP and CO2 at 62.5; 8 numerics at 1 Hz):

| | rate | payload |
|---|---|---|
| 5 wave objects at 3.906 packets/s | 19.5 packets/s | 1992 B/s |
| 8 numerics at 1 Hz | 8 records/s | small |
| **per case** | **~27.5 records/s** | **~2.0 KB/s** |
| **50 concurrent cases** | **~1,375 records/s** | **~100 KB/s** |

Bandwidth is negligible. The load is dominated by **record count and per-record
processing**, which is the regime RQ2 is about.

## 2. The gap: VitalDB versus the production feed

Measured on the 50 cached cases in `data/vitaldb/`.

| Dimension | PIC iX / DWC | VitalDB | Gap |
|---|---|---|---|
| Packet cadence | 256 ms, all wave types | **1000 ms**, all wave types | 3.9x coarser |
| Array size | 128 / 64 / 32 / 16 | 500 / 62 / 128 (= 1 s at native rate) | follows from cadence |
| ECG rate | 500 sps | 500 sps (`SNUADC/ECG_II`, `ECG_V5`) | **match** |
| Other waves | 125 or 62.5 sps | **500** (`ART`, `PLETH`), 62.5 (`Primus/AWP`, `CO2`), 128 (`BIS/EEG`) | no native 125; ART and PLETH 4x too fast |
| Numerics | 1 Hz real-time | **0.5 Hz** (Solar8000), 1 Hz (BIS), **0.157 Hz** (Primus) | 2x to 6x too slow, device-dependent |
| Scaling | `ScaleRangeSpec16`, scaled/absolute pairs | `gain`/`bias`, verified `physical = raw*gain + bias` | equivalent linear map, different parameterisation |
| Per-sample validity | `SA_FIX_*` masks, index lists | **nulls inside `ivals`**, 0.01% of samples | same concept, different encoding |
| Validity scope | per-observation `MeasurementState` too | absent | no observation-level state |
| Dropouts | expected, client must detect missing samples | **none**: 1.0000 s cadence, zero gaps > 1.5x block, across all cases checked | VitalDB is unrealistically clean |
| Ordering authority | `c_sequence_number`; timestamps unreliable | `dt` only, monotonic and regular | no sequence number, and no disorder to handle |
| Event-time precision | ~1 s wall clock, 2 ms relative | exact by construction | VitalDB has no timestamp noise |
| Channel composition | dynamic, sensor-dependent, 3 ECG + 8 non-ECG budget | fixed per case for whole recording | no mid-case appearance or disappearance |
| Keying | `p_patnr`, patient/bed | `caseid`, surgical case | patient-to-case mapping needed for DWC, not for VitalDB |
| Case identity | 77 OR and ICU beds | 6,388 cases, 3,643 with ART + ART_MBP + ECG_II | ample |

Two entries matter more than the rest.

**VitalDB is too clean.** No dropouts, perfectly regular cadence, exact
timestamps, no reordering. A pipeline validated only against it will not have
exercised late data, watermark behaviour under disorder, or gap handling, which
are precisely the mechanisms proposal 2.2 identifies as coupling cases together.

**Numerics are slower, waves are faster.** VitalDB over-delivers waveform samples
and under-delivers numerics relative to the real feed. Naive replay would get
per-case load wrong in both directions at once.

## 3. The bridge

Design rule: **emulate the load and timing characteristics of the production
feed, not the clinical signal content.** That is defensible because inference is
a configurable stub in this thesis, so sample values never influence any
measurement. It stops being defensible the moment a real model consumes these
streams, and that limitation is recorded here and in the sprint report.

### 3.1 Transformations

| # | Gap | Transformation |
|---|---|---|
| T1 | 1000 ms blocks vs 256 ms packets | Treat each track as a continuous sample stream (block `dt` plus `i/srate`), re-packetise on fixed 256 ms boundaries, carry the remainder across source-block edges. 1000/256 is not integral, so remainder carry is mandatory, not an optimisation |
| T2 | ART and PLETH at 500 sps vs 125 | Decimate 4:1. **No anti-alias filter**: load fidelity is the goal, spectral fidelity is not. Recorded as a limitation |
| T3 | ECG at 500 sps | Pass through. Already correct |
| T4 | `Primus/AWP`, `CO2` at 62.5 | Pass through. Already correct |
| T5 | Numerics at 0.5 / 0.157 Hz vs 1 Hz | Resample to 1 Hz by zero-order hold, the semantics a monitor already has for a value that has not been refreshed |
| T6 | `gain`/`bias` vs `ScaleRangeSpec16` | Emit both: keep raw scaled ints plus `c_scale_*` and `c_calibration_abs_*` derived from gain and bias, so consumers use the DWC contract |
| T7 | Nulls in `ivals` vs `SA_FIX_*` | Convert null sample positions into `c_invalid_samples` index lists. Preserves the 0.01% rate and the real encoding |
| T8 | No sequence number | Generate a per `(patient, label)` monotonic `c_sequence_number`, as LIVIA does |
| T9 | Fixed composition | Assign a **sensor profile** per case from a small set within the 3 ECG + 8 non-ECG budget, so per-case load is heterogeneous |
| T10 | No dropouts, exact timestamps | **Injectable impairments**, default off: packet loss, timestamp jitter, reordering, module disconnect. Off by default so baseline runs stay clean; on to test watermark behaviour |

### 3.2 Deliberately not emulated, in this sprint

- Mid-case dynamic channel creation and deletion (T9 covers between-case
  heterogeneity only). Realistic, but it varies offered load mid-run and would
  confound the baseline before one exists
- Compound 250 sps ECG. Single-lead 500 sps covers the ECG case
- Clinical signal fidelity, per the design rule above
- `p_patnr` to surgical case mapping. Irrelevant for VitalDB, where one case is
  one recording. Needed only when DWC lands and CORR defines case boundaries

### 3.3 Open items that would change this

1. `c_n_samples` distribution in `te_wave`: confirms or refutes T1
2. Which PIC iX egress actually feeds the pipeline: HL7, MDIP or DWC
3. Whether DWC preserves `c_sequence_number` from the poll `sequence_no`
4. Observed timestamp disorder in real DWC, which sets realistic T10 parameters

## 3.4 What LIVIA's replay job tells us, and where it must not be copied

`real_waveform_producer.py` replays exported DWC Parquet into `dwc-waveform`.
It is a useful schema reference and a useful warning.

**Evidence it provides.** It sets no rate: pacing comes entirely from
`c_time_stamp_wave_sample` spacing. Two details are still informative.
`c_hz = 1000 // c_sample_period` only makes sense against {2, 4, 8, 16} ms, the
Philips ladder, and incidentally mislabels 62.5 sps as 62 Hz. And
`MAX_BURST_PER_SEC = 20` per (patient, label) implies steady-state packet rate
well below 20/s per signal, which rules out per-sample delivery and is
consistent with 3.906/s from 256 ms packets.

**Measured defect: relative-sleep pacing accumulates drift.** 400 concurrent
streams at 256 ms cadence, serializing a 128-sample payload per tick:

| Strategy | Overshoot per packet | Drift over a 3 h case |
|---|---|---|
| `await asyncio.sleep(gap)` | 1.480 ms | 62.4 s |
| absolute deadline schedule | 0.007 ms | 0.3 s |

62 s of harness drift sits inside the 60-170 s clinical budget and is
indistinguishable from the pipeline falling behind. Hence T10 and the
open-loop test assertions below.

**Other properties to avoid:**

- dropping past `MAX_BURST_PER_SEC`, with `seq` advanced only on send, so the
  loss is deliberately invisible downstream
- `producer.send()` is synchronous inside a coroutine; when the buffer fills it
  blocks the event loop and stalls every stream at once
- `_ingest_ts` stamped at record-build time, before the asynchronous send, so it
  excludes producer buffering and batching
- `recording_start` taken per (patient, label), which erases real offsets
  between a patient's signals and degrades further on each loop restart

**Properties to keep:** the wire schema, ordering by `c_sequence_number`,
rebasing event time to now, preserving genuine gaps, and the defensive null
handling, which documents real DWC data quality.

## 4. What the playback tests must assert

Derived directly from sections 1 and 3, so the fidelity claim is executable.

**Packet shape**
- every wave packet carries exactly `256 ms * srate / 1000` samples: 128 at 500
  sps, 32 at 125, 16 at 62.5
- packet inter-arrival in event time is 256 ms +/- tolerance, per track
- remainder carry: no short packets at source-block boundaries, and no sample
  duplicated or dropped across a boundary
- total samples emitted equals total samples read, minus decimation, exactly

**Rates**
- per case, wave packets at 3.906/s per track, numerics at 1.0/s
- a `standard_anaesthesia` case yields ~27.5 records/s within tolerance

**Content**
- decoded value equals `raw * gain + bias` within float tolerance
- `c_scale_*` and `c_calibration_abs_*` round-trip to the same physical value
- null input samples appear in `c_invalid_samples`, and the index list is
  correct after re-packetisation, which is where off-by-one bugs will live
- `c_sequence_number` strictly increases per `(patient, label)` with no gaps

**Timing, the open-loop property**
- emission follows **absolute deadlines** from t0, and schedule error does not
  accumulate over a long run. This is the specific defect in LIVIA's
  `await asyncio.sleep(gap)`
- under artificial downstream stalling, offered rate is unchanged: no dropping,
  no slowing. Contrast with LIVIA's `MAX_BURST_PER_SEC` drop
- schedule adherence is recorded per run, and a run that fell behind is marked
  invalid rather than silently reported

**Governance**
- the `cloud` source policy refuses a `dwc` source
- no test writes anything under `data/` into git
