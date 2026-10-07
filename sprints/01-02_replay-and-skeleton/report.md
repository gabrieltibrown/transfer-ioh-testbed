# Sprint 01-02 report: replay harness, Kafka, calibration

Proposal weeks 1-2, rescoped on 2026-10-06 (see `plan.md`). Companion files:
`findings.md` (facts), `configs/` (frozen inputs), `results/` (run artefacts).

## What was built

A replay harness that emits recorded cases as a live monitor feed, open-loop,
into Kafka, with every run stamped for reproducibility.

| Component | Where | What it settles |
|---|---|---|
| Source model | `docs/stream-model.md`, `docs/philips-data-egress.md` | what the production feed looks like, where VitalDB differs, how the gap is bridged |
| Data cache + manifest | `replay/fetch_vitaldb.py`, `manifest.json` | 50 VitalDB cases selected on Parquet content, SHA-256 recorded |
| Config model | `replay/config.py`, `configs/` | `packet_ms` required per run; sensor profiles within the Philips wave budget |
| Reader | `replay/reader.py` | row-per-block Parquet into gap-preserving segments; window-cropped, float32 |
| Packetizer | `replay/packetize.py` | fixed-cadence packets, decimation, ZOH numerics, `invalid` vs `unavailable` samples |
| Wire records | `replay/records.py` | DWC `te_wave` field set, `ScaleRangeSpec16` round-trip, gapless sequence numbers |
| Schedule + pacer | `replay/schedule.py`, `replay/pacer.py` | one `t0`, per-case offset, min-heap of absolute deadlines, hybrid spin wait, adherence verdict |
| Kafka | `replay/kafka_sink.py`, `src/compose/` | non-blocking producer, `LogAppendTime` topics, pinned single broker with explicit limits |
| Provenance | `benchmark/stamp.py` | git, config hashes, host and Docker VM descriptor, raw lateness samples per run |
| Calibration | `ioh-replay --calibrate` | the harness's own ceiling per host and sink |

80 tests, none needing data or a broker; `-m vitaldb` and `-m kafka` opt in.

## Definitions pinned

These change the headline numbers and were ambiguous in the proposal. They are
now fixed in code and must not be silently revised.

- **Testbed ingress** is Kafka `LogAppendTime`, stamped by the broker. Pipeline
  latency in sprint 2 is `t_receipt - t_append`. The harness's own
  `t_sched`/`t_produce` are carried in the record so producer delay and harness
  lateness are separately observable and never contaminate `T_pipeline`.
- **Event time** is rebased once per run: `event_ts = t0 + (t_source - case.t_start) / speed`,
  the same offset for every stream of a case. At `speed != 1` latency results
  are invalid and the metadata says so.
- **Concurrency** is controlled: all selected cases start at `t0` for one
  observation window; shorter cases are ineligible.
- **Packet cadence** is a required input. Baseline `dwc_10s` (10 s, derived from
  published DWC row counts), sensitivity `dei_256ms` (the monitor's own export
  cadence). Not a swept axis.
- **Validity** has two kinds: `invalid` (acquired, null in the source) and
  `unavailable` (padding past a segment end), mirroring DWC's two index lists.
  Every packet is exactly `packet_ms * target_hz / 1000` samples.
- **Open loop** is enforced, not assumed: absolute deadlines, no dropping, a
  non-blocking sink, and a per-run verdict (`OK` / `DEGRADED` / `INVALID`) with
  cadence-relative thresholds.

## Results

Full tables and interpretation in `findings.md`; artefacts in `results/`.

**The harness is not the bottleneck.** On this laptop the Kafka-sink ceiling
is ~30,400 records/s (31 MB/s) for 10 s packets and ~12,500 records/s for
256 ms packets, with no record ever refused by the broker. Fifty concurrent
cases need ~425 and ~1,375 records/s respectively: 70x and 9x headroom. Any
operating boundary found locally in sprint 2 below those ceilings belongs to
the pipeline, not the instrument.

**The real-time run reconciles exactly.** 5 cases, 300 s, speed 1, into
Kafka: 9,265 records scheduled, emitted, delivered and counted on the broker,
verdict `OK`, harness lateness p50 0.33 ms and p99 9.7 ms against a 10 s
cadence. The definition of done for sprint 1 is met.

**Two measured facts about the host.** The harness is JSON-bound near 33 MB/s
regardless of sink for large packets, and per-record producer overhead bound
near 12.5k records/s for small ones. macOS timer coalescing leaves a ~17 ms
p99 floor at low rates, which motivated cadence-relative verdict thresholds.

## What sprint 1 leaves ready for sprint 2

- Topics `dwc-waveform` and `dwc-numeric`, keyed by case id, `LogAppendTime`,
  partition count as a config input (a proposal 2.2 interference variable)
- A JSON wire schema matching LIVIA's DWC field set, so the Flink job is
  DWC-compatible on day one; swapping the source touches only `reader.py`
- `_t_sched`, `_t_produce`, `_event_ts` in every record; `_packet_ms` so a
  consumer knows the staleness floor it is operating under
- Per-host harness ceilings, which bound what any later experiment on this
  laptop may claim

## Moved to sprint 2

Flink job (Java, Maven in Docker, async I/O verification, window definition),
inference stub, interface sink, latency and staleness analysis, impairment
injection beyond the T10 flag, cloud provisioning, DWC reader.

## Limitations recorded

- Load fidelity, not signal fidelity: decimation is by stride without an
  anti-alias filter; numerics are zero-order held. Defensible only while
  inference is a stub.
- The 10 s cadence is a derived estimate from another hospital's aggregates. The
  `c_n_samples` distribution in `te_wave` settles it when DWC access lands.
- The dev laptop's Docker VM is 3.83 GB; sprint 2's Flink components will need
  measuring against that ceiling or a different host.
- macOS timer coalescing puts a ~17 ms floor under p99 harness lateness at low
  rates; immaterial to the cadences in use, and ~1 ms on Linux.
