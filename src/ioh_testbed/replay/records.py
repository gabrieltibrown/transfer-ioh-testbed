"""Wire records: the DWC ``te_wave`` field set for waveforms, an analogous record
for numerics, and the sequence counter that is the downstream ordering authority.

Field names follow LIVIA's ``real_waveform_producer.py`` so the Flink job built
in sprint 2 is DWC-compatible from day one; swapping VitalDB for DWC then touches
only the reader. Testbed-specific fields carry a leading underscore, as LIVIA's
own ``_source`` / ``_ingest_ts`` do. Timing is split explicitly:

    _event_ts   rebased event time of the first sample (epoch seconds)
    _event_ts_last  event time of the last sample in a wave packet; the newest
                evidence a consumer holds once the packet arrives
    _t_sched    the harness's scheduled emission deadline
    _t_produce  wall time when the record was handed to the producer

and the authoritative testbed ingress, Kafka's ``LogAppendTime``, is stamped by
the broker and therefore not in the payload. See docs/stream-model.md.
"""

from __future__ import annotations

import json
import math
from collections import defaultdict
from datetime import UTC, datetime, timezone

import numpy as np

from .packetize import WavePacket
from .reader import SampleStream

SOURCE_TAG = "ioh-testbed-replay"

# ScaleRangeSpec16 parameterisation: scaled range is the full int16 range, and the
# absolute pair is chosen so the DWC linear map reproduces raw*gain + bias exactly.
SCALE_LOWER, SCALE_UPPER = -32768, 32767

_DWC_TS_FMT = "%Y-%m-%d %H:%M:%S.%f"


def format_dwc_ts(epoch_s: float) -> str:
    """DWC string timestamp, always UTC, millisecond precision: '2024-09-26 23:41:53.480 +00:00'."""
    dt = datetime.fromtimestamp(epoch_s, tz=UTC)
    return dt.strftime(_DWC_TS_FMT)[:-3] + " +00:00"


def parse_dwc_ts(s: str) -> float:
    body, _, offset = s.rpartition(" ")
    sign = 1 if offset.startswith("+") else -1
    oh, om = offset[1:].split(":")
    tz = timezone(sign * (datetime.strptime(f"{oh}:{om}", "%H:%M") - datetime(1900, 1, 1)))
    return datetime.strptime(body, _DWC_TS_FMT).replace(tzinfo=tz).timestamp()


class SequenceCounter:
    """Per (case, label) monotonic sequence numbers. Every emitted record consumes
    one; nothing is ever skipped, so a gap downstream means loss in transit."""

    def __init__(self) -> None:
        self._next: dict[tuple[str, str], int] = defaultdict(lambda: 1)

    def next(self, case_id: str, label: str) -> int:
        k = (case_id, label)
        n = self._next[k]
        self._next[k] = n + 1
        return n


def wave_record(
    pkt: WavePacket,
    stream: SampleStream,
    *,
    case_id: str,
    label: str,
    seq: int,
    event_ts: float,
    packet_ms: int,
    t_sched: float | None = None,
    t_produce: float | None = None,
) -> dict:
    sample_period_ms = 1000.0 / pkt.srate
    values = np.where(np.isnan(pkt.values), 0, pkt.values)
    return {
        "p_patnr": case_id,
        "c_label": label,
        "c_sequence_number": seq,
        "c_time_stamp_wave_sample": format_dwc_ts(event_ts),
        "c_sample_period": sample_period_ms if sample_period_ms != int(sample_period_ms) else int(sample_period_ms),
        "c_hz": float(pkt.srate),  # float: 62.5 stays 62.5, unlike LIVIA's 1000 // period
        "c_unit_label": stream.unit,
        "c_value": [int(v) for v in values],
        "c_n_samples": pkt.n,
        "c_scale_lower": SCALE_LOWER,
        "c_scale_upper": SCALE_UPPER,
        "c_calibration_abs_lower": SCALE_LOWER * stream.gain + stream.bias,
        "c_calibration_abs_upper": SCALE_UPPER * stream.gain + stream.bias,
        "c_invalid_samples": pkt.invalid.tolist(),
        "c_unavailable_samples": pkt.unavailable.tolist(),
        "c_unparsable_wave_sample": False,
        "_case_id": case_id,
        "_source": SOURCE_TAG,
        "_packet_ms": packet_ms,
        "_event_ts": event_ts,
        "_event_ts_last": event_ts + (pkt.n - 1) / pkt.srate,
        "_t_sched": t_sched,
        "_t_produce": t_produce,
    }


def numeric_record(
    *,
    case_id: str,
    label: str,
    seq: int,
    event_ts: float,
    value: float,
    unit: str,
    t_sched: float | None = None,
    t_produce: float | None = None,
) -> dict:
    """Numeric record. DWC's numeric table schema is not public; this mirrors the
    wave record's conventions and is our design, marked as such."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        raise ValueError(f"{case_id}/{label}: numeric value must be finite, got {value!r}")
    return {
        "p_patnr": case_id,
        "c_label": label,
        "c_sequence_number": seq,
        "c_time_stamp": format_dwc_ts(event_ts),
        "c_value": float(value),
        "c_unit_label": unit,
        "_case_id": case_id,
        "_source": SOURCE_TAG,
        "_event_ts": event_ts,
        "_t_sched": t_sched,
        "_t_produce": t_produce,
    }


def decode_dwc(raw: float, rec: dict) -> float:
    """The consumer-side ScaleRangeSpec16 linear map, for tests and downstream reference."""
    lo_s, hi_s = rec["c_scale_lower"], rec["c_scale_upper"]
    lo_a, hi_a = rec["c_calibration_abs_lower"], rec["c_calibration_abs_upper"]
    return lo_a + (raw - lo_s) * (hi_a - lo_a) / (hi_s - lo_s)


def to_json_bytes(rec: dict) -> bytes:
    # allow_nan=False: a NaN that reached the wire would be a bug, not data.
    return json.dumps(rec, separators=(",", ":"), allow_nan=False).encode("utf-8")
