"""Tap: turn the pipeline's own Kafka records into what the demo page draws.

Pure functions first (tested on synthetic records), then a consumer thread
that routes records by key to beds and keeps a rolling state per bed. Nothing
here is measured that the benchmark does not measure: ingress delay is
``t_append - _t_sched``, prediction latency is ``t_receipt - tAppendNewest``
with the clock offset applied, completeness is received versus due.
"""

from __future__ import annotations

import json
import math
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field

import numpy as np

from ..replay.records import decode_dwc

TOPICS = ("dwc-waveform", "dwc-numeric", "predictions", "progress")
DRAW_BUCKET_S = 0.01  # min/max pairs per 10 ms: 100 pairs/s whatever the sample rate


# ---------------------------------------------------------------- pure functions

def reduce_wave(rec: dict, bucket_s: float = DRAW_BUCKET_S) -> dict:
    """Decode a wave record to physical units and reduce it for drawing: one
    ``[t, min, max]`` per bucket, with invalid and unavailable samples as gaps
    (``null``). Rates at or below one sample per bucket pass through as
    ``[t, v, v]``."""
    n = int(rec["c_n_samples"])
    hz = float(rec["c_hz"])
    t0 = float(rec["_event_ts"])
    raw = np.asarray(rec["c_value"], dtype=float)
    phys = decode_dwc(raw, rec)
    bad = np.zeros(n, dtype=bool)
    for idx in (rec.get("c_invalid_samples") or [], rec.get("c_unavailable_samples") or []):
        if idx:
            bad[np.asarray(idx, dtype=int)] = True
    per = max(1, int(round(bucket_s * hz)))
    points = []
    for start in range(0, n, per):
        chunk = phys[start:start + per]
        ok = ~bad[start:start + per]
        t = t0 + start / hz
        if ok.any():
            v = chunk[ok]
            points.append([round(t, 3), float(v.min()), float(v.max())])
        else:
            points.append([round(t, 3), None, None])
    return {
        "label": rec["c_label"],
        "hz": hz,
        "unit": rec.get("c_unit_label", ""),
        "t_first": t0,
        "t_last": float(rec.get("_event_ts_last", t0 + (n - 1) / hz)),
        "n": n,
        "n_invalid": len(rec.get("c_invalid_samples") or []),
        "n_unavailable": len(rec.get("c_unavailable_samples") or []),
        "points": points,
    }


def ingress_terms(rec: dict, t_append_s: float) -> dict:
    """Per-record ingress figures, seconds. ``delay`` is harness lateness plus
    producer plus broker; ``staleness`` is how old the newest sample was when
    the broker stamped it."""
    t_sched = rec.get("_t_sched")
    t_last = rec.get("_event_ts_last", rec.get("_event_ts"))
    return {
        "delay": (t_append_s - float(t_sched)) if t_sched is not None else float("nan"),
        "staleness": (t_append_s - float(t_last)) if t_last is not None else float("nan"),
    }


def percentile(xs, q: float) -> float | None:
    """None, not NaN, when there is nothing to summarise: NaN is not JSON and the
    browser rejects a whole message that contains it."""
    a = np.asarray([x for x in xs if not (isinstance(x, float) and math.isnan(x))], dtype=float)
    return float(np.percentile(a, q)) if a.size else None


PREDICTION_GRACE_S = 2.0  # wall seconds a due window may take to arrive before it counts as missing


def records_due(last_event_s: float, channel_first: dict[str, float], channel_rates: dict[str, float]) -> float:
    """Records the schedule implies so far: for each channel this case has produced,
    its rate times the time since that channel's own first record. A channel that
    was never connected is absent from the recording, not missing; one that
    connects late is counted from when it did."""
    return sum(
        channel_rates.get(label, 0.0) * max(0.0, last_event_s - t_first)
        for label, t_first in channel_first.items()
    )


def windows_due(event_now_s: float, first_event_s: float, window_s: float, slide_s: float, grace_s: float = 0.0) -> int:
    """Full windows that could have fired by ``event_now`` for a case whose first
    event was at ``first_event``, on the epoch-aligned slide grid. ``grace_s``
    allows for delivery time so a window is not counted missing while in flight."""
    event_now_s -= grace_s
    # The first full window starts at the first slide-grid point at or after the
    # first event and ends one window later; windows fire once event time passes the end.
    first_full_end = math.ceil(first_event_s / slide_s) * slide_s + window_s
    if event_now_s < first_full_end:
        return 0
    return int((event_now_s - first_full_end) // slide_s) + 1


# ---------------------------------------------------------------- per-bed state

@dataclass
class BedState:
    key: str
    channel_rates: dict[str, float] = field(default_factory=dict)  # label -> records/s at this grain
    speed: float = 1.0
    started_wall: float = 0.0
    first_event: float | None = None
    last_event: float | None = None
    waves: dict[str, deque] = field(default_factory=dict)      # label -> deque of reduced packets
    numerics: dict[str, deque] = field(default_factory=dict)   # label -> deque of (t, v)
    units: dict[str, str] = field(default_factory=dict)
    recent_ingress: deque = field(default_factory=lambda: deque(maxlen=2000))  # (t_wall, delay, staleness)
    n_records: int = 0
    n_wave: int = 0
    n_numeric: int = 0
    n_invalid_samples: int = 0
    n_samples: int = 0
    channels_seen: set = field(default_factory=set)
    channel_first: dict[str, float] = field(default_factory=dict)  # label -> event time of its first record
    predictions: deque = field(default_factory=lambda: deque(maxlen=200))
    progress: deque = field(default_factory=lambda: deque(maxlen=600))
    n_pred_by_status: dict[str, int] = field(default_factory=dict)
    # new material since the last frame, so frames carry deltas
    new_waves: list = field(default_factory=list)
    new_numerics: list = field(default_factory=list)
    new_predictions: list = field(default_factory=list)

    def reset(self) -> None:
        self.__init__(self.key, self.channel_rates, self.speed)

    def add_wave(self, rec: dict, t_append_s: float) -> None:
        red = reduce_wave(rec)
        self.waves.setdefault(red["label"], deque(maxlen=64)).append(red)
        self.units[red["label"]] = red["unit"]
        self.new_waves.append(red)
        self.n_wave += 1
        self.n_samples += red["n"]
        self.n_invalid_samples += red["n_invalid"]
        self._seen(rec, t_append_s, red["t_first"], red["t_last"])

    def add_numeric(self, rec: dict, t_append_s: float) -> None:
        t = float(rec["_event_ts"])
        v = float(rec["c_value"])
        self.numerics.setdefault(rec["c_label"], deque(maxlen=600)).append((t, v))
        self.units[rec["c_label"]] = rec.get("c_unit_label", "")
        self.new_numerics.append([rec["c_label"], round(t, 3), v])
        self.n_numeric += 1
        self._seen(rec, t_append_s, t, t)

    def _seen(self, rec: dict, t_append_s: float, t_first: float, t_last: float) -> None:
        self.n_records += 1
        self.channels_seen.add(rec["c_label"])
        self.channel_first[rec["c_label"]] = min(self.channel_first.get(rec["c_label"], t_first), t_first)
        self.first_event = t_first if self.first_event is None else min(self.first_event, t_first)
        self.last_event = t_last if self.last_event is None else max(self.last_event, t_last)
        terms = ingress_terms(rec, t_append_s)
        self.recent_ingress.append((time.time(), terms["delay"], terms["staleness"]))

    def add_prediction(self, rec: dict, t_receipt: float, t_pred_append_s: float, offset_s: float) -> None:
        inf = rec.get("inference", {})
        status = inf.get("status", "?")
        p = {
            "window_end": rec["windowEndMs"] / 1000.0,
            "window_start": rec["windowStartMs"] / 1000.0,
            "partial": bool(rec.get("partial")),
            "status": status,
            "risk": inf.get("risk"),
            "t_receipt": t_receipt,
            "t_pipeline": t_receipt - (rec["tAppendNewestMs"] / 1000.0 - offset_s),
            "staleness": t_receipt - rec["tEventNewestMs"] / 1000.0,
            "inference_s": ((inf.get("tReturnedMs") or 0) - (inf.get("tSentMs") or 0)) / 1000.0 if inf.get("tSentMs") else None,
            "n_records": rec.get("nRecords"),
        }
        self.predictions.append(p)
        self.new_predictions.append(p)
        self.n_pred_by_status[status] = self.n_pred_by_status.get(status, 0) + 1

    def add_progress(self, rec: dict, offset_s: float) -> None:
        t_wall = rec["tWallMs"] / 1000.0 - offset_s
        self.progress.append((t_wall, t_wall - rec["tEventNewestSeenMs"] / 1000.0, rec.get("recordsSeen", 0)))

    def stats(self, window_s: float, slide_s: float, now: float | None = None) -> dict:
        """Everything the page shows except the waveform and numeric series. Does
        not consume frame deltas, so the snapshot endpoint can call it freely."""
        now = now or time.time()
        recent = [x for x in self.recent_ingress if now - x[0] <= 30.0]
        elapsed_patient = (self.last_event - self.first_event) if self.first_event is not None and self.last_event else 0.0
        due = records_due(self.last_event, self.channel_first, self.channel_rates) if self.last_event else 0.0
        preds_ok = [p for p in self.predictions if p["status"] == "ok" and not p["partial"]]
        n_due = (windows_due(self.last_event, self.first_event, window_s, slide_s, PREDICTION_GRACE_S * self.speed)
                 if self.first_event is not None and self.last_event else 0)
        n_full = sum(1 for p in self.predictions if not p["partial"])
        prog = self.progress[-1] if self.progress else None
        return {
            "key": self.key,
            "first_event": self.first_event,
            "last_event": self.last_event,
            "elapsed_patient_s": elapsed_patient,
            "units": self.units,
            "ingress": {
                "n_records": self.n_records,
                "n_wave": self.n_wave,
                "n_numeric": self.n_numeric,
                "records_per_s_30s": len(recent) / 30.0,
                "delay_p50": percentile([x[1] for x in recent], 50),
                "delay_p99": percentile([x[1] for x in recent], 99),
                "staleness_p50": percentile([x[2] for x in recent], 50),
                "completeness": min(1.0, self.n_records / due) if due > 0 else None,
                "invalid_fraction": self.n_invalid_samples / self.n_samples if self.n_samples else 0.0,
                "channels": sorted(self.channels_seen),
                "channels_expected": sorted(self.channel_rates),
            },
            "prediction_stats": {
                "n": len(self.predictions),
                "n_full": n_full,
                "n_due": n_due,
                "completeness": min(1.0, n_full / n_due) if n_due > 0 else None,
                "by_status": dict(self.n_pred_by_status),
                "latency_p50": percentile([p["t_pipeline"] for p in preds_ok], 50),
                "latency_p99": percentile([p["t_pipeline"] for p in preds_ok], 99),
                "staleness_p50": percentile([p["staleness"] for p in preds_ok], 50),
                "last_risk": next((p["risk"] for p in reversed(self.predictions) if p["status"] == "ok"), None),
            },
            "progress_lag_s": prog[1] if prog else None,
        }

    def frame(self, window_s: float, slide_s: float, now: float | None = None, delta: bool = True) -> dict:
        out = self.stats(window_s, slide_s, now)
        if delta:
            waves, numerics, preds = self.new_waves, self.new_numerics, self.new_predictions
            self.new_waves, self.new_numerics, self.new_predictions = [], [], []
        else:
            waves = [p for d in self.waves.values() for p in d]
            numerics = [[label, round(t, 3), v] for label, d in self.numerics.items() for t, v in d]
            preds = list(self.predictions)
        out.update({"waves": waves, "numerics": numerics, "predictions": preds})
        return out


# ---------------------------------------------------------------- consumer thread

class Tap(threading.Thread):
    """Consumes the four topics from ``latest`` and routes by key to bed states.
    ``beds`` maps Kafka key (case id with suffix) to a BedState; keys not in the
    map are ignored."""

    def __init__(self, bootstrap: str, beds: dict[str, BedState], offset_s: float = 0.0):
        super().__init__(daemon=True, name="tap")
        self.bootstrap = bootstrap
        self.beds = beds
        self.offset_s = offset_s
        self.lock = threading.Lock()
        self.stop_flag = threading.Event()
        self.n_seen = 0
        self.error: str | None = None

    def run(self) -> None:
        from confluent_kafka import OFFSET_END, Consumer, TopicPartition

        c = Consumer({
            "bootstrap.servers": self.bootstrap,
            "group.id": f"ioh-demo-{uuid.uuid4().hex[:8]}",
            "enable.auto.commit": False,
            "auto.offset.reset": "latest",
            "fetch.wait.max.ms": 10,
        })
        try:
            md = c.list_topics(timeout=10)
            parts = [TopicPartition(t, p, OFFSET_END) for t in TOPICS if t in md.topics for p in md.topics[t].partitions]
            c.assign(parts)
            while not self.stop_flag.is_set():
                m = c.poll(0.05)
                if m is None or m.error():
                    continue
                key = (m.key() or b"").decode()
                bed = self.beds.get(key)
                if bed is None:
                    continue
                self.n_seen += 1
                _, ts_ms = m.timestamp()
                t_append = ts_ms / 1000.0 - self.offset_s
                rec = json.loads(m.value())
                with self.lock:
                    topic = m.topic()
                    if topic == "dwc-waveform":
                        bed.add_wave(rec, t_append)
                    elif topic == "dwc-numeric":
                        bed.add_numeric(rec, t_append)
                    elif topic == "predictions":
                        bed.add_prediction(rec, time.time(), t_append, self.offset_s)
                    elif topic == "progress":
                        bed.add_progress(rec, self.offset_s)
        except Exception as e:  # noqa: BLE001
            self.error = str(e)
        finally:
            c.close()

    def stop(self) -> None:
        self.stop_flag.set()

