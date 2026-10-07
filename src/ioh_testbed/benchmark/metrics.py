"""The three headline metrics and the latency decomposition, as pure functions.

Pinned definitions (sprint 03-06 plan; proposal 6). Every term is in seconds on
the **host** clock. Kafka and Flink run on the Docker VM clock, which is
``offset_s`` ahead of the host; VM-stamped times are converted with
``t_host = t_vm - offset_s`` before any cross-domain subtraction. Event time is
host-stamped by the harness and needs no conversion.

    T_pipeline      t_receipt - host(tAppendNewest)       processing-time latency of a
                                                          window result (Karimov et al. 2018)
    staleness       t_receipt - tEventNewest              age of the newest evidence at delivery
    progress_lag    host(tWall) - tEventNewestSeen        per case, per heartbeat; the slope of
                                                          this series over the window is the
                                                          Theodolite stability signal

Decomposition of T_pipeline, in order along the path:

    window_wait     host(tWindowFired) - host(tAppendNewest)   ingress of the newest record to
                                                               window firing; includes the
                                                               watermark wait
    queue_excess    host(tWindowFired) - (windowEnd + bound)   the part of window_wait that is
                                                               not the unavoidable watermark wait
    inference       tReturned - tSent                          same clock, no offset
    sink            host(tPredAppend) - host(tReturned)        Flink exit to broker append
    fetch           t_receipt - host(tPredAppend)              broker to interface

and the offset-free check ``T_pipeline_broker = tPredAppend - tAppendNewest``,
both broker-stamped, which must equal ``T_pipeline - fetch``.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

MS = 1000.0


@dataclass(frozen=True)
class Clock:
    """Host-to-VM clock relation: ``vm = host + offset_s``."""

    offset_s: float = 0.0

    def host(self, t_vm_ms: float) -> float:
        return t_vm_ms / MS - self.offset_s


def prediction_terms(p: dict, clock: Clock, bound_ms: float) -> dict:
    """``p`` is one line of predictions.jsonl: the Flink record plus the consumer's
    ``t_receipt`` (host seconds) and ``t_pred_append_ms`` (broker)."""
    inf = p.get("inference", {})
    t_receipt = float(p["t_receipt"])
    t_append_newest = clock.host(p["tAppendNewestMs"])
    t_fired = clock.host(p["tWindowFiredMs"])
    t_pred_append = clock.host(p["t_pred_append_ms"])
    t_sent = inf.get("tSentMs")
    t_ret = inf.get("tReturnedMs")
    out = {
        "case_id": p["caseId"],
        "window_end": p["windowEndMs"] / MS,
        "partial": bool(p.get("partial", False)),
        "status": inf.get("status"),
        "t_pipeline": t_receipt - t_append_newest,
        "t_pipeline_broker": (p["t_pred_append_ms"] - p["tAppendNewestMs"]) / MS,
        "staleness": t_receipt - p["tEventNewestMs"] / MS,
        "window_wait": t_fired - t_append_newest,
        "queue_excess": t_fired - (p["windowEndMs"] + bound_ms) / MS,
        "inference": (t_ret - t_sent) / MS if t_sent and t_ret else float("nan"),
        "sink": t_pred_append - clock.host(t_ret) if t_ret else float("nan"),
        "fetch": t_receipt - t_pred_append,
    }
    stub = inf.get("stub") or {}
    out["stub_queue_wait"] = stub.get("queue_wait_ms", float("nan")) / MS
    out["stub_service"] = stub.get("service_ms", float("nan")) / MS
    return out


def progress_lag(h: dict, clock: Clock) -> float:
    return clock.host(h["tWallMs"]) - h["tEventNewestSeenMs"] / MS


def percentiles(x, qs=(50, 90, 99)) -> dict:
    a = np.asarray([v for v in x if not np.isnan(v)], dtype=float)  # drop NaN
    if a.size == 0:
        return {"N": 0}
    d = {"N": int(a.size)}
    for q in qs:
        d[f"p{q}"] = float(np.percentile(a, q))
    d["max"] = float(a.max())
    d["mean"] = float(a.mean())
    return d


def slope(t: np.ndarray, y: np.ndarray) -> dict:
    """Least-squares slope of y on t (units of y per second of t), with r^2 and
    the fitted change over the span, the Theodolite stability statistic."""
    t = np.asarray(t, dtype=float)
    y = np.asarray(y, dtype=float)
    if t.size < 3:
        return {"n": int(t.size), "slope": float("nan"), "r2": float("nan"), "span_s": 0.0, "fitted_change": float("nan")}
    tc = t - t.mean()
    denom = float((tc * tc).sum())
    if denom == 0.0:
        return {"n": int(t.size), "slope": float("nan"), "r2": float("nan"), "span_s": 0.0, "fitted_change": float("nan")}
    b = float((tc * (y - y.mean())).sum() / denom)
    yhat = y.mean() + b * tc
    ss_res = float(((y - yhat) ** 2).sum())
    ss_tot = float(((y - y.mean()) ** 2).sum())
    span = float(t.max() - t.min())
    return {
        "n": int(t.size),
        "slope": b,
        "r2": 1.0 - ss_res / ss_tot if ss_tot > 0 else float("nan"),
        "span_s": span,
        "fitted_change": b * span,
    }
