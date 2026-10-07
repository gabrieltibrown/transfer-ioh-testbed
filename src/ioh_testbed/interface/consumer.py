"""The minimal interface: the prediction-receipt measurement boundary.

The proposal's fallback for the TRANSFER clinician interface. It consumes the
``predictions`` and ``progress`` topics from ``latest``, stamps each message with
the host wall clock on receipt (``t_receipt``) and the broker's LogAppendTime of
the message (``t_pred_append_ms``), and appends one JSON line per message to the
run folder. It computes nothing else; analysis is offline
(``benchmark/analyze.py``) so the consumer's own cost stays negligible.

Run it before the replay starts and stop it with SIGINT/SIGTERM or
``--duration``. ``ioh-run`` does both.
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import time
import uuid
from pathlib import Path

TOPIC_PREDICTIONS = "predictions"
TOPIC_PROGRESS = "progress"


def parse_args(argv=None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(prog="ioh-interface", description=__doc__.split("\n\n")[0])
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--out", type=Path, required=True, help="run folder; predictions.jsonl and progress.jsonl are appended")
    ap.add_argument("--duration", type=float, default=None, help="stop after this many seconds (default: until signalled)")
    ap.add_argument("--report-every", type=float, default=10.0)
    ap.add_argument("--ready-file", type=Path, default=None, help="touched once partitions are assigned at latest")
    return ap.parse_args(argv)


def run(args) -> int:
    from confluent_kafka import OFFSET_END, Consumer, TopicPartition

    args.out.mkdir(parents=True, exist_ok=True)
    c = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": f"ioh-interface-{uuid.uuid4().hex[:8]}",
        "enable.auto.commit": False,
        "auto.offset.reset": "latest",
        "fetch.wait.max.ms": 5,  # poll latency is part of the fetch term; keep it small and stated
    })
    parts = []
    md = c.list_topics(timeout=10)
    for topic in (TOPIC_PREDICTIONS, TOPIC_PROGRESS):
        if topic not in md.topics:
            print(f"topic {topic} missing", file=sys.stderr)
            return 2
        parts += [TopicPartition(topic, p, OFFSET_END) for p in md.topics[topic].partitions]
    c.assign(parts)
    if args.ready_file:
        args.ready_file.touch()

    stop = {"flag": False}

    def _sig(*_):
        stop["flag"] = True

    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    files = {
        TOPIC_PREDICTIONS: (args.out / "predictions.jsonl").open("a"),
        TOPIC_PROGRESS: (args.out / "progress.jsonl").open("a"),
    }
    counts = {TOPIC_PREDICTIONS: 0, TOPIC_PROGRESS: 0}
    t_start = time.time()
    next_report = t_start + args.report_every
    try:
        while not stop["flag"]:
            if args.duration is not None and time.time() - t_start >= args.duration:
                break
            m = c.poll(0.1)
            now = time.time()
            if m is None or m.error():
                if now >= next_report:
                    _report(counts, now - t_start)
                    next_report = now + args.report_every
                continue
            ts_type, ts_ms = m.timestamp()
            rec = json.loads(m.value())
            rec["t_receipt"] = now
            rec["t_pred_append_ms"] = ts_ms
            rec["append_ts_type"] = int(ts_type)
            rec["partition"] = m.partition()
            rec["offset"] = m.offset()
            f = files[m.topic()]
            f.write(json.dumps(rec, separators=(",", ":")) + "\n")
            counts[m.topic()] += 1
            if now >= next_report:
                _report(counts, now - t_start)
                for fh in files.values():
                    fh.flush()
                next_report = now + args.report_every
    finally:
        for fh in files.values():
            fh.close()
        c.close()
    _report(counts, time.time() - t_start)
    return 0


def _report(counts: dict, elapsed: float) -> None:
    print(
        f"[interface] {elapsed:7.1f} s  predictions={counts[TOPIC_PREDICTIONS]}  progress={counts[TOPIC_PROGRESS]}",
        file=sys.stderr,
    )


def main(argv=None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
