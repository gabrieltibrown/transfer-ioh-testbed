"""End-to-end against the compose broker. Opt-in: ``uv run pytest -m kafka``.
Skipped when no broker answers on localhost:9092."""

import asyncio
import json
import socket
import time
import uuid

import numpy as np
import pytest

from ioh_testbed.replay.config import WAVE
from ioh_testbed.replay.packetize import WavePacket
from ioh_testbed.replay.pacer import TOPIC_NUMERIC, TOPIC_WAVE, Pacer
from ioh_testbed.replay.reader import SampleStream
from ioh_testbed.replay.schedule import CasePlan, Emission, Schedule, StreamEmitter

BOOTSTRAP = "localhost:9092"
pytestmark = pytest.mark.kafka


def _broker_up() -> bool:
    try:
        with socket.create_connection(("localhost", 9092), timeout=1.0):
            return True
    except OSError:
        return False


@pytest.fixture(scope="module")
def broker():
    if not _broker_up():
        pytest.skip("no Kafka broker on localhost:9092; start src/compose first")
    from ioh_testbed.replay.kafka_sink import ensure_topics
    return ensure_topics(BOOTSTRAP, partitions=8)


def small_schedule(case_id: str, n_wave=20, n_num=20, cadence=0.02):
    pk = WavePacket("SNUADC/ART", 0.0, 500.0, np.zeros(128), np.empty(0, int), np.empty(0, int))
    stream = SampleStream("SNUADC/ART", 500.0, 1.0, 0.0, "mmHg", [])
    w = StreamEmitter(case_id, "ART", WAVE, "mmHg", stream, (Emission(i * cadence, pk) for i in range(n_wave)))
    n = StreamEmitter(case_id, "HR", "numeric", "/min", None, (Emission(i * cadence, 80.0) for i in range(n_num)))
    return Schedule([CasePlan(case_id, "p", 0.0, [w, n], ())], 10.0, 1.0, 256)


def consume_all(topic: str, key: bytes, timeout_s=15.0):
    from confluent_kafka import Consumer, TopicPartition
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"t-{uuid.uuid4().hex}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    parts = [TopicPartition(topic, p.id, 0) for p in c.list_topics(topic, timeout=10).topics[topic].partitions.values()]
    c.assign(parts)
    out, deadline = [], time.time() + timeout_s
    while time.time() < deadline:
        m = c.poll(0.5)
        if m is None or m.error():
            continue
        if m.key() == key:
            out.append(m)
    c.close()
    return out


def test_topics_have_log_append_time(broker):
    for t in (TOPIC_WAVE, TOPIC_NUMERIC):
        assert broker[t]["message.timestamp.type"] == "LogAppendTime"
        assert broker[t]["partitions"] >= 1


def test_round_trip_counts_keys_and_broker_timestamps(broker):
    from confluent_kafka import TIMESTAMP_LOG_APPEND_TIME
    from ioh_testbed.replay.kafka_sink import KafkaSink

    case_id = f"t{uuid.uuid4().hex[:8]}"
    sink = KafkaSink(BOOTSTRAP)
    stats = asyncio.run(Pacer(sink, tolerance_ms=50.0).run(small_schedule(case_id)))
    assert stats.n_scheduled == 40 and stats.n_backpressure == 0 and stats.n_undelivered == 0
    assert stats.sink["delivered"] == 40 and stats.sink["failed"] == 0
    assert stats.verdict == "OK", stats.summary()

    waves = consume_all(TOPIC_WAVE, case_id.encode())
    nums = consume_all(TOPIC_NUMERIC, case_id.encode())
    assert len(waves) == 20 and len(nums) == 20

    for m in waves + nums:
        ts_type, ts_ms = m.timestamp()
        rec = json.loads(m.value())
        assert ts_type == TIMESTAMP_LOG_APPEND_TIME
        # Broker append time is the ingress reference: never before the producer handed it over.
        # Same host, same clock; allow 1 ms for broker timestamp granularity.
        assert ts_ms / 1000.0 >= rec["_t_produce"] - 0.001, (ts_ms / 1000.0, rec["_t_produce"])
        assert rec["_case_id"] == case_id
    seqs = sorted(json.loads(m.value())["c_sequence_number"] for m in waves)
    assert seqs == list(range(1, 21))
