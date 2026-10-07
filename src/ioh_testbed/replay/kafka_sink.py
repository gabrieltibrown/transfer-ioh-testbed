"""Kafka sink for the pacer, on confluent-kafka (librdkafka).

Non-blocking by construction, which is what the open-loop pacer requires:
``produce()`` enqueues and returns; when the local queue is full it raises
``BufferError``, which the pacer counts rather than waits on. Delivery
callbacks run inside ``poll()``, which the pacer calls once per emission, so
the event loop is never blocked by the producer.

The broker stamps ``LogAppendTime`` on each record (topic config, see
src/compose/docker-compose.yml). That timestamp is the testbed's ingress
reference, so this sink deliberately does not set a message timestamp.

Compression is off: it would change the byte profile of the load under test.
"""

from __future__ import annotations

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from .pacer import TOPIC_NUMERIC, TOPIC_WAVE

TOPICS = (TOPIC_WAVE, TOPIC_NUMERIC)


class KafkaSink:
    def __init__(
        self,
        bootstrap: str,
        *,
        acks: str = "all",
        linger_ms: int = 5,
        queue_max_messages: int = 100_000,
        queue_max_kbytes: int = 1_048_576,
        extra: dict | None = None,
    ):
        conf = {
            "bootstrap.servers": bootstrap,
            "acks": acks,
            "linger.ms": linger_ms,
            "compression.type": "none",
            "queue.buffering.max.messages": queue_max_messages,
            "queue.buffering.max.kbytes": queue_max_kbytes,
            "enable.idempotence": False,
            "client.id": "ioh-replay",
        }
        if extra:
            conf.update(extra)
        self.conf = conf
        self._producer = Producer(conf)
        self.delivered = 0
        self.failed = 0
        self.bytes = 0
        self.last_error: str | None = None

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self.failed += 1
            self.last_error = str(err)
        else:
            self.delivered += 1
            self.bytes += len(msg.value())

    def emit(self, topic: str, key: bytes, value: bytes) -> None:
        # Raises BufferError when the local queue is full; the pacer handles it.
        self._producer.produce(topic, value=value, key=key, on_delivery=self._on_delivery)

    def poll(self) -> None:
        self._producer.poll(0)

    def flush(self, timeout_s: float) -> int:
        return int(self._producer.flush(timeout_s))

    def stats(self) -> dict:
        return {
            "sink": "kafka",
            "bootstrap": self.conf["bootstrap.servers"],
            "acks": self.conf["acks"],
            "linger_ms": self.conf["linger.ms"],
            "delivered": self.delivered,
            "failed": self.failed,
            "bytes": self.bytes,
            "last_error": self.last_error,
        }


def ensure_topics(bootstrap: str, partitions: int, timeout_s: float = 30.0) -> dict[str, dict]:
    """Create the testbed topics if missing, with LogAppendTime, and return each
    topic's partition count and timestamp type as actually configured on the broker.
    Idempotent. A mismatch with the requested partition count is reported, not fixed:
    changing partitions on a live topic would silently change the experiment."""
    from confluent_kafka.admin import ConfigResource

    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = admin.list_topics(timeout=timeout_s).topics
    to_create = [
        NewTopic(t, num_partitions=partitions, replication_factor=1,
                 config={"message.timestamp.type": "LogAppendTime"})
        for t in TOPICS if t not in existing
    ]
    if to_create:
        for t, fut in admin.create_topics(to_create).items():
            try:
                fut.result(timeout_s)
            except KafkaException as e:  # pragma: no cover
                if "already exists" not in str(e).lower():
                    raise
    meta = admin.list_topics(timeout=timeout_s).topics
    cfgs = admin.describe_configs([ConfigResource(ConfigResource.Type.TOPIC, t) for t in TOPICS])
    out = {}
    for res, fut in cfgs.items():
        c = fut.result(timeout_s)
        out[res.name] = {
            "partitions": len(meta[res.name].partitions),
            "message.timestamp.type": c["message.timestamp.type"].value,
        }
    return out
