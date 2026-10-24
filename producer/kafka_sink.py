"""Publishing envelopes to the raw HFP topic with an idempotent Kafka producer."""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field

from confluent_kafka import KafkaError, KafkaException, Message, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from producer.hfp import Envelope
from transit_platform.config import StreamSettings

logger = logging.getLogger(__name__)

PRODUCER_CONFIG = {
    "enable.idempotence": True,
    "acks": "all",
    "linger.ms": 50,
    "compression.type": "zstd",
    "delivery.timeout.ms": 120_000,
}


def ensure_topic(settings: StreamSettings, timeout_s: float = 30.0) -> None:
    """Create the raw topic if it does not exist yet; an existing topic is left as it is."""
    admin = AdminClient({"bootstrap.servers": settings.bootstrap_servers})
    topic = NewTopic(
        settings.raw_topic,
        num_partitions=settings.raw_partitions,
        replication_factor=1,
        config={"retention.ms": str(settings.raw_retention_ms)},
    )
    future = admin.create_topics([topic], operation_timeout=timeout_s)[settings.raw_topic]
    try:
        future.result(timeout=timeout_s)
        logger.info(
            "event=topic_created topic=%s partitions=%d",
            settings.raw_topic,
            settings.raw_partitions,
        )
    except KafkaException as error:
        if error.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise
        logger.info("event=topic_exists topic=%s", settings.raw_topic)


@dataclass
class DeliveryStats:
    sent: int = 0
    delivered: int = 0
    failed: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def add(self, sent: int = 0, delivered: int = 0, failed: int = 0) -> None:
        with self.lock:
            self.sent += sent
            self.delivered += delivered
            self.failed += failed


class KafkaSink:
    """Sends envelopes keyed by vehicle; delivery results are counted in ``stats``.

    ``send`` may be called from the MQTT thread. Delivery callbacks run in whichever thread
    calls ``poll`` or ``flush``.
    """

    def __init__(self, settings: StreamSettings, producer: Producer | None = None) -> None:
        self.topic = settings.raw_topic
        self.producer = producer or Producer(
            {"bootstrap.servers": settings.bootstrap_servers, **PRODUCER_CONFIG}
        )
        self.stats = DeliveryStats()

    def send(self, envelope: Envelope) -> None:
        while True:
            try:
                self.producer.produce(
                    self.topic,
                    key=envelope.key,
                    value=envelope.to_json().encode(),
                    on_delivery=self.on_delivery,
                )
                break
            except BufferError:
                self.producer.poll(0.1)
        self.stats.add(sent=1)

    def on_delivery(self, error: KafkaError | None, message: Message) -> None:
        if error is None:
            self.stats.add(delivered=1)
        else:
            self.stats.add(failed=1)
            logger.error("event=delivery_failed key=%r error=%s", message.key(), error)

    def poll(self, timeout_s: float) -> None:
        self.producer.poll(timeout_s)

    def flush(self, timeout_s: float = 30.0) -> int:
        """Wait for outstanding deliveries; returns how many are still undelivered."""
        remaining = self.producer.flush(timeout_s)
        logger.info(
            "event=producer_flushed sent=%d delivered=%d failed=%d undelivered=%d",
            self.stats.sent,
            self.stats.delivered,
            self.stats.failed,
            remaining,
        )
        return remaining
