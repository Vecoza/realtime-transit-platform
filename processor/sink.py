"""Storage sink: consume raw HFP messages, store valid events, dead-letter the rest.

Each batch follows the same order:

1. consume up to ``batch_max_messages`` or until ``batch_max_ms`` has passed;
2. drop exact copies within the batch, then decode and validate; rejected messages go to the
   dead-letter topic, which is flushed;
3. write the events and the batch metrics in one database transaction and commit it;
4. commit the Kafka offsets of the batch.

A crash before step 3 completes loses nothing: the offsets were not committed, so the batch
is consumed again. A crash between steps 3 and 4 replays a batch that is already stored, and
the unique keys turn the second insert into a no-op. Dead-lettered messages can appear twice
in that case; the dead-letter topic is at least once.
"""

from __future__ import annotations

import json
import logging
import statistics
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime

import psycopg
from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    Producer,
    TopicPartition,
)

from processor.decode import Event, Rejection, decode
from processor.store import (
    WriteResult,
    ensure_partitions,
    event_days,
    upcoming_days,
    write_events,
)
from producer.hfp import Envelope
from producer.kafka_sink import PRODUCER_CONFIG, create_topic
from transit_platform.config import DatabaseSettings, ProcessorSettings, StreamSettings
from transit_platform.logs import configure_logging
from transit_platform.shutdown import stop_event_on_signals

logger = logging.getLogger(__name__)

DLQ_RETENTION_MS = 7 * 24 * 60 * 60 * 1000


@dataclass
class Hooks:
    """Points where tests can interrupt a batch to simulate a crash."""

    before_db_commit: Callable[[], None] = lambda: None
    before_offset_commit: Callable[[], None] = lambda: None


@dataclass
class BatchOutcome:
    messages: int
    invalid: int
    result: WriteResult
    timings_ms: dict[str, int] = field(default_factory=dict)


class StorageSink:
    def __init__(
        self,
        consumer: Consumer,
        dlq: Producer,
        conn: psycopg.Connection,
        settings: ProcessorSettings,
        hooks: Hooks | None = None,
    ) -> None:
        self.consumer = consumer
        self.dlq = dlq
        self.conn = conn
        self.settings = settings
        self.hooks = hooks or Hooks()
        days = upcoming_days(datetime.now(UTC).date())
        ensure_partitions(conn, days)
        self.known_days: set[date] = set(days)

    def run(self, stop: threading.Event) -> None:
        while not stop.is_set():
            self.process_batch()

    def process_batch(self) -> BatchOutcome | None:
        started = time.perf_counter()
        messages = [
            message
            for message in self.consumer.consume(
                self.settings.batch_max_messages, self.settings.batch_max_ms / 1000
            )
            if not self._is_error(message)
        ]
        if not messages:
            return None
        polled = time.perf_counter()

        events: list[Event] = []
        rejected = copies = 0
        seen: set[tuple[str, str]] = set()
        for message in messages:
            outcome = parse_envelope(message)
            if isinstance(outcome, Envelope):
                marker = (outcome.topic, outcome.payload)
                if marker in seen:
                    copies += 1
                    continue
                seen.add(marker)
                outcome = decode(outcome)
            if isinstance(outcome, Rejection):
                self._dead_letter(message, outcome)
                rejected += 1
            else:
                events.append(outcome)
        if rejected:
            self._flush_dead_letters()
        decoded = time.perf_counter()

        new_days = event_days(events) - self.known_days
        if new_days:
            ensure_partitions(self.conn, new_days)
            self.known_days |= new_days
        with self.conn.transaction():
            result = write_events(self.conn, events).with_copies(copies)
            timings = {
                "poll_ms": _ms(started, polled),
                "decode_ms": _ms(polled, decoded),
                "db_ms": _ms(decoded, time.perf_counter()),
            }
            self._record_metrics(len(messages), rejected, result, timings)
            self.hooks.before_db_commit()
        self.hooks.before_offset_commit()
        self.consumer.commit(offsets=next_offsets(messages), asynchronous=False)
        outcome = BatchOutcome(len(messages), rejected, result, timings)
        logger.info(
            "event=batch_committed messages=%d inserted=%d duplicates=%d invalid=%d "
            "unmatched=%d poll_ms=%d decode_ms=%d db_ms=%d",
            len(messages),
            result.inserted,
            result.duplicates,
            rejected,
            result.unmatched,
            timings["poll_ms"],
            timings["decode_ms"],
            timings["db_ms"],
        )
        return outcome

    def _is_error(self, message: Message) -> bool:
        error = message.error()
        if error is None:
            return False
        if error.fatal():
            raise KafkaException(error)
        if error.code() != KafkaError._PARTITION_EOF:
            logger.error("event=consume_error error=%s", error)
        return True

    def _dead_letter(self, message: Message, rejection: Rejection) -> None:
        record = {
            "reason": rejection.reason,
            "detail": rejection.detail,
            "source": {
                "topic": message.topic(),
                "partition": message.partition(),
                "offset": message.offset(),
            },
            "original": (message.value() or b"").decode("utf-8", errors="replace"),
        }
        self.dlq.produce(
            self.settings.dlq_topic, key=message.key(), value=json.dumps(record).encode()
        )

    def _flush_dead_letters(self) -> None:
        if self.dlq.flush(30) != 0:
            raise RuntimeError("dead-letter messages were not delivered; batch not committed")

    def _record_metrics(
        self, messages: int, invalid: int, result: WriteResult, timings: dict[str, int]
    ) -> None:
        p50, p95 = percentiles(result.latencies_ms)
        self.conn.execute(
            """
            INSERT INTO meta.consumer_metrics (
                group_id, messages, invalid, inserted, duplicates, unmatched, consumer_lag,
                poll_ms, decode_ms, db_ms, latency_p50_ms, latency_p95_ms
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                self.settings.group_id,
                messages,
                invalid,
                result.inserted,
                result.duplicates,
                result.unmatched,
                self.consumer_lag(),
                timings["poll_ms"],
                timings["decode_ms"],
                timings["db_ms"],
                p50,
                p95,
            ),
        )

    def consumer_lag(self) -> int | None:
        """Messages behind the end of each assigned partition, from cached watermarks."""
        try:
            lag = 0
            for position in self.consumer.position(self.consumer.assignment()):
                _low, high = self.consumer.get_watermark_offsets(position, cached=True)
                if position.offset >= 0 and high >= 0:
                    lag += max(high - position.offset, 0)
            return lag
        except Exception:
            return None


def parse_envelope(message: Message) -> Envelope | Rejection:
    try:
        return Envelope.from_json(message.value())
    except (ValueError, KeyError, TypeError) as error:
        return Rejection("invalid_envelope", str(error))


def next_offsets(messages: list[Message]) -> list[TopicPartition]:
    """For each partition in the batch, the offset after its last message."""
    highest: dict[tuple[str, int], int] = {}
    for message in messages:
        key = (message.topic(), message.partition())
        highest[key] = max(highest.get(key, -1), message.offset())
    return [
        TopicPartition(topic, partition, offset + 1)
        for (topic, partition), offset in highest.items()
    ]


def percentiles(values: list[int]) -> tuple[int | None, int | None]:
    if len(values) < 2:
        return (values[0], values[0]) if values else (None, None)
    cuts = statistics.quantiles(values, n=20, method="inclusive")
    return round(statistics.median(values)), round(cuts[18])


def _ms(start: float, end: float) -> int:
    return round((end - start) * 1000)


def consumer_config(stream: StreamSettings, settings: ProcessorSettings) -> dict[str, object]:
    """Manual offset commits and, with an instance id, static group membership.

    A static member that restarts within the session timeout gets its partitions back at
    once, without a rebalance. A second process with the same id is fenced off.
    """
    config: dict[str, object] = {
        "bootstrap.servers": stream.bootstrap_servers,
        "group.id": settings.group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "partition.assignment.strategy": "cooperative-sticky",
    }
    if settings.instance_id:
        config["group.instance.id"] = settings.instance_id
    return config


def build_consumer(stream: StreamSettings, settings: ProcessorSettings) -> Consumer:
    consumer = Consumer(consumer_config(stream, settings))
    consumer.subscribe(
        [stream.raw_topic],
        on_assign=lambda _c, parts: logger.info("event=assigned partitions=%s", _names(parts)),
        on_revoke=lambda _c, parts: logger.info("event=revoked partitions=%s", _names(parts)),
    )
    return consumer


def _names(partitions: list[TopicPartition]) -> str:
    return ",".join(f"{p.topic}[{p.partition}]" for p in partitions)


def main() -> int:
    configure_logging()
    stop = stop_event_on_signals()
    stream = StreamSettings.from_env()
    settings = ProcessorSettings.from_env()
    create_topic(stream.bootstrap_servers, settings.dlq_topic, 1, DLQ_RETENTION_MS)
    consumer = build_consumer(stream, settings)
    dlq = Producer({"bootstrap.servers": stream.bootstrap_servers, **PRODUCER_CONFIG})
    with psycopg.connect(DatabaseSettings.from_env().conninfo, autocommit=True) as conn:
        try:
            StorageSink(consumer, dlq, conn, settings).run(stop)
        finally:
            consumer.close()
            dlq.flush(30)
    return 0


if __name__ == "__main__":
    sys.exit(main())
