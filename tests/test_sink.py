import json
import threading
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, replace
from pathlib import Path

import psycopg
import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient

from processor.decode import Event, decode
from processor.sink import Hooks, StorageSink, build_consumer
from producer.kafka_sink import PRODUCER_CONFIG, KafkaSink, create_topic, ensure_topic
from producer.recording import read_envelopes
from producer.replay import replay
from transit_platform.config import DatabaseSettings, ProcessorSettings, StreamSettings

pytestmark = [pytest.mark.db, pytest.mark.stream]

FIXTURE = Path(__file__).parent / "fixtures" / "hfp_sample.jsonl"
BATCH = 400


class Crash(Exception):
    """Stands in for the process dying at a chosen point."""


def expected_rows() -> tuple[int, int]:
    keys = {"vp": set(), "stop": set()}
    for envelope in read_envelopes([FIXTURE]):
        event = decode(envelope)
        if isinstance(event, Event):
            table = "vp" if event.event_type == "vp" else "stop"
            keys[table].add(
                (
                    event.operator_id,
                    event.vehicle_number,
                    event.event_type,
                    event.tst,
                    event.payload_hash,
                )
            )
    return len(keys["vp"]), len(keys["stop"])


@dataclass
class Pipeline:
    stream: StreamSettings
    processor: ProcessorSettings
    database: DatabaseSettings

    def publish_fixture(self) -> int:
        sink = KafkaSink(self.stream)
        result = replay(
            read_envelopes([FIXTURE]), sink.send, 0, threading.Event(), poll=lambda: sink.poll(0)
        )
        assert sink.flush() == 0
        return result.published

    def run_sink(self, until_messages: int, hooks: Hooks | None = None) -> int:
        """Process batches until ``until_messages`` were committed; returns that count."""
        consumer = build_consumer(self.stream, self.processor)
        dlq = Producer({"bootstrap.servers": self.stream.bootstrap_servers, **PRODUCER_CONFIG})
        processed = idle = 0
        try:
            with psycopg.connect(self.database.conninfo, autocommit=True) as conn:
                sink = StorageSink(consumer, dlq, conn, self.processor, hooks)
                while processed < until_messages and idle < 40:
                    outcome = sink.process_batch()
                    if outcome is None:
                        idle += 1
                    else:
                        processed += outcome.messages
        finally:
            consumer.close()
        return processed

    def counts(self) -> tuple[int, int]:
        with psycopg.connect(self.database.conninfo) as conn:
            positions = conn.execute("SELECT count(*) FROM rt.vehicle_positions").fetchone()[0]
            stops = conn.execute("SELECT count(*) FROM rt.stop_events").fetchone()[0]
        return positions, stops

    def dead_letters(self) -> list[dict]:
        consumer = Consumer(
            {
                "bootstrap.servers": self.stream.bootstrap_servers,
                "group.id": f"dlq-reader-{uuid.uuid4().hex[:6]}",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([self.processor.dlq_topic])
        records = []
        for _ in range(20):
            message = consumer.poll(1)
            if message is not None and message.error() is None:
                records.append(json.loads(message.value()))
            elif records:
                break
        consumer.close()
        return records


@pytest.fixture
def pipeline(
    stream_settings: StreamSettings, migrated: DatabaseSettings, conn: psycopg.Connection
) -> Iterator[Pipeline]:
    suffix = uuid.uuid4().hex[:8]
    stream = replace(stream_settings, raw_topic=f"test.hfp.raw.{suffix}")
    processor = ProcessorSettings(
        group_id=f"test-sink-{suffix}",
        dlq_topic=f"test.hfp.dlq.{suffix}",
        batch_max_messages=BATCH,
        batch_max_ms=500,
    )
    ensure_topic(stream)
    create_topic(stream.bootstrap_servers, processor.dlq_topic, 1, 3_600_000)
    yield Pipeline(stream, processor, migrated)
    admin = AdminClient({"bootstrap.servers": stream.bootstrap_servers})
    for future in admin.delete_topics([stream.raw_topic, processor.dlq_topic]).values():
        future.result(timeout=30)


def test_fixture_is_stored_once_and_invalid_messages_are_dead_lettered(
    pipeline: Pipeline,
) -> None:
    published = pipeline.publish_fixture()

    assert pipeline.run_sink(published) == published

    assert pipeline.counts() == expected_rows()
    reasons = sorted(record["reason"] for record in pipeline.dead_letters())
    assert reasons == ["invalid_json", "schema_violation"]
    with psycopg.connect(pipeline.database.conninfo) as conn:
        messages, inserted, duplicates, invalid = conn.execute(
            "SELECT sum(messages), sum(inserted), sum(duplicates), sum(invalid)"
            " FROM meta.consumer_metrics"
        ).fetchone()
    assert (messages, invalid) == (published, 2)
    assert inserted == sum(expected_rows())
    assert duplicates == published - 2 - inserted


def test_replaying_the_fixture_again_changes_nothing(pipeline: Pipeline) -> None:
    published = pipeline.publish_fixture()
    pipeline.run_sink(published)
    first = pipeline.counts()

    pipeline.publish_fixture()
    assert pipeline.run_sink(published) == published

    assert pipeline.counts() == first == expected_rows()


def test_crash_after_database_commit_before_offset_commit_creates_no_duplicates(
    pipeline: Pipeline,
) -> None:
    published = pipeline.publish_fixture()
    crashes = {"left": 1}

    def crash_once() -> None:
        if crashes["left"]:
            crashes["left"] -= 1
            raise Crash

    with pytest.raises(Crash):
        pipeline.run_sink(published, Hooks(before_offset_commit=crash_once))
    stored_before_restart = sum(pipeline.counts())
    assert stored_before_restart > 0

    assert pipeline.run_sink(published) == published

    assert pipeline.counts() == expected_rows()
    with psycopg.connect(pipeline.database.conninfo) as conn:
        (messages,) = conn.execute("SELECT sum(messages) FROM meta.consumer_metrics").fetchone()
    assert messages > published


def test_crash_before_database_commit_loses_nothing(pipeline: Pipeline) -> None:
    published = pipeline.publish_fixture()

    def crash() -> None:
        raise Crash

    with pytest.raises(Crash):
        pipeline.run_sink(published, Hooks(before_db_commit=crash))
    assert pipeline.counts() == (0, 0)

    assert pipeline.run_sink(published) == published

    assert pipeline.counts() == expected_rows()
