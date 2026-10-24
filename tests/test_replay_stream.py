import threading
import uuid
from collections import defaultdict
from collections.abc import Callable, Iterator
from dataclasses import replace
from pathlib import Path

import pytest
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

from producer.kafka_sink import KafkaSink, ensure_topic
from producer.recording import read_envelopes
from producer.replay import replay
from transit_platform.config import StreamSettings

pytestmark = pytest.mark.stream

FIXTURE = Path(__file__).parent / "fixtures" / "hfp_sample.jsonl"


@pytest.fixture
def new_topic(stream_settings: StreamSettings) -> Iterator[Callable[[], StreamSettings]]:
    """Settings pointing at a fresh raw topic per call; the topics are deleted afterwards."""
    created: list[str] = []

    def make() -> StreamSettings:
        settings = replace(stream_settings, raw_topic=f"test.hfp.raw.{uuid.uuid4().hex[:8]}")
        created.append(settings.raw_topic)
        return settings

    yield make
    admin = AdminClient({"bootstrap.servers": stream_settings.bootstrap_servers})
    for future in admin.delete_topics(created).values():
        future.result(timeout=30)


def replay_into(topic_settings: StreamSettings) -> StreamSettings:
    ensure_topic(topic_settings)
    sink = KafkaSink(topic_settings)
    replay(read_envelopes([FIXTURE]), sink.send, 0, threading.Event(), poll=lambda: sink.poll(0))
    assert sink.flush() == 0
    assert sink.stats.failed == 0
    return topic_settings


def consume_all(settings: StreamSettings) -> list[tuple[int, int, bytes | None, bytes]]:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": f"test-{uuid.uuid4().hex[:8]}",
            "enable.auto.commit": False,
        }
    )
    metadata = consumer.list_topics(settings.raw_topic, timeout=10)
    partitions = sorted(metadata.topics[settings.raw_topic].partitions)
    ends = {
        p: consumer.get_watermark_offsets(TopicPartition(settings.raw_topic, p))[1]
        for p in partitions
    }
    consumer.assign([TopicPartition(settings.raw_topic, p, 0) for p in partitions])
    messages = []
    remaining = sum(ends.values())
    while remaining:
        message = consumer.poll(10)
        assert message is not None, "timed out before reading every message"
        assert message.error() is None
        messages.append((message.partition(), message.offset(), message.key(), message.value()))
        remaining -= 1
    consumer.close()
    return sorted(messages)


def test_replayed_fixture_arrives_complete_and_in_order_per_vehicle(
    new_topic: Callable[[], StreamSettings],
) -> None:
    expected = list(read_envelopes([FIXTURE]))

    received = consume_all(replay_into(new_topic()))

    assert len(received) == len(expected)
    partitions_per_key = defaultdict(set)
    for partition, _offset, key, _value in received:
        partitions_per_key[key].add(partition)
    assert all(len(partitions) == 1 for partitions in partitions_per_key.values())

    expected_per_key = defaultdict(list)
    for envelope in expected:
        expected_per_key[envelope.key].append(envelope.to_json().encode())
    received_per_key = defaultdict(list)
    for _partition, _offset, key, value in received:
        received_per_key[key].append(value)
    assert received_per_key == expected_per_key


def test_replaying_twice_gives_identical_partitions_offsets_and_bytes(
    new_topic: Callable[[], StreamSettings],
) -> None:
    first = consume_all(replay_into(new_topic()))
    second = consume_all(replay_into(new_topic()))

    assert first == second
