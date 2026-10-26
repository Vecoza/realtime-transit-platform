"""Wait until the sink and the aggregator have consumed the raw topic to its end.

Used after a replay, for example by the demo: exits 0 once both consumer groups have committed
every partition up to its end, and 1 if that has not happened within the timeout.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

from processor.aggregate import kafka_storage_offsets
from transit_platform.config import AggregatorSettings, ProcessorSettings, StreamSettings
from transit_platform.logs import configure_logging

logger = logging.getLogger(__name__)


def remaining(bounds: dict[int, tuple[int, int]], committed: dict[int, int]) -> int:
    """Messages between each partition's committed offset (or its start) and its end."""
    return sum(
        max(high - max(committed.get(partition, low), low), 0)
        for partition, (low, high) in bounds.items()
    )


def partition_bounds(consumer: Consumer, topic: str) -> dict[int, tuple[int, int]]:
    partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
    return {
        p: consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10) for p in partitions
    }


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--timeout", type=float, default=900.0, help="seconds to wait at most")
    args = parser.parse_args(argv)
    stream = StreamSettings.from_env()
    groups = (ProcessorSettings.from_env().group_id, AggregatorSettings.from_env().group_id)
    admin = AdminClient({"bootstrap.servers": stream.bootstrap_servers})
    consumer = Consumer({"bootstrap.servers": stream.bootstrap_servers, "group.id": "wait"})
    deadline = time.monotonic() + args.timeout
    try:
        while True:
            bounds = partition_bounds(consumer, stream.raw_topic)
            behind = {
                group: remaining(
                    bounds, kafka_storage_offsets(admin, group, stream.raw_topic)(list(bounds))
                )
                for group in groups
            }
            logger.info("event=waiting %s", " ".join(f"{g}={n}" for g, n in behind.items()))
            if not any(behind.values()):
                print(f"{' and '.join(groups)} have consumed {stream.raw_topic} to its end")
                return 0
            if time.monotonic() >= deadline:
                print(f"still behind after {args.timeout:.0f} s: {behind}", file=sys.stderr)
                return 1
            time.sleep(2)
    finally:
        consumer.close()


if __name__ == "__main__":
    sys.exit(main())
