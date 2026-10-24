"""Command line entry point: forward the live HFP stream from MQTT to the raw Kafka topic."""

from __future__ import annotations

import logging
import sys
import time

from producer.kafka_sink import KafkaSink, ensure_topic
from producer.mqtt_source import HfpSubscriber
from transit_platform.config import SourceSettings, StreamSettings
from transit_platform.logs import configure_logging
from transit_platform.shutdown import stop_event_on_signals

logger = logging.getLogger(__name__)

STATS_INTERVAL_S = 10.0
POLL_TIMEOUT_S = 0.5


def main() -> int:
    configure_logging()
    stop = stop_event_on_signals()
    stream = StreamSettings.from_env()
    ensure_topic(stream)
    sink = KafkaSink(stream)
    subscriber = HfpSubscriber(SourceSettings.from_env(), sink.send)
    subscriber.start()
    last_sent, last_time = 0, time.monotonic()
    try:
        while not stop.is_set():
            sink.poll(POLL_TIMEOUT_S)
            now = time.monotonic()
            if now - last_time >= STATS_INTERVAL_S:
                stats = sink.stats
                logger.info(
                    "event=producer_stats sent=%d delivered=%d failed=%d rate_per_s=%.1f "
                    "connected=%s",
                    stats.sent,
                    stats.delivered,
                    stats.failed,
                    (stats.sent - last_sent) / (now - last_time),
                    subscriber.connected,
                )
                last_sent, last_time = stats.sent, now
    finally:
        subscriber.stop()
        undelivered = sink.flush()
    return 1 if undelivered else 0


if __name__ == "__main__":
    sys.exit(main())
