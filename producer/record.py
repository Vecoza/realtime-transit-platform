"""Command line entry point: record the live HFP stream to hourly gzipped JSON lines files."""

from __future__ import annotations

import logging
import sys
import time

from producer.mqtt_source import HfpSubscriber
from producer.recording import RecordingWriter
from transit_platform.config import SourceSettings
from transit_platform.logs import configure_logging
from transit_platform.shutdown import stop_event_on_signals

logger = logging.getLogger(__name__)

STATS_INTERVAL_S = 30.0


def main() -> int:
    configure_logging()
    stop = stop_event_on_signals()
    settings = SourceSettings.from_env()
    writer = RecordingWriter(settings.recordings_dir)
    subscriber = HfpSubscriber(settings, writer.write)
    subscriber.start()
    last_count, last_time = 0, time.monotonic()
    try:
        while not stop.wait(STATS_INTERVAL_S):
            now, count = time.monotonic(), writer.written
            logger.info(
                "event=recording_stats written=%d rate_per_s=%.1f connected=%s",
                count,
                (count - last_count) / (now - last_time),
                subscriber.connected,
            )
            last_count, last_time = count, now
    finally:
        subscriber.stop()
        writer.close()
    print(f"recorded {writer.written} messages to {settings.recordings_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
