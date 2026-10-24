"""Publish a recording to the raw topic, keeping the recorded spacing between messages.

``speed`` scales time: 1 is real time, 10 is ten times faster, and 0 publishes as fast as
the producer allows. Envelopes are published unchanged, including their original
``received_at``, so replaying the same recording always produces the same messages.
"""

from __future__ import annotations

import argparse
import logging
import sys
import threading
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path

from producer.hfp import Envelope
from producer.kafka_sink import KafkaSink, ensure_topic
from producer.recording import read_envelopes, recording_files
from transit_platform.config import StreamSettings
from transit_platform.logs import configure_logging
from transit_platform.shutdown import stop_event_on_signals

logger = logging.getLogger(__name__)

POLL_EVERY = 1000


@dataclass(frozen=True)
class ReplayResult:
    published: int
    seconds: float

    @property
    def rate_per_s(self) -> float:
        return self.published / self.seconds if self.seconds else 0.0


def replay(
    envelopes: Iterable[Envelope],
    send: Callable[[Envelope], None],
    speed: float,
    stop: threading.Event,
    clock: Callable[[], float] = time.monotonic,
    poll: Callable[[], None] = lambda: None,
) -> ReplayResult:
    """Send envelopes in order, waiting until each one's scaled offset from the first."""
    started = clock()
    first: float | None = None
    published = 0
    for envelope in envelopes:
        if stop.is_set():
            break
        if speed > 0:
            recorded = envelope.received_datetime.timestamp()
            first = recorded if first is None else first
            delay = started + (recorded - first) / speed - clock()
            if delay > 0 and stop.wait(delay):
                break
        send(envelope)
        published += 1
        if published % POLL_EVERY == 0:
            poll()
    return ReplayResult(published, clock() - started)


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path, help="recording file or directory of recordings")
    parser.add_argument("--speed", type=float, default=1.0, help="time factor; 0 = no waiting")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = parse_args(argv)
    stop = stop_event_on_signals()
    settings = StreamSettings.from_env()
    ensure_topic(settings)
    sink = KafkaSink(settings)
    try:
        result = replay(
            read_envelopes(recording_files(args.source)),
            sink.send,
            args.speed,
            stop,
            poll=lambda: sink.poll(0),
        )
    finally:
        undelivered = sink.flush()
    logger.info(
        "event=replay_finished published=%d seconds=%.1f rate_per_s=%.0f speed=%s",
        result.published,
        result.seconds,
        result.rate_per_s,
        args.speed,
    )
    print(f"replayed {result.published} messages in {result.seconds:.1f}s")
    return 1 if undelivered or sink.stats.failed else 0


if __name__ == "__main__":
    sys.exit(main())
