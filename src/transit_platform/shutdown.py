"""Turn SIGTERM and SIGINT into a stop event that long-running loops check."""

from __future__ import annotations

import logging
import signal
import threading
from types import FrameType

logger = logging.getLogger(__name__)


def stop_event_on_signals() -> threading.Event:
    stop = threading.Event()

    def request_stop(signum: int, _frame: FrameType | None) -> None:
        logger.info("event=stop_requested signal=%s", signal.Signals(signum).name)
        stop.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    return stop
