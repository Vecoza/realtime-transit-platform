"""Recordings: envelopes as gzipped JSON lines, one file per UTC hour."""

from __future__ import annotations

import gzip
import logging
import threading
import zlib
from collections.abc import Iterable, Iterator
from datetime import datetime
from pathlib import Path
from typing import TextIO

from producer.hfp import Envelope

logger = logging.getLogger(__name__)


def hour_file(directory: Path, moment: datetime) -> Path:
    return directory / f"hfp-{moment:%Y%m%dT%H}.jsonl.gz"


class RecordingWriter:
    """Appends envelopes to the file of the hour they were received in.

    Files are opened in append mode, so a restarted recorder adds a new gzip member to the
    current hour's file instead of overwriting it. Writes come from the MQTT thread and
    ``close`` from the main thread, hence the lock.
    """

    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path: Path | None = None
        self.handle: TextIO | None = None
        self.written = 0
        self.lock = threading.Lock()

    def write(self, envelope: Envelope) -> None:
        path = hour_file(self.directory, envelope.received_datetime)
        with self.lock:
            if path != self.path:
                self._open(path)
            self.handle.write(envelope.to_json() + "\n")
            self.written += 1

    def close(self) -> None:
        with self.lock:
            self._close()

    def _open(self, path: Path) -> None:
        self._close()
        self.path = path
        self.handle = gzip.open(path, "at", encoding="utf-8")
        logger.info("event=recording_file_opened path=%s", path)

    def _close(self) -> None:
        if self.handle is not None:
            self.handle.close()
            logger.info("event=recording_file_closed path=%s", self.path)
        self.handle = None
        self.path = None


def open_recording(path: Path) -> TextIO:
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open(encoding="utf-8")


def read_envelopes(paths: Iterable[Path]) -> Iterator[Envelope]:
    """Envelopes from plain or gzipped JSON lines files, in file order.

    A gzip file cut short by a crash is read up to the last complete line.
    """
    for path in paths:
        with open_recording(path) as lines:
            try:
                for line in lines:
                    if line.endswith("\n"):
                        yield Envelope.from_json(line)
            except (EOFError, zlib.error):
                logger.warning("event=recording_truncated path=%s", path)


def recording_files(source: Path) -> list[Path]:
    """A single file, or every recording in a directory in name (= time) order."""
    if source.is_dir():
        return sorted([*source.glob("*.jsonl.gz"), *source.glob("*.jsonl")])
    return [source]
