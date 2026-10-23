"""Download of a GTFS zip with retries and a content checksum."""

from __future__ import annotations

import hashlib
import logging
import ssl
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

CHUNK_SIZE = 1 << 20


@dataclass(frozen=True)
class FeedFile:
    """A GTFS zip on local disk together with where and when it came from."""

    path: Path
    source_url: str
    sha256: str
    size_bytes: int
    downloaded_at: datetime

    @classmethod
    def from_path(cls, path: Path) -> FeedFile:
        digest, size = sha256_of(path)
        modified = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
        return cls(path, path.resolve().as_uri(), digest, size, modified)


def sha256_of(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(CHUNK_SIZE):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def is_retryable(error: Exception) -> bool:
    if isinstance(error, urllib.error.HTTPError):
        return error.code >= 500 or error.code == 429
    if isinstance(error, urllib.error.URLError) and isinstance(error.reason, ssl.SSLError):
        return False
    return isinstance(error, urllib.error.URLError | TimeoutError | ConnectionError)


def download_feed(
    url: str,
    download_dir: Path,
    attempts: int = 4,
    backoff_seconds: float = 2.0,
    timeout_seconds: float = 60.0,
    sleep: Callable[[float], None] = time.sleep,
) -> FeedFile:
    """Download ``url`` into ``download_dir``, retrying transient failures with backoff."""
    download_dir.mkdir(parents=True, exist_ok=True)
    partial = download_dir / "download.partial"
    for attempt in range(1, attempts + 1):
        try:
            digest, size = _fetch(url, partial, timeout_seconds)
            break
        except Exception as error:
            if attempt == attempts or not is_retryable(error):
                partial.unlink(missing_ok=True)
                raise
            delay = backoff_seconds * 2 ** (attempt - 1)
            logger.warning(
                "event=download_retry attempt=%d delay_s=%.1f error=%r", attempt, delay, error
            )
            sleep(delay)
    target = download_dir / f"gtfs-{digest[:12]}.zip"
    partial.replace(target)
    logger.info("event=downloaded url=%s bytes=%d sha256=%s", url, size, digest)
    return FeedFile(target, url, digest, size, datetime.now(UTC))


def _fetch(url: str, destination: Path, timeout_seconds: float) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with (
        urllib.request.urlopen(url, timeout=timeout_seconds) as response,
        destination.open("wb") as handle,
    ):
        while chunk := response.read(CHUNK_SIZE):
            digest.update(chunk)
            handle.write(chunk)
            size += len(chunk)
    return digest.hexdigest(), size
