import hashlib
import io
import ssl
import urllib.error
from pathlib import Path

import pytest

from gtfs import download
from gtfs.download import FeedFile, download_feed

PAYLOAD = b"PK fake zip content" * 1000


class FakeResponse(io.BytesIO):
    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def fake_urlopen(outcomes: list[object]):
    calls: list[str] = []

    def urlopen(url: str, timeout: float) -> FakeResponse:
        calls.append(url)
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return FakeResponse(outcome)

    return urlopen, calls


def http_error(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError("https://example.org/gtfs.zip", code, "error", {}, None)


def test_download_retries_transient_errors_then_stores_by_checksum(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    urlopen, calls = fake_urlopen([urllib.error.URLError("reset"), http_error(503), PAYLOAD])
    monkeypatch.setattr(download.urllib.request, "urlopen", urlopen)
    delays: list[float] = []

    feed = download_feed("https://example.org/gtfs.zip", tmp_path, sleep=delays.append)

    digest = hashlib.sha256(PAYLOAD).hexdigest()
    assert len(calls) == 3
    assert delays == [2.0, 4.0]
    assert feed.sha256 == digest
    assert feed.size_bytes == len(PAYLOAD)
    assert feed.path == tmp_path / f"gtfs-{digest[:12]}.zip"
    assert feed.path.read_bytes() == PAYLOAD
    assert not (tmp_path / "download.partial").exists()


@pytest.mark.parametrize(
    "error",
    [http_error(404), urllib.error.URLError(ssl.SSLCertVerificationError("bad certificate"))],
)
def test_download_does_not_retry_permanent_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, error: Exception
) -> None:
    urlopen, calls = fake_urlopen([error])
    monkeypatch.setattr(download.urllib.request, "urlopen", urlopen)

    with pytest.raises(urllib.error.URLError):
        download_feed("https://example.org/gtfs.zip", tmp_path, sleep=lambda _: None)
    assert len(calls) == 1
    assert list(tmp_path.iterdir()) == []


def test_download_gives_up_after_the_last_attempt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    urlopen, calls = fake_urlopen([http_error(502)] * 3)
    monkeypatch.setattr(download.urllib.request, "urlopen", urlopen)

    with pytest.raises(urllib.error.HTTPError):
        download_feed("https://example.org/gtfs.zip", tmp_path, attempts=3, sleep=lambda _: None)
    assert len(calls) == 3


def test_local_feed_file_has_checksum_and_file_url(tmp_path: Path) -> None:
    path = tmp_path / "feed.zip"
    path.write_bytes(PAYLOAD)

    feed = FeedFile.from_path(path)

    assert feed.sha256 == hashlib.sha256(PAYLOAD).hexdigest()
    assert feed.source_url == path.resolve().as_uri()
