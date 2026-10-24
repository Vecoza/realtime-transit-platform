import gzip
from datetime import UTC, datetime
from pathlib import Path

from producer.hfp import Envelope
from producer.recording import RecordingWriter, read_envelopes, recording_files

TOPIC = "/hfp/v2/journey/ongoing/vp/bus/0018/01090/4611/1/X/21:49/1/5/60;24/1/2/3"


def envelope_at(hour: int, minute: int, n: int = 0) -> Envelope:
    moment = datetime(2026, 10, 23, hour, minute, n, tzinfo=UTC)
    return Envelope.from_mqtt(TOPIC, f'{{"VP":{{"n":{n}}}}}'.encode(), moment)


def test_writer_starts_a_new_file_each_utc_hour(tmp_path: Path) -> None:
    writer = RecordingWriter(tmp_path)
    for envelope in (envelope_at(18, 59), envelope_at(19, 0), envelope_at(19, 30)):
        writer.write(envelope)
    writer.close()

    files = recording_files(tmp_path)
    assert [path.name for path in files] == [
        "hfp-20261023T18.jsonl.gz",
        "hfp-20261023T19.jsonl.gz",
    ]
    assert [e.received_at for e in read_envelopes(files)] == [
        envelope_at(18, 59).received_at,
        envelope_at(19, 0).received_at,
        envelope_at(19, 30).received_at,
    ]


def test_restarted_writer_appends_to_the_current_hour(tmp_path: Path) -> None:
    for n in range(2):
        writer = RecordingWriter(tmp_path)
        writer.write(envelope_at(19, 0, n))
        writer.close()

    envelopes = list(read_envelopes(recording_files(tmp_path)))
    assert [e.payload for e in envelopes] == ['{"VP":{"n":0}}', '{"VP":{"n":1}}']


def test_reader_stops_cleanly_at_a_truncated_gzip_file(tmp_path: Path) -> None:
    path = tmp_path / "hfp-20261023T19.jsonl.gz"
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        for n in range(200):
            handle.write(envelope_at(19, 0, n % 60).to_json() + "\n")
    data = path.read_bytes()
    path.write_bytes(data[: len(data) // 2])

    envelopes = list(read_envelopes([path]))

    assert 0 < len(envelopes) < 200


def test_reader_skips_an_incomplete_last_line_of_a_plain_file(tmp_path: Path) -> None:
    path = tmp_path / "sample.jsonl"
    path.write_text(envelope_at(19, 0).to_json() + "\n" + '{"topic": "/hfp/v2', encoding="utf-8")

    assert len(list(read_envelopes([path]))) == 1


def test_recording_files_accepts_a_single_file(tmp_path: Path) -> None:
    path = tmp_path / "sample.jsonl"
    assert recording_files(path) == [path]
