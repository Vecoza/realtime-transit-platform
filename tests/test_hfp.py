import json
from datetime import UTC, datetime, timedelta, timezone

import pytest

from producer.hfp import Envelope, TopicInfo, message_key, parse_topic

VP_TOPIC = (
    "/hfp/v2/journey/ongoing/vp/bus/0018/01090/4611/1/Tikkurila as./21:49/1113140/5/60;24/19/85/27"
)


def test_parse_topic_reads_the_leading_levels() -> None:
    assert parse_topic(VP_TOPIC) == TopicInfo(
        journey_type="journey",
        temporal_type="ongoing",
        event_type="vp",
        transport_mode="bus",
        operator_id="0018",
        vehicle_number="01090",
    )


@pytest.mark.parametrize(
    "topic",
    [
        "",
        "/hfp/v1/journey/ongoing/vp/bus/0018/01090",
        "/hfp/v2/journey/ongoing/vp/bus/0018",
        "/hfp/v2/journey/ongoing/vp/bus//01090/4611",
        "hfp/v2/journey/ongoing/vp/bus/0018/01090/x",
    ],
)
def test_parse_topic_rejects_other_topics(topic: str) -> None:
    assert parse_topic(topic) is None


def test_message_key_is_owner_operator_and_vehicle_from_the_topic() -> None:
    assert message_key(VP_TOPIC) == b"0018/01090"
    assert (
        message_key(
            "/hfp/v2/journey/ongoing/arr/tram/0040/00649/1013/1/Ilmala/21:45/1173429/4/60;24/29/04/03"
        )
        == b"0040/00649"
    )


def test_message_key_is_none_for_unparseable_topics() -> None:
    assert message_key("/not/hfp") is None


def test_envelope_key_falls_back_to_the_topic() -> None:
    assert Envelope.from_mqtt("/not/hfp", b"{}").key == b"/not/hfp"


def test_envelope_keeps_payload_text_and_records_utc_processing_time() -> None:
    payload = '{"VP":{"veh":1090,"tst":"2026-10-23T18:57:09.687Z"}}'
    helsinki = timezone(timedelta(hours=3))
    envelope = Envelope.from_mqtt(
        VP_TOPIC, payload.encode(), datetime(2026, 10, 23, 21, 57, 10, 5, tzinfo=helsinki)
    )

    assert envelope.payload == payload
    assert envelope.received_at == "2026-10-23T18:57:10.000005+00:00"
    assert envelope.received_datetime == datetime(2026, 10, 23, 18, 57, 10, 5, tzinfo=UTC)
    assert envelope.key == b"0018/01090"


def test_envelope_replaces_bytes_that_are_not_utf8() -> None:
    envelope = Envelope.from_mqtt(VP_TOPIC, b'{"VP":\xff}')
    assert envelope.payload == '{"VP":�}'


def test_envelope_json_round_trip_is_lossless() -> None:
    envelope = Envelope.from_mqtt(VP_TOPIC, '{"VP":{"desi":"Käpylä"}}'.encode())
    line = envelope.to_json()

    assert Envelope.from_json(line) == envelope
    assert json.loads(line)["payload"] == '{"VP":{"desi":"Käpylä"}}'
    assert "\n" not in line
