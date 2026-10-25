from datetime import UTC, date, datetime, time, timedelta

import pytest

from processor.decode import Event, Rejection, decode, start_offset_seconds
from producer.hfp import Envelope
from tests.hfp_events import hfp_envelope


def decoded(envelope: Envelope) -> Event:
    result = decode(envelope)
    assert isinstance(result, Event), result
    return result


def test_vehicle_position_is_decoded_with_topic_identity_and_gtfs_direction() -> None:
    event = decoded(hfp_envelope(stop=1010108, dir="2", oper=22))

    assert (event.event_type, event.operator_id, event.vehicle_number) == ("vp", 40, 649)
    assert event.transport_mode == "tram"
    assert event.payload.oper == 22
    assert event.direction_id == 1
    assert event.payload.stop == "1010108"
    assert event.next_stop_id == "1010108"
    assert event.tst == datetime(2026, 10, 20, 2, 38, tzinfo=UTC)
    assert event.start_offset_seconds == 5 * 3600 + 37 * 60


@pytest.mark.parametrize(("dl", "live_delay"), [(-90, 90), (45, -45), (0, 0), (None, None)])
def test_live_delay_is_positive_when_late(dl: int | None, live_delay: int | None) -> None:
    assert decoded(hfp_envelope(dl=dl)).live_delay_s == live_delay


def test_end_of_line_is_not_a_next_stop() -> None:
    assert decoded(hfp_envelope(next_stop="EOL")).next_stop_id is None


def test_identical_payloads_hash_equal_and_different_ones_do_not() -> None:
    first = decoded(hfp_envelope())
    again = decoded(hfp_envelope())
    moved = decoded(hfp_envelope(lat=60.18))

    assert first.payload_hash == again.payload_hash
    assert first.payload_hash != moved.payload_hash
    assert len(first.payload_hash) == 16


@pytest.mark.parametrize(
    ("oday", "start", "tst", "offset"),
    [
        (date(2026, 10, 24), "23:50", datetime(2026, 10, 24, 21, 0, tzinfo=UTC), 85800),
        (date(2026, 10, 24), "01:10", datetime(2026, 10, 24, 22, 12, tzinfo=UTC), 90600),
        (date(2026, 10, 24), "03:30", datetime(2026, 10, 25, 0, 40, tzinfo=UTC), 99000),
        (date(2026, 10, 20), "05:37", datetime(2026, 10, 20, 2, 38, tzinfo=UTC), 20220),
    ],
)
def test_start_offset_moves_trips_after_midnight_to_the_operating_day(
    oday: date, start: str, tst: datetime, offset: int
) -> None:
    hours, minutes = map(int, start.split(":"))
    assert start_offset_seconds(oday, time(hours, minutes), tst) == offset


@pytest.mark.parametrize(
    ("envelope", "reason"),
    [
        (
            Envelope(
                "/hfp/v2/journey/ongoing/vp/bus/0018/01090/x", '{"VP":', "2026-10-20T02:38:00+00:00"
            ),
            "invalid_json",
        ),
        (hfp_envelope(lat="sixty north"), "schema_violation"),
        (hfp_envelope(hdg=400), "schema_violation"),
        (hfp_envelope(veh=None), "schema_violation"),
        (hfp_envelope(dir="3"), "schema_violation"),
        (Envelope("/somewhere/else", "{}", "2026-10-20T02:38:00+00:00"), "unexpected_topic"),
        (
            Envelope(
                "/hfp/v2/journey/ongoing/due/bus/0018/01090/x", "{}", "2026-10-20T02:38:00+00:00"
            ),
            "unexpected_topic",
        ),
    ],
)
def test_invalid_messages_are_rejected_with_a_reason(envelope: Envelope, reason: str) -> None:
    result = decode(envelope)
    assert isinstance(result, Rejection)
    assert result.reason == reason


def test_payload_key_must_match_the_topic_event_type() -> None:
    envelope = hfp_envelope("arr")
    wrong = Envelope(
        envelope.topic, envelope.payload.replace('{"ARR"', '{"VP"'), envelope.received_at
    )

    result = decode(wrong)

    assert isinstance(result, Rejection) and result.reason == "unexpected_payload_shape"


@pytest.mark.parametrize("shift", [timedelta(days=-2), timedelta(minutes=30)])
def test_event_time_far_from_receive_time_is_rejected(shift: timedelta) -> None:
    received = datetime(2026, 10, 20, 2, 38, tzinfo=UTC) - shift

    result = decode(hfp_envelope(received_at=received))

    assert isinstance(result, Rejection) and result.reason == "tst_out_of_range"
