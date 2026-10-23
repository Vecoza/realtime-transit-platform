import io

import pytest

from gtfs.files import STOP_TIMES, TRIPS
from gtfs.loader import InvalidFeedError, check_columns, read_header


def test_read_header_strips_byte_order_mark_and_whitespace() -> None:
    stream = io.BytesIO(b"\xef\xbb\xbftrip_id, arrival_time\r\nT1,05:00:00\r\n")
    assert read_header(stream, "stop_times.txt") == ["trip_id", "arrival_time"]
    assert stream.read() == b"T1,05:00:00\r\n"


@pytest.mark.parametrize("line", [b"\n", b"trip_id,Trip Id\n", b"trip_id,trip_id\n"])
def test_read_header_rejects_unusable_columns(line: bytes) -> None:
    with pytest.raises(InvalidFeedError):
        read_header(io.BytesIO(line), "trips.txt")


def test_check_columns_names_the_missing_required_columns() -> None:
    with pytest.raises(InvalidFeedError, match="departure_time"):
        check_columns(STOP_TIMES, ["trip_id", "arrival_time", "stop_id", "stop_sequence"])
    check_columns(TRIPS, ["route_id", "service_id", "trip_id"])
