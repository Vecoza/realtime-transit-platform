from datetime import UTC, datetime, timedelta

import pytest

from processor.decode import Event, decode
from processor.windows import (
    Outcome,
    RouteWindow,
    StopWindow,
    Watermark,
    WindowAggregator,
    WindowRules,
    journey_of,
    percentile_cont,
)
from tests.hfp_events import hfp_envelope

T0 = datetime(2026, 10, 24, 12, 0, tzinfo=UTC)
RULES = WindowRules(
    allowed_lateness=timedelta(seconds=10),
    correction_horizon=timedelta(minutes=10),
    idle_timeout_s=30,
    stale_after=timedelta(minutes=2),
)


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def at(seconds: float) -> str:
    return (T0 + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")


def event(
    seconds: float,
    *,
    received: float | None = None,
    kind: str = "vp",
    vehicle: str = "00649",
    **fields,
) -> Event:
    received_at = T0 + timedelta(seconds=seconds if received is None else received)
    decoded = decode(
        hfp_envelope(kind, tst=at(seconds), vehicle=vehicle, received_at=received_at, **fields)
    )
    assert isinstance(decoded, Event)
    return decoded


def aggregator(partitions: int = 1) -> tuple[WindowAggregator, Clock]:
    clock = Clock()
    watermark = Watermark(RULES.idle_timeout_s, clock)
    watermark.assign(range(partitions), {})
    return WindowAggregator(RULES, watermark), clock


ROUTE = RouteWindow("1001", 0, T0)


def test_out_of_order_events_within_the_lateness_are_counted_on_time() -> None:
    windows, _ = aggregator()
    outcomes = [
        windows.add(event(30, dl=-60), 0),
        windows.add(event(5, received=31, dl=-30), 0),
        windows.add(event(65, dl=0), 0),
        windows.add(event(50, received=66, vehicle="00650", start="05:52", dl=-90), 0),
    ]
    emission = windows.advance()

    assert outcomes == [Outcome.APPLIED] * 4
    assert emission.routes == []
    windows.add(event(71), 0)
    (minute,) = windows.advance().routes
    assert (minute.window_start, minute.vehicles, minute.positions) == (T0, 2, 3)
    assert (minute.live_delay_samples, minute.journeys) == (3, 2)
    assert (minute.avg_live_delay_s, minute.p50_live_delay_s) == (67.5, 67.5)
    assert (minute.p90_live_delay_s, minute.late_events) == (85.5, 0)


def test_coupled_units_on_one_journey_count_once() -> None:
    windows, _ = aggregator()
    for unit in ("06308", "06320", "06328"):
        windows.add(event(10, vehicle=unit, dl=-2800), 0)
    windows.add(event(20, vehicle="06100", start="05:52", dl=-60), 0)
    windows.add(event(30, vehicle="06200", start="06:07", dl=-120), 0)
    windows.add(event(75), 0)

    (minute,) = windows.advance().routes

    assert (minute.vehicles, minute.live_delay_samples, minute.journeys) == (5, 5, 3)
    assert minute.p50_live_delay_s == 120.0


def test_late_event_corrects_the_emitted_window_with_full_values() -> None:
    windows, _ = aggregator()
    windows.add(event(10, dl=-30), 0)
    windows.add(event(75), 0)
    (first,) = windows.advance().routes

    outcome = windows.add(event(20, received=80, vehicle="00650", dl=-90), 0)
    (correction,) = windows.advance().routes

    assert outcome == Outcome.LATE
    assert (first.positions, first.vehicles, first.late_events) == (1, 1, 0)
    assert (correction.positions, correction.vehicles, correction.late_events) == (2, 2, 1)
    assert correction.avg_live_delay_s == 60.0
    assert [late.outcome for late in windows.take_late_events()] == ["corrected"]


def test_event_beyond_the_correction_horizon_is_dropped_not_counted() -> None:
    windows, _ = aggregator()
    windows.add(event(10), 0)
    windows.add(event(60 + 600 + 1), 0)
    windows.advance()

    outcome = windows.add(event(20, received=700, vehicle="00650"), 0)

    assert outcome == Outcome.DROPPED
    assert ROUTE not in windows.windows
    assert all(minute.window_start != T0 for minute in windows.advance().routes)
    assert [late.outcome for late in windows.take_late_events()] == ["dropped"]


def test_copies_and_replays_never_count_twice() -> None:
    windows, _ = aggregator()
    original = event(10)
    assert windows.add(original, 0) == Outcome.APPLIED
    assert windows.add(original, 0) == Outcome.DUPLICATE
    windows.add(event(75), 0)
    (minute,) = windows.advance().routes

    assert windows.add(original, 0) == Outcome.DUPLICATE
    assert minute.positions == 1
    assert windows.advance().routes == []


def test_watermark_is_the_slowest_partition_and_skips_idle_ones() -> None:
    windows, clock = aggregator(partitions=2)
    windows.add(event(100), 0)
    windows.add(event(40, vehicle="00650"), 1)
    windows.advance()
    assert windows.watermark.value == T0 + timedelta(seconds=40)

    clock.now = 20
    windows.add(event(130), 0)
    windows.advance()
    assert windows.watermark.value == T0 + timedelta(seconds=40)

    clock.now = 31
    windows.advance()
    assert windows.watermark.idle() == {1}
    assert windows.watermark.value == T0 + timedelta(seconds=130)


def test_watermark_waits_for_a_partition_until_it_is_idle_and_never_moves_back() -> None:
    windows, clock = aggregator(partitions=2)
    windows.add(event(100), 0)
    windows.advance()
    assert windows.watermark.value is None

    clock.now = 30
    windows.advance()
    assert windows.watermark.value == T0 + timedelta(seconds=100)

    windows.add(event(50, vehicle="00650"), 1)
    windows.advance()
    assert windows.watermark.value == T0 + timedelta(seconds=100)


def test_event_time_ahead_of_receipt_does_not_move_the_watermark() -> None:
    windows, _ = aggregator()
    windows.add(event(300, received=30), 0)
    windows.advance()
    assert windows.watermark.value == T0 + timedelta(seconds=30)


def test_arrivals_count_towards_their_stop() -> None:
    windows, _ = aggregator()
    windows.add(event(10, kind="arr", stop="1010108", dl=-40), 0)
    windows.add(event(20, kind="arr", vehicle="00650", stop="1010108", dl=None), 0)
    windows.add(event(30, kind="dep", stop="1010108"), 0)
    windows.add(event(75), 0)
    (stop,) = windows.advance().stops

    assert (stop.stop_id, stop.window_start) == ("1010108", T0)
    assert (stop.arrivals, stop.live_delay_samples, stop.avg_live_delay_s) == (2, 1, 40.0)
    assert (stop.p50_live_delay_s, stop.p90_live_delay_s) == (40.0, 40.0)
    assert StopWindow("1010108", T0) in windows.windows


def test_vehicle_silent_for_two_minutes_of_event_time_becomes_stale_and_returns() -> None:
    windows, _ = aggregator()
    windows.add(event(0), 0)
    windows.add(event(5, vehicle="00650"), 0)
    windows.advance()

    for second in range(10, 126, 5):
        windows.add(event(second, vehicle="00650"), 0)
    emission = windows.advance()

    stale = [v for v in emission.vehicles if v.status == "stale"]
    assert emission.stale == 1
    assert [(v.vehicle_number, v.silent_since) for v in stale] == [(649, T0 + timedelta(minutes=2))]

    windows.add(event(130), 0)
    back = [v for v in windows.advance().vehicles if v.vehicle_number == 649]
    assert [(v.status, v.silent_since) for v in back] == [("active", None)]


def test_silent_vehicle_is_stale_until_its_trip_is_planned_to_end_then_finished() -> None:
    windows, _ = aggregator()
    journey = journey_of(event(0))
    assert journey is not None and windows.unknown_journeys([event(0)]) == {journey}
    windows.set_trip_ends({journey: T0 + timedelta(minutes=5)})
    windows.add(event(0), 0)

    windows.add(event(125, vehicle="00650"), 0)
    stale = windows.advance()
    assert (stale.stale, stale.finished) == (1, 0)
    assert [(v.status, v.silent_since) for v in stale.vehicles if v.vehicle_number == 649] == [
        ("stale", T0 + timedelta(minutes=2))
    ]

    windows.add(event(240, vehicle="00650"), 0)
    assert windows.advance().stale == 0

    windows.add(event(300, vehicle="00650"), 0)
    finished = windows.advance()
    assert (finished.stale, finished.finished) == (0, 1)
    assert [(v.status, v.silent_since) for v in finished.vehicles if v.vehicle_number == 649] == [
        ("finished", T0 + timedelta(minutes=2))
    ]
    assert 649 not in {v.vehicle_number for v in windows.vehicles.values()}


def test_vehicle_silent_after_its_trip_is_planned_to_end_is_finished() -> None:
    windows, _ = aggregator()
    journey = journey_of(event(0))
    assert journey is not None
    windows.set_trip_ends({journey: T0 + timedelta(minutes=1)})
    windows.add(event(0), 0)

    windows.add(event(125, vehicle="00650"), 0)
    emission = windows.advance()

    assert (emission.stale, emission.finished) == (0, 1)
    assert [v.status for v in emission.vehicles if v.vehicle_number == 649] == ["finished"]


def test_vehicle_on_a_journey_long_past_its_end_is_suspect_and_its_delay_not_counted() -> None:
    windows, _ = aggregator()
    journey = journey_of(event(0))
    assert journey is not None
    windows.set_trip_ends({journey: T0 - timedelta(minutes=16)})
    windows.add(event(10, dl=-3600), 0)
    windows.add(event(20, vehicle="00650", start="05:52", dl=-60), 0)
    windows.add(event(75, vehicle="00650", start="05:52"), 0)

    emission = windows.advance()

    (minute,) = emission.routes
    assert (minute.vehicles, minute.positions, minute.live_delay_samples) == (2, 2, 1)
    assert minute.p50_live_delay_s == 60.0
    assert [(v.vehicle_number, v.suspect) for v in emission.vehicles] == [(649, True), (650, False)]


def test_vehicle_within_the_suspect_margin_is_counted() -> None:
    windows, _ = aggregator()
    journey = journey_of(event(0))
    assert journey is not None
    windows.set_trip_ends({journey: T0 - timedelta(minutes=14)})
    windows.add(event(10, dl=-600), 0)
    windows.add(event(75), 0)

    (minute,) = windows.advance().routes
    assert (minute.live_delay_samples, minute.p50_live_delay_s) == (1, 600.0)


@pytest.mark.parametrize(
    ("values", "expected"),
    [([], None), ([7], 7.0), ([1, 2], 1.9), ([0, 10, 20, 30, 40], 36.0), ([5, 5, 9, 100], 72.7)],
)
def test_percentile_matches_postgresql_percentile_cont(values, expected) -> None:
    result = percentile_cont(values, 0.9)
    assert result == pytest.approx(expected) if expected is not None else result is None
