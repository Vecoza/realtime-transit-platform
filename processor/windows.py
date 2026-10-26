"""Event-time windows: watermark, one-minute tumbling windows, late events, stale vehicles.

Everything here is in memory and free of I/O, so the rules can be tested on their own:

- The watermark is the minimum over partitions of the highest event time each partition
  has delivered. A window closes, and is emitted, when the watermark passes its end plus
  the allowed lateness.
- A window keeps every event it counted, keyed by the event's identity, so a copy or a
  replayed message is recognised and never counted twice. Results are always computed in
  full from that state.
- An event for a closed window is late: it is added and the window is emitted again as a
  correction. Once the watermark is the correction horizon past a window's end, the window
  is forgotten and events for it are dropped.
- A vehicle on a trip that has sent nothing for ``stale_after`` of event time is waiting
  before its trip's planned start, stale while the trip is planned to run, and finished once
  the trip's planned end has passed. Without a timetable trip there is no plan to compare
  with, and the silent vehicle is unmatched.
- A vehicle still reporting a journey whose planned end passed more than ``suspect_after``
  ago is a suspect sign-on: its position counts, its delay does not.
- Delay figures are per journey: each journey's median delay in the minute counts once, so
  the coupled units of one train, which report separately, do not count two or three times.
"""

from __future__ import annotations

import enum
import math
import struct
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from datetime import time as clock_time
from typing import NamedTuple

from gtfs.service_time import to_utc
from processor.decode import Event

MINUTE = timedelta(minutes=1)


@dataclass(frozen=True)
class WindowRules:
    allowed_lateness: timedelta = timedelta(seconds=10)
    correction_horizon: timedelta = timedelta(minutes=10)
    idle_timeout_s: float = 30.0
    stale_after: timedelta = timedelta(minutes=2)
    suspect_after: timedelta = timedelta(minutes=15)


class RouteWindow(NamedTuple):
    route_id: str
    direction_id: int
    start: datetime


class StopWindow(NamedTuple):
    stop_id: str
    start: datetime


WindowKey = RouteWindow | StopWindow


class Journey(NamedTuple):
    """HFP's durable trip identity; ``start_offset_seconds`` counts like GTFS times."""

    route_id: str
    direction_id: int
    oday: date
    start_offset_seconds: int


class Outcome(enum.StrEnum):
    APPLIED = "applied"
    LATE = "late"
    DUPLICATE = "duplicate"
    DROPPED = "dropped"
    NO_WINDOW = "no_window"


@dataclass(frozen=True)
class RouteMinute:
    route_id: str
    direction_id: int
    window_start: datetime
    vehicles: int
    positions: int
    live_delay_samples: int
    journeys: int
    avg_live_delay_s: float | None
    p50_live_delay_s: float | None
    p90_live_delay_s: float | None
    late_events: int


@dataclass(frozen=True)
class StopMinute:
    stop_id: str
    window_start: datetime
    arrivals: int
    live_delay_samples: int
    journeys: int
    avg_live_delay_s: float | None
    p50_live_delay_s: float | None
    p90_live_delay_s: float | None
    late_events: int


@dataclass(frozen=True)
class LateEvent:
    event: Event
    window: WindowKey
    watermark: datetime
    outcome: str


class Counted(NamedTuple):
    """What a window keeps of an event; the delay is in seconds, positive when late."""

    vehicle: int
    live_delay_s: int | None
    suspect: bool = False
    journey: Journey | None = None


@dataclass
class WindowState:
    """Counted events by identity."""

    events: dict[bytes, Counted] = field(default_factory=dict)
    late: set[bytes] = field(default_factory=set)
    emitted: bool = False
    changed: bool = True


@dataclass
class VehicleState:
    operator_id: int
    vehicle_number: int
    last_tst: datetime
    last_received_at: datetime
    transport_mode: str
    route_id: str | None
    direction_id: int | None
    oday: date | None
    start_time: clock_time | None
    lat: float | None
    lon: float | None
    planned_end: datetime | None = None
    status: str = "active"
    silent_since: datetime | None = None
    suspect: bool = False
    planned_start: datetime | None = None


@dataclass
class Emission:
    routes: list[RouteMinute] = field(default_factory=list)
    stops: list[StopMinute] = field(default_factory=list)
    vehicles: list[VehicleState] = field(default_factory=list)
    stale: int = 0
    finished: int = 0
    waiting: int = 0
    unmatched: int = 0


def event_key(operator_id: int, vehicle_number: int, payload_hash: bytes) -> bytes:
    """The identity of an event inside a window: the unique key of the stored row."""
    return payload_hash + struct.pack(">HI", operator_id, vehicle_number)


def vehicle_id(operator_id: int, vehicle_number: int) -> int:
    return operator_id * 100_000 + vehicle_number


def journey_of(event: Event) -> Journey | None:
    p = event.payload
    if None in (p.route, event.direction_id, p.oday, event.start_offset_seconds):
        return None
    return Journey(p.route, event.direction_id, p.oday, event.start_offset_seconds)


def minute_start(moment: datetime) -> datetime:
    return moment.replace(second=0, microsecond=0)


def window_of(event: Event) -> WindowKey | None:
    """Positions count towards their route and direction, arrivals towards their stop."""
    p = event.payload
    start = minute_start(event.tst)
    if event.event_type == "vp" and p.route is not None and event.direction_id is not None:
        return RouteWindow(p.route, event.direction_id, start)
    if event.event_type == "arr" and p.stop is not None:
        return StopWindow(p.stop, start)
    return None


def percentile_cont(ordered: list[int] | list[float], fraction: float) -> float | None:
    """Linear interpolation between the closest ranks, as PostgreSQL's percentile_cont."""
    if not ordered:
        return None
    position = fraction * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    return ordered[lower] + (position - lower) * (ordered[upper] - ordered[lower])


@dataclass(frozen=True)
class DelayFigures:
    """Counted delays and, per journey, the median of each journey's delays in order."""

    samples: int
    journey_delays: list[float]

    @property
    def average(self) -> float | None:
        delays = self.journey_delays
        return sum(delays) / len(delays) if delays else None


def delays_of(state: WindowState) -> DelayFigures:
    """Leaves out suspect sign-ons; events without a journey stand for their vehicle."""
    by_journey: dict[Journey | int, list[int]] = {}
    for e in state.events.values():
        if e.live_delay_s is not None and not e.suspect:
            group = e.journey if e.journey is not None else e.vehicle
            by_journey.setdefault(group, []).append(e.live_delay_s)
    medians = sorted(percentile_cont(sorted(d), 0.5) for d in by_journey.values())
    return DelayFigures(sum(len(d) for d in by_journey.values()), medians)


def route_minute(window: RouteWindow, state: WindowState) -> RouteMinute:
    figures = delays_of(state)
    delays = figures.journey_delays
    return RouteMinute(
        route_id=window.route_id,
        direction_id=window.direction_id,
        window_start=window.start,
        vehicles=len({e.vehicle for e in state.events.values()}),
        positions=len(state.events),
        live_delay_samples=figures.samples,
        journeys=len(delays),
        avg_live_delay_s=figures.average,
        p50_live_delay_s=percentile_cont(delays, 0.5),
        p90_live_delay_s=percentile_cont(delays, 0.9),
        late_events=len(state.late),
    )


def stop_minute(window: StopWindow, state: WindowState) -> StopMinute:
    figures = delays_of(state)
    delays = figures.journey_delays
    return StopMinute(
        stop_id=window.stop_id,
        window_start=window.start,
        arrivals=len(state.events),
        live_delay_samples=figures.samples,
        journeys=len(delays),
        avg_live_delay_s=figures.average,
        p50_live_delay_s=percentile_cont(delays, 0.5),
        p90_live_delay_s=percentile_cont(delays, 0.9),
        late_events=len(state.late),
    )


@dataclass
class PartitionProgress:
    max_event_time: datetime | None
    last_seen: float


class Watermark:
    """The minimum over partitions of the highest event time each has delivered.

    Event time is capped at the moment the producer received the message, so a vehicle
    whose clock runs ahead cannot push the watermark forward. A partition that has delivered
    nothing for ``idle_timeout_s`` of processing time is left out, so a quiet partition does
    not stall the others; when every partition is idle, the highest one counts. The
    watermark never moves back.
    """

    def __init__(self, idle_timeout_s: float, clock: Callable[[], float] = time.monotonic):
        self.idle_timeout_s = idle_timeout_s
        self.clock = clock
        self.partitions: dict[int, PartitionProgress] = {}
        self.value: datetime | None = None

    def assign(self, partitions: Iterable[int], max_event_times: dict[int, datetime]) -> None:
        """Track newly assigned partitions; partitions already tracked keep their progress."""
        now = self.clock()
        for partition in partitions:
            if partition not in self.partitions:
                self.partitions[partition] = PartitionProgress(max_event_times.get(partition), now)

    def observe(self, partition: int, tst: datetime, received_at: datetime) -> None:
        progress = self.partitions.setdefault(partition, PartitionProgress(None, self.clock()))
        moment = min(tst, received_at)
        if progress.max_event_time is None or moment > progress.max_event_time:
            progress.max_event_time = moment
        progress.last_seen = self.clock()

    def idle(self) -> set[int]:
        now = self.clock()
        return {
            partition
            for partition, progress in self.partitions.items()
            if now - progress.last_seen >= self.idle_timeout_s
        }

    def advance(self) -> datetime | None:
        idle = self.idle()
        active = [p for partition, p in self.partitions.items() if partition not in idle]
        if active:
            if any(p.max_event_time is None for p in active):
                return self.value
            candidate = min(p.max_event_time for p in active)
        else:
            known = [p.max_event_time for p in self.partitions.values() if p.max_event_time]
            if not known:
                return self.value
            candidate = max(known)
        if self.value is None or candidate > self.value:
            self.value = candidate
        return self.value


class WindowAggregator:
    """Per-minute route and stop windows over event time, with corrections for late events."""

    def __init__(self, rules: WindowRules, watermark: Watermark) -> None:
        self.rules = rules
        self.watermark = watermark
        self.windows: dict[WindowKey, WindowState] = {}
        self.vehicles: dict[int, VehicleState] = {}
        self.changed_vehicles: set[int] = set()
        self.late_events: list[LateEvent] = []
        self.trip_ends: dict[Journey, datetime | None] = {}
        self.journeys: dict[Journey, Journey] = {}

    def unknown_journeys(self, events: Iterable[Event]) -> set[Journey]:
        """Journeys whose planned end has not been looked up yet."""
        journeys = {journey_of(event) for event in events}
        return {j for j in journeys if j is not None and j not in self.trip_ends}

    def set_trip_ends(self, ends: dict[Journey, datetime | None]) -> None:
        self.trip_ends.update(ends)

    def add(self, event: Event, partition: int) -> Outcome:
        self.watermark.observe(partition, event.tst, event.received_at)
        self._track_vehicle(event)
        window = window_of(event)
        if window is None:
            return Outcome.NO_WINDOW
        end = window.start + MINUTE
        watermark = self.watermark.value
        key = event_key(event.operator_id, event.vehicle_number, event.payload_hash)
        state = self.windows.get(window)
        if state is not None and key in state.events:
            return Outcome.DUPLICATE
        if watermark is not None and watermark >= end + self.rules.correction_horizon:
            self.late_events.append(LateEvent(event, window, watermark, "dropped"))
            return Outcome.DROPPED
        if state is None:
            state = self.windows[window] = WindowState()
        state.events[key] = self.counted(
            vehicle_id(event.operator_id, event.vehicle_number),
            journey_of(event),
            event.tst,
            event.live_delay_s,
        )
        state.changed = True
        if watermark is not None and watermark >= end + self.rules.allowed_lateness:
            state.late.add(key)
            self.late_events.append(LateEvent(event, window, watermark, "corrected"))
            return Outcome.LATE
        return Outcome.APPLIED

    def counted(
        self, vehicle: int, journey: Journey | None, tst: datetime, live_delay_s: int | None
    ) -> Counted:
        journey = None if journey is None else self.journeys.setdefault(journey, journey)
        return Counted(vehicle, live_delay_s, self.is_suspect(journey, tst), journey)

    def is_suspect(self, journey: Journey | None, tst: datetime) -> bool:
        """Still reporting a journey whose planned end is more than ``suspect_after`` past."""
        end = None if journey is None else self.trip_ends.get(journey)
        return end is not None and tst > end + self.rules.suspect_after

    def advance(self) -> Emission:
        """Move the watermark; emit closed windows and corrections, forget old windows."""
        watermark = self.watermark.advance()
        emission = Emission()
        if watermark is None:
            return self._vehicles_into(emission)
        for window, state in list(self.windows.items()):
            end = window.start + MINUTE
            if watermark >= end + self.rules.allowed_lateness and state.changed:
                if isinstance(window, RouteWindow):
                    emission.routes.append(route_minute(window, state))
                else:
                    emission.stops.append(stop_minute(window, state))
                state.emitted = True
                state.changed = False
            if watermark >= end + self.rules.correction_horizon:
                del self.windows[window]
        for vid, vehicle in self.vehicles.items():
            status = self._silent_status(vehicle, watermark)
            if status == vehicle.status:
                continue
            if vehicle.status == "active":
                vehicle.silent_since = vehicle.last_tst + self.rules.stale_after
            vehicle.status = status
            self.changed_vehicles.add(vid)
            if status == "stale":
                emission.stale += 1
            elif status == "waiting":
                emission.waiting += 1
            elif status == "unmatched":
                emission.unmatched += 1
            else:
                emission.finished += 1
        return self._vehicles_into(emission)

    def _silent_status(self, vehicle: VehicleState, watermark: datetime) -> str:
        """Waiting before the trip's start, stale while it is planned to run, finished after.

        A journey without a timetable trip has no planned end: its silent vehicle is unmatched.
        """
        if vehicle.route_id is None or watermark < vehicle.last_tst + self.rules.stale_after:
            return vehicle.status
        if vehicle.planned_end is None:
            return "unmatched"
        if watermark >= vehicle.planned_end:
            return "finished"
        if vehicle.planned_start is not None and watermark < vehicle.planned_start:
            return "waiting"
        return "stale"

    def take_late_events(self) -> list[LateEvent]:
        late, self.late_events = self.late_events, []
        return late

    def retained_events(self) -> int:
        return sum(len(state.events) for state in self.windows.values())

    def open_windows(self) -> int:
        return sum(1 for state in self.windows.values() if not state.emitted)

    def restore_window(
        self,
        window: WindowKey,
        events: dict[bytes, Counted],
        late: set[bytes],
        emitted: bool,
    ) -> None:
        """Put back a window rebuilt from stored data; emitted windows are checked again."""
        self.windows[window] = WindowState(events, late, emitted, changed=True)

    def restore_vehicle(self, vehicle: VehicleState) -> None:
        self.vehicles[vehicle_id(vehicle.operator_id, vehicle.vehicle_number)] = vehicle

    def _track_vehicle(self, event: Event) -> None:
        vid = vehicle_id(event.operator_id, event.vehicle_number)
        vehicle = self.vehicles.get(vid)
        if vehicle is not None and event.tst <= vehicle.last_tst:
            return
        p = event.payload
        if vehicle is None:
            vehicle = self.vehicles[vid] = VehicleState(
                event.operator_id,
                event.vehicle_number,
                event.tst,
                event.received_at,
                event.transport_mode,
                p.route,
                event.direction_id,
                p.oday,
                p.start,
                p.lat,
                p.long,
            )
        else:
            vehicle.last_tst = event.tst
            vehicle.last_received_at = event.received_at
            vehicle.route_id = p.route
            vehicle.direction_id = event.direction_id
            vehicle.oday = p.oday
            vehicle.start_time = p.start
            if p.lat is not None and p.long is not None:
                vehicle.lat, vehicle.lon = p.lat, p.long
            vehicle.status = "active"
            vehicle.silent_since = None
        journey = journey_of(event)
        vehicle.planned_end = None if journey is None else self.trip_ends.get(journey)
        vehicle.planned_start = (
            None if journey is None else to_utc(journey.oday, journey.start_offset_seconds)
        )
        vehicle.suspect = self.is_suspect(journey, event.tst)
        self.changed_vehicles.add(vid)

    def _vehicles_into(self, emission: Emission) -> Emission:
        emission.vehicles = [self.vehicles[vid] for vid in sorted(self.changed_vehicles)]
        self.changed_vehicles.clear()
        for vehicle in emission.vehicles:
            if vehicle.status in ("finished", "unmatched"):
                del self.vehicles[vehicle_id(vehicle.operator_id, vehicle.vehicle_number)]
        return emission
