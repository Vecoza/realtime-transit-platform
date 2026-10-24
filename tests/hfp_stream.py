"""A deterministic, live-like HFP stream with the disorder the real feed shows.

Twelve vehicles on three routes report a position every two seconds and arrive at a stop
every thirty seconds for six minutes. The stream is ordered by the time the producer
received each message, and it contains:

- copies of about a fifth of the messages, a few seconds after the original;
- small disorder: some messages are received up to eight seconds after their event time;
- a backlog burst: vehicle 0012/01603 is offline from 2:00 to 3:00 and sends what it
  buffered at 4:20, late but within the correction horizon used by the tests;
- very late data: vehicle 0018/01141's messages from 1:00 to 1:30 arrive at 5:30;
- a silent vehicle: 0022/00027 sends nothing from 3:20 to 5:40 and becomes stale.
"""

from __future__ import annotations

import random
from dataclasses import replace
from datetime import UTC, datetime, timedelta

from producer.hfp import Envelope
from tests.hfp_events import hfp_envelope

START = datetime(2026, 10, 24, 12, 0, tzinfo=UTC)
DURATION_S = 360
VEHICLES = [
    ("bus", "0012", "01603", "1001", "1"),
    ("bus", "0012", "01829", "1001", "2"),
    ("bus", "0018", "01141", "1002", "1"),
    ("bus", "0018", "00147", "1002", "2"),
    ("bus", "0022", "00027", "1003", "1"),
    ("bus", "0022", "01535", "1003", "2"),
    ("bus", "0022", "01096", "1001", "1"),
    ("bus", "0012", "01821", "1001", "2"),
    ("bus", "0012", "02241", "1002", "1"),
    ("bus", "0018", "01018", "1002", "2"),
    ("tram", "0040", "00455", "1003", "1"),
    ("tram", "0040", "00649", "1003", "2"),
]
BACKLOG = ("01603", 120, 180, 260)
VERY_LATE = ("01141", 60, 90, 330)
SILENT = ("00027", 200, 340)


def _iso(moment: datetime) -> str:
    return moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def stream(seed: int = 7) -> list[Envelope]:
    rng = random.Random(seed)
    timed: list[tuple[datetime, int, Envelope]] = []

    def emit(kind: str, vehicle: tuple[str, ...], second: float, delay: float, **fields) -> None:
        mode, operator, number, route, direction = vehicle
        tst = START + timedelta(seconds=second)
        received = tst + timedelta(seconds=delay)
        envelope = hfp_envelope(
            kind,
            tst=_iso(tst),
            mode=mode,
            operator=operator,
            vehicle=number,
            received_at=received,
            route=route,
            dir=direction,
            oday="2026-10-24",
            start="14:00",
            **fields,
        )
        timed.append((received, len(timed), envelope))
        if rng.random() < 0.2:
            copy_at = received + timedelta(seconds=rng.uniform(0.01, 3))
            copy = replace(envelope, received_at=copy_at.isoformat(timespec="microseconds"))
            timed.append((copy_at, len(timed), copy))

    for index, vehicle in enumerate(VEHICLES):
        number = vehicle[2]
        offset = index * 0.17
        for step in range(DURATION_S // 2):
            second = step * 2 + offset
            if SILENT[0] == number and SILENT[1] <= second < SILENT[2]:
                continue
            delay = rng.uniform(0.0, 1.5)
            if rng.random() < 0.1:
                delay += rng.uniform(0, 8)
            if BACKLOG[0] == number and BACKLOG[1] <= second < BACKLOG[2]:
                delay = BACKLOG[3] - second + rng.uniform(0, 2)
            if VERY_LATE[0] == number and VERY_LATE[1] <= second < VERY_LATE[2]:
                delay = VERY_LATE[3] - second + rng.uniform(0, 2)
            dl = None if rng.random() < 0.05 else rng.randint(-150, 60)
            emit("vp", vehicle, second, delay, dl=dl, spd=round(rng.uniform(0, 15), 2))
            if step % 15 == 7:
                stop = f"10{index % 4}0{step // 15 % 5}"
                emit("arr", vehicle, second + 0.5, delay, stop=stop, dl=dl)
                emit("dep", vehicle, second + 1.2, delay, stop=stop, dl=dl)
    timed.sort(key=lambda item: (item[0], item[1]))
    return [envelope for _, _, envelope in timed]
