"""Validation of raw HFP envelopes into rows for the database, or a reason for rejecting them."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from functools import lru_cache
from typing import Any, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError, field_validator

from gtfs.service_time import service_day_start
from producer.hfp import Envelope, parse_topic

EVENT_KEYS = {"vp": "VP", "arr": "ARR", "dep": "DEP"}
MAX_EVENT_AGE = timedelta(days=1)
MAX_CLOCK_AHEAD = timedelta(minutes=10)
NEXT_SERVICE_DAY_AFTER = timedelta(hours=12)


class HfpPayload(BaseModel):
    """The fields of an HFP v2 event that the platform uses; others are ignored."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    veh: int
    oper: int | None = None
    tst: AwareDatetime
    lat: float | None = Field(default=None, ge=-90, le=90)
    long: float | None = Field(default=None, ge=-180, le=180)
    spd: float | None = Field(default=None, ge=0)
    hdg: int | None = Field(default=None, ge=0, le=360)
    dl: int | None = None
    oday: date | None = None
    start: time | None = None
    route: str | None = None
    dir: Literal["1", "2"] | None = None
    stop: str | None = None
    loc: str | None = None
    ttarr: AwareDatetime | None = None
    ttdep: AwareDatetime | None = None

    @field_validator("stop", "route", "dir", mode="before")
    @classmethod
    def number_to_text(cls, value: Any) -> Any:
        return str(value) if isinstance(value, int) and not isinstance(value, bool) else value


@dataclass(frozen=True)
class Event:
    """One validated HFP event, shaped like a row of rt.vehicle_positions or rt.stop_events."""

    event_type: str
    tst: datetime
    operator_id: int
    vehicle_number: int
    payload_hash: bytes
    received_at: datetime
    transport_mode: str
    next_stop_id: str | None
    payload: HfpPayload
    start_offset_seconds: int | None

    @property
    def direction_id(self) -> int | None:
        return None if self.payload.dir is None else int(self.payload.dir) - 1


@dataclass(frozen=True)
class Rejection:
    reason: str
    detail: str


@lru_cache(maxsize=64)
def _day_start(oday: date) -> datetime:
    return service_day_start(oday)


def start_offset_seconds(oday: date, start: time, tst: datetime) -> int:
    """GTFS-style offset of the trip's scheduled start on its operating day.

    HFP gives ``start`` on a 24-hour clock, so a trip that starts at 25:10 in GTFS reports
    01:10. When ``start`` read on the operating day lies far before the event, the trip must
    have started after midnight, and a day is added.
    """
    offset = start.hour * 3600 + start.minute * 60
    if tst - (_day_start(oday) + timedelta(seconds=offset)) > NEXT_SERVICE_DAY_AFTER:
        offset += 86400
    return offset


def payload_hash(payload: str) -> bytes:
    return hashlib.blake2b(payload.encode(), digest_size=16).digest()


def decode(envelope: Envelope) -> Event | Rejection:
    """Validate one envelope; every failure becomes a Rejection with a short reason."""
    info = parse_topic(envelope.topic)
    if info is None or info.event_type not in EVENT_KEYS:
        return Rejection("unexpected_topic", envelope.topic)
    try:
        operator_id, vehicle_number = int(info.operator_id), int(info.vehicle_number)
    except ValueError:
        return Rejection("unexpected_topic", envelope.topic)

    try:
        document = json.loads(envelope.payload)
    except json.JSONDecodeError as error:
        return Rejection("invalid_json", str(error))
    expected_key = EVENT_KEYS[info.event_type]
    if not isinstance(document, dict) or list(document) != [expected_key]:
        return Rejection("unexpected_payload_shape", f"expected a single {expected_key} object")
    try:
        payload = HfpPayload.model_validate(document[expected_key])
    except ValidationError as error:
        return Rejection("schema_violation", _short(error))

    received_at = envelope.received_datetime
    if not received_at - MAX_EVENT_AGE <= payload.tst <= received_at + MAX_CLOCK_AHEAD:
        return Rejection("tst_out_of_range", f"tst={payload.tst} received_at={received_at}")

    offset = None
    if payload.oday is not None and payload.start is not None:
        offset = start_offset_seconds(payload.oday, payload.start, payload.tst)
    return Event(
        event_type=info.event_type,
        tst=payload.tst,
        operator_id=operator_id,
        vehicle_number=vehicle_number,
        payload_hash=payload_hash(envelope.payload),
        received_at=received_at,
        transport_mode=info.transport_mode,
        next_stop_id=_next_stop(envelope.topic),
        payload=payload,
        start_offset_seconds=offset,
    )


def _next_stop(topic: str) -> str | None:
    levels = topic.split("/")
    value = levels[13] if len(levels) > 13 else ""
    return value if value and value != "EOL" else None


def _short(error: ValidationError) -> str:
    return "; ".join(
        f"{'.'.join(map(str, item['loc']))}: {item['msg']}" for item in error.errors()[:3]
    )
