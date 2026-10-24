"""Builders for hand-made HFP envelopes used by processor tests."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from producer.hfp import Envelope

EVENT_KEYS = {"vp": "VP", "arr": "ARR", "dep": "DEP"}


def hfp_envelope(
    event: str = "vp",
    *,
    tst: str = "2026-10-20T02:38:00.000Z",
    mode: str = "tram",
    operator: str = "0040",
    vehicle: str = "00649",
    next_stop: str = "1010108",
    received_at: datetime | None = None,
    **fields: Any,
) -> Envelope:
    """An envelope with sensible defaults; ``fields`` override or add payload fields."""
    body: dict[str, Any] = {
        "desi": "1",
        "dir": "1",
        "oper": int(operator),
        "veh": int(vehicle),
        "tst": tst,
        "tsi": 0,
        "spd": 5.5,
        "hdg": 90,
        "lat": 60.17,
        "long": 24.94,
        "dl": -30,
        "oday": "2026-10-20",
        "start": "05:37",
        "loc": "GPS",
        "stop": None,
        "route": "1001",
    }
    body.update(fields)
    topic = (
        f"/hfp/v2/journey/ongoing/{event}/{mode}/{operator}/{vehicle}/{body.get('route')}/"
        f"{body.get('dir')}/Käpylä/{body.get('start')}/{next_stop}/4/60;24/17/94/55"
    )
    moment = received_at or datetime.fromisoformat(tst.replace("Z", "+00:00"))
    payload = json.dumps({EVENT_KEYS[event]: body}, separators=(",", ":"))
    return Envelope.from_mqtt(topic, payload.encode(), moment.astimezone(UTC))
