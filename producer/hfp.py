"""HFP topics, message keys and the envelope every raw message travels in."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime

TOPIC_PREFIX = ("", "hfp", "v2")


@dataclass(frozen=True)
class TopicInfo:
    journey_type: str
    temporal_type: str
    event_type: str
    transport_mode: str
    operator_id: str
    vehicle_number: str


def parse_topic(topic: str) -> TopicInfo | None:
    """The leading levels of an HFP v2 topic, or None if the topic is not one."""
    levels = topic.split("/")
    if tuple(levels[:3]) != TOPIC_PREFIX or len(levels) < 9 or not all(levels[3:9]):
        return None
    return TopicInfo(*levels[3:9])


def message_key(topic: str) -> bytes | None:
    """``operator_id/vehicle_number`` from the topic: one physical vehicle, one partition.

    The topic carries the operator that owns the vehicle, which is stable for the vehicle.
    The payload's ``oper`` is the operator running the trip and can change between trips.
    """
    info = parse_topic(topic)
    if info is None:
        return None
    return f"{info.operator_id}/{info.vehicle_number}".encode()


@dataclass(frozen=True)
class Envelope:
    """One MQTT message as received: original topic, untouched payload, processing time."""

    topic: str
    payload: str
    received_at: str

    @classmethod
    def from_mqtt(cls, topic: str, payload: bytes, received_at: datetime | None = None) -> Envelope:
        moment = received_at or datetime.now(UTC)
        return cls(
            topic=topic,
            payload=payload.decode("utf-8", errors="replace"),
            received_at=moment.astimezone(UTC).isoformat(timespec="microseconds"),
        )

    @classmethod
    def from_json(cls, line: str | bytes) -> Envelope:
        data = json.loads(line)
        return cls(topic=data["topic"], payload=data["payload"], received_at=data["received_at"])

    def to_json(self) -> str:
        return json.dumps(
            {"topic": self.topic, "payload": self.payload, "received_at": self.received_at},
            ensure_ascii=False,
            separators=(",", ":"),
        )

    @property
    def key(self) -> bytes:
        """The vehicle key, or the whole topic for a topic that cannot be parsed.

        A key is always set, so the partition of every message is deterministic.
        """
        return message_key(self.topic) or self.topic.encode()

    @property
    def received_datetime(self) -> datetime:
        return datetime.fromisoformat(self.received_at)
