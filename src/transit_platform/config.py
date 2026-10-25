"""Settings loaded from environment variables (.env supported)."""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv
from psycopg.conninfo import make_conninfo

load_dotenv()


@dataclass(frozen=True)
class DatabaseSettings:
    user: str
    password: str
    host: str
    port: int
    name: str

    @classmethod
    def from_env(cls) -> DatabaseSettings:
        return cls(
            user=os.getenv("POSTGRES_USER", "transit"),
            password=os.getenv("POSTGRES_PASSWORD", "rtp_local_pw_2026"),
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            name=os.getenv("POSTGRES_DB", "transit"),
        )

    @property
    def url(self) -> str:
        return (
            f"postgresql+psycopg://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"
        )

    @property
    def conninfo(self) -> str:
        """Connection string for psycopg, used where the COPY protocol is needed."""
        return make_conninfo(
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            dbname=self.name,
        )


@dataclass(frozen=True)
class StreamSettings:
    bootstrap_servers: str
    raw_topic: str = "hfp.raw"
    raw_partitions: int = 6
    raw_retention_ms: int = 24 * 60 * 60 * 1000

    @classmethod
    def from_env(cls) -> StreamSettings:
        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
            raw_topic=os.getenv("HFP_RAW_TOPIC", "hfp.raw"),
            raw_partitions=int(os.getenv("HFP_RAW_PARTITIONS", "6")),
            raw_retention_ms=int(os.getenv("HFP_RAW_RETENTION_MS", str(24 * 60 * 60 * 1000))),
        )


@dataclass(frozen=True)
class ProcessorSettings:
    group_id: str = "sink"
    dlq_topic: str = "hfp.dlq"
    batch_max_messages: int = 5000
    batch_max_ms: int = 500
    instance_id: str | None = None

    @classmethod
    def from_env(cls) -> ProcessorSettings:
        group_id = os.getenv("SINK_GROUP_ID", "sink")
        return cls(
            group_id=group_id,
            dlq_topic=os.getenv("HFP_DLQ_TOPIC", "hfp.dlq"),
            batch_max_messages=int(os.getenv("SINK_BATCH_MAX_MESSAGES", "5000")),
            batch_max_ms=int(os.getenv("SINK_BATCH_MAX_MS", "500")),
            instance_id=os.getenv("SINK_INSTANCE_ID") or f"{group_id}-{socket.gethostname()}",
        )


@dataclass(frozen=True)
class AggregatorSettings:
    group_id: str = "aggregator"
    storage_group_id: str = "sink"
    batch_max_messages: int = 5000
    batch_max_ms: int = 500
    allowed_lateness_s: int = 10
    correction_horizon_s: int = 600
    idle_timeout_s: float = 30.0
    stale_after_s: int = 120
    storage_wait_s: float = 300.0
    instance_id: str | None = None

    @classmethod
    def from_env(cls) -> AggregatorSettings:
        group_id = os.getenv("AGGREGATOR_GROUP_ID", "aggregator")
        return cls(
            group_id=group_id,
            storage_group_id=os.getenv("SINK_GROUP_ID", "sink"),
            batch_max_messages=int(os.getenv("AGGREGATOR_BATCH_MAX_MESSAGES", "5000")),
            batch_max_ms=int(os.getenv("AGGREGATOR_BATCH_MAX_MS", "500")),
            allowed_lateness_s=int(os.getenv("AGGREGATOR_ALLOWED_LATENESS_S", "10")),
            correction_horizon_s=int(os.getenv("AGGREGATOR_CORRECTION_HORIZON_S", "600")),
            idle_timeout_s=float(os.getenv("AGGREGATOR_IDLE_TIMEOUT_S", "30")),
            stale_after_s=int(os.getenv("AGGREGATOR_STALE_AFTER_S", "120")),
            storage_wait_s=float(os.getenv("AGGREGATOR_STORAGE_WAIT_S", "300")),
            instance_id=os.getenv("AGGREGATOR_INSTANCE_ID") or f"{group_id}-{socket.gethostname()}",
        )


# The sink accepts events up to a day old, so a day's partition changes until two days later.
MIN_KEEP_DAYS = 2


@dataclass(frozen=True)
class ArchiveSettings:
    """Days each kind of data stays in PostgreSQL before it moves to Parquet."""

    directory: Path = Path("data/archive")
    positions_days: int = 2
    stop_events_days: int = 7
    minute_stats_days: int = 7
    timetable_days: int = 2

    def __post_init__(self) -> None:
        for name in ("positions_days", "stop_events_days", "minute_stats_days", "timetable_days"):
            if getattr(self, name) < MIN_KEEP_DAYS:
                raise ValueError(f"{name} must be at least {MIN_KEEP_DAYS}")

    def keep_days(self, table: str) -> int:
        return {
            "vehicle_positions": self.positions_days,
            "stop_events": self.stop_events_days,
            "route_minute_stats": self.minute_stats_days,
            "stop_minute_stats": self.minute_stats_days,
        }[table]

    @classmethod
    def from_env(cls) -> ArchiveSettings:
        return cls(
            directory=Path(os.getenv("ARCHIVE_DIR", "data/archive")),
            positions_days=int(os.getenv("ARCHIVE_KEEP_DAYS_POSITIONS", "2")),
            stop_events_days=int(os.getenv("ARCHIVE_KEEP_DAYS_STOP_EVENTS", "7")),
            minute_stats_days=int(os.getenv("ARCHIVE_KEEP_DAYS_MINUTE_STATS", "7")),
            timetable_days=int(os.getenv("ARCHIVE_KEEP_DAYS_TIMETABLE", "2")),
        )


@dataclass(frozen=True)
class SourceSettings:
    mqtt_host: str
    mqtt_port: int
    event_types: tuple[str, ...] = ("vp", "arr", "dep")
    recordings_dir: Path = Path("data/recordings")

    @classmethod
    def from_env(cls) -> SourceSettings:
        return cls(
            mqtt_host=os.getenv("HSL_MQTT_HOST", "mqtt.hsl.fi"),
            mqtt_port=int(os.getenv("HSL_MQTT_PORT", "8883")),
            event_types=tuple(
                event.strip()
                for event in os.getenv("HFP_EVENT_TYPES", "vp,arr,dep").split(",")
                if event.strip()
            ),
            recordings_dir=Path(os.getenv("RECORDINGS_DIR", "data/recordings")),
        )

    @property
    def mqtt_topics(self) -> list[str]:
        """One subscription per event type, for ongoing journeys only."""
        return [f"/hfp/v2/journey/ongoing/{event}/#" for event in self.event_types]


@dataclass(frozen=True)
class GtfsSettings:
    url: str
    download_dir: Path

    @classmethod
    def from_env(cls) -> GtfsSettings:
        return cls(
            url=os.getenv("GTFS_URL", "https://infopalvelut.storage.hsldev.com/gtfs/hsl.zip"),
            download_dir=Path(os.getenv("GTFS_DOWNLOAD_DIR", "data/gtfs")),
        )
