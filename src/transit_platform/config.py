"""Settings loaded from environment variables (.env supported)."""

from __future__ import annotations

import os
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
