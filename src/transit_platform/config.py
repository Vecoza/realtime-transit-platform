"""Settings loaded from environment variables (.env supported)."""

from __future__ import annotations

import os
from dataclasses import dataclass

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

    @classmethod
    def from_env(cls) -> StreamSettings:
        return cls(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"))


@dataclass(frozen=True)
class SourceSettings:
    mqtt_host: str
    mqtt_port: int

    @classmethod
    def from_env(cls) -> SourceSettings:
        return cls(
            mqtt_host=os.getenv("HSL_MQTT_HOST", "mqtt.hsl.fi"),
            mqtt_port=int(os.getenv("HSL_MQTT_PORT", "8883")),
        )
