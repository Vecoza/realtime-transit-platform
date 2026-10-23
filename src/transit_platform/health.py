"""Connectivity checks for the database and the event stream."""

from __future__ import annotations

import socket
import sys

from sqlalchemy import create_engine, text

from transit_platform.config import DatabaseSettings, StreamSettings


def check_database() -> str:
    engine = create_engine(DatabaseSettings.from_env().url, pool_pre_ping=True)
    with engine.connect() as conn:
        return conn.execute(text("SELECT version()")).scalar_one()


def check_stream(timeout: float = 3.0) -> str:
    first = StreamSettings.from_env().bootstrap_servers.split(",")[0]
    host, port = first.rsplit(":", 1)
    with socket.create_connection((host, int(port)), timeout=timeout):
        return f"{host}:{port} reachable"


def main() -> int:
    failed = False
    for name, check in (("database", check_database), ("stream", check_stream)):
        try:
            print(f"{name}: {check()}")
        except Exception as exc:
            print(f"{name}: failed ({exc})", file=sys.stderr)
            failed = True
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
