"""Command line entry point: download the configured GTFS feed (or use a local zip) and load it."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import psycopg

from gtfs.download import FeedFile, download_feed
from gtfs.loader import current_feed_id, load_feed
from transit_platform.config import DatabaseSettings, GtfsSettings
from transit_platform.logs import configure_logging


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", type=Path, help="load this local zip instead of downloading")
    parser.add_argument(
        "--if-missing",
        action="store_true",
        help="do nothing if a timetable version is already current (first start of a deployment)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = parse_args(argv)
    settings = GtfsSettings.from_env()
    database = DatabaseSettings.from_env()
    if args.if_missing:
        with psycopg.connect(database.conninfo) as conn:
            feed_id = current_feed_id(conn)
        if feed_id is not None:
            print(f"current: feed_id={feed_id}, nothing to load")
            return 0
    feed = (
        FeedFile.from_path(args.file)
        if args.file
        else download_feed(settings.url, settings.download_dir)
    )
    with psycopg.connect(database.conninfo) as conn:
        result = load_feed(conn, feed)
    print(f"{result.status}: feed_id={result.feed_id} sha256={feed.sha256[:12]}")
    for table, rows in result.row_counts.items():
        print(f"  {table:<15} {rows:>10}")
    if result.row_counts:
        print(f"  load time       {result.duration_seconds:>9.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
