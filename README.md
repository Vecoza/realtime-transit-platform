# Realtime Transit Platform

A streaming data platform that ingests live vehicle positions from Helsinki's public transport network, processes them in real time, and compares what actually happens on the streets with the published timetable.

> **Status:** In development. The platform is being built in phases; see the roadmap below.

## Background

My earlier project, an academic data platform, was built around batch processing: data moves once a day, and a few hours of delay do not matter. Many real systems cannot wait that long. A public transport operator wants to know *now* that a line is running late, that two buses are travelling bumper to bumper, or that a vehicle has stopped reporting.

I wanted to learn how data engineering works when data never stops arriving, so I built this project in my free time. I chose Helsinki because its transport authority, HSL, publishes the position of every bus, tram, metro and train as an open real-time feed, together with the full timetable. It is real data, at real volume, with all the problems real data has: duplicates, late messages, gaps and vehicles that go silent.

## The problem

Every vehicle in the network reports its position about once per second. During a weekday peak that is well over a thousand messages per second. Each message on its own says little. The questions people actually ask need the stream to be cleaned, joined with the timetable and aggregated over time:

- Which lines are running late right now, and by how much?
- Which stops have the worst punctuality, and at what time of day?
- Where do vehicles bunch together instead of keeping their scheduled interval?
- Which vehicles have stopped sending data?

The data arrives continuously, out of order and sometimes twice. The platform has to process it with low latency, never lose or double-count a message, and still keep a reliable history for analysis.

## What the project builds

**1. Timetable loader (batch)**
Downloads HSL's static timetable in GTFS format (routes, trips, stops and scheduled stop times) and loads it into PostgreSQL. Each timetable version is kept, so a delay is always measured against the timetable that was valid on that day. A load is a single transaction that switches the current version only when every file has loaded and validated, and a timetable that is already known is never loaded twice. Scheduled times past midnight and on daylight saving change days are converted to UTC exactly as the GTFS reference defines them. The full HSL timetable (8.4 million scheduled stop times) loads in under three minutes.

**2. Stream producer**
Subscribes to HSL's live MQTT feed and publishes every message to a Kafka-compatible event stream (Redpanda). It can also record the live stream to files and replay it later at any speed, which makes development and testing independent of the live feed. Messages are keyed by vehicle, so each vehicle's events stay in order. An idempotent producer means retries never duplicate a message, and a replay of the same recording always produces exactly the same topic contents. On a Saturday afternoon the feed delivers about 1,100 messages per second, more than a quarter of them duplicates. A single producer process can publish about 28,000 per second.

**3. Stream processor**
Consumes the stream, validates every message against a schema, removes duplicates, enriches positions with timetable data, and writes them to PostgreSQL. Invalid messages go to a dead-letter topic instead of being lost. Writes are idempotent and offsets are committed only after the data is stored, so a crash never loses or duplicates data. Every event keeps the durable identity of its trip (route, direction, operating day and scheduled start) together with the matching trip in the timetable that was valid that day. Arrivals and departures also get their planned time and a delay computed by the platform, stored next to the operator's own delay value. On a 30-minute Saturday sample, 28% of all messages were duplicates and 99.8% of the stored events matched a scheduled trip. The processor stores about 7,500 messages per second, and live events are in the database about half a second after the vehicle sent them (p95 1.1 s).

**4. Real-time aggregation**
A second, independent consumer of the same stream computes per-minute statistics by event time: active vehicles and delay per line and direction, arrivals and delay per stop, and which vehicles have gone silent. How long to wait for late messages is taken from the measured data: 99.6% of messages arrive within ten seconds, and most of the rest are bursts from vehicles that lost their connection and later send what they buffered. Each minute is therefore published ten seconds after it ends, and later messages correct it for up to ten minutes. A duplicate or a message consumed again after a crash never counts twice, because every minute keeps the identities of its events and is always rewritten in full. After a restart the open minutes are rebuilt from the stored events. The streamed results equal a batch recomputation in SQL over the same data, also when the process is killed in the middle of a replay.

**5. Storage lifecycle and analytics**
Recent data lives in daily-partitioned PostgreSQL tables. After seven days a partition is exported to Parquet, the file is checked row for row against the database, and only then is the partition dropped; every step is logged, and an interrupted run continues where it stopped. A day of vehicle positions takes seven times less space in Parquet, and a timetable version 88 times less. Old timetable versions are retired the same way. Analytical models in DuckDB read recent days from PostgreSQL and older days from Parquet, so the history stays complete while the operational database stays small: punctuality per line and hour at the timetable's timing points, delay per stop, bunching measured against the scheduled interval between two trips, and how much of the timetable the data covers. The models give identical results before and after a day is archived. A small scheduler container refreshes the timetable and archives daily and rebuilds the models hourly.

**6. Monitoring dashboard**
Grafana dashboards, provisioned as code, show live delays per line alongside the health of the pipeline itself: throughput, consumer lag and dead-letter counts.

## Architecture

```
┌──────────────┐   ┌──────────┐   ┌───────────┐   ┌──────────────┐   ┌────────────┐
│ HSL MQTT     │──▶│ Producer │──▶│ Redpanda  │──▶│ Stream       │──▶│ PostgreSQL │──▶ Grafana
│ live feed    │   │ (Python) │   │ (Kafka)   │   │ processor    │   │ partitions │
└──────────────┘   └──────────┘   └───────────┘   └──────────────┘   └─────┬──────┘
                                        │                                  │
                                   dead-letter                     Parquet archive
┌──────────────┐                                                   + DuckDB, dbt
│ GTFS         │──▶ timetable tables in PostgreSQL ──▶ enrichment
│ timetable    │
└──────────────┘
```

## Tech stack

| Area | Tools |
|---|---|
| Language | Python 3.11+ |
| Event streaming | Redpanda (Kafka API) |
| Source protocol | MQTT |
| Database | PostgreSQL 16 (native partitioning) |
| Archive and analytics | Parquet, DuckDB, dbt |
| Monitoring | Grafana |
| Infrastructure | Docker, Docker Compose |
| Quality | pytest, ruff, GitHub Actions |

## Repository structure

```
realtime-transit-platform/
├── src/transit_platform/   Shared package: configuration, database and stream connections
├── gtfs/                   Timetable download and loading
├── producer/               MQTT subscriber, recorder and replayer
├── processor/              Stream validation, enrichment and storage
├── archive/                Partition export to Parquet
├── dbt/                    Analytical models and tests
├── grafana/                Dashboards and data sources as code
├── docker/                 Scheduled jobs image, database initialization
├── docs/                   Architecture and design notes
└── tests/                  Python tests
```

## Roadmap

- [x] Project foundation, containerized database and event stream, continuous integration
- [x] Timetable loader (GTFS)
- [x] Stream producer with record and replay
- [x] Stream processor: validation, deduplication, enrichment, dead-letter topic
- [x] Event-time aggregation with late data handling
- [x] Partitioned storage, Parquet archive and analytical models
- [ ] Monitoring dashboards
- [ ] Documentation: architecture, design notes and results

## Possible improvements

- **Faster `shapes` and `trips` loads.** Together they take about 55 of the 173 seconds of a full timetable load, mostly spent on per-row foreign key checks. Partitioning them by feed version like `stop_times` would let each version load into a plain table and be attached afterwards, with the index built in one pass and the foreign keys checked in one query.

- **Incremental models.** The analytical models are rebuilt in full every hour, which takes seconds for weeks of data. With a year of history they would be built incrementally by service day, rebuilding only the last few days, the only ones that can still change.
- **Trips seen by position.** Metro trains and ferries send positions but no arrival or departure events, so the completeness model leaves them out. A small per-trip summary written by the stream processor would cover every mode without reading all positions.
- **More than one aggregator.** A line's vehicles are spread over all partitions of the raw topic, so one aggregator reads all of them. Republishing the events keyed by line would let several instances share the work.

## Data source

Real-time and timetable data: © Helsinki Region Transport (HSL), published as open data under the [Creative Commons Attribution 4.0](https://creativecommons.org/licenses/by/4.0/) licence. Details are in [docs/data_sources.md](docs/data_sources.md).

## License

MIT
