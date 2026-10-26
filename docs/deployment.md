# Deployment on a Linux server

The whole platform runs from `docker-compose.yml`. A clean checkout and one `.env` file are all a server needs: no Python on the host, no manual migration, no manual topic or role setup.

## What runs

| Service | What it does | Restart policy |
|---|---|---|
| `postgres` | PostgreSQL 16; schemas created on the first start of its volume | `unless-stopped` |
| `redpanda` | Single-node broker for the raw and dead-letter topics | `unless-stopped` |
| `init` | One-off preparation, then exits: database migrations, the password of the dashboards' read-only role, a timetable if none is loaded yet, and the timetable's Parquet export | none (runs on every `up`) |
| `producer` | HSL's MQTT feed into the raw topic | `unless-stopped` |
| `sink` | Raw topic into PostgreSQL, invalid messages to the dead-letter topic | `unless-stopped` |
| `aggregator` | Per-minute event-time statistics and vehicle status | `unless-stopped` |
| `scheduler` | Daily timetable refresh, storage lifecycle, hourly dbt build and publish of the marts | `unless-stopped` |
| `grafana` | Dashboards and alert rules, provisioned from `grafana/` | `unless-stopped` |

`producer`, `sink`, `aggregator` and `scheduler` start only after `init` has finished successfully and, for the stream services, once Redpanda is healthy. They share one image built from `docker/app.Dockerfile`. The sink and the aggregator use fixed static member ids (`sink-1`, `aggregator-1`), so a restarted container rejoins its consumer group as the same member instead of waiting for the old one to time out.

Every step of `init` is idempotent, so it runs on every `docker compose up`: migrations already applied are skipped, the password is set again from `.env`, and the timetable is downloaded only when no version is current. After that, the scheduler refreshes the timetable every night.

## Requirements

- Linux with Docker Engine and the Compose plugin (v2.20 or later).
- 4 CPU cores and 8 GB of memory are comfortable. At the weekday peak the feed delivers about 1,600 messages per second; one sink and one aggregator process each handle several times that.
- Disk: PostgreSQL holds two days of positions (up to about 20 GB a day) and three timetable versions (about 4.5 GB). The raw topic keeps one day of messages, a few GB. The Parquet archive grows by about 2–3 GB a day (positions take 117 MB per busy hour in Parquet) until it is moved elsewhere. Plan for at least 100 GB.
- Outbound access to `mqtt.hsl.fi:8883` (MQTT over TLS) and HTTPS to `infopalvelut.storage.hsldev.com` (timetable). No inbound port has to be open.

## Steps

1. Clone the repository and change into it.
2. Create `.env` from `.env.example` and set new passwords. Generate them rather than choosing words, for example with `openssl rand -hex 16`, once for each of `POSTGRES_PASSWORD`, `GRAFANA_ADMIN_PASSWORD` and `GRAFANA_READER_PASSWORD`. Keep the file readable only by the deploying user (`chmod 600 .env`). It is listed in `.gitignore` and never committed.
3. Build and start everything:

   ```
   docker compose up -d --build
   ```

   The first start downloads and loads the timetable (8.8 million stop times) before the stream services start. On the development laptop the whole `init` step took about two minutes (see below). `docker compose up -d` returns only once `init` has finished; follow it with `docker compose logs -f init`.
4. Check that `init` exited with code 0 and the other services are running:

   ```
   docker compose ps -a
   docker compose logs init
   docker compose logs -f sink aggregator
   ```

   The sink logs one `event=batch_committed` line per batch, about every half second.
5. Open Grafana through an SSH tunnel. All published ports are bound to `127.0.0.1` of the server, so nothing is reachable from outside:

   ```
   ssh -L 3000:127.0.0.1:3000 user@server
   ```

   Then browse to `http://localhost:3000`. The *Pipeline health* dashboard shows within a minute whether messages are flowing and stored.

## Updating

```
git pull
docker compose up -d --build
```

Changed services are recreated; `init` applies new migrations before the stream services restart. A consumer stopped with SIGTERM finishes and commits its current batch, so an update loses nothing, and the messages that arrive meanwhile wait in the topic.

## Tested from a clean checkout

The steps above were run on a fresh clone with nothing but `.env`, as a separate Compose project with empty volumes:

- `init` applied all migrations, set the read-only password, loaded the timetable and exported it to Parquet, then exited with code 0 after 126 seconds: download 4 s, staging every file 15 s, `shapes` 13 s, `trips` 11 s, `stop_times` 30 s plus 21 s to attach the partition, 118 s for the whole load, and 4 s for the Parquet export. This is the same load-then-attach path as on the host. It is the fastest of all measured loads and the figure quoted elsewhere: macOS throttles this laptop's CPU under sustained load, which only ever makes a load slower. A run started at full speed on 26 October was throttled to 33–93% within seconds and took 230 s; one under heavy throttling took 345 s.
- A first attempt took 49 minutes. The container code was the same; the laptop had gone to sleep three times during the load, for 42 of those minutes, and Docker's virtual machine sleeps with it. The measured run kept the laptop awake with `caffeinate`. On a server this does not arise.
- The producer, sink, aggregator and scheduler started by themselves afterwards. Within a minute the producer was forwarding about 1,320 messages per second and the sink was storing them, with almost every position matched to a trip.
- Grafana queried PostgreSQL as `grafana_reader`, and both alert rules loaded.
- The hourly job built the 14 models with their 37 tests in the scheduler container and published the marts.
- The laptop suspended twice for about 15 minutes during the test. Each time, the sink and the aggregator found on their first commit that the broker had removed them from their group, exited, and were restarted by Docker; they resumed from their last committed offsets without losing or duplicating data.

## Demo without the live feed

```
make demo
```

runs the platform in containers on the recordings committed with the tests, without the live feed and without an internet connection once the image is built:

1. creates `.env` from `.env.example` if there is none, builds the application image and starts PostgreSQL and Redpanda;
2. applies the migrations, sets the read-only password, loads the fixture timetable (an extract of HSL's timetable of 24 October 2026 with lines 79 and 400) and exports it to Parquet;
3. starts the sink, the aggregator and Grafana, but not the producer;
4. replays the fixture recording of those two lines (1,528 real messages of Saturday 24 October) as fast as possible;
5. waits until the sink and the aggregator have consumed the topic to its end (`python -m processor.wait`);
6. builds and tests the dbt models and publishes the marts;
7. prints the address of the *Punctuality* dashboard for that day.

The live panels of *Network now* stay empty, as they show only the last minutes; *Punctuality* and *Pipeline health* show the replayed data. `make down` stops everything; the data stays in the Docker volumes.

`make demo FULL=1` downloads HSL's current timetable instead and replays the recordings in `RECORDING` (a file or directory, by default `data/recordings`, which a fresh clone does not have; record some with `make record`). A recording only matches the timetable when it was made within the timetable's validity period, so recent recordings are the ones to use.

**Tested offline.** From a fresh clone with no `.env`, with every service except Grafana attached to a Docker network marked `internal`, which has no route outside the host (a request from that network to HSL's server timed out, the same request from the default network got an answer): the demo finished in about nine minutes including the image build, the replay took 2 seconds, and dbt built 15 models and passed 38 tests. The image is built so that a code change rebuilds without the network: the dependencies and DuckDB's PostgreSQL extension are installed in layers before the code is copied, and the package itself is installed without fetching build tools.

## Data

| Volume | Contents |
|---|---|
| `pgdata` | PostgreSQL |
| `redpanda` | Topics; the raw topic keeps one day |
| `appdata` | Timetable downloads, the Parquet archive and the DuckDB file of the analytical models |
| `grafana` | Grafana's own database (alert state, sessions); the dashboards themselves come from the repository |

`pgdata` and `appdata` hold everything worth backing up. `docker compose down` keeps all volumes; only `docker compose down -v` deletes them.

## Limits of this setup

- Redpanda runs as one node in its development mode, without replication. A broker failure stops ingestion until it restarts, and messages the producer could not deliver within two minutes are lost.
- Grafana allows anonymous viewing, which is safe only because its port is bound to localhost. Exposing it beyond a tunnel would need a reverse proxy with TLS and anonymous access turned off.
- The alert rules have no notification channel; alerts show in Grafana's alerting view.
- The containers and processes started on a development machine with the Makefile (`make process`, `make archive`, …) keep their files in `./data`, while the containers use the `appdata` volume. Run one or the other against a database, not both.
