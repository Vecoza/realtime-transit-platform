# Storage lifecycle: design notes

PostgreSQL holds the recent days, where the sink writes and dashboards read. Older days move to Parquet files, which DuckDB and the analytical models read. The job that moves them is `archive.lifecycle`; it also exports and retires timetable versions ([design_gtfs.md](design_gtfs.md#retention)).

```
data/archive/
├── vehicle_positions/utc_day=2026-10-24/part-0.parquet
├── stop_events/utc_day=2026-10-24/part-0.parquet
├── route_minute_stats/utc_day=2026-10-24/part-0.parquet
├── stop_minute_stats/utc_day=2026-10-24/part-0.parquet
└── gtfs/<stop_times|trips|service_dates|routes|stops>/feed_id=2/part-0.parquet
```

One directory per table and UTC day, named like a Hive partition, so DuckDB reads the day as a column (`utc_day`) and skips directories a query does not need. The day is the UTC day of the PostgreSQL partition, not the operating day: a night bus at 01:10 Helsinki time on 25 October has `oday` 24 October and lies in the 24 October UTC partition, exactly as in PostgreSQL.

## What moves, and when

Every daily partition of `rt.vehicle_positions`, `rt.stop_events`, `rt.route_minute_stats` and `rt.stop_minute_stats` whose day is more than `ARCHIVE_KEEP_DAYS` (7) days old. By then the partition no longer changes: the sink rejects events more than a day older than their receipt, and the aggregator's correction horizon is ten minutes. The same run creates the partitions for the next seven days, so the sink never has to.

## Steps

Each partition goes through four steps, and each is recorded in `meta.archive_runs` with its row count:

| Step | What happens |
|---|---|
| `exported` | DuckDB, with PostgreSQL attached read-only, copies the partition to `part-0.parquet.partial` (zstd), which is then renamed into place. A reader never sees a half-written file. |
| `verified` | The rows in the file equal both the rows DuckDB wrote and `count(*)` of the partition in PostgreSQL. Otherwise a `failed` step is recorded and the run stops. |
| `detached` | `ALTER TABLE … DETACH PARTITION` |
| `dropped` | `DROP TABLE` of the detached partition |

Detach and drop run in one transaction together with their two records, so a partition is either still attached or gone and logged as gone; there is no state in between. `DETACH PARTITION CONCURRENTLY` would not block the sink's inserts, but PostgreSQL does not allow it on a table with a default partition, and these tables have one. A plain detach takes an exclusive lock on the parent for a moment; the job waits at most ten seconds for it (`lock_timeout`) and otherwise fails and tries again on its next run.

**Idempotent and resumable.** A run decides what to do from the database, not from what it did last time:

- A partition that still exists is exported unless the last recorded step is `verified` for exactly its current row count and the file is in place with those rows. A run stopped after the export continues with the drop and does not export again.
- A run stopped in the middle of an export leaves only a `.partial` file and no record; the next run exports again.
- An export that does not match is never dropped. The test replaces the export with one that is one row short: the run stops with `exported`, `failed`, and the partition is untouched.
- A second run with nothing to do adds no records and takes about a second.
- If a partition for an archived day appears again (a replay of an old recording would create it), the job records a `failed` step and leaves both the partition and the archive alone, instead of overwriting the archive with a part of the day.
- Two runs never work at the same time: a run takes a PostgreSQL advisory lock and exits at once if another run holds it.

## Measured

The 30-minute Saturday sample (24 October), archived on this machine. PostgreSQL sizes are after `VACUUM FULL`, with indexes; a changed column sign had rewritten every row and left the positions partition at twice its size until then.

| Table | Rows | PostgreSQL | Parquet | Ratio | Export |
|---|---:|---:|---:|---:|---:|
| `vehicle_positions` | 1,396,803 | 410.0 MB | 58.4 MB | 7.0× | 19.7 s |
| `stop_events` | 28,637 | 9.3 MB | 1.5 MB | 6.2× | 0.7 s |
| `route_minute_stats` | 8,951 | 1.2 MB | 0.12 MB | 10.6× | 0.1 s |
| `stop_minute_stats` | 13,179 | 1.6 MB | 0.10 MB | 15.1× | 0.1 s |
| GTFS `stop_times`, one version | 8,755,099 | 1,507.7 MB | 17.2 MB | 88× | 15.5 s |

Positions are the bulk of the data. At the Saturday-afternoon rate they take about 820 MB an hour in PostgreSQL and 117 MB in Parquet. The timetable compresses best: `trip_id` and `stop_id` repeat in long runs, which Parquet's dictionary and run-length encoding store once. The whole run, including the export of both timetable versions in the database and the retirement of the older one, took 74 seconds.

## Scheduling: supercronic, not Airflow

Three jobs run on a schedule in one container (`docker/app.Dockerfile`, `docker/crontab`), all times UTC:

| When | Job |
|---|---|
| 03:30 daily | GTFS refresh, then the lifecycle job, which exports the new version to Parquet |
| 00:40 daily | Lifecycle: archive old days, retire timetable versions |
| :15 every hour | `dbt build`: the analytical models and their tests |

The scheduler is [supercronic](https://github.com/aptible/supercronic), a single binary that runs a crontab inside a container, logs to standard output and, unless told otherwise (`-overlapping`), does not start a job while its previous run is still going. The jobs carry no state between runs: each one works out from the database what is due, so a run that was missed or failed is caught up by the next one, and a job can be run by hand at any time with `make`.

Airflow would add a scheduler, a web server and a metadata database to run three commands, two of them in a fixed order that one `&&` expresses. What it does better (dependency graphs across many jobs, backfills by date, retries with alerting, a UI to see history) does not matter at this size. The history is in `meta.archive_runs` and in the dbt run results. If the jobs grew into many dependent pipelines with date-based backfills, an orchestrator would pay for itself; the jobs would move over unchanged, as they are plain idempotent commands.

**Nothing is downloaded at run time.** The image installs DuckDB's PostgreSQL extension while it is built, and both the lifecycle job and dbt run with `autoinstall_known_extensions` off. With the container on a Docker network that reaches PostgreSQL but not the internet, `dbt build` ran and passed all 42 models and tests; the same container could not resolve the extension server's name. The supercronic binary is pinned to a release and checked against its published SHA-1 checksum.

## Disk usage on the development machine

Docker Desktop keeps everything in one disk image with a limit of 1,000 GB (`Docker.raw`), of which 161 GB are in use, almost all of it by images (159 GB, 148 GB of which unused and reclaimable) and build cache (20 GB) from other work. This project's data:

| Store | Size | Contents |
|---|---:|---|
| PostgreSQL volume | 2.3 GB | database 1.8 GB: the current timetable version (1.5 GB of `stop_times`), the remaining days |
| Redpanda volume | 913 MB | `hfp.raw` (193 MB, 24-hour retention) and replay and benchmark topics of the Saturday sample (`replay.sat` 353 MB, `bench.sat` 193 MB) |
| Parquet archive | 100 MB | the archived Saturday day (60 MB) and two timetable versions (40 MB) |
| DuckDB file | 20 MB | the analytical models |
| Recordings | 186 MB | compressed HFP recordings |
