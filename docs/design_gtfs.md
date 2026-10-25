# Timetable loader: design notes

The static timetable is the reference every live event is compared against. A delay only means something when it is measured against the timetable that was valid on the vehicle's operating day. The loader therefore keeps every timetable version it has seen, and it never changes the version other components read until a new one is fully loaded.

## Feed versions

`gtfs.feed_versions` has one row per distinct zip, identified by the sha256 of its bytes. A partial unique index (`WHERE is_current`) allows at most one current version. Every other table has `feed_id` as the first column of its primary key. Two versions with the same `trip_id` can therefore never collide, and a reader picks a version with a single equality filter.

The checksum is taken over the downloaded bytes, not over HTTP headers. `ETag` and `Last-Modified` belong to the storage service, and they could change while the content stays the same (or the reverse).

| Situation | Result |
|---|---|
| Checksum unknown | Load as a new version and make it current |
| Checksum equals the current version | Nothing happens (`unchanged`) |
| Checksum equals an older version | That version becomes current again (`reactivated`); no rows are copied |

The third case covers a publisher that rolls back to an earlier build.

A content hash over the extracted files was considered as the "already loaded" check, so that a zip that is repacked with unchanged content would not load again. The archived HSL builds show it would make no difference. Same-day rebuilds are byte-identical, and builds on different days always change rows, because `service_id` and `trip_id` contain the start date of a rolling window (see [data_sources.md](data_sources.md#how-the-feed-changes-between-builds)). `feed_info.feed_version` is no better: it is the build timestamp. The zip sha256 is the cheapest check that is correct for this source.

## One transaction per load

The whole load is one PostgreSQL transaction:

1. Take a transaction-level advisory lock, so two loaders never run concurrently.
2. Look up the checksum.
3. `COPY` every file into a temporary staging table, all columns `TEXT`.
4. Derive the validity range from the staged calendar files.
5. Insert the `feed_versions` row and create the feed's `stop_times` partition.
6. `INSERT … SELECT` from staging into the typed tables, in foreign key order.
7. Expand `service_dates`.
8. `ANALYZE` the touched tables (for `stop_times` only the new partition).
9. Flip `is_current`.

If any step fails, nothing is visible afterwards: no version row, no rows, no partition (PostgreSQL DDL is transactional). Readers keep using the previous current feed throughout, because the switch is the last statement before the commit.

## Staging and validation

Each file is copied with the binary COPY protocol straight from the zip stream, without decompressing it to disk first. The header is read separately, so the loader can:

- strip the UTF-8 byte order mark (present in HSL's `stop_times.txt`);
- reject the file early if a required column is missing, naming the columns;
- accept files whose columns come in any order, or that carry extra columns;
- add absent optional columns to the staging table as NULLs, so the `INSERT … SELECT` statements are static SQL.

Type conversion happens in SQL during the insert (`::INTEGER`, `to_date`, `gtfs.time_to_seconds`). A malformed value fails the cast and aborts the transaction. Constraints (`ck_stop_times_departure_after_arrival`, coordinate ranges, `direction_id IN (0, 1)`) and foreign keys catch values that parse but make no sense. Blank and whitespace-only values become NULL, because HSL writes a single space for empty optional fields.

## Times past midnight

GTFS times are not clock times. `25:10:00` on service date 24 October means 01:10 on the 25th, and that trip belongs to the 24th's timetable. Stop times are therefore stored as integer offsets in seconds (`arrival_offset_seconds`), never as `TIME`. An instant is only computed together with a service date.

The GTFS reference defines the offset origin as "noon minus 12 hours" on the service date in the agency's time zone. On most days that is local midnight. On the two days a year when the clocks change it is not:

| Service date | Origin (local) | Origin (UTC) | `05:00:00` becomes |
|---|---|---|---|
| 15 January | 00:00 EET | 14 Jan 22:00 | 05:00 EET |
| 29 March (clocks forward at 03:00) | 28 March 23:00 EET | 28 Mar 21:00 | 05:00 EEST |
| 25 October (clocks back at 04:00) | 01:00 EEST | 24 Oct 22:00 | 05:00 EET |

Times after the change keep their printed wall-clock value, which is how the timetable is published. `gtfs.to_utc(service_date, offset_seconds)` and the Python helper `gtfs.service_time.to_utc` implement the same rule. A test checks that they agree for ordinary days, both change days and offsets past 24 hours, with the database session in a different time zone.

## Service dates

`calendar` stores weekly patterns over a date range. `calendar_dates` stores exceptions (added or removed days), and a service can exist in `calendar_dates` alone. Asking "which trips run on date D" would otherwise repeat this logic in every query. `gtfs.service_dates` is that logic computed once per feed: the weekly pattern for each day in the range, minus removed days, plus added days. In the HSL feed of 23 October 2026 it holds 26,759 rows; on Monday 26 October, 23,703 trips run.

## Partitioning `stop_times`

`stop_times` holds 94% of the rows (8.4 million per feed). It is list-partitioned by `feed_id`:

- Removing an old version is `DROP TABLE` on one partition instead of deleting millions of rows.
- Queries filter on one `feed_id`, so the planner reads only that partition.
- `ANALYZE` after a load covers only the new partition.

The smaller tables are not partitioned. They are cleaned up through `ON DELETE CASCADE` from `feed_versions`.

### Loading `stop_times` without row-by-row checks

The first version created the partition up front and inserted into it. Every one of the 8.4 million rows then updated the primary key index and ran two foreign key triggers, each an index lookup. That took 2,003 seconds.

The loader now creates the partition as a plain table (`LIKE gtfs.stop_times`, with the check constraints and an extra `CHECK (feed_id = n)`), inserts into it with no index and no triggers, and then runs `ALTER TABLE gtfs.stop_times ATTACH PARTITION`. The attach builds the primary key in one sorted pass. It validates each foreign key with a single anti-join query, and the `feed_id` check lets PostgreSQL skip scanning for the partition bound. The guarantees are unchanged: a stop time pointing to an unknown trip or stop still fails the load (`fk_stop_times_stop`). The attach is still inside the load transaction, so readers never see a partially attached table.

## Measured on the real feed

PostgreSQL 16 in Docker Desktop on a MacBook (default server settings), feed of 23 October 2026 (57.8 MiB zip, 8.4 million stop times).

| Step | Partition first | Load, then attach |
|---|---:|---:|
| COPY all files into staging | 16.1 s | about 18 s |
| Insert `shapes` (451,298 rows) | 29.5 s | 30.6 s |
| Insert `trips` (316,924 rows) | 27.7 s | 25.1 s |
| Insert `stop_times` (8,407,774 rows) | 2,003.3 s | 65.1 s |
| Attach `stop_times` partition (index + foreign keys) | – | 27.9 s |
| Whole load, including service dates and `ANALYZE` | **2,080.0 s** | **172.6 s** |
| Reload of the same zip (checksum match) | | 0.98 s |

The download itself takes about 5 seconds. `shapes` and `trips` are now the next largest cost at 55 seconds together, again mostly per-row foreign key checks. They could use the same staging pattern if load time ever matters, since the feed is loaded once a day.

The staging time of the second run is derived from the total minus the logged durations of the other steps.

Storage per feed version: `stop_times` 1,380 MB (906 MB heap, the rest is the primary key), `shapes` 64 MB, `trips` 55 MB, everything else under 4 MB.

## Retention

Every daily HSL build is a new version (see above), and each one adds about 8.4 million `stop_times` rows, about 1.4 GB. Keeping all of them is not an option. Keeping none but the current one is not either: an event received at 00:30 belongs to the previous service day, and a late or replayed event must still be matched against the timetable that was current when it happened.

**What stays in PostgreSQL.** PostgreSQL needs a version for one reason only: the sink matches each arriving event against the version that was current on the event's operating day. Stored events already carry their planned time and trip, and the analytical models read every version from its Parquet export. So a version has to stay exactly as long as the sink can still receive an event whose operating day falls in the time that version was current.

That window is short. The sink rejects an event whose `tst` is more than a day before its receipt. A live event received today therefore has `tst` no earlier than yesterday, and its operating day is at most one day before the local date of `tst` (a night trip after midnight belongs to the previous day). The oldest operating day the sink can still see is **two days ago**.

A version is current from its build date until the next version's build date (the same rule that picks the version for an operating day, see [design_processing.md](design_processing.md#trip-identity-and-matching)). The set to keep is the current version plus every version that was current on any of the last two days:

```sql
SELECT feed_id
FROM (
    SELECT feed_id,
           lead(build_date) OVER (ORDER BY build_date, feed_id) AS replaced_on
    FROM gtfs.feed_versions
) AS versions
WHERE replaced_on IS NULL
   OR replaced_on > current_date - 2
```

With one build a day this is three versions, about 4.5 GB, almost all of it `stop_times`; the window is `ARCHIVE_KEEP_DAYS_TIMETABLE` (2, and never less). The version that is current is never dropped, even when an older version was reactivated.

The window holds for live data. A replay of a recording older than two days carries its original receive times, so the sink accepts it, but the version it needs may be gone. Such a replay needs the version loaded again first, from the downloaded zip that `data/gtfs` keeps.

**What happens to an older version.** The storage lifecycle job ([design_lifecycle.md](design_lifecycle.md)) exports every version to Parquet as soon as it is loaded, not only before it is dropped, because the analytical models read the timetable from there:

1. Export `stop_times`, `trips`, `service_dates`, `routes` and `stops` of that `feed_id` to `data/archive/gtfs/<table>/feed_id=<n>/`.
2. Verify the row count of every file against PostgreSQL, and record both steps in `meta.archive_runs`.
3. Once the version falls outside the set above: `DROP TABLE gtfs.stop_times_<n>`, then `DELETE FROM gtfs.feed_versions WHERE feed_id = <n>`; the foreign keys cascade to the small tables. This happens in one transaction, together with its `dropped` record.

Each step is idempotent. A version whose verified export is still in place for the same row count is not exported again, and nothing is dropped without a verified export of every table. A version is 8.76 million `stop_times` rows: 1,508 MB in PostgreSQL with its indexes, 17 MB in Parquet, exported in 15 seconds. Dropping it takes about 50 seconds, most of it the cascading deletes of trips and shapes. The export keeps the schedule history available in DuckDB, which the analytical models use for days whose version is no longer in PostgreSQL.

**Why stored history never needs an old feed in PostgreSQL.** The stream processor matches each event to the current feed when the event arrives. It stores the result on the event row: the planned time, `trip_id`, `route_id`, direction and `feed_id`. From then on, delay and punctuality are computed from the event row alone. Old versions are kept for the window in which events can still arrive late or be replayed, not for reading stored history. This also matters because `trip_id` values change with every build: resolving an old event against a later feed would fail.

## Not loaded

`translations.txt`, `fare_*.txt`, `areas.txt`, `stop_areas.txt`, `emissions.txt`, `transfers.txt` and HSL's non-standard `trips2.txt` are not used by the platform. Adding one later means adding a `GtfsFile` entry, a table and a migration.
