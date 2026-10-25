# Analytical models: design notes

The analytical models answer the questions the stream itself does not: how punctual a line was over a day, which stops lose the most time, where vehicles bunch, and how much of the timetable the data actually covers. They are dbt models in DuckDB (`dbt/`), built every hour into one DuckDB file.

## Where the data comes from

DuckDB attaches PostgreSQL read-only and reads the Parquet archive directly:

| Data | Recent days | Archived days |
|---|---|---|
| Stop events | PostgreSQL `rt.stop_events` | `stop_events/utc_day=…/` |
| Timetable | Parquet export of each version | Parquet export of each version |

**Staging unions both, and never counts a day twice.** `stg_stop_events` takes the PostgreSQL rows of every UTC day that has not been archived and the Parquet rows of every day that has, as recorded by a `dropped` step in `meta.archive_runs`. A day becomes archived in the same transaction that drops its partition, so at every moment it is in exactly one of the two. Before the first archive run there are no files at all; `read_parquet` fails on an empty pattern, so the archived half is only added when files exist.

**The timetable is read from Parquet only.** One timetable version is 8.8 million `stop_times` rows; reading it from PostgreSQL through the attached connection every hour would move 1.5 GB of table, while the Parquet export is 17 MB and read in a fraction of a second. Every version is exported as soon as it is loaded (the cron entry for the timetable refresh runs the export right after it), and a version never changes after that. Reading it from one place also means the models give the same answer when a version has been dropped from PostgreSQL, which is what the analysis of an archived day needs: the scheduled headways of a day three weeks ago come from the version its events were matched to.

**Only arrivals and departures are read.** The marts need the trips and stops vehicles departed from, which `rt.stop_events` has. Positions are about 50 times as many rows and are not read by any mart.

## Layers

| Model | Grain | What it does |
|---|---|---|
| `stg_stop_events` | event | PostgreSQL ∪ Parquet, as above |
| `stg_gtfs_*` | timetable row | trips, stop times (with the timing-point flag), service dates, routes, stops of every version |
| `int_departures` | trip × stop × service day | matched `dep` events with the platform's `measured_delay_s`. Coupled units of a train or tram each report the trip; the first departure counts |
| `int_scheduled_departures` | trip × stop × service day | every departure the timetable planned on the observed days, in the version the events were matched to that day |
| `mart_on_time_performance` | service day × local hour × route | punctuality at timing points |
| `mart_stop_punctuality` | service day × local hour × stop | delay at every stop |
| `mart_bunching` | service day × local hour × route × direction × stop | observed against scheduled headway |
| `mart_data_completeness` | service day × route | trips planned against trips seen |

**Local time.** The marts report the operating day (`oday`, the service day of the timetable) and the hour in Helsinki time. The hour is taken from the planned time, so a late departure counts in the hour it was due. On the night the clocks go back, both 03:00 hours are reported as hour 3.

## Definitions

**On time** is a departure no more than 60 seconds early and less than 180 seconds late (`-60 ≤ measured_delay_s < 180`). An early departure is worse for passengers than a late one, as they miss the vehicle, so the early side is narrower. `mart_on_time_performance` counts departures at **timing points** only: about 8% of the scheduled stops, the ones whose times are binding. Elsewhere the published times are interpolated and differ from HSL's own by a minute or two for about 1% of events (see [design_processing.md](design_processing.md#trip-identity-and-matching)). `mart_stop_punctuality` covers every stop, as it is about what a passenger at that stop experiences, and flags timing points.

**Delay** is `measured_delay_s`, the platform's delay against the published timetable ([design_processing.md](design_processing.md#two-delays-side-by-side)), not HSL's live value.

**Bunching** compares two consecutive departures of a route and direction at a stop. "Consecutive" is defined by the timetable: the trips are ordered by planned departure, so every pair has a scheduled headway, including when the trip in between was not observed. A pair counts when both trips were observed at that stop. It is **bunched** when the observed headway is less than half the scheduled one, and a **gap** when it is more than one and a half times. The thresholds are dbt variables.

**Completeness** compares the trips planned with at least one departure inside the period the stream covered that day with the trips that reported an arrival or departure. Metro and ferries report positions only, so their routes are left out rather than counted as missing.

## Tests

`dbt build` runs 33 tests after the models: unique keys on every model, accepted values (event types, directions, match statuses, modes), ranges (shares between 0 and 1, local hours, delays within two hours early and four hours late), and one test per mart:

- on time + early + late = departures;
- the departures in `mart_stop_punctuality` add up to `int_departures` per day;
- bunched and gap pairs never exceed the observed pairs;
- seen trips never exceed expected trips.

Two tests check the data across systems: the planned time the sink stored on each event equals the departure in the exported timetable, and every archived day still holds exactly the rows the archive log verified (the reconciliation of PostgreSQL, Parquet and what was stored).

## Published to PostgreSQL

The dashboards read only PostgreSQL, so after every hourly build the four marts are copied into the schema `marts` (`analytics.publish`). Each one is written to a new table next to the current one (`mart_bunching__next`), given its primary key and analysed, and then swapped in: drop the old table and rename the new one, in one transaction. A dashboard query sees the previous build or the new one, never a half-filled table, and waits at most for the moment of the swap. Readers get access through the schema's default privileges, so a new table is readable as soon as it exists.

The publish step also records the dbt run in `meta.dbt_runs`: when it ran, how long it took, how many models and tests ran and how many failed, and whether the marts were published. A run with a failed model or test is recorded and publishes nothing, so the dashboards keep the last build that passed its tests. Publishing the four marts of the Saturday sample (9,255 rows) takes 0.4 seconds.

## Plausibility: measured against live delay

The marts use the platform's `measured_delay_s`. HSL's own estimate, `live_delay_s`, comes with every event and is computed independently, so the two should tell the same story for the same departures. For the five routes and five stops with the highest average delay on the Saturday sample (at least 15 departures each):

| Route | Departures at timing points | Measured | Live | Median difference | Correlation | Same "late" verdict |
|---|---:|---:|---:|---:|---:|---:|
| 30 | 27 | 243 s | 220 s | +25 s | 0.997 | 96% |
| 114 | 15 | 228 s | 206 s | +23 s | 0.999 | 100% |
| 52 | 22 | 213 s | 203 s | +12 s | 0.990 | 100% |
| 200 | 20 | 213 s | 186 s | +29 s | 0.997 | 100% |
| 57 | 24 | 198 s | 186 s | +16 s | 0.992 | 100% |

| Stop | Departures | Measured | Live | Correlation | Same "late" verdict |
|---|---:|---:|---:|---:|---:|
| Apollonkatu | 16 | 409 s | 378 s | 0.998 | 100% |
| Sörnäinen (M) | 27 | 306 s | 284 s | 0.999 | 100% |
| Haapaniemi | 25 | 281 s | 257 s | 0.999 | 100% |
| Hakaniemi | 18 | 281 s | 156 s | 0.992 | 67% |
| Ruskeasuon varikko | 20 | 226 s | 211 s | 0.999 | 100% |

They agree. Measured delay is higher by 12–29 seconds (median), because `dep` is sent after the vehicle leaves the stop area, later than the moment HSL's estimate refers to (see [design_processing.md](design_processing.md#two-delays-side-by-side)). The "late" verdict (180 s or more) is the same for 99.6% of all 14,009 departures.

Hakaniemi is the exception, and the reason is the timetable, not the measurement. It is not a timing point, and at every departure there the GTFS time is 60 to 120 seconds earlier than HSL's own planned time (`ttdep`); measured against HSL's time, the delay matches the live value. This is the interpolation difference between timing points described in the processing notes. `mart_stop_punctuality` therefore flags timing points, and a stop that is not one can show up to two minutes more delay than HSL's own figures.

## Same results before and after archiving

A test stores real messages of two Saturday routes (`tests/fixtures/hfp_saturday_routes.jsonl` with the matching part of the timetable), builds the models, then archives the day, loads a newer timetable version and retires the one the events were matched to, and builds again. All four marts are identical row for row, and the archived rows equal the stored ones.

The same check on the whole Saturday sample: the marts and both intermediate models (498,814 scheduled departures) were identical before and after the day was archived and the older timetable version dropped.

## Measured

On the Saturday sample (30 minutes, 28,637 stop events), three runs each:

| | `dbt build` (models and tests) | Including dbt's start |
|---|---:|---:|
| Day in PostgreSQL | 6.9–8.8 s | 18–21 s |
| Day in Parquet | 4.3–4.5 s | 11–14 s |

The largest model is the scheduled departures of the day, half a million rows from the timetable, built in about half a second; most of the rest is dbt's own work per model and test.

Sample rows (24 October, 15:00–16:00 Helsinki time). Every mart carries the names a reader needs next to its keys: route short name and mode, stop code and name.

`mart_on_time_performance`

| route | mode | departures | on time | early | late | on-time share | avg delay | p90 delay |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 79 | bus | 31 | 28 | 0 | 3 | 0.903 | 89 s | 169 s |
| 520 | bus | 29 | 26 | 1 | 2 | 0.897 | 79 s | 157 s |
| 30 | bus | 27 | 10 | 0 | 17 | 0.370 | 243 s | 497 s |

`mart_stop_punctuality`

| stop | name | modes | departures | routes | on-time share | avg delay | p90 delay | timing point |
|---|---|---|---:|---:|---:|---:|---:|---|
| H2410 | Sörnäinen (M) | bus | 27 | 19 | 0.333 | 306 s | 628 s | no |
| H2406 | Haapaniemi | bus | 25 | 19 | 0.360 | 281 s | 630 s | no |
| H1928 | Ruskeasuo | bus | 21 | 11 | 0.429 | 163 s | 360 s | no |

`mart_bunching`

| route | mode | direction | stop | name | observed pairs | bunched | gaps | scheduled headway | observed headway | median ratio |
|---|---|---:|---|---|---:|---:|---:|---:|---:|---:|
| 400 | bus | 0 | H1949 | Kaarelankulku | 4 | 2 | 1 | 615 s | 393 s | 0.271 |
| 400 | bus | 0 | H1953 | Kaarelantie | 4 | 2 | 1 | 615 s | 395 s | 0.317 |
| 52 | bus | 0 | H1409 | Gert Skytten puisto | 4 | 1 | 0 | 615 s | 437 s | 0.859 |

`mart_data_completeness`

| route | mode | expected trips | seen | share |
|---|---|---:|---:|---:|
| 560 | bus | 20 | 19 | 0.950 |
| 570 | bus | 18 | 17 | 0.944 |
| 520 | bus | 18 | 18 | 1.000 |

Over the half hour, 1,141 of 1,184 expected trips were seen (96.4%), 1,211 departures at timing points were 79.8% on time, and 408 of 5,632 observed pairs were bunched (7.2%).

## Not done yet

The marts are rebuilt in full every hour. That is fine for weeks of data; for a year of history they would become incremental by service day, rebuilding only the last few days, which are the only ones that can still change.
