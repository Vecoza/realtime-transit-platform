# Stream processor (storage sink): design notes

The sink is the consumer group `sink` on `hfp.raw`. It turns raw envelopes into validated, deduplicated, timetable-matched rows in PostgreSQL. Anything it cannot use goes to the dead-letter topic `hfp.dlq` with a reason, so no message disappears silently.

```
hfp.raw ──▶ consume batch ──▶ drop copies in batch ──▶ validate ──▶ invalid ──▶ hfp.dlq (flushed)
                                                          │
                                                          ▼ valid
                   COPY to staging ──▶ match journeys ──▶ INSERT … ON CONFLICT DO NOTHING
                                                          │
                                         metrics row ──▶ COMMIT ──▶ commit Kafka offsets
```

## Delivery: at least once, stored exactly once

Offsets are committed manually and only after the database transaction has committed. Two crash windows follow from that order, and both are safe:

| Crash | Effect on restart | Why it is safe |
|---|---|---|
| Before the database commit | The batch is consumed again | The transaction rolled back, so nothing of it was stored: **no loss** |
| After the database commit, before the offset commit | The batch is consumed again | Every row is already there; the unique key turns each insert into a no-op: **no duplicates** |

The dead-letter topic is flushed before the database commit. In the second crash window a rejected message can therefore be dead-lettered twice. The dead-letter topic is at least once, which is acceptable for a topic that people read to investigate.

Rebalances need no special handling. The loop is single-threaded and commits synchronously at the end of each batch. Partitions are only revoked inside `consume`, so there is never an uncommitted batch at that point. The group uses `cooperative-sticky` assignment, so a rebalance only moves the partitions that change owner.

## Deduplication

The unique key is the partition column plus what identifies one distinct message:

| Table | Primary key |
|---|---|
| `rt.vehicle_positions` | `(operator_id, vehicle_number, tst, payload_hash)` |
| `rt.stop_events` | `(operator_id, vehicle_number, event_type, tst, payload_hash)` |

- The vehicle comes from the topic (`operator_id/vehicle_number`, the owner), as in the message key.
- `tst` is the partition column, so PostgreSQL can enforce the key per partition.
- `payload_hash` is a 16-byte BLAKE2b of the raw payload text. Exact copies collide and are dropped. Two different positions with the same `tst` are kept: metros report `tst` in whole seconds, and a key without the hash would silently drop real positions. In the positions table the event type is implied by the table.

Copies are removed twice. Within a batch, before decoding, using `(topic, payload)`; this saves validation and database work, since most copies arrive within the same second. Across batches and restarts, the primary key with `ON CONFLICT DO NOTHING` catches the rest.

On the 30-minute Saturday recording (1,982,660 messages), **557,220 messages (28.1%) were duplicates**. 1,425,440 rows were stored and none were invalid.

## Validation and the dead-letter topic

Each payload is parsed and validated in one step by pydantic (`{"VP": {…}}` with `extra="forbid"`). Rejections carry one of these reasons:

| Reason | Example |
|---|---|
| `invalid_envelope` | The Kafka value is not an envelope |
| `unexpected_topic` | Not an HFP v2 `vp`/`arr`/`dep` topic |
| `invalid_json` | Payload cut off mid-JSON |
| `unexpected_payload_shape` | Topic says `arr`, payload is `{"VP": …}` |
| `schema_violation` | `lat` is a string, `hdg` 400, `dir` "3", `veh` missing |
| `tst_out_of_range` | Event time more than a day before or ten minutes after `received_at` |

The last rule keeps nonsense timestamps out of the daily partitions. A dead letter holds the reason, a short detail, the source topic, partition and offset, and the original envelope.

The real feed is clean by these rules. The Saturday replay (1,982,660 messages) and the five-minute live run left the dead-letter topic empty: zero messages for every reason. What the feed does send is incomplete data inside a valid shape, which is stored, not rejected: 9,831 positions (0.7%) without coordinates and 15,366 without speed. The reasons themselves are covered by the tests with constructed messages, one per rule.

## Trip identity and matching

The durable identity of an HFP trip is **(route, direction, operating day, scheduled start)**. It is stored on every row, together with the `trip_id` and `feed_id` it was matched to:

- `route_id`: HFP `route`, which equals GTFS `route_id`.
- `direction_id`: HFP `dir` minus one, stored in GTFS convention (0/1).
- `oday`: HFP operating day.
- `start_time`: HFP `start` as given (`HH:mm`, 24-hour local).
- `start_offset_seconds`: the same start as a GTFS offset on `oday`. HFP gives `start` as local wall-clock time, so a trip that starts at `25:10` in GTFS reports `01:10`. When `start` read on `oday` lies more than 12 hours before the event, the trip started after midnight, on the next calendar day. The local time is converted to UTC in the Helsinki zone and counted from the GTFS reference point of `oday` (noon minus twelve hours); on ordinary days that equals hours and minutes, on the nights the clocks change it does not (below).

The feed is the one that was current on the operating day (`gtfs.feed_for_service_date`): among the versions whose validity covers `oday`, the one HSL built most recently on or before `oday`. The build date comes from `feed_info.feed_version`, which HSL fills with the build time; other feeds fall back to the files' dates in the zip, and a version without any build date counts as built on the day it was loaded. Choosing by build date rather than load time keeps backfills correct: a build loaded today for a day last week is used for that day, and does not replace the newer build for later days. If no version was built by `oday`, the oldest covering one is used. The trip is the one in that feed with the same route and direction, a start within that minute, and a service that runs on `oday` according to `gtfs.service_dates`. For `arr` and `dep`, the stop is the occurrence of the event's stop on that trip whose planned time is closest to `tst`; trips can visit a stop twice. The planned time is the arrival offset for `arr` and the departure offset for `dep`, converted with `gtfs.to_utc(oday, offset)`.

Matching is set-based. Rows are copied into a temporary staging table, and the distinct journeys of the batch (about 750 in 5,000 messages) are matched once each. The feed is looked up once per distinct operating day. Rows that do not match are stored anyway with a reason:

| `match_status` | Meaning |
|---|---|
| `matched` | Trip found (and, for stop events, the stop on it) |
| `missing_journey` | Route, direction, operating day or start missing in the message |
| `no_feed` | No loaded feed covers the operating day |
| `route_not_in_feed` | The feed has no such route |
| `no_trip` | The route exists, but no trip with that direction and start runs that day |
| `missing_stop` | Stop event without a stop id (stop events only) |
| `stop_not_on_trip` | The trip does not serve that stop (stop events only) |

Saturday recording, matched against the 24 October build:

| Table | Rows | Matched | Not matched |
|---|---:|---:|---|
| `rt.vehicle_positions` | 1,396,803 | 99.81% | 2,683 `route_not_in_feed` |
| `rt.stop_events` | 28,637 | 99.94% (trip and stop) | 17 `route_not_in_feed` |

All unmatched rows come from two HFP routes that do not exist in GTFS: `3002X3` (a train) and `1009TX` (a tram).

HSL's own scheduled times (`ttarr`/`ttdep`) are an independent check of the matching. They equal the planned time found through the GTFS feed for **98.9% of arrivals and 99.0% of departures**. The remaining 291 events are all buses and differ by whole minutes, mostly one to three, with the GTFS time earlier. They are not wrong matches:

- **Not ambiguous.** Every one of the 1,214 matched journeys has exactly one candidate trip with that route, direction, operating day and start, so the matching never has to pick between trips.
- **Not another build.** The trip in the previous day's build has the same times; none of the 291 equals HSL's time there either.
- **Approximate stops.** 284 of the 291 (97.6%) are at stops that GTFS marks `timepoint=0`, where the published times are approximate. On those stops 1.1% of events disagree, on timing points 0.3%. The differences appear at the same stops on consecutive trips of a route and are back to zero at the next timing point. HSL's vehicle timetable interpolates between timing points differently from its GTFS export.

The platform's delay is therefore exact at timing points and can be off by a minute or two between them. The `timepoint` flag is not loaded yet; it is the column a punctuality analysis would filter on.

## The night the clocks go back

On 25 October 2026 at 04:00 summer time (01:00 UTC) the clocks in Helsinki went back to 03:00, so the local hour 03:00–04:00 happened twice. GTFS has no repeated hour: its times count from noon minus twelve hours on the service day, and the 24 October timetable has night trips at `27:13` (00:13 UTC, 03:13 summer time) and `28:13` (01:13 UTC, 03:13 winter time) on the same routes. Both report `start` `03:13` if HFP gives wall-clock time, as its documentation says.

Two things follow:

- **After the change, wall clock and GTFS offset differ by an hour.** A trip at `29:10` starts at 04:10 on the wall clock. Reading `04:10` as hours and minutes would give `28:10` and match the wrong trip, or none. The decoder therefore converts the local time to UTC and counts from the service day's reference point.
- **A start in the repeated hour has two readings.** The decoder takes the later one (winter time) once it is at most ten minutes ahead of the event, and the earlier one otherwise. On the Saturday sample, the first position of a journey came within 4.6 minutes before its start at the 90th percentile and 14.7 minutes at the 99th, and night trips starting in that hour take 35 minutes (median) to 53 minutes (90th percentile). The rule misreads the last minutes of a summer-time trip that runs more than 50 minutes, and the first positions of a winter-time trip that logs on more than ten minutes early. Telling them apart for certain would need the vehicle's earlier messages or the trip's duration.

The recording of that night stopped after 21 minutes, before the change: the recording machine went to sleep at 23:51 UTC. It holds 206,134 messages from 23:30 to 23:51 UTC (02:30–02:51 summer time) and none from the repeated hour; HSL publishes no archive of past HFP messages to fill the gap. The tests therefore combine two sources. Real messages of five night journeys from that recording (`tests/fixtures/hfp_dst_night.jsonl`) are stored with their UTC times, operating day 24 October, and the planned times of the real timetable, which equal HSL's own `ttarr`/`ttdep` except at one approximate stop. The repeated hour is covered by arrivals built from the two real `1009N` trips at `27:13` and `28:13`: one at 03:16 summer time and one at 03:16 winter time, both with `start` `03:13`, each matched to its own trip and planned time. That HSL reports `start` as wall-clock time on this night is taken from the documentation and still to be confirmed against a recording of the next such night (31 October 2027).

## Two delays, side by side

Every delay in the platform is positive when the vehicle is late. There are two of them, with different sources and uses:

- **`live_delay_s`**: HSL's own deviation from schedule, `−dl`. HSL's `dl` is negative when late; the sign is turned once, when a message is decoded, and nowhere else. It is in every message, so it needs nothing but the stream. The aggregator uses it for the per-minute statistics, which must be computable from the stream alone.
- **`measured_delay_s`**: `tst − planned_time`, the platform's own delay against the GTFS timetable that was valid on the operating day. It needs the trip match and the timetable join, which only the sink does. It is stored on stop events and is what the analytical models use, because its reference point is defined by the published timetable.

Positions carry `live_delay_s` only; stop events carry both. They measure different moments. For matched events with both values, the difference `measured_delay_s − live_delay_s` is:

| Event | Events | Median | p10 | p90 | Same sign when more than 60 s off |
|---|---:|---:|---:|---:|---:|
| `arr` | 14,096 | −29 s | −75 s | +13 s | 97.3% |
| `dep` | 14,103 | +16 s | +4 s | +34 s | 99.7% |

`arr` is sent when a vehicle enters the stop radius, about half a minute before the moment HSL's offset refers to, so the platform's arrival delay reads about 30 s lower. `dep` is sent after the vehicle leaves the radius, so the departure delay reads about 16 s higher. On average over the Saturday arrivals, `live_delay_s` is 115 s and `measured_delay_s` 77 s. The two agree on whether a vehicle is more than a minute late or early in 97–99.7% of events, so either is fine for a live view; for punctuality against the timetable, the analytical models use `measured_delay_s`.

## Partitions

Both tables are range-partitioned by `tst` on UTC days (`rt.vehicle_positions_20261024` holds `[2026-10-24 00:00Z, 2026-10-25 00:00Z)`). `oday` is a separate column. A night bus at 01:10 local time on 25 October with `oday` 24 October lies in the 24 October UTC partition, and service-day queries filter on `oday` instead. `rt.ensure_day_partitions(first, last)` creates missing partitions idempotently. The sink calls it in its own short transaction for every event day it has not seen yet. A `DEFAULT` partition exists as a safety net; the `tst_out_of_range` rule keeps it empty, and a non-empty default partition means something is wrong.

## Batching and metrics

A batch is up to 5,000 messages or 500 ms, whichever comes first. Live, at about 800–1,100 messages per second, the time limit decides (about 800 messages per batch). In a replay or catch-up, the count limit decides. Each batch writes one row to `meta.consumer_metrics` in the same transaction as its data: messages, inserted, duplicates, invalid, unmatched, consumer lag, the time spent consuming, decoding and writing, and the p50/p95 of `stored_at − tst` for the inserted rows. The check `inserted + duplicates + invalid = messages` holds for every batch.

## Measured

Docker Desktop on a MacBook, PostgreSQL 16 with default settings (128 MB `shared_buffers`), Redpanda single node, one sink process.

**Throughput.** The 30-minute Saturday recording was replayed into a fresh topic, then consumed from the beginning:

| Version | Messages/s | Decode per batch | Database per batch |
|---|---:|---:|---:|
| First version | 3,885 | 420 ms | 829 ms |
| Feed looked up once per day, copies dropped before the database, payload parsed by pydantic | **7,473** | 263 ms | 370 ms |

The first version evaluated the feed lookup once per output row instead of once per operating day, because PostgreSQL inlined the lateral subquery into the projection. `EXPLAIN ANALYZE` showed about half of the insert's 940 ms going there. Materialising the `feeds` and `matches` CTEs fixed it. Dropping copies inside the batch removed 28% of the rows before validation and before the unique-index probes. 7,473 messages per second is about seven times the live rate.

**End-to-end latency against the live feed** (Saturday 24 October, 16:30 Helsinki time, 5 minutes, 238,714 stored rows; producer and sink running together):

| | p50 | p95 | p99 |
|---|---:|---:|---:|
| Event time → stored (`stored_at − tst`) | 0.52 s | 1.11 s | 1.93 s |
| Received → stored (`stored_at − received_at`) | 0.56 s | 1.09 s | |
| Event time → received (`received_at − tst`) | −0.05 s | 0.11 s | |

The platform's own share is dominated by the 500 ms batch window plus the producer's 50 ms linger. The source is usually fast, but not always. 188 bus positions arrived more than a minute after their `tst`, the latest 2 minutes 53 seconds after, when vehicles sent buffered messages in a burst. Those late events are what the event-time aggregation has to handle.

## Crash tests

**Automated, in CI.** The fixture is replayed into a fresh topic and the sink is interrupted at an exact point by raising an exception there, which closes the consumer without committing offsets, as a dying process would:

| Test | Result |
|---|---|
| Crash after the database commit, before the offset commit; restart | The interrupted batch is consumed twice (`sum(messages)` in the metrics exceeds the fixture). The tables hold exactly the expected rows: **no duplicates** |
| Crash before the database commit; restart | Nothing is stored after the crash; after the restart every expected row is there: **no loss** |
| Replay the whole fixture a second time | Row counts unchanged; every message of the second pass counts as a duplicate |

**Manual, with real processes.** The sink consumed the 1,982,660-message Saturday topic and was killed with `kill -9` six times, 19 to 39 seconds after each start. It was then left to finish until the lag was zero. The result was identical to the uninterrupted run: 1,396,803 positions and 28,637 stop events, with every message accounted for exactly once in `meta.consumer_metrics`.

## Restarts and static membership

After a `kill -9` the dead consumer cannot leave the group. With ordinary (dynamic) membership the broker keeps its partitions assigned until the session timeout (45 s) expires, and a restarted process joins as a new member and waits for that. The sink therefore uses static membership: it joins with a fixed `group.instance.id` (`SINK_INSTANCE_ID`, by default `<group>-<hostname>`). A restarted process with the same id is recognised as the same member and gets its partitions back without a rebalance.

Measured by killing the sink with `kill -9` every 8 seconds while it consumed the Saturday topic, and timing from the kill to the restarted process's first committed batch (six rounds each):

| Membership | Median | Range |
|---|---:|---:|
| Dynamic | 45.2 s | 45.2–45.4 s |
| Static | **0.9 s** | 0.8–1.0 s |

The cost is on the other side: a static member that stops for good, also after a clean shutdown, holds its partitions for the session timeout before they move to another member. That is the right trade for a sink that is restarted far more often than it is scaled down. Two processes with the same id fence each other off; the sink stops on that fatal error instead of running as a second copy.
