# Event-time aggregation: design notes

The aggregator is the consumer group `aggregator` on `hfp.raw`. It reads the same topic as the storage sink, with its own offsets, and turns the stream into per-minute statistics that stay correct when messages arrive late, twice, or after a crash.

| Table | One row per | Values |
|---|---|---|
| `rt.route_minute_stats` | route, direction, minute | vehicles, positions, live delay samples, average, median and p90 live delay |
| `rt.stop_minute_stats` | stop, minute | arrivals, live delay samples, average, median and p90 live delay |
| `rt.vehicle_status` | vehicle | last event, position, journey and its planned end; `active`, `stale` or `finished`; whether it is a suspect sign-on |
| `rt.late_events` | late event | the window it belonged to, the watermark when it arrived, `corrected` or `dropped` |

Every window row also carries `late_events`, `revision`, `first_emitted_at` and `emitted_at`.

The median and p90 are exact for the minute, computed from every counted event in the window, and the dashboards use them rather than the average: a vehicle still signed on to a journey it finished hours ago reports hours of delay, and one such vehicle can move a line's average by minutes while its median does not change. Windows emitted before the median was added have it empty.

The delay in these tables is `live_delay_s`, HSL's own deviation from schedule with the platform's sign (positive when late), taken from the message itself. A stream aggregate has to be computable from the stream alone; the platform's `measured_delay_s` against the timetable needs the timetable join that the sink does, and it is in `rt.stop_events` for analyses.

## Event time and processing time

A window is defined by **event time**: `tst`, the moment the vehicle reported. The minute in which a message is *received* or *processed* does not matter. Processing time is used only for two things that are about the platform itself: deciding that a partition has gone quiet, and the `emitted_at` stamps.

The cost of event time is that the aggregator never knows for sure that a minute is complete: a message about 12:00:30 can still arrive at 12:05. It has to decide when to stop waiting, and what to do with what comes after.

## How late is the data?

Lateness is `received_at − tst`, measured on the 30-minute Saturday recording (1,425,440 events after removing duplicates):

| | Events | p50 | p99 | p99.9 | max |
|---|---:|---:|---:|---:|---:|
| All | 1,425,440 | 0.0 s | 6.4 s | 987 s | 2,577 s |
| Bus | 1,074,029 | 0.0 s | 6.7 s | 1,504 s | 2,577 s |
| Tram | 185,164 | 0.0 s | 5.4 s | 7.6 s | 34 s |
| Train | 110,363 | 0.0 s | 5.8 s | 10.5 s | 35 s |
| Metro | 55,402 | 0.8 s | 5.5 s | 7.9 s | 58 s |

The distribution has two parts. Almost everything arrives within seconds: 96.8% within 2 s and 99.6% within 10 s. Then there is a long, thin tail that is not network delay at all. Two buses (`12/1603` and `12/1829`) lost their connection and later sent what they had buffered, up to 43 minutes old, in bursts of a few seconds. They account for 2,847 of the 3,208 events that arrived more than a minute late.

## Choosing the allowed lateness

A window closes when the watermark has passed its end plus the allowed lateness `L`. A larger `L` makes the first result more complete; a smaller `L` makes it earlier. What is left over arrives as a correction.

| Allowed lateness | Events later than that | First result for 12:00–12:01 at about |
|---:|---:|---:|
| 2 s | 3.24% | 12:01:02 |
| 5 s | 1.64% | 12:01:05 |
| **10 s** | **0.41%** | **12:01:10** |
| 30 s | 0.27% | 12:01:30 |
| 60 s | 0.23% | 12:02:00 |
| 120 s | 0.19% | 12:03:00 |

The curve flattens after 10 seconds. Going from 10 s to 2 minutes delays every result by 110 seconds and only moves the late share from 0.41% to 0.19%, because what remains is the buffered backlog, which no reasonable wait would cover. **The aggregator uses `L = 10 s`**: it covers the p99 with room to spare, and a dashboard shows a minute about ten seconds after it ends. The share above is an upper bound for the late share of windows: an event that is 15 s late but belongs to the first seconds of its minute still makes it in time.

The **correction horizon** `H` is how long a window is kept after it closed, so that late events can still correct it. It costs memory: the aggregator keeps every counted event of the last `H` minutes so that it can recompute a window in full. **`H = 10 minutes`** keeps about half a million events in memory on a Saturday afternoon (the process peaked at 283 MB) and covers everything but the backlog bursts: 0.13% of the events are more than ten minutes late. Events older than that are dropped from the live statistics and recorded in `rt.late_events`; they are still stored by the sink, so the analytical models, which are computed from stored events, include them.

## Watermark

The watermark is the minimum, over the topic's partitions, of the highest event time each partition has delivered. Messages are keyed by vehicle, so a partition's order is the order in which its vehicles' messages were received. A partition that has delivered events up to 12:00:40 will rarely deliver anything older, and the slowest partition decides how far all of them have got.

Three details keep it honest:

- **Capped at receipt.** A partition's event time is `min(tst, received_at)`. A vehicle whose clock runs ahead cannot push the watermark into the future and make everyone else late.
- **Idle partitions are left out.** A partition that has delivered nothing for 30 seconds (processing time) does not hold the others back. At night some partitions can be quiet for a while; without this the whole watermark would stall. When every partition is idle, the highest one counts, so the last windows of a quiet period still close.
- **It never moves back.** A partition that comes back after being idle cannot lower it; its older events are simply late.

## No double counting

Two kinds of repetition reach the aggregator: about 28% of the raw messages are copies, and after a crash some messages are consumed again. Neither may change a result.

- **Windows keep identities, not counters.** A window holds its events in a map keyed by the event's identity, the same key that makes a stored row unique (vehicle and payload hash; the event type is implied by the window). Adding an event that is already there is a no-op and is counted as a duplicate.
- **Results are full values.** Every emission recomputes the window from its events and writes all values: `INSERT … ON CONFLICT DO UPDATE SET vehicles = excluded.vehicles, …`. Nothing is ever incremented in the database, so writing the same window twice gives the same row. The update only happens when a value differs, and then raises `revision` by one.
- **`late_events` is a full value too.** It is the size of the window's set of late event identities, and every late event is logged in `rt.late_events` under the same unique key, so neither can count an event twice.

## Late events

An event is **late** if its window had already closed when it arrived. It is added to the window, the window is emitted again with all values recomputed (`revision` + 1, `late_events` + 1), and the event is logged as `corrected`. An event whose window is more than the correction horizon behind the watermark is **dropped**: the window is no longer in memory, so it cannot be recomputed in full, and incrementing the stored row would break the rule above. It is logged as `dropped`.

A dropped *copy* does not mean the event was not counted: its first copy may have arrived in time. The log keeps the `received_at` of the copy it saw, and the stored event row keeps the `received_at` of the first copy. An event is excluded from the statistics only when the two are the same.

## Restarts

The aggregator's state is the windows in memory, and it must survive a `kill -9` without gaps or double counts.

**Offsets live in the database.** Each batch writes, in one transaction: the window results, the late-event log, vehicle status, the batch metrics, and for every partition the next offset and the highest event time, plus the watermark. When the consumer is assigned its partitions, it seeks to the offsets in the database. A crash before the commit loses the whole batch, which is then consumed again; a crash after it loses nothing. Offsets are also committed to Kafka after the transaction, but only so that the group's lag is visible.

**Windows are rebuilt from stored events.** The windows within the correction horizon are rebuilt from the rows the sink stored. Every stored row records the partition and offset of the message it came from, so the rebuild takes exactly the events below the aggregator's offsets. The windows come back as they were at the last commit, not as they would be with everything the sink has stored since then. Later events are consumed again and classified as on time, late or dropped as usual. Window rows already emitted, the late-event log and the active vehicles are read back as well.

**The sink has to be there first.** Before rebuilding, the aggregator waits until the sink's committed offsets are at least its own on every partition, so that every event it had counted is stored. It waits up to five minutes and then stops with an error, rather than starting with windows that are missing events.

## Stale vehicles

A vehicle on a trip that has sent nothing for two minutes has gone silent. This is an event-time timer: it runs out when the watermark passes the vehicle's last `tst` plus two minutes, so a backlog in the stream cannot make every vehicle look silent. `silent_since` is the event time at which the two minutes ran out. Any newer message makes the vehicle `active` again. The update only applies when its `last_tst` is not older than the stored one, so a late event cannot move a vehicle back in time.

Going silent means two different things. A vehicle that stops sending in the middle of its trip is a problem worth showing; a vehicle that has reached the end of its trip has simply left the journey topics (HSL reports that phase on `deadrun` topics, which the platform does not subscribe to). The aggregator tells them apart by the trip's **planned end**, the scheduled arrival at its last stop:

- **`stale`**: silent while the planned end is still ahead of the watermark. The vehicle should be running and is not reporting.
- **`finished`**: silent after the planned end, or still silent when the watermark reaches it. The vehicle is then forgotten.

The planned end is looked up in the timetable once per journey, the first time a batch contains it, with the same matching as the sink (route, direction, operating day and start in the feed valid on that day), and kept on the vehicle row. A journey without a timetable trip has no planned end; its vehicle is marked stale and forgotten, as it cannot be told apart. After a restart, active vehicles and stale vehicles with a planned end are read back, so a stale vehicle still turns finished.

## Suspect sign-ons

A vehicle tells HFP which journey it runs, and that sign-on is sometimes wrong. On Monday 26 October between 16:58 and 17:13 Helsinki time, 13 of 1,279 vehicles were still reporting a journey whose planned end had passed more than 15 minutes earlier:

- nine commuter train units, in four trains, on lines D, R and Z, running at full speed on a journey an hour or more older than the one they were actually running, and reporting between 67 and 116 minutes of delay;
- a tram and three buses: one bus parked at its terminus and still signed on, the others running 15 to 33 minutes behind schedule near the end of their trip.

Their reported delays were 91 minutes on average. Counted like any other vehicle, one such train put its line at the top of every delay ranking, and since coupled units report separately, even a median could not hold it back.

**The rule.** An event is suspect when its journey matched a timetable trip and the event's `tst` is more than `AGGREGATOR_SUSPECT_AFTER_S` (900 s) after that trip's planned end. A suspect event still counts as a position and its vehicle stays on the map, but its delay is left out of every delay figure of the route and stop windows. `rt.vehicle_status.suspect` marks the vehicle as long as its latest event is suspect, and the live dashboard lists those vehicles in a table of their own.

The rule cannot tell a wrong sign-on from a vehicle that really is more than 15 minutes late at the end of its trip; both are shown in the suspect table rather than ranked. Journeys without a timetable trip have no planned end and are never suspect.

## Scaling

One aggregator instance owns every partition. A route's vehicles are spread over all partitions, so a route's window can only be computed where all of them meet. Scaling out would mean repartitioning by route first (a second topic keyed by route), and a watermark per input partition of that topic. At about 1,000 messages per second, one instance has plenty of headroom (see below), so the aggregator uses static membership and a single member.

## Verified

**The key test** compares the streamed results with a batch recomputation in SQL over the events the sink stored, for every window the watermark has passed:

```sql
SELECT route_id, direction_id, date_trunc('minute', tst, 'UTC'),
       count(DISTINCT (operator_id, vehicle_number)), count(*),
       count(live_delay_s), avg(live_delay_s),
       percentile_cont(0.9) WITHIN GROUP (ORDER BY live_delay_s)
FROM rt.vehicle_positions          -- minus the events dropped as too late
GROUP BY 1, 2, 3
```

The test stream has 2,839 messages from 12 vehicles over six minutes, with about 20% copies, out-of-order messages, a backlog burst that is late but within the horizon, messages beyond the horizon, and a vehicle that goes silent. It is published in small chunks, as live data would arrive. Every window is equal in all values, and every `late_events` value equals the late-event log, in these cases:

| Case | Result |
|---|---|
| Uninterrupted | Equal; late, dropped and stale paths all exercised |
| Crash before the database commit, restart | Equal |
| Crash after the database commit, before the Kafka commit, restart | Equal |
| Clean stop mid-stream, restart with open windows | Equal |
| `kill -9` of the aggregator process mid-stream, restart | Equal |

**The Saturday recording, replayed at real speed.** The 30-minute recording (1,982,660 messages) was replayed into a fresh topic at its original pace, with the sink and the aggregator running, as they would live. The aggregator was killed with `kill -9` after 10 and after 20 minutes and restarted three seconds later.

| | |
|---|---:|
| Route windows equal to the batch recomputation | **8,951 of 8,951** |
| Stop windows equal to the batch recomputation | **13,179 of 13,179** |
| Events counted | 1,409,524 |
| Late, corrected | 1,953 (0.14%), in 118 window corrections |
| Dropped, beyond the correction horizon | 1,806 (0.13%) |
| Copies and duplicates not counted | 557,220 |
| First result after a minute ends | median 10.5 s, p95 13.4 s |
| Stale vehicle transitions | 252 |

The late share is far below the 0.41% that arrive more than ten seconds after their event time, because most of those events are in the backlog bursts that end up beyond the horizon, and an event a few seconds late is often still within its window's grace period.

**Restarts.** Each restart rebuilt about 7,950 windows and 470,000 events from the stored rows in 2.2–2.4 seconds. The first batch was committed 6.4 and 6.7 seconds after the kill, three of them the deliberate pause before the restart. Static membership gave the partitions back at once.

**Throughput.** Catching up on the same 1,982,660 messages from a topic, without waiting, the aggregator processes **12,665 messages per second**: 343 ms of computation and 26 ms of database work per 5,000-message batch. A replay at full speed shows fewer late events than live (835 instead of 1,953), because the watermark is then held back by whichever partition is read last; lateness is only meaningful at the original pace.
