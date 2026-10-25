# Monitoring: design notes

Grafana shows the network as the stream sees it, punctuality as the analytical models measure it, and the health of the pipeline itself. Everything is provisioned from files in `grafana/`: the data source, three dashboards and two alert rules. A fresh `docker compose up grafana` comes up complete, with nothing to click.

## Reading only PostgreSQL, as a read-only role

Grafana has one data source: PostgreSQL, as the role `grafana_reader`. The role can read the schemas `rt` (live events and per-minute statistics), `marts` (the analytical marts, published after every dbt build) and `meta` (metrics and job logs), and nothing else:

- `SELECT` on every table in those schemas, and default privileges so that tables created later are readable too: tomorrow's partitions, created by the sink, and the marts, which are replaced by a new table on every publish.
- No access to `gtfs`. Route and stop names come from `marts.dim_routes` and `marts.dim_stops`, built from the timetable by dbt.
- Sessions are read-only (`default_transaction_read_only`) with a 30-second statement timeout, so a dashboard can neither change data nor hold the database with a runaway query.

A migration creates the role without a password; `make reader-role` sets it from `GRAFANA_READER_PASSWORD`. Roles belong to the whole PostgreSQL server, so the migration creates the role only if it does not exist yet.

A test runs every query of every dashboard, template variable and alert rule as `grafana_reader` against a database holding real messages and freshly published marts. A query that reads a table the role may not read, or that no longer matches the schema, fails the test suite instead of a dashboard.

## Dashboards

![Network now](images/network_now.png)

**Network now** (refreshes every 30 seconds) is the live view, built from the aggregator's tables:

- a map of every vehicle that reported in the last 60 seconds (`rt.vehicle_status`), on OpenStreetMap;
- lines and stops ordered by delay over the last 10 and 15 minutes, from the per-minute windows;
- average delay by mode over time;
- stale vehicles: on a trip that is still planned to run and silent for two minutes. Vehicles whose trip has ended are `finished` and not shown.

All delays here are labelled **live delay, HSL estimate**: the value HSL sends in every message, which is all a live view can use.

![Punctuality](images/punctuality.png)

**Punctuality** shows the analytical marts for one service day (chosen at the top) and the modes selected: on-time share at timing points, departures, bunching, completeness, and per line, stop and hour. Its delays are labelled **measured delay vs timetable**, the platform's own measurement against the published timetable. Keeping the two labels apart matters: the live estimate and the measured delay agree closely but not exactly (see [design_analytics.md](design_analytics.md#plausibility-measured-against-live-delay)).

![Pipeline health](images/pipeline_health.png)

**Pipeline health** (every 30 seconds):

| Panel | Source |
|---|---|
| Messages per second consumed and stored, duplicates dropped | `meta.consumer_metrics`, one row per sink batch |
| Consumer lag per group | sink and aggregator batch metrics, both recording how far behind the end of the raw topic they are |
| Dead-letter rate | invalid messages per batch |
| Late events | the aggregator's corrected and dropped events |
| Stored latency | p95 of `stored_at − tst` per batch |
| Partition sizes | catalog sizes of the `rt` partitions, including the default partitions, which should stay empty |
| Last archive runs, time since the last one | `meta.archive_runs` |
| Last dbt runs, time since the last one | `meta.dbt_runs` |
| Time since data was last stored, sink lag now | the latest sink batches |

"Messages in" is what the sink consumed from the raw topic. The producer's own rate is not in PostgreSQL; Grafana reads nothing else.

## Alert rules

Two rules, provisioned from `grafana/provisioning/alerting/rules.yaml` and evaluated every minute:

| Rule | Fires when | Severity |
|---|---|---|
| Consumer lag | a consumer group's latest lag is above 30,000 messages (about 30 seconds of the feed) for five minutes | warning |
| No data for 5 minutes | the sink has not stored a single new event for more than five minutes, or has stored nothing at all in the last day | critical |

The lag rule is evaluated per consumer group, so the sink and the aggregator are separate alerts. If no batch has been committed for ten minutes, the lag rule has no data and stays quiet; the no-data rule covers that case. No notification channel is configured; the alerts show in Grafana's alerting view.

## Observed while building the dashboards

The screenshots are from a live run on Sunday evening, 25 October, with the producer, sink and aggregator reading HSL's feed for about 25 minutes.

- **A slow host shows up as lag.** Between 21:02 and 21:10 Helsinki time the development laptop was overloaded (load average up to 81, CPU limited to 74%). Decoding a batch of 5,000 messages took 3 seconds instead of 260 ms, and the sink fell up to 59,000 messages behind. Once the load dropped it caught up in about a minute, at 85,000 messages that minute. The bump is visible in the lag and latency panels. The lag rule went to pending at 21:06:30 and back to normal at 21:10:30: four minutes above the threshold, one short of firing.
- **One vehicle can dominate a line's live delay.** Commuter train line K led the list with an average of 24 minutes: one train unit stood still while still signed on to its 17:22 journey and reported 3.7 hours of delay. The panel's description says so, and the measured delay in the marts is not affected (it uses departures from stops).
- **Trips missing from the timetable.** A tram on HFP route `100HG3`, which the timetable does not contain, has no planned end, so it cannot be told apart from a vehicle that stopped mid-trip. The stale panel shows such vehicles for an hour and then drops them.
- **Map tiles.** CARTO's tiles, Grafana's default base map, now require an API key; the map uses OpenStreetMap's tiles, with their attribution.

## Running locally

Grafana listens on `127.0.0.1` only. Anonymous visitors can view the dashboards (and nothing else), which keeps local use and the screenshots free of a login; the admin password comes from `GRAFANA_ADMIN_PASSWORD`. A deployment beyond one machine would turn anonymous access off.
