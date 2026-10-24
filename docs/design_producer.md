# Stream producer: design notes

The producer is the only component that talks to HSL's live feed. Its job is narrow: receive every MQTT message, wrap it without interpreting it, and hand it to Redpanda durably and in order per vehicle. Validation, deduplication and enrichment happen downstream, where a mistake can be fixed by replaying the topic. A message that never reached the topic is lost for good.

```
mqtt.hsl.fi:8883 ──TLS──▶ HfpSubscriber ──▶ KafkaSink ──▶ hfp.raw (6 partitions, 24 h)
                         (paho thread)       │
                                             └─ RecordingWriter ──▶ data/recordings/*.jsonl.gz
recordings ──▶ replay(speed) ──▶ KafkaSink ──▶ hfp.raw
```

The live producer, the recorder and the replayer share the same parts. The subscriber feeds either a Kafka sink or a recording writer, and the replayer feeds recorded envelopes into the same Kafka sink. Everything downstream therefore cannot tell live data from a replay.

## The envelope

Every message is stored as one JSON object:

```json
{"topic": "/hfp/v2/journey/ongoing/vp/bus/0018/01090/4611/1/…", "payload": "{\"VP\":{…}}", "received_at": "2026-10-24T12:12:00.123456+00:00"}
```

- `topic` is the full MQTT topic. Several facts exist only there (transport mode, owner operator, next stop, geohash).
- `payload` is the original payload text, not parsed and not re-serialised. A payload that is not valid JSON is still forwarded; the processor sends it to the dead-letter topic with the reason.
- `received_at` is processing time in UTC, taken when the message arrives. Event time is `tst` inside the payload. Keeping both is what lets the processor measure latency and lateness later.

The same line format is used for Kafka message values and for recordings, so a recording is literally a list of topic messages.

## Partitioning and ordering

The message key is `operator_id/vehicle_number` from the topic, for example `0018/01090`. All messages of one vehicle land in the same partition, in the order the producer sent them. That is the ordering the processor needs (position after position, arrival before departure). Ordering across vehicles is never needed.

The key comes from the topic, not from the payload's `oper` and `veh`:

- The topic's operator is the one that owns the vehicle. According to the HFP documentation, the payload's `oper` is the operator running the trip, which can differ when a trip is subcontracted. The owner is the stable identity of the physical vehicle.
- The key can be computed without parsing JSON, so malformed payloads still get a correct key.

A topic that cannot be parsed falls back to the topic string itself as the key. Every message has a key, and the partition of every message is deterministic. That is a requirement for reproducible replays (see below).

Six partitions are more than one consumer needs today. The number caps how far a consumer group can scale out, and it cannot be lowered later without re-keying. With about a thousand vehicles, keys spread evenly: in a 650,000-message replay the partitions held between 94,080 and 122,804 messages.

## Delivery guarantees

| Setting | Value | Why |
|---|---|---|
| `enable.idempotence` | `true` | Retries after a lost acknowledgement do not duplicate messages, and ordering per partition survives retries |
| `acks` | `all` | A message counts as delivered only once the broker has stored it |
| `linger.ms` | `50` | Lets the producer batch messages: up to 50 ms of extra latency, far fewer requests |
| `compression.type` | `zstd` | HFP JSON is repetitive and compresses well |
| `delivery.timeout.ms` | `120000` | Rides out a broker restart of up to two minutes before a message is reported as failed |

The guarantee from MQTT to Redpanda is **at most once on the MQTT side and at least once with no duplicates on the Kafka side**. The subscription uses QoS 0 with a clean session, so messages published while the subscriber is disconnected are not kept for it and cannot be recovered. For a feed that repeats every vehicle's position every second, a short gap is acceptable. Recovering it would need persistent sessions on the broker side. Everything after `on_message` is counted: `sent`, `delivered` and `failed` are logged every ten seconds and on shutdown.

When the producer's local queue is full (`BufferError`), `send` waits on `poll` until there is room instead of dropping the message. Back-pressure therefore blocks the MQTT network thread. If that lasted long enough, the broker would drop QoS 0 messages for this client: the loss would show up as a lower received rate in the logs, not as messages silently discarded inside the producer.

## Reconnects and shutdown

- **MQTT.** paho runs the network loop in its own thread and reconnects with exponential backoff between 1 and 60 seconds. Subscriptions are renewed in `on_connect`, so a reconnect restores them. During development the laptop slept overnight with the recorder running. On wake-up the broker connection had timed out (`Keep alive timeout`), and the subscriber reconnected and resubscribed within 1.6 seconds without intervention.
- **Kafka.** librdkafka reconnects to the broker on its own and keeps messages queued until `delivery.timeout.ms` expires.
- **SIGTERM / SIGINT** set a stop event. The main loop exits, the MQTT client disconnects, and the producer flushes everything still queued before the process exits. The exit code is non-zero if anything stayed undelivered. The recorder closes its gzip file, so the file stays valid.

## Topic creation

`hfp.raw` is created at start-up with 6 partitions and `retention.ms` = 24 hours. An existing topic is left as it is (`TOPIC_ALREADY_EXISTS` is not an error), so starting the producer is idempotent. 24 hours is long enough to replay a day after a processor bug, and short enough that the topic never becomes the archive. Long-term history lives in PostgreSQL and Parquet.

## Recording and replay

The recorder writes envelopes to `data/recordings/hfp-YYYYMMDDTHH.jsonl.gz`, one file per UTC hour of `received_at`. Files are opened in append mode: a restarted recorder adds a new gzip member to the current hour's file, which gzip readers handle as one stream. The reader stops cleanly at a truncated file (a process killed with SIGKILL), keeping every complete line.

The replayer publishes recorded envelopes unchanged, including `received_at`, and keeps the recorded spacing between them divided by a speed factor:

| `SPEED` | Behaviour |
|---|---|
| `1` | Real time |
| `10` | Ten times faster: the 179.9-second fixture replays in 18.0 s |
| `0` | No waiting; as fast as the producer accepts messages |

Each message is scheduled against the replay's start time, not against the previous message, so small sleep overshoots do not add up over a long recording.

**Determinism.** A replay of the same file always gives the same topic contents: same keys, same partitions, same offsets, same bytes. This holds because envelopes are not modified, every message has a key, the partitioner is a hash of the key, and the idempotent producer keeps per-partition order. The integration test replays the fixture into two fresh topics and compares every `(partition, offset, key, value)`.

## Test fixture

`tests/fixtures/hfp_sample.jsonl` (1.19 MB, 2,252 envelopes) is cut from a Saturday afternoon recording. It holds every message from eight vehicles between 12:12 and 12:15 UTC on 24 October 2026: three buses, two trams, a metro, a train and a ferry. The selection covers:

- all three event types (`vp` 2,223, `arr` 13, `dep` 16);
- 1,115 exact duplicates, mostly from the trams;
- 7 metro positions that share `(oper, veh, tst)` with a different payload;
- two messages edited by hand on real topics, in the middle of the stream: one with a payload cut off mid-JSON, one with a non-numeric `lat`.

It is plain JSON lines, so a diff shows exactly what changed. CI replays it into Redpanda without any access to the live feed.

## Measured

Docker Desktop on a MacBook, single Redpanda node (`--smp=1`, 1 GB), Python 3.11, one producer process.

| Measurement | Result |
|---|---|
| Live input, Saturday afternoon | 1,102 messages/s mean, 2,945 in the busiest second |
| Live producer, 60 s, Friday evening | 46,714 sent, 46,714 delivered, 0 failed; the sum of all partition high-watermarks is 46,714 |
| Replay as fast as possible, 649,608 messages | 22.6 s and 24.3 s (28,706 and 26,751 messages/s), 0 failed |
| Same replay, twice into two topics | Identical per-partition counts: 100,077 / 111,366 / 122,804 / 113,124 / 94,080 / 108,157 |
| Replay at `SPEED=10`, 179.9 s fixture | 18.0 s |
| Shutdown on SIGTERM | 2–3 ms from signal to flushed producer (live) or closed gzip file (recorder) |

The producer handles about 25 times the live input rate. In a replay it is limited by Python reading and decompressing the gzip file and building the envelopes, not by the broker.
