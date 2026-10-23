# Data sources

## Static timetable: HSL GTFS

| | |
|---|---|
| Publisher | Helsinki Region Transport (HSL, Helsingin seudun liikenne) |
| Format | GTFS static, one zip |
| URL | `https://infopalvelut.storage.hsldev.com/gtfs/hsl.zip` |
| Older URL | `https://dev.hsl.fi/gtfs/hsl.zip` answers with a `301` redirect to the URL above |
| Update rhythm | Rebuilt daily in the early morning (Helsinki time); the `Last-Modified` header and `feed_info.feed_version` carry the build time |
| Licence | Creative Commons Attribution 4.0 International (CC BY 4.0), as stated on HSL's open data page for all HSL data sets and APIs |
| Attribution | "© Helsinki Region Transport (HSL)" together with the year the data was retrieved |

The URL, the redirect and the HTTP headers below were checked on 23 October 2026. HSL's open data page (`https://www.hsl.fi/en/opendata`) rejects scripted requests, so the licence statement has to be read in a browser.

### Feed checked on 23 October 2026

| | |
|---|---|
| `Content-Length` | 60,570,684 bytes (57.8 MiB compressed) |
| Uncompressed | 893,803,501 bytes (852 MiB) |
| `Last-Modified` | Fri, 23 Oct 2026 01:49:04 GMT |
| sha256 | `6b16aef034be3ae637dccab47d189cc69a166c393a4c96aef48588e1f1b4c657` |
| `feed_info` | version `2026-10-23 04:11:14`, valid 2026-10-22 to 2026-12-20 |
| Agency | one, `HSL`, time zone `Europe/Helsinki` |

| File | Rows | Loaded |
|---|---:|:---:|
| `agency.txt` | 1 | yes |
| `routes.txt` | 460 | yes |
| `stops.txt` | 8,376 | yes |
| `calendar.txt` | 6,403 | yes |
| `calendar_dates.txt` | 526 | yes |
| `trips.txt` | 316,924 | yes |
| `stop_times.txt` | 8,407,774 | yes |
| `shapes.txt` | 451,298 | yes |
| `feed_info.txt` | 1 | version only |
| `trips2.txt` | 316,924 | no |
| `translations.txt` | 628,411 | no |
| `areas.txt` | 4,716 | no |
| `stop_areas.txt` | 8,255 | no |
| `emissions.txt` | 433 | no |
| `fare_attributes.txt` | 10 | no |
| `fare_rules.txt` | 31 | no |
| `transfers.txt` | 4 | no |

`stop_times.txt` is 755 MB uncompressed and makes up 94% of all rows that are loaded.

### Observations from the real feed

- **Byte order mark.** `stop_times.txt` starts with a UTF-8 BOM, the other files do not. The loader strips it from the header line.
- **Whitespace instead of empty values.** Empty optional fields in `stops.txt` are a single space (`parent_station`, `platform_code`). The loader treats blank and whitespace-only values as NULL.
- **Times past midnight.** 347,647 stop times (4.1%) are at or after `24:00:00`. The latest is `29:47:00`. No arrival or departure time is empty.
- **Extended route types.** Besides the basic GTFS types (`0` tram, `1` metro, `4` ferry, `109` suburban rail), HSL uses the extended types `701`, `702` and `704` for bus services and `900` for the light rail. `route_type` is stored as a plain integer.
- **Calendar exceptions.** `calendar_dates.txt` contains only removals (`exception_type = 2`), concentrated on a few days (21 November, 31 October and 7–8 November have over 70 each). One service exists only in `calendar_dates.txt`, so `trips.service_id` cannot reference `calendar`.
- **Service ids contain spaces**, for example `1001 5_20261031_20261031_La`. They are opaque keys and are never split.
- **Referential integrity holds.** Every trip's route and service, every stop time's trip and stop, and every parent station exists.
- **Direction.** `direction_id` is `0` or `1` (161,715 and 155,209 trips). HFP uses `1` and `2`; the mapping is verified against live data when the stream is matched to trips.
- **`trips2.txt`** repeats `trips.txt` with an extra `block_id` column (vehicle blocks). It is not standard GTFS and is not loaded.

### How the feed changes between builds

The storage container behind the download URL is publicly listable (`…/gtfs?restype=container&comp=list`). Next to `hsl.zip` it keeps dated copies of earlier builds (`hsl_YYYYMMDDTHHMMSSZ.zip`, about six weeks back). Eight of them were compared on 23 October 2026.

**Rebuilds on the same day are byte-identical.** HSL sometimes publishes more than once a day (three builds on 14 October, two on 20 October). Those copies have the same sha256, including `feed_info.feed_version`, which still carries the time of the day's first build. The zip checksum already recognises them.

**Builds on different days always differ, and mostly in identifiers.** Each daily build moves the window one day forward. `service_id` and `trip_id` contain the first day of their validity period (`1001_20261022_20261030_Ke`, `1001_20261022_Ke_1_0537`), so that date changes and the rows get new keys:

| Build compared with previous day | `trips` rows changed | `stop_times` rows changed | `trips` changed with dates masked |
|---|---:|---:|---:|
| 21 Oct | −239,963 / +240,760 of 307,723 | −4.35 M / +4.37 M of 8.26 M | |
| 22 Oct | −240,760 / +241,003 of 307,966 | −4.34 M / +4.34 M of 8.26 M | |
| 23 Oct | −240,782 / +249,740 of 316,924 | −4.37 M / +4.52 M of 8.41 M | −0 / +2,365 |

With every 8-digit date masked, 23 October's `trips.txt` removes nothing and adds 2,365 rows (0.7%), and `calendar.txt` adds 76 rows. The timetable itself changes little from day to day, but no daily build is identical to the one before it, neither in bytes nor in rows.

Two consequences:

- **Change detection.** A hash over the extracted files, or `feed_info.feed_version`, would skip exactly the same builds as the zip checksum: the same-day copies. So the zip sha256 stays the "already loaded" check. Each new day's build is loaded as a new version.
- **Ids are only stable within one build.** A `trip_id` from one day's feed usually does not exist in the next day's feed. Real-time events must therefore be matched to the feed that is current when they arrive, and they must keep the planned time, trip and route they were matched to rather than a reference to be resolved later.

### Route types in the feed

| `route_type` | Routes | Mode |
|---:|---:|---|
| 0 | 32 | Tram |
| 1 | 4 | Metro |
| 4 | 3 | Ferry |
| 109 | 13 | Commuter train |
| 701 | 347 | Bus |
| 702 | 14 | Trunk bus (for example 20, 30, 500) |
| 704 | 46 | Local bus (for example 31–34) |
| 900 | 1 | Light rail (line 15) |
