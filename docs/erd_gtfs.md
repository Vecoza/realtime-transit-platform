# Timetable model (schema `gtfs`)

Every table carries `feed_id`, and `feed_id` is the first column of every primary key, so any number of timetable versions can live side by side. `stop_times` is list-partitioned by `feed_id`, one partition per feed version.

```mermaid
erDiagram
    feed_versions ||--o{ agencies : contains
    feed_versions ||--o{ stops : contains
    feed_versions ||--o{ calendar : contains
    feed_versions ||--o{ calendar_dates : contains
    feed_versions ||--o{ service_dates : "expanded into"
    feed_versions ||--o{ shapes : contains
    agencies ||--o{ routes : operates
    routes ||--o{ trips : has
    trips ||--|{ stop_times : "stops at"
    stops ||--o{ stop_times : "served by"
    stops |o--o{ stops : "parent station of"

    feed_versions {
        bigint feed_id PK
        text source_url
        text sha256 UK
        bigint size_bytes
        text feed_version
        date build_date "day the publisher built it"
        timestamptz downloaded_at
        timestamptz loaded_at
        date valid_from
        date valid_to
        boolean is_current "at most one true"
    }
    agencies {
        bigint feed_id PK, FK
        text agency_id PK
        text agency_name
        text agency_url
        text agency_timezone
    }
    routes {
        bigint feed_id PK, FK
        text route_id PK
        text agency_id FK
        text route_short_name
        text route_long_name
        integer route_type
    }
    stops {
        bigint feed_id PK, FK
        text stop_id PK
        text stop_code
        text stop_name
        double stop_lat
        double stop_lon
        text zone_id
        smallint location_type
        text parent_station FK
        text platform_code
    }
    calendar {
        bigint feed_id PK, FK
        text service_id PK
        boolean monday "… sunday"
        date start_date
        date end_date
    }
    calendar_dates {
        bigint feed_id PK, FK
        text service_id PK
        date service_date PK
        smallint exception_type "1 added, 2 removed"
    }
    service_dates {
        bigint feed_id PK, FK
        date service_date PK
        text service_id PK
    }
    trips {
        bigint feed_id PK, FK
        text trip_id PK
        text route_id FK
        text service_id
        text trip_headsign
        smallint direction_id "0 or 1"
        text shape_id
    }
    stop_times {
        bigint feed_id PK, FK
        text trip_id PK, FK
        integer stop_sequence PK
        text stop_id FK
        integer arrival_offset_seconds
        integer departure_offset_seconds
        text stop_headsign
        smallint pickup_type
        smallint drop_off_type
        smallint timepoint
    }
    shapes {
        bigint feed_id PK, FK
        text shape_id PK
        integer shape_pt_sequence PK
        double shape_pt_lat
        double shape_pt_lon
        double shape_dist_traveled
    }
```

`trips.service_id` has no foreign key. A service can be defined in `calendar`, in `calendar_dates`, or in both, so there is no single table to reference. `service_dates` is the place where both definitions meet. `trips.shape_id` has no foreign key either, because `shapes` has no table with one row per shape.

## Functions

| Function | Returns | Purpose |
|---|---|---|
| `gtfs.time_to_seconds(text)` | `integer` | `'25:10:00'` → `90600`; used while loading `stop_times` |
| `gtfs.service_day_start(date, time_zone)` | `timestamptz` | The instant GTFS offsets count from: local noon minus 12 hours |
| `gtfs.to_utc(date, offset_seconds, time_zone)` | `timestamptz` | Scheduled instant of a stop time on a service date |

`time_zone` defaults to `Europe/Helsinki`.
