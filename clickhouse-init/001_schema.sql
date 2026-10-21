CREATE DATABASE IF NOT EXISTS pulsedrop;

CREATE TABLE IF NOT EXISTS pulsedrop.playback_events
(
    event_id String,
    occurred_at DateTime64(3, 'UTC'),
    received_at DateTime64(3, 'UTC'),
    event_type LowCardinality(String),
    viewer_id String,
    session_id String,
    content_id LowCardinality(String),
    position_ms UInt64,
    watch_delta_ms UInt32,
    bitrate_kbps UInt32,
    buffer_duration_ms UInt32,
    device_type LowCardinality(String),
    country LowCardinality(String),
    is_live Bool
)
ENGINE = ReplacingMergeTree(received_at)
PARTITION BY toYYYYMM(occurred_at)
ORDER BY event_id
TTL toDateTime(occurred_at) + INTERVAL 7 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS pulsedrop.content_window_metrics
(
    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),
    content_id LowCardinality(String),
    total_events UInt64,
    unique_viewers UInt64,
    active_sessions UInt64,
    play_events UInt64,
    heartbeat_events UInt64,
    rebuffer_events UInt64,
    error_events UInt64,
    complete_events UInt64,
    watch_time_ms UInt64,
    buffer_time_ms UInt64,
    average_bitrate_kbps Float64,
    engagement_ratio Float64,
    rebuffer_ratio Float64,
    error_rate Float64,
    updated_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(window_start)
ORDER BY (window_start, content_id)
TTL toDateTime(window_start) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE VIEW IF NOT EXISTS pulsedrop.latest_content_health AS
SELECT
    content_id,
    sum(total_events) AS events,
    sum(unique_viewers) AS viewer_windows,
    sum(watch_time_ms) AS watch_time_ms,
    sum(buffer_time_ms) AS buffer_time_ms,
    sum(error_events) / greatest(sum(total_events), 1) AS error_rate,
    sum(rebuffer_events) / greatest(sum(total_events), 1) AS rebuffer_ratio
FROM pulsedrop.content_window_metrics FINAL
WHERE window_start >= now() - INTERVAL 5 MINUTE
GROUP BY content_id;
