package com.rutvapatel.pulsedrop.api;

import com.rutvapatel.pulsedrop.config.AppConfig;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class AnalyticsRepository {
    private final AppConfig config;

    AnalyticsRepository(AppConfig config) {
        this.config = config;
    }

    boolean healthy() {
        try (Connection connection = connection(); Statement statement = connection.createStatement()) {
            ResultSet result = statement.executeQuery("""
                    SELECT count() = 2 AS ready
                    FROM system.tables
                    WHERE database = 'pulsedrop'
                      AND name IN ('playback_events', 'content_window_metrics')
                    """);
            return result.next() && result.getBoolean("ready");
        } catch (SQLException exception) {
            return false;
        }
    }

    Map<String, Object> overview() throws SQLException {
        long started = System.nanoTime();
        String sql = """
                SELECT
                  count() AS events_last_minute,
                  uniqExactIf(viewer_id, occurred_at >= now() - INTERVAL 5 MINUTE) AS active_viewers,
                  if(count() = 0, 0, avg(buffer_duration_ms)) AS average_buffer_ms,
                  countIf(event_type = 'ERROR') / greatest(count(), 1) AS error_rate,
                  countIf(event_type = 'REBUFFER') / greatest(count(), 1) AS rebuffer_ratio
                FROM pulsedrop.playback_events FINAL
                WHERE occurred_at >= now() - INTERVAL 1 MINUTE
                """;
        try (Connection connection = connection();
             Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql)) {
            result.next();
            long eventsLastMinute = result.getLong("events_last_minute");
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("status", "live");
            response.put("generatedAt", Instant.now());
            response.put("eventsLastMinute", eventsLastMinute);
            response.put("eventsPerSecond", eventsLastMinute / 60.0);
            response.put("activeViewers", result.getLong("active_viewers"));
            response.put("averageBufferMs", result.getDouble("average_buffer_ms"));
            response.put("errorRate", result.getDouble("error_rate"));
            response.put("rebufferRatio", result.getDouble("rebuffer_ratio"));
            response.put("queryLatencyMs", elapsedMillis(started));
            return response;
        }
    }

    List<Map<String, Object>> topContent() throws SQLException {
        String sql = """
                SELECT
                  content_id,
                  sum(total_events) AS total_events,
                  sum(unique_viewers) AS viewer_windows,
                  sum(watch_time_ms) AS watch_time_ms,
                  sum(buffer_time_ms) AS buffer_time_ms,
                  sum(error_events) / greatest(sum(total_events), 1) AS error_rate,
                  sum(watch_time_ms) / greatest(sum(watch_time_ms) + sum(buffer_time_ms), 1) AS engagement_ratio
                FROM pulsedrop.content_window_metrics FINAL
                WHERE window_start >= now() - INTERVAL 5 MINUTE
                GROUP BY content_id
                ORDER BY total_events DESC
                LIMIT 8
                """;
        List<Map<String, Object>> rows = new ArrayList<>();
        try (Connection connection = connection();
             Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql)) {
            while (result.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("contentId", result.getString("content_id"));
                row.put("totalEvents", result.getLong("total_events"));
                row.put("viewerWindows", result.getLong("viewer_windows"));
                row.put("watchTimeMs", result.getLong("watch_time_ms"));
                row.put("bufferTimeMs", result.getLong("buffer_time_ms"));
                row.put("errorRate", result.getDouble("error_rate"));
                row.put("engagementRatio", result.getDouble("engagement_ratio"));
                rows.add(row);
            }
        }
        return rows;
    }

    List<Map<String, Object>> timeSeries() throws SQLException {
        String sql = """
                SELECT
                  toStartOfInterval(occurred_at, INTERVAL 10 SECOND) AS bucket,
                  count() AS events,
                  uniqExact(viewer_id) AS viewers,
                  countIf(event_type = 'REBUFFER') AS rebuffers,
                  countIf(event_type = 'ERROR') AS errors
                FROM pulsedrop.playback_events FINAL
                WHERE occurred_at >= now() - INTERVAL 10 MINUTE
                GROUP BY bucket
                ORDER BY bucket
                """;
        List<Map<String, Object>> rows = new ArrayList<>();
        try (Connection connection = connection();
             Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql)) {
            while (result.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("timestamp", result.getTimestamp("bucket").toInstant());
                row.put("events", result.getLong("events"));
                row.put("viewers", result.getLong("viewers"));
                row.put("rebuffers", result.getLong("rebuffers"));
                row.put("errors", result.getLong("errors"));
                rows.add(row);
            }
        }
        return rows;
    }

    private Connection connection() throws SQLException {
        return DriverManager.getConnection(
                config.clickHouseJdbcUrl(), config.clickHouseUser(), config.clickHousePassword());
    }

    private static double elapsedMillis(long started) {
        return Math.round((System.nanoTime() - started) / 100_000.0) / 10.0;
    }
}
