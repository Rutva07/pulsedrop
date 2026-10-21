package com.rutvapatel.pulsedrop.sink;

import com.rutvapatel.pulsedrop.config.AppConfig;
import com.rutvapatel.pulsedrop.model.ContentWindowMetric;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

public final class ContentMetricClickHouseSink extends BatchingClickHouseSink<ContentWindowMetric> {
    public ContentMetricClickHouseSink(AppConfig config) {
        super(config);
    }

    @Override
    protected String insertSql() {
        return """
                INSERT INTO pulsedrop.content_window_metrics
                (window_start, window_end, content_id, total_events, unique_viewers, active_sessions,
                 play_events, heartbeat_events, rebuffer_events, error_events, complete_events,
                 watch_time_ms, buffer_time_ms, average_bitrate_kbps, engagement_ratio,
                 rebuffer_ratio, error_rate, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now64(3))
                """;
    }

    @Override
    protected void bind(PreparedStatement statement, ContentWindowMetric metric) throws SQLException {
        statement.setTimestamp(1, Timestamp.from(metric.windowStart()));
        statement.setTimestamp(2, Timestamp.from(metric.windowEnd()));
        statement.setString(3, metric.contentId());
        statement.setLong(4, metric.totalEvents());
        statement.setLong(5, metric.uniqueViewers());
        statement.setLong(6, metric.activeSessions());
        statement.setLong(7, metric.playEvents());
        statement.setLong(8, metric.heartbeatEvents());
        statement.setLong(9, metric.rebufferEvents());
        statement.setLong(10, metric.errorEvents());
        statement.setLong(11, metric.completeEvents());
        statement.setLong(12, metric.watchTimeMs());
        statement.setLong(13, metric.bufferTimeMs());
        statement.setDouble(14, metric.averageBitrateKbps());
        statement.setDouble(15, metric.engagementRatio());
        statement.setDouble(16, metric.rebufferRatio());
        statement.setDouble(17, metric.errorRate());
    }
}
