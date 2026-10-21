package com.rutvapatel.pulsedrop.sink;

import com.rutvapatel.pulsedrop.config.AppConfig;
import com.rutvapatel.pulsedrop.model.PlaybackEvent;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

public final class PlaybackEventClickHouseSink extends BatchingClickHouseSink<PlaybackEvent> {
    public PlaybackEventClickHouseSink(AppConfig config) {
        super(config);
    }

    @Override
    protected String insertSql() {
        return """
                INSERT INTO pulsedrop.playback_events
                (event_id, occurred_at, received_at, event_type, viewer_id, session_id, content_id,
                 position_ms, watch_delta_ms, bitrate_kbps, buffer_duration_ms, device_type, country, is_live)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
    }

    @Override
    protected void bind(PreparedStatement statement, PlaybackEvent event) throws SQLException {
        statement.setString(1, event.eventId());
        statement.setTimestamp(2, Timestamp.from(event.occurredAt()));
        statement.setTimestamp(3, Timestamp.from(event.receivedAt()));
        statement.setString(4, event.eventType().name());
        statement.setString(5, event.viewerId());
        statement.setString(6, event.sessionId());
        statement.setString(7, event.contentId());
        statement.setLong(8, event.positionMs());
        statement.setLong(9, event.watchDeltaMs());
        statement.setInt(10, event.bitrateKbps());
        statement.setLong(11, event.bufferDurationMs());
        statement.setString(12, event.deviceType());
        statement.setString(13, event.country());
        statement.setBoolean(14, event.live());
    }
}
