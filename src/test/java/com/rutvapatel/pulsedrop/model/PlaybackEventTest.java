package com.rutvapatel.pulsedrop.model;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class PlaybackEventTest {
    private final Instant now = Instant.parse("2026-10-19T12:00:00Z");

    @Test
    void acceptsWellFormedTelemetry() {
        assertThat(validEvent().validationErrors(now)).isEmpty();
    }

    @Test
    void identifiesBadIdentifiersRangesAndClockSkew() {
        PlaybackEvent event = new PlaybackEvent(
                "", now.plusSeconds(11 * 60), now, EventType.HEARTBEAT,
                "viewer-1", "session-1", "content-1", -1, 120_001,
                100_001, 120_001, "web", "US", false);

        assertThat(event.validationErrors(now))
                .contains(
                        "eventId is required",
                        "positionMs cannot be negative",
                        "watchDeltaMs is out of range",
                        "bitrateKbps is out of range",
                        "bufferDurationMs is out of range",
                        "occurredAt exceeds allowed clock skew");
    }

    private PlaybackEvent validEvent() {
        return new PlaybackEvent(
                "evt-1", now.minusMillis(500), now, EventType.HEARTBEAT,
                "viewer-1", "session-1", "content-1", 10_000, 5_000,
                3_000, 0, "web", "US", false);
    }
}
