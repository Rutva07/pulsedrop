package com.rutvapatel.pulsedrop.serde;

import static org.assertj.core.api.Assertions.assertThat;

import com.rutvapatel.pulsedrop.model.EventType;
import com.rutvapatel.pulsedrop.model.PlaybackEvent;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class JsonSupportTest {
    @Test
    void roundTripsPlaybackEventWithIsoTimestamp() throws Exception {
        Instant now = Instant.parse("2026-10-19T12:00:00.123Z");
        PlaybackEvent expected = new PlaybackEvent(
                "evt-1", now, now, EventType.PLAY, "viewer-1", "session-1", "live-finals",
                0, 0, 6_000, 0, "tv", "US", true);

        String json = JsonSupport.MAPPER.writeValueAsString(expected);
        PlaybackEvent actual = JsonSupport.MAPPER.readValue(json, PlaybackEvent.class);

        assertThat(actual).isEqualTo(expected);
        assertThat(json).contains("2026-10-19T12:00:00.123Z", "\"eventType\":\"PLAY\"");
    }
}
