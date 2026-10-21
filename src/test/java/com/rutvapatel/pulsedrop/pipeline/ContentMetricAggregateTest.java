package com.rutvapatel.pulsedrop.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import com.rutvapatel.pulsedrop.model.EventType;
import com.rutvapatel.pulsedrop.model.PlaybackEvent;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class ContentMetricAggregateTest {
    @Test
    void aggregatesCountsDurationsAndDistinctActors() {
        ContentMetricAggregate.Aggregate aggregate = new ContentMetricAggregate.Aggregate();
        ContentMetricAggregate.Accumulator accumulator = aggregate.createAccumulator();
        aggregate.add(event("1", "viewer-1", "session-1", EventType.PLAY, 0, 0, 1_500), accumulator);
        aggregate.add(event("2", "viewer-1", "session-1", EventType.HEARTBEAT, 5_000, 0, 3_000), accumulator);
        aggregate.add(event("3", "viewer-2", "session-2", EventType.REBUFFER, 0, 600, 6_000), accumulator);
        aggregate.add(event("4", "viewer-2", "session-2", EventType.ERROR, 0, 0, 3_000), accumulator);

        ContentMetricAggregate.Stats stats = aggregate.getResult(accumulator);
        assertThat(stats.totalEvents()).isEqualTo(4);
        assertThat(stats.uniqueViewers()).isEqualTo(2);
        assertThat(stats.activeSessions()).isEqualTo(2);
        assertThat(stats.playEvents()).isEqualTo(1);
        assertThat(stats.heartbeatEvents()).isEqualTo(1);
        assertThat(stats.rebufferEvents()).isEqualTo(1);
        assertThat(stats.errorEvents()).isEqualTo(1);
        assertThat(stats.watchTimeMs()).isEqualTo(5_000);
        assertThat(stats.bufferTimeMs()).isEqualTo(600);
        assertThat(stats.averageBitrateKbps()).isEqualTo(3_375.0);
    }

    private PlaybackEvent event(
            String id, String viewer, String session, EventType type,
            long watchMs, long bufferMs, int bitrate) {
        Instant now = Instant.parse("2026-10-19T12:00:00Z");
        return new PlaybackEvent(
                "evt-" + id, now, now, type, viewer, session, "content-1",
                0, watchMs, bitrate, bufferMs, "web", "US", false);
    }
}
