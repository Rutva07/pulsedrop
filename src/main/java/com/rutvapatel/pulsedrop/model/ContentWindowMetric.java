package com.rutvapatel.pulsedrop.model;

import java.time.Instant;

public record ContentWindowMetric(
        Instant windowStart,
        Instant windowEnd,
        String contentId,
        long totalEvents,
        long uniqueViewers,
        long activeSessions,
        long playEvents,
        long heartbeatEvents,
        long rebufferEvents,
        long errorEvents,
        long completeEvents,
        long watchTimeMs,
        long bufferTimeMs,
        double averageBitrateKbps,
        double engagementRatio,
        double rebufferRatio,
        double errorRate) {}
