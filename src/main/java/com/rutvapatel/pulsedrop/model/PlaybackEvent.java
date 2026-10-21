package com.rutvapatel.pulsedrop.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public record PlaybackEvent(
        String eventId,
        Instant occurredAt,
        Instant receivedAt,
        EventType eventType,
        String viewerId,
        String sessionId,
        String contentId,
        long positionMs,
        long watchDeltaMs,
        int bitrateKbps,
        long bufferDurationMs,
        String deviceType,
        String country,
        boolean live) {

    private static final int MAX_CLOCK_SKEW_MINUTES = 10;

    @JsonIgnore
    public List<String> validationErrors(Instant now) {
        List<String> errors = new ArrayList<>();
        require(errors, eventId, "eventId");
        require(errors, viewerId, "viewerId");
        require(errors, sessionId, "sessionId");
        require(errors, contentId, "contentId");
        require(errors, deviceType, "deviceType");
        require(errors, country, "country");
        if (occurredAt == null) errors.add("occurredAt is required");
        if (receivedAt == null) errors.add("receivedAt is required");
        if (eventType == null) errors.add("eventType is required");
        if (positionMs < 0) errors.add("positionMs cannot be negative");
        if (watchDeltaMs < 0 || watchDeltaMs > 120_000) errors.add("watchDeltaMs is out of range");
        if (bitrateKbps < 0 || bitrateKbps > 100_000) errors.add("bitrateKbps is out of range");
        if (bufferDurationMs < 0 || bufferDurationMs > 120_000) errors.add("bufferDurationMs is out of range");
        if (occurredAt != null && Math.abs(Duration.between(now, occurredAt).toMinutes()) > MAX_CLOCK_SKEW_MINUTES) {
            errors.add("occurredAt exceeds allowed clock skew");
        }
        return errors;
    }

    private static void require(List<String> errors, String value, String field) {
        if (value == null || value.isBlank()) errors.add(field + " is required");
    }
}
