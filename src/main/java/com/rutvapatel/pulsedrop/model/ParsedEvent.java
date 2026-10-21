package com.rutvapatel.pulsedrop.model;

public record ParsedEvent(
        PlaybackEvent event,
        String rawPayload,
        String error,
        long kafkaTimestamp,
        int partition,
        long offset) {

    public boolean valid() {
        return event != null && error == null;
    }
}
