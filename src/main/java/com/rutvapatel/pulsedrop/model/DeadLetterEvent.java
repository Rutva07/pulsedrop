package com.rutvapatel.pulsedrop.model;

import java.time.Instant;

public record DeadLetterEvent(
        Instant failedAt,
        String reason,
        String rawPayload,
        long kafkaTimestamp,
        int partition,
        long offset) {}
