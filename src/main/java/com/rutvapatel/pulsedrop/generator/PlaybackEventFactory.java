package com.rutvapatel.pulsedrop.generator;

import com.rutvapatel.pulsedrop.model.EventType;
import com.rutvapatel.pulsedrop.model.PlaybackEvent;
import java.time.Instant;
import java.util.SplittableRandom;

public final class PlaybackEventFactory {
    private static final String[] DEVICES = {"web", "mobile", "tv", "tablet", "console"};
    private static final String[] COUNTRIES = {"US", "IN", "GB", "CA", "DE", "BR", "JP", "AU"};
    private static final String[] CONTENT = {
        "live-finals", "deep-ocean", "city-sessions", "orbit", "after-hours",
        "wild-trails", "open-source", "match-day", "documentary-42", "news-live"
    };

    private final SplittableRandom random;
    private long sequence;

    public PlaybackEventFactory(long seed) {
        this.random = new SplittableRandom(seed);
    }

    public PlaybackEvent next() {
        long id = ++sequence;
        Instant now = Instant.now();
        EventType eventType = weightedType(random.nextInt(100));
        long bufferDuration = eventType == EventType.REBUFFER ? random.nextLong(120, 4_500) : 0;
        long watchDelta = eventType == EventType.HEARTBEAT ? random.nextLong(4_000, 10_001) : 0;
        int viewer = random.nextInt(1, 80_001);
        int session = random.nextInt(1, 120_001);
        String contentId = CONTENT[random.nextInt(CONTENT.length)];
        return new PlaybackEvent(
                "evt-" + Long.toUnsignedString(id, 36) + "-" + Long.toUnsignedString(random.nextLong(), 36),
                now.minusMillis(random.nextLong(0, 2_500)),
                now,
                eventType,
                "viewer-" + viewer,
                "session-" + session,
                contentId,
                random.nextLong(0, 7_200_001),
                watchDelta,
                switch (random.nextInt(5)) {
                    case 0 -> 700;
                    case 1 -> 1_500;
                    case 2 -> 3_000;
                    case 3 -> 6_000;
                    default -> 12_000;
                },
                bufferDuration,
                DEVICES[random.nextInt(DEVICES.length)],
                COUNTRIES[random.nextInt(COUNTRIES.length)],
                contentId.endsWith("live") || contentId.equals("live-finals"));
    }

    private static EventType weightedType(int bucket) {
        if (bucket < 8) return EventType.PLAY;
        if (bucket < 75) return EventType.HEARTBEAT;
        if (bucket < 82) return EventType.PAUSE;
        if (bucket < 88) return EventType.SEEK;
        if (bucket < 94) return EventType.QUALITY_CHANGE;
        if (bucket < 97) return EventType.REBUFFER;
        if (bucket < 99) return EventType.COMPLETE;
        return EventType.ERROR;
    }
}
