package com.rutvapatel.pulsedrop.pipeline;

import com.rutvapatel.pulsedrop.model.ContentWindowMetric;
import com.rutvapatel.pulsedrop.model.EventType;
import com.rutvapatel.pulsedrop.model.PlaybackEvent;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public final class ContentMetricAggregate {
    private ContentMetricAggregate() {}

    public static final class Accumulator {
        public long totalEvents;
        public long playEvents;
        public long heartbeatEvents;
        public long rebufferEvents;
        public long errorEvents;
        public long completeEvents;
        public long watchTimeMs;
        public long bufferTimeMs;
        public long bitrateSum;
        public Set<String> viewers = new HashSet<>();
        public Set<String> sessions = new HashSet<>();
    }

    public record Stats(
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
            double averageBitrateKbps) {}

    public static final class Aggregate implements AggregateFunction<PlaybackEvent, Accumulator, Stats> {
        @Override
        public Accumulator createAccumulator() {
            return new Accumulator();
        }

        @Override
        public Accumulator add(PlaybackEvent event, Accumulator accumulator) {
            accumulator.totalEvents++;
            accumulator.viewers.add(event.viewerId());
            accumulator.sessions.add(event.sessionId());
            accumulator.watchTimeMs += event.watchDeltaMs();
            accumulator.bufferTimeMs += event.bufferDurationMs();
            accumulator.bitrateSum += event.bitrateKbps();

            if (event.eventType() == EventType.PLAY) accumulator.playEvents++;
            if (event.eventType() == EventType.HEARTBEAT) accumulator.heartbeatEvents++;
            if (event.eventType() == EventType.REBUFFER) accumulator.rebufferEvents++;
            if (event.eventType() == EventType.ERROR) accumulator.errorEvents++;
            if (event.eventType() == EventType.COMPLETE) accumulator.completeEvents++;
            return accumulator;
        }

        @Override
        public Stats getResult(Accumulator accumulator) {
            double averageBitrate = accumulator.totalEvents == 0
                    ? 0.0
                    : (double) accumulator.bitrateSum / accumulator.totalEvents;
            return new Stats(
                    accumulator.totalEvents,
                    accumulator.viewers.size(),
                    accumulator.sessions.size(),
                    accumulator.playEvents,
                    accumulator.heartbeatEvents,
                    accumulator.rebufferEvents,
                    accumulator.errorEvents,
                    accumulator.completeEvents,
                    accumulator.watchTimeMs,
                    accumulator.bufferTimeMs,
                    averageBitrate);
        }

        @Override
        public Accumulator merge(Accumulator left, Accumulator right) {
            left.totalEvents += right.totalEvents;
            left.playEvents += right.playEvents;
            left.heartbeatEvents += right.heartbeatEvents;
            left.rebufferEvents += right.rebufferEvents;
            left.errorEvents += right.errorEvents;
            left.completeEvents += right.completeEvents;
            left.watchTimeMs += right.watchTimeMs;
            left.bufferTimeMs += right.bufferTimeMs;
            left.bitrateSum += right.bitrateSum;
            left.viewers.addAll(right.viewers);
            left.sessions.addAll(right.sessions);
            return left;
        }
    }

    public static final class AddWindow
            extends ProcessWindowFunction<Stats, ContentWindowMetric, String, TimeWindow> {
        @Override
        public void process(
                String contentId,
                Context context,
                Iterable<Stats> values,
                Collector<ContentWindowMetric> out) {
            Stats stats = values.iterator().next();
            double engagementRatio = safeRatio(stats.watchTimeMs(), stats.watchTimeMs() + stats.bufferTimeMs());
            double rebufferRatio = safeRatio(stats.rebufferEvents(), stats.totalEvents());
            double errorRate = safeRatio(stats.errorEvents(), stats.totalEvents());
            out.collect(new ContentWindowMetric(
                    Instant.ofEpochMilli(context.window().getStart()),
                    Instant.ofEpochMilli(context.window().getEnd()),
                    contentId,
                    stats.totalEvents(),
                    stats.uniqueViewers(),
                    stats.activeSessions(),
                    stats.playEvents(),
                    stats.heartbeatEvents(),
                    stats.rebufferEvents(),
                    stats.errorEvents(),
                    stats.completeEvents(),
                    stats.watchTimeMs(),
                    stats.bufferTimeMs(),
                    stats.averageBitrateKbps(),
                    engagementRatio,
                    rebufferRatio,
                    errorRate));
        }

        private static double safeRatio(long numerator, long denominator) {
            return denominator == 0 ? 0.0 : (double) numerator / denominator;
        }
    }
}
