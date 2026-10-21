package com.rutvapatel.pulsedrop.pipeline;

import com.rutvapatel.pulsedrop.model.DeadLetterEvent;
import com.rutvapatel.pulsedrop.model.ParsedEvent;
import com.rutvapatel.pulsedrop.model.PlaybackEvent;
import java.time.Instant;
import java.util.List;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public final class EventValidator extends ProcessFunction<ParsedEvent, PlaybackEvent> {
    public static final OutputTag<DeadLetterEvent> DEAD_LETTERS = new OutputTag<>("dead-letter-events") {};

    private transient LongCounter acceptedEvents;
    private transient LongCounter rejectedEvents;

    @Override
    public void open(Configuration parameters) {
        acceptedEvents = getRuntimeContext().getLongCounter("accepted-events");
        rejectedEvents = getRuntimeContext().getLongCounter("rejected-events");
    }

    @Override
    public void processElement(ParsedEvent parsed, Context context, Collector<PlaybackEvent> out) {
        if (!parsed.valid()) {
            reject(parsed, parsed.error(), context);
            return;
        }

        List<String> errors = parsed.event().validationErrors(Instant.now());
        if (!errors.isEmpty()) {
            reject(parsed, String.join("; ", errors), context);
            return;
        }

        acceptedEvents.add(1L);
        out.collect(parsed.event());
    }

    private void reject(ParsedEvent parsed, String reason, Context context) {
        rejectedEvents.add(1L);
        context.output(DEAD_LETTERS, new DeadLetterEvent(
                Instant.now(), reason, parsed.rawPayload(), parsed.kafkaTimestamp(), parsed.partition(), parsed.offset()));
    }
}
