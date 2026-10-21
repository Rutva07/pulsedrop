package com.rutvapatel.pulsedrop.serde;

import com.rutvapatel.pulsedrop.model.DeadLetterEvent;
import org.apache.flink.api.common.serialization.SerializationSchema;

public final class DeadLetterSerializer implements SerializationSchema<DeadLetterEvent> {
    @Override
    public byte[] serialize(DeadLetterEvent event) {
        try {
            return JsonSupport.MAPPER.writeValueAsBytes(event);
        } catch (Exception exception) {
            throw new IllegalStateException("Unable to serialize dead-letter event", exception);
        }
    }
}
