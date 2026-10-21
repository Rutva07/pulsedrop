package com.rutvapatel.pulsedrop.serde;

import com.rutvapatel.pulsedrop.model.ParsedEvent;
import com.rutvapatel.pulsedrop.model.PlaybackEvent;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public final class PlaybackEventDeserializer implements KafkaRecordDeserializationSchema<ParsedEvent> {
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<ParsedEvent> out) throws IOException {
        String payload = record.value() == null ? "" : new String(record.value(), StandardCharsets.UTF_8);
        try {
            PlaybackEvent event = JsonSupport.MAPPER.readValue(payload, PlaybackEvent.class);
            out.collect(new ParsedEvent(event, payload, null, record.timestamp(), record.partition(), record.offset()));
        } catch (Exception exception) {
            out.collect(new ParsedEvent(null, payload, compactMessage(exception), record.timestamp(), record.partition(), record.offset()));
        }
    }

    @Override
    public TypeInformation<ParsedEvent> getProducedType() {
        return TypeInformation.of(ParsedEvent.class);
    }

    private static String compactMessage(Exception exception) {
        String message = exception.getMessage();
        if (message == null || message.isBlank()) return exception.getClass().getSimpleName();
        return message.length() <= 500 ? message : message.substring(0, 500);
    }
}
