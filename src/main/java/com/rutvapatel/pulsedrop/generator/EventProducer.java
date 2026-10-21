package com.rutvapatel.pulsedrop.generator;

import com.rutvapatel.pulsedrop.config.AppConfig;
import com.rutvapatel.pulsedrop.model.PlaybackEvent;
import com.rutvapatel.pulsedrop.serde.JsonSupport;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public final class EventProducer {
    private EventProducer() {}

    public static void main(String[] args) throws Exception {
        AppConfig config = AppConfig.fromEnvironment();
        int eventsPerSecond = positiveEnvironmentInt("EVENTS_PER_SECOND", 5_000, false);
        int durationSeconds = positiveEnvironmentInt("GENERATOR_DURATION_SECONDS", 0, true);
        long generatorSeed = Long.parseLong(System.getenv().getOrDefault(
                "GENERATOR_SEED", Long.toString(System.nanoTime())));
        PlaybackEventFactory factory = new PlaybackEventFactory(generatorSeed);

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "10");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "131072");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "pulsedrop-generator");

        long generated = 0;
        Instant started = Instant.now();
        Instant stopAt = durationSeconds == 0 ? Instant.MAX : started.plusSeconds(durationSeconds);
        System.out.printf("Producing %,d events/sec to %s%n", eventsPerSecond, config.inputTopic());

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties)) {
            while (Instant.now().isBefore(stopAt)) {
                long tickStarted = System.nanoTime();
                Future<RecordMetadata> lastSend = null;
                for (int index = 0; index < eventsPerSecond; index++) {
                    PlaybackEvent event = factory.next();
                    byte[] key = event.contentId().getBytes(StandardCharsets.UTF_8);
                    byte[] value = JsonSupport.MAPPER.writeValueAsBytes(event);
                    lastSend = producer.send(new ProducerRecord<>(config.inputTopic(), key, value));
                }
                if (lastSend != null) lastSend.get();
                generated += eventsPerSecond;
                paceOneSecond(tickStarted);
                System.out.printf("generated=%,d elapsed=%ds target_rate=%,d/s%n",
                        generated, Duration.between(started, Instant.now()).toSeconds(), eventsPerSecond);
            }
            producer.flush();
        }
    }

    private static void paceOneSecond(long tickStarted) throws InterruptedException {
        long remainingNanos = 1_000_000_000L - (System.nanoTime() - tickStarted);
        if (remainingNanos > 0) {
            Thread.sleep(remainingNanos / 1_000_000L, (int) (remainingNanos % 1_000_000L));
        }
    }

    private static int positiveEnvironmentInt(String name, int fallback, boolean allowZero) {
        int value = Integer.parseInt(System.getenv().getOrDefault(name, Integer.toString(fallback)));
        if (value < 0 || (!allowZero && value == 0)) {
            throw new IllegalArgumentException(name + (allowZero ? " cannot be negative" : " must be positive"));
        }
        return value;
    }
}
