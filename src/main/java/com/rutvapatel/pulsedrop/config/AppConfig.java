package com.rutvapatel.pulsedrop.config;

import java.util.Map;

public record AppConfig(
        String kafkaBootstrapServers,
        String inputTopic,
        String dlqTopic,
        String consumerGroup,
        String clickHouseJdbcUrl,
        String clickHouseUser,
        String clickHousePassword,
        int clickHouseBatchSize,
        long clickHouseFlushMillis,
        String flinkCheckpointPath,
        int flinkParallelism,
        int apiPort) {

    public static AppConfig fromEnvironment() {
        return from(System.getenv());
    }

    static AppConfig from(Map<String, String> environment) {
        return new AppConfig(
                environment.getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094"),
                environment.getOrDefault("KAFKA_INPUT_TOPIC", "playback-events"),
                environment.getOrDefault("KAFKA_DLQ_TOPIC", "playback-events-dlq"),
                environment.getOrDefault("KAFKA_CONSUMER_GROUP", "pulsedrop-flink-v1"),
                environment.getOrDefault("CLICKHOUSE_JDBC_URL", "jdbc:ch://localhost:8123/pulsedrop"),
                environment.getOrDefault("CLICKHOUSE_USER", "pulsedrop"),
                environment.getOrDefault("CLICKHOUSE_PASSWORD", "pulsedrop"),
                positiveInt(environment, "CLICKHOUSE_BATCH_SIZE", 1_000),
                positiveLong(environment, "CLICKHOUSE_FLUSH_MS", 1_000L),
                environment.getOrDefault("FLINK_CHECKPOINT_PATH", "file:///tmp/pulsedrop-checkpoints"),
                positiveInt(environment, "FLINK_PARALLELISM", 2),
                positiveInt(environment, "API_PORT", 8_080));
    }

    private static int positiveInt(Map<String, String> environment, String name, int fallback) {
        int value = Integer.parseInt(environment.getOrDefault(name, Integer.toString(fallback)));
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static long positiveLong(Map<String, String> environment, String name, long fallback) {
        long value = Long.parseLong(environment.getOrDefault(name, Long.toString(fallback)));
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }
}
