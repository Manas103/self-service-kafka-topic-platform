package com.manas.kafkaplatform.clientlib;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Generates 200 distinct, individually-valid producer and consumer configurations (100 each) by
 * systematically varying schema type, acks value, client/group id suffix, and declared
 * processing semantics, always keeping every field internally consistent. Used to measure the
 * false-rejection rate of {@link KafkaClientConfigValidator}: every one of these 200 is expected
 * to pass.
 */
public final class ValidConfigGenerator {

    private static final String[] SCHEMA_TYPES = {"AVRO", "JSON", "STRING", "BYTES"};
    private static final String[] ACKS_VALUES = {"all", "-1"};

    private ValidConfigGenerator() {
    }

    public static List<Map<String, String>> generateProducerConfigs(int count) {
        List<Map<String, String>> configs = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String schemaType = SCHEMA_TYPES[i % SCHEMA_TYPES.length];
            Map<String, String> m = new HashMap<>();
            m.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            m.put("value.serializer", serializerFor(schemaType));
            m.put("schema.type", schemaType);
            m.put("acks", ACKS_VALUES[i % ACKS_VALUES.length]);
            m.put("client.id", "team-" + i + "-producer");
            configs.add(m);
        }
        return configs;
    }

    public static List<Map<String, String>> generateConsumerConfigs(int count) {
        List<Map<String, String>> configs = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String schemaType = SCHEMA_TYPES[i % SCHEMA_TYPES.length];
            Map<String, String> m = new HashMap<>();
            m.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            m.put("value.deserializer", deserializerFor(schemaType));
            m.put("schema.type", schemaType);
            m.put("group.id", "team-" + i + "-consumer");

            int semanticsBucket = i % 3;
            if (semanticsBucket == 0) {
                // no semantics declared: offset-reset-conflict rule is inert
                m.put("enable.auto.commit", "true");
                m.put("auto.offset.reset", "latest");
            } else if (semanticsBucket == 1) {
                m.put("processing.semantics", "at-least-once");
                m.put("enable.auto.commit", "false");
                m.put("auto.offset.reset", "earliest");
            } else {
                m.put("processing.semantics", "at-most-once");
                m.put("enable.auto.commit", "true");
                m.put("auto.offset.reset", "latest");
            }
            configs.add(m);
        }
        return configs;
    }

    private static String serializerFor(String schemaType) {
        return switch (schemaType) {
            case "AVRO" -> "com.manas.kafkaplatform.serde.AvroSerializer";
            case "JSON" -> "com.manas.kafkaplatform.serde.JsonSerializer";
            case "STRING" -> "org.apache.kafka.common.serialization.StringSerializer";
            case "BYTES" -> "org.apache.kafka.common.serialization.ByteArraySerializer";
            default -> throw new IllegalArgumentException("unknown schema type " + schemaType);
        };
    }

    private static String deserializerFor(String schemaType) {
        return switch (schemaType) {
            case "AVRO" -> "com.manas.kafkaplatform.serde.AvroDeserializer";
            case "JSON" -> "com.manas.kafkaplatform.serde.JsonDeserializer";
            case "STRING" -> "org.apache.kafka.common.serialization.StringDeserializer";
            case "BYTES" -> "org.apache.kafka.common.serialization.ByteArrayDeserializer";
            default -> throw new IllegalArgumentException("unknown schema type " + schemaType);
        };
    }
}
