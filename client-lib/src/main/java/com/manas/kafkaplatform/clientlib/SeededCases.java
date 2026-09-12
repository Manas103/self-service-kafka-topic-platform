package com.manas.kafkaplatform.clientlib;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The 40 seeded misconfigurations used as the reference-oracle table for
 * {@link KafkaClientConfigValidator}: 5 distinct cases per rule, across all 8 rules. Every case
 * starts from a known-valid baseline config and changes exactly the field(s) needed to trip one
 * rule, so each case has one unambiguous expected outcome, checked one at a time in fixed rule
 * order (see {@link KafkaClientConfigValidator}).
 */
public final class SeededCases {

    private SeededCases() {
    }

    public static Map<String, String> validProducerBaseline() {
        Map<String, String> m = new HashMap<>();
        m.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        m.put("value.serializer", "com.manas.kafkaplatform.serde.AvroSerializer");
        m.put("schema.type", "AVRO");
        m.put("acks", "all");
        m.put("client.id", "orders-producer");
        return m;
    }

    public static Map<String, String> validConsumerBaseline() {
        Map<String, String> m = new HashMap<>();
        m.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        m.put("value.deserializer", "com.manas.kafkaplatform.serde.AvroDeserializer");
        m.put("schema.type", "AVRO");
        m.put("group.id", "orders-consumer");
        m.put("processing.semantics", "at-least-once");
        m.put("enable.auto.commit", "false");
        m.put("auto.offset.reset", "earliest");
        return m;
    }

    private static Map<String, String> producer(java.util.function.Consumer<Map<String, String>> mutate) {
        Map<String, String> m = validProducerBaseline();
        mutate.accept(m);
        return m;
    }

    private static Map<String, String> consumer(java.util.function.Consumer<Map<String, String>> mutate) {
        Map<String, String> m = validConsumerBaseline();
        mutate.accept(m);
        return m;
    }

    public static List<SeededCase> all() {
        List<SeededCase> cases = new ArrayList<>();

        // -- MISSING_SERIALIZER: S01-S05 --------------------------------------
        cases.add(new SeededCase("S01", ConfigKind.PRODUCER,
                producer(m -> m.remove("key.serializer")),
                ValidationRule.MISSING_SERIALIZER, "key.serializer absent"));
        cases.add(new SeededCase("S02", ConfigKind.PRODUCER,
                producer(m -> m.remove("value.serializer")),
                ValidationRule.MISSING_SERIALIZER, "value.serializer absent"));
        cases.add(new SeededCase("S03", ConfigKind.PRODUCER,
                producer(m -> m.put("key.serializer", "")),
                ValidationRule.MISSING_SERIALIZER, "key.serializer blank"));
        cases.add(new SeededCase("S04", ConfigKind.PRODUCER,
                producer(m -> m.put("value.serializer", "   ")),
                ValidationRule.MISSING_SERIALIZER, "value.serializer whitespace-only"));
        cases.add(new SeededCase("S05", ConfigKind.PRODUCER,
                producer(m -> {
                    m.remove("key.serializer");
                    m.remove("value.serializer");
                }),
                ValidationRule.MISSING_SERIALIZER, "both serializers absent"));

        // -- SERIALIZER_SCHEMA_MISMATCH: S06-S10 ------------------------------
        cases.add(new SeededCase("S06", ConfigKind.PRODUCER,
                producer(m -> {
                    m.put("schema.type", "AVRO");
                    m.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                }),
                ValidationRule.SERIALIZER_SCHEMA_MISMATCH, "AVRO schema with StringSerializer"));
        cases.add(new SeededCase("S07", ConfigKind.PRODUCER,
                producer(m -> {
                    m.put("schema.type", "JSON");
                    m.put("value.serializer", "com.manas.kafkaplatform.serde.AvroSerializer");
                }),
                ValidationRule.SERIALIZER_SCHEMA_MISMATCH, "JSON schema with AvroSerializer"));
        cases.add(new SeededCase("S08", ConfigKind.PRODUCER,
                producer(m -> {
                    m.put("schema.type", "STRING");
                    m.put("value.serializer", "com.manas.kafkaplatform.serde.JsonSerializer");
                }),
                ValidationRule.SERIALIZER_SCHEMA_MISMATCH, "STRING schema with JsonSerializer"));
        cases.add(new SeededCase("S09", ConfigKind.PRODUCER,
                producer(m -> {
                    m.put("schema.type", "BYTES");
                    m.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                }),
                ValidationRule.SERIALIZER_SCHEMA_MISMATCH, "BYTES schema with StringSerializer"));
        cases.add(new SeededCase("S10", ConfigKind.PRODUCER,
                producer(m -> {
                    m.put("schema.type", "AVRO");
                    m.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
                }),
                ValidationRule.SERIALIZER_SCHEMA_MISMATCH, "AVRO schema with ByteArraySerializer"));

        // -- UNSAFE_ACKS: S11-S15 ----------------------------------------------
        cases.add(new SeededCase("S11", ConfigKind.PRODUCER,
                producer(m -> m.put("acks", "0")),
                ValidationRule.UNSAFE_ACKS, "acks=0"));
        cases.add(new SeededCase("S12", ConfigKind.PRODUCER,
                producer(m -> m.put("acks", "1")),
                ValidationRule.UNSAFE_ACKS, "acks=1"));
        cases.add(new SeededCase("S13", ConfigKind.PRODUCER,
                producer(m -> m.remove("acks")),
                ValidationRule.UNSAFE_ACKS, "acks absent"));
        cases.add(new SeededCase("S14", ConfigKind.PRODUCER,
                producer(m -> m.put("acks", "")),
                ValidationRule.UNSAFE_ACKS, "acks blank"));
        cases.add(new SeededCase("S15", ConfigKind.PRODUCER,
                producer(m -> m.put("acks", "2")),
                ValidationRule.UNSAFE_ACKS, "acks=2 (not a valid acks value at all)"));

        // -- CLIENT_ID_CONVENTION: S16-S20 --------------------------------------
        cases.add(new SeededCase("S16", ConfigKind.PRODUCER,
                producer(m -> m.put("client.id", "OrdersProducer")),
                ValidationRule.CLIENT_ID_CONVENTION, "uppercase letters"));
        cases.add(new SeededCase("S17", ConfigKind.PRODUCER,
                producer(m -> m.put("client.id", "orders_producer")),
                ValidationRule.CLIENT_ID_CONVENTION, "underscore not allowed"));
        cases.add(new SeededCase("S18", ConfigKind.PRODUCER,
                producer(m -> m.put("client.id", "1orders")),
                ValidationRule.CLIENT_ID_CONVENTION, "starts with a digit"));
        cases.add(new SeededCase("S19", ConfigKind.PRODUCER,
                producer(m -> m.put("client.id", "ab")),
                ValidationRule.CLIENT_ID_CONVENTION, "shorter than 3 characters"));
        cases.add(new SeededCase("S20", ConfigKind.PRODUCER,
                producer(m -> m.put("client.id", "orders producer")),
                ValidationRule.CLIENT_ID_CONVENTION, "contains a space"));

        // -- MISSING_DESERIALIZER: S21-S25 --------------------------------------
        cases.add(new SeededCase("S21", ConfigKind.CONSUMER,
                consumer(m -> m.remove("key.deserializer")),
                ValidationRule.MISSING_DESERIALIZER, "key.deserializer absent"));
        cases.add(new SeededCase("S22", ConfigKind.CONSUMER,
                consumer(m -> m.remove("value.deserializer")),
                ValidationRule.MISSING_DESERIALIZER, "value.deserializer absent"));
        cases.add(new SeededCase("S23", ConfigKind.CONSUMER,
                consumer(m -> m.put("key.deserializer", "")),
                ValidationRule.MISSING_DESERIALIZER, "key.deserializer blank"));
        cases.add(new SeededCase("S24", ConfigKind.CONSUMER,
                consumer(m -> m.put("value.deserializer", "   ")),
                ValidationRule.MISSING_DESERIALIZER, "value.deserializer whitespace-only"));
        cases.add(new SeededCase("S25", ConfigKind.CONSUMER,
                consumer(m -> {
                    m.remove("key.deserializer");
                    m.remove("value.deserializer");
                }),
                ValidationRule.MISSING_DESERIALIZER, "both deserializers absent"));

        // -- DESERIALIZER_SCHEMA_MISMATCH: S26-S30 -------------------------------
        cases.add(new SeededCase("S26", ConfigKind.CONSUMER,
                consumer(m -> {
                    m.put("schema.type", "AVRO");
                    m.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                }),
                ValidationRule.DESERIALIZER_SCHEMA_MISMATCH, "AVRO schema with StringDeserializer"));
        cases.add(new SeededCase("S27", ConfigKind.CONSUMER,
                consumer(m -> {
                    m.put("schema.type", "JSON");
                    m.put("value.deserializer", "com.manas.kafkaplatform.serde.AvroDeserializer");
                }),
                ValidationRule.DESERIALIZER_SCHEMA_MISMATCH, "JSON schema with AvroDeserializer"));
        cases.add(new SeededCase("S28", ConfigKind.CONSUMER,
                consumer(m -> {
                    m.put("schema.type", "STRING");
                    m.put("value.deserializer", "com.manas.kafkaplatform.serde.JsonDeserializer");
                }),
                ValidationRule.DESERIALIZER_SCHEMA_MISMATCH, "STRING schema with JsonDeserializer"));
        cases.add(new SeededCase("S29", ConfigKind.CONSUMER,
                consumer(m -> {
                    m.put("schema.type", "BYTES");
                    m.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                }),
                ValidationRule.DESERIALIZER_SCHEMA_MISMATCH, "BYTES schema with StringDeserializer"));
        cases.add(new SeededCase("S30", ConfigKind.CONSUMER,
                consumer(m -> {
                    m.put("schema.type", "AVRO");
                    m.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
                }),
                ValidationRule.DESERIALIZER_SCHEMA_MISMATCH, "AVRO schema with ByteArrayDeserializer"));

        // -- GROUP_ID_CONVENTION: S31-S35 -----------------------------------------
        cases.add(new SeededCase("S31", ConfigKind.CONSUMER,
                consumer(m -> m.put("group.id", "OrdersConsumer")),
                ValidationRule.GROUP_ID_CONVENTION, "uppercase letters"));
        cases.add(new SeededCase("S32", ConfigKind.CONSUMER,
                consumer(m -> m.put("group.id", "orders_consumer")),
                ValidationRule.GROUP_ID_CONVENTION, "underscore not allowed"));
        cases.add(new SeededCase("S33", ConfigKind.CONSUMER,
                consumer(m -> m.put("group.id", "9orders")),
                ValidationRule.GROUP_ID_CONVENTION, "starts with a digit"));
        cases.add(new SeededCase("S34", ConfigKind.CONSUMER,
                consumer(m -> m.put("group.id", "ab")),
                ValidationRule.GROUP_ID_CONVENTION, "shorter than 3 characters"));
        cases.add(new SeededCase("S35", ConfigKind.CONSUMER,
                consumer(m -> m.put("group.id", "orders.consumer")),
                ValidationRule.GROUP_ID_CONVENTION, "dot not allowed"));

        // -- OFFSET_RESET_CONFLICT: S36-S40 ---------------------------------------
        cases.add(new SeededCase("S36", ConfigKind.CONSUMER,
                consumer(m -> {
                    m.put("enable.auto.commit", "true");
                    m.put("auto.offset.reset", "earliest");
                }),
                ValidationRule.OFFSET_RESET_CONFLICT, "at-least-once with auto-commit left on"));
        cases.add(new SeededCase("S37", ConfigKind.CONSUMER,
                consumer(m -> {
                    m.put("enable.auto.commit", "false");
                    m.put("auto.offset.reset", "none");
                }),
                ValidationRule.OFFSET_RESET_CONFLICT, "at-least-once with auto.offset.reset=none"));
        cases.add(new SeededCase("S38", ConfigKind.CONSUMER,
                consumer(m -> {
                    m.put("enable.auto.commit", "true");
                    m.put("auto.offset.reset", "none");
                }),
                ValidationRule.OFFSET_RESET_CONFLICT, "at-least-once with both auto-commit on and reset=none"));
        cases.add(new SeededCase("S39", ConfigKind.CONSUMER,
                consumer(m -> {
                    m.remove("enable.auto.commit");
                    m.put("auto.offset.reset", "earliest");
                }),
                ValidationRule.OFFSET_RESET_CONFLICT, "at-least-once with enable.auto.commit unset (defaults true)"));
        cases.add(new SeededCase("S40", ConfigKind.CONSUMER,
                consumer(m -> {
                    m.put("enable.auto.commit", "false");
                    m.remove("auto.offset.reset");
                }),
                ValidationRule.OFFSET_RESET_CONFLICT, "at-least-once with auto.offset.reset unset"));

        return cases;
    }
}
