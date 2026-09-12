package com.manas.kafkaplatform.clientlib;

/**
 * The named rule set enforced by {@link KafkaClientConfigValidator}. Each rule has a stable
 * id (used in log lines and in the reference oracle table in tests) and a human message
 * template. A failing config is rejected by naming exactly one of these, never a generic
 * "invalid config" message.
 */
public enum ValidationRule {

    MISSING_SERIALIZER(
            "missing-serializer",
            "producer config must set both key.serializer and value.serializer"),

    MISSING_DESERIALIZER(
            "missing-deserializer",
            "consumer config must set both key.deserializer and value.deserializer"),

    SERIALIZER_SCHEMA_MISMATCH(
            "serializer-schema-mismatch",
            "value.serializer does not match the declared schema.type"),

    DESERIALIZER_SCHEMA_MISMATCH(
            "deserializer-schema-mismatch",
            "value.deserializer does not match the declared schema.type"),

    UNSAFE_ACKS(
            "unsafe-acks",
            "producer acks must be 'all' (or '-1'); a lower value risks silent data loss"),

    CLIENT_ID_CONVENTION(
            "client-id-convention",
            "client.id must match ^[a-z][a-z0-9-]{2,49}$ (lowercase, hyphenated, 3-50 chars)"),

    GROUP_ID_CONVENTION(
            "group-id-convention",
            "group.id must match ^[a-z][a-z0-9-]{2,49}$ (lowercase, hyphenated, 3-50 chars)"),

    OFFSET_RESET_CONFLICT(
            "offset-reset-conflict",
            "processing.semantics=at-least-once requires enable.auto.commit=false and "
                    + "auto.offset.reset != none");

    private final String ruleId;
    private final String message;

    ValidationRule(String ruleId, String message) {
        this.ruleId = ruleId;
        this.message = message;
    }

    public String ruleId() {
        return ruleId;
    }

    public String message() {
        return message;
    }
}
