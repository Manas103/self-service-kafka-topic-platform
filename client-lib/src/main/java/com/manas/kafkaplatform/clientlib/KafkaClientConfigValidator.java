package com.manas.kafkaplatform.clientlib;

import java.util.Map;
import java.util.regex.Pattern;

/**
 * Fail-fast startup validator for producer and consumer configuration maps. Applications call
 * {@link #validateProducerConfig(Map)} or {@link #validateConsumerConfig(Map)} once, before
 * building the actual {@code KafkaProducer}/{@code KafkaConsumer}, so a bad config never gets
 * past process startup. Each check is independent and each failure names exactly one
 * {@link ValidationRule}; rules run in a fixed order (declared below) so the same config always
 * fails on the same rule, which is what makes the reference-oracle table in the test suite
 * possible to write by hand.
 *
 * <p>Recognised (string) keys, all optional unless a rule below requires them:
 * <ul>
 *   <li>{@code key.serializer}, {@code value.serializer} (producer)</li>
 *   <li>{@code key.deserializer}, {@code value.deserializer} (consumer)</li>
 *   <li>{@code schema.type}: one of AVRO, JSON, STRING, BYTES; used only to cross-check the
 *       serializer/deserializer class name, not enforced by itself</li>
 *   <li>{@code acks} (producer)</li>
 *   <li>{@code client.id} (producer), {@code group.id} (consumer)</li>
 *   <li>{@code processing.semantics}: "at-least-once" or "at-most-once" (consumer, optional;
 *       the offset-reset-conflict rule only fires when this is declared)</li>
 *   <li>{@code enable.auto.commit}, {@code auto.offset.reset} (consumer)</li>
 * </ul>
 */
public final class KafkaClientConfigValidator {

    private static final Pattern ID_PATTERN = Pattern.compile("^[a-z][a-z0-9-]{2,49}$");

    private KafkaClientConfigValidator() {
    }

    public static void validateProducerConfig(Map<String, String> config) {
        checkMissingSerializer(config);
        checkSerializerSchemaMismatch(config);
        checkUnsafeAcks(config);
        checkClientIdConvention(config);
    }

    public static void validateConsumerConfig(Map<String, String> config) {
        checkMissingDeserializer(config);
        checkDeserializerSchemaMismatch(config);
        checkGroupIdConvention(config);
        checkOffsetResetConflict(config);
    }

    // -- producer rules -----------------------------------------------------

    private static void checkMissingSerializer(Map<String, String> c) {
        if (isBlank(c.get("key.serializer")) || isBlank(c.get("value.serializer"))) {
            throw new ConfigValidationException(ValidationRule.MISSING_SERIALIZER,
                    "key.serializer=" + c.get("key.serializer") + ", value.serializer=" + c.get("value.serializer"));
        }
    }

    private static void checkSerializerSchemaMismatch(Map<String, String> c) {
        String schemaType = c.get("schema.type");
        if (isBlank(schemaType)) {
            return;
        }
        String valueSerializer = c.get("value.serializer");
        if (valueSerializer == null || !valueSerializer.contains(expectedSuffix(schemaType, "Serializer"))) {
            throw new ConfigValidationException(ValidationRule.SERIALIZER_SCHEMA_MISMATCH,
                    "schema.type=" + schemaType + ", value.serializer=" + valueSerializer);
        }
    }

    private static void checkUnsafeAcks(Map<String, String> c) {
        String acks = c.get("acks");
        if (acks == null || !(acks.equals("all") || acks.equals("-1"))) {
            throw new ConfigValidationException(ValidationRule.UNSAFE_ACKS, "acks=" + acks);
        }
    }

    private static void checkClientIdConvention(Map<String, String> c) {
        String clientId = c.get("client.id");
        if (clientId == null || !ID_PATTERN.matcher(clientId).matches()) {
            throw new ConfigValidationException(ValidationRule.CLIENT_ID_CONVENTION, "client.id=" + clientId);
        }
    }

    // -- consumer rules -------------------------------------------------------

    private static void checkMissingDeserializer(Map<String, String> c) {
        if (isBlank(c.get("key.deserializer")) || isBlank(c.get("value.deserializer"))) {
            throw new ConfigValidationException(ValidationRule.MISSING_DESERIALIZER,
                    "key.deserializer=" + c.get("key.deserializer") + ", value.deserializer=" + c.get("value.deserializer"));
        }
    }

    private static void checkDeserializerSchemaMismatch(Map<String, String> c) {
        String schemaType = c.get("schema.type");
        if (isBlank(schemaType)) {
            return;
        }
        String valueDeserializer = c.get("value.deserializer");
        if (valueDeserializer == null || !valueDeserializer.contains(expectedSuffix(schemaType, "Deserializer"))) {
            throw new ConfigValidationException(ValidationRule.DESERIALIZER_SCHEMA_MISMATCH,
                    "schema.type=" + schemaType + ", value.deserializer=" + valueDeserializer);
        }
    }

    private static void checkGroupIdConvention(Map<String, String> c) {
        String groupId = c.get("group.id");
        if (groupId == null || !ID_PATTERN.matcher(groupId).matches()) {
            throw new ConfigValidationException(ValidationRule.GROUP_ID_CONVENTION, "group.id=" + groupId);
        }
    }

    private static void checkOffsetResetConflict(Map<String, String> c) {
        String semantics = c.get("processing.semantics");
        if (semantics == null || !semantics.equals("at-least-once")) {
            return;
        }
        String autoCommit = c.get("enable.auto.commit");
        String offsetReset = c.get("auto.offset.reset");
        boolean autoCommitOn = autoCommit == null || Boolean.parseBoolean(autoCommit);
        boolean offsetResetMissingOrNone = offsetReset == null || offsetReset.equals("none");
        if (autoCommitOn || offsetResetMissingOrNone) {
            throw new ConfigValidationException(ValidationRule.OFFSET_RESET_CONFLICT,
                    "processing.semantics=at-least-once, enable.auto.commit=" + autoCommit
                            + ", auto.offset.reset=" + offsetReset);
        }
    }

    // -- shared helpers ---------------------------------------------------

    private static String expectedSuffix(String schemaType, String kindSuffix) {
        return switch (schemaType) {
            case "AVRO" -> "Avro" + kindSuffix;
            case "JSON" -> "Json" + kindSuffix;
            case "STRING" -> "String" + kindSuffix;
            case "BYTES" -> "ByteArray" + kindSuffix;
            default -> "__unknown_schema_type__";
        };
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
