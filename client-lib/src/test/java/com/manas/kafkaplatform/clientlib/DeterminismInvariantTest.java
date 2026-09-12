package com.manas.kafkaplatform.clientlib;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Invariant: the validator is a pure function of its input config. The same config, validated
 * twice (even interleaved with other configs in between), must never accept on one run and
 * reject on another, and if it rejects both times it must name the same rule both times.
 */
class DeterminismInvariantTest {

    @Test
    void seededCasesAreDeterministicAcrossRepeatedRuns() {
        for (SeededCase c : SeededCases.all()) {
            String first = outcomeOf(c.kind(), c.config());
            String second = outcomeOf(c.kind(), c.config());
            String third = outcomeOf(c.kind(), c.config());
            assertEquals(first, second, c.id() + " outcome changed between run 1 and run 2");
            assertEquals(first, third, c.id() + " outcome changed between run 1 and run 3");
        }
    }

    @Test
    void validConfigsAreDeterministicAcrossRepeatedRuns() {
        List<Map<String, String>> producers = ValidConfigGenerator.generateProducerConfigs(100);
        List<Map<String, String>> consumers = ValidConfigGenerator.generateConsumerConfigs(100);

        for (Map<String, String> cfg : producers) {
            String first = outcomeOf(ConfigKind.PRODUCER, cfg);
            String second = outcomeOf(ConfigKind.PRODUCER, cfg);
            assertEquals(first, second, "producer " + cfg.get("client.id") + " outcome changed between runs");
            assertEquals("ACCEPTED", first, "producer " + cfg.get("client.id") + " expected to be accepted");
        }
        for (Map<String, String> cfg : consumers) {
            String first = outcomeOf(ConfigKind.CONSUMER, cfg);
            String second = outcomeOf(ConfigKind.CONSUMER, cfg);
            assertEquals(first, second, "consumer " + cfg.get("group.id") + " outcome changed between runs");
            assertEquals("ACCEPTED", first, "consumer " + cfg.get("group.id") + " expected to be accepted");
        }
    }

    private static String outcomeOf(ConfigKind kind, Map<String, String> config) {
        try {
            if (kind == ConfigKind.PRODUCER) {
                KafkaClientConfigValidator.validateProducerConfig(config);
            } else {
                KafkaClientConfigValidator.validateConsumerConfig(config);
            }
            return "ACCEPTED";
        } catch (ConfigValidationException e) {
            return "REJECTED:" + e.rule().ruleId();
        }
    }
}
