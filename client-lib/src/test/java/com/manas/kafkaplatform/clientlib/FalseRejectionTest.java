package com.manas.kafkaplatform.clientlib;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Runs 200 independently generated, individually-valid configurations (100 producer, 100
 * consumer) through the validator and asserts none are rejected. This is the other half of the
 * honesty check: a validator that rejects everything would trivially pass the seeded-misconfig
 * suite, so it must also be shown not to reject good configs.
 */
class FalseRejectionTest {

    @Test
    void noneOfTwoHundredValidProducerConfigsAreRejected() {
        List<Map<String, String>> configs = ValidConfigGenerator.generateProducerConfigs(100);
        assertEquals(100, configs.size());
        int rejections = 0;
        StringBuilder failures = new StringBuilder();
        for (Map<String, String> cfg : configs) {
            try {
                KafkaClientConfigValidator.validateProducerConfig(cfg);
            } catch (ConfigValidationException e) {
                rejections++;
                failures.append(cfg.get("client.id")).append(" -> ").append(e.getMessage()).append('\n');
            }
        }
        assertEquals(0, rejections, "false rejections among valid producer configs:\n" + failures);
    }

    @Test
    void noneOfTwoHundredValidConsumerConfigsAreRejected() {
        List<Map<String, String>> configs = ValidConfigGenerator.generateConsumerConfigs(100);
        assertEquals(100, configs.size());
        int rejections = 0;
        StringBuilder failures = new StringBuilder();
        for (Map<String, String> cfg : configs) {
            try {
                KafkaClientConfigValidator.validateConsumerConfig(cfg);
            } catch (ConfigValidationException e) {
                rejections++;
                failures.append(cfg.get("group.id")).append(" -> ").append(e.getMessage()).append('\n');
            }
        }
        assertEquals(0, rejections, "false rejections among valid consumer configs:\n" + failures);
    }
}
