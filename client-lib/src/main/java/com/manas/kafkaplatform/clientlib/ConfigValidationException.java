package com.manas.kafkaplatform.clientlib;

/**
 * Thrown by {@link KafkaClientConfigValidator} when a producer or consumer configuration
 * fails at startup. Always names the single {@link ValidationRule} that failed; callers can
 * branch on {@link #rule()} or just log {@link #getMessage()}, which already embeds the rule id.
 */
public class ConfigValidationException extends RuntimeException {

    private final ValidationRule rule;

    public ConfigValidationException(ValidationRule rule, String detail) {
        super("[" + rule.ruleId() + "] " + rule.message() + (detail == null ? "" : " (" + detail + ")"));
        this.rule = rule;
    }

    public ValidationRule rule() {
        return rule;
    }
}
