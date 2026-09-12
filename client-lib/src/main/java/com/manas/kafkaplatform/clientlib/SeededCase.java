package com.manas.kafkaplatform.clientlib;

import java.util.Map;

/**
 * One row of the reference-oracle table: a config that is expected to fail exactly one named
 * rule. {@code id} is a stable label (S01..S40) used in test output and in the committed raw
 * run logs under {@code docs/}.
 */
public record SeededCase(String id, ConfigKind kind, Map<String, String> config,
                          ValidationRule expectedRule, String description) {
}
