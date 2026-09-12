package com.manas.kafkaplatform.controlplane;

import java.time.Instant;

/**
 * What comes back from the one {@code POST /topics} call: confirmation the topic exists, plus
 * the paths of the two artifacts that were produced as a side effect of that same call.
 */
public record TopicProvisionResponse(
        String topicName,
        int partitions,
        int replicationFactor,
        long retentionMs,
        String dashboardPath,
        String costReportPath,
        double estimatedMonthlyCostUsd,
        Instant createdAt) {
}
