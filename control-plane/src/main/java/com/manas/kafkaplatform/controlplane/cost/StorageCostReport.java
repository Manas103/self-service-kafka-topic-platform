package com.manas.kafkaplatform.controlplane.cost;

/**
 * A per-topic storage-cost estimate. This is an ESTIMATE derived from declared message-rate and
 * message-size assumptions, a retention window, and a fixed $/GB-month rate, not a measurement of
 * actual on-disk bytes and not a billed cloud invoice. See the "methodology" field and the
 * README's honest-framing section for why: a single embedded broker's disk usage does not
 * represent what a production multi-broker cluster would actually cost.
 */
public record StorageCostReport(
        String topicName,
        int partitions,
        int replicationFactor,
        long retentionMs,
        long estimatedMessagesPerDay,
        int estimatedAvgMessageSizeBytes,
        double estimatedBytesPerDay,
        double estimatedRetainedGb,
        double ratePerGbMonthUsd,
        double estimatedMonthlyCostUsd,
        String methodology) {
}
