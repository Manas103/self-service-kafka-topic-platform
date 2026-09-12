package com.manas.kafkaplatform.controlplane.cost;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Produces an estimated, not measured, per-topic storage-cost report. The estimate is:
 *
 * <pre>
 * bytesPerDay      = estimatedMessagesPerDay * estimatedAvgMessageSizeBytes * replicationFactor
 * retainedBytes    = bytesPerDay * (retentionMs in days)     // steady-state volume held at once
 * retainedGb       = retainedBytes / 1024^3
 * monthlyCostUsd   = retainedGb * ratePerGbMonthUsd
 * </pre>
 *
 * The rate defaults to $0.023/GB-month (a commodity object-storage list price used here only as
 * a proxy, not a claim about real Kafka broker disk pricing). This is deliberately an estimate
 * from declared assumptions, not a measurement of actual on-disk bytes on the embedded broker:
 * one embedded, single-partition-replica broker's disk usage would not represent what a real
 * multi-broker production cluster actually costs, so measuring it would produce a falsely
 * precise-looking number for a quantity that is inherently an estimate at request time (before
 * any real traffic exists).
 */
@Component
public class StorageCostEstimator {

    private static final double BYTES_PER_GB = 1024.0 * 1024.0 * 1024.0;
    private static final double MS_PER_DAY = 24.0 * 60 * 60 * 1000;

    private final double ratePerGbMonthUsd;

    public StorageCostEstimator(@Value("${platform.cost.rate-usd-per-gb-month:0.023}") double ratePerGbMonthUsd) {
        this.ratePerGbMonthUsd = ratePerGbMonthUsd;
    }

    public StorageCostReport estimate(String topicName, int partitions, int replicationFactor, long retentionMs,
                                       long estimatedMessagesPerDay, int estimatedAvgMessageSizeBytes) {
        double bytesPerDay = (double) estimatedMessagesPerDay * estimatedAvgMessageSizeBytes * replicationFactor;
        double retentionDays = retentionMs / MS_PER_DAY;
        double retainedBytes = bytesPerDay * retentionDays;
        double retainedGb = retainedBytes / BYTES_PER_GB;
        double monthlyCostUsd = retainedGb * ratePerGbMonthUsd;

        return new StorageCostReport(
                topicName, partitions, replicationFactor, retentionMs,
                estimatedMessagesPerDay, estimatedAvgMessageSizeBytes,
                bytesPerDay, retainedGb, ratePerGbMonthUsd, monthlyCostUsd,
                "ESTIMATE from declared message-rate/size assumptions and retention window; "
                        + "not a measurement of actual broker disk usage and not a billed cloud cost");
    }
}
