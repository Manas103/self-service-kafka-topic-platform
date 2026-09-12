package com.manas.kafkaplatform.controlplane;

/**
 * The declarative request body for {@code POST /topics}. Every field except {@code topicName} is
 * optional and defaults to a sane value, so the smallest possible self-service request is just a
 * topic name.
 */
public record TopicProvisionRequest(
        String topicName,
        Integer partitions,
        Integer replicationFactor,
        Long retentionMs,
        Long estimatedMessagesPerDay,
        Integer estimatedAvgMessageSizeBytes) {

    private static final long DEFAULT_RETENTION_MS = 7L * 24 * 60 * 60 * 1000; // 7 days

    public TopicProvisionRequest {
        if (partitions == null) {
            partitions = 1;
        }
        if (replicationFactor == null) {
            replicationFactor = 1;
        }
        if (retentionMs == null) {
            retentionMs = DEFAULT_RETENTION_MS;
        }
        if (estimatedMessagesPerDay == null) {
            estimatedMessagesPerDay = 0L;
        }
        if (estimatedAvgMessageSizeBytes == null) {
            estimatedAvgMessageSizeBytes = 0;
        }
    }
}
