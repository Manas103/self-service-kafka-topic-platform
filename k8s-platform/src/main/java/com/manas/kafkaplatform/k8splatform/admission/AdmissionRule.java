package com.manas.kafkaplatform.k8splatform.admission;

/**
 * The named rule set the cluster-request admission policy enforces, the
 * same "always name exactly one failing rule" convention this portfolio's
 * {@code client-lib} module uses for its own config validator.
 */
public enum AdmissionRule {

    INVALID_CLUSTER_NAME(
            "invalid-cluster-name",
            "clusterName must match ^[a-z][a-z0-9-]{2,49}$ (a valid Kubernetes DNS-1123 label)"),

    INVALID_NAMESPACE(
            "invalid-namespace",
            "namespace must match ^[a-z][a-z0-9-]{2,49}$ (a valid Kubernetes DNS-1123 label)"),

    INVALID_BROKER_COUNT(
            "invalid-broker-count",
            "brokers must be an odd number between 1 and 7 (a valid KRaft controller-quorum size)"),

    INVALID_TOPIC_NAME(
            "invalid-topic-name",
            "topic must match ^[a-zA-Z0-9._-]{1,249}$ (a valid Kafka topic name)"),

    INVALID_PARTITIONS(
            "invalid-partitions",
            "partitions must be between 1 and 100"),

    NEGATIVE_PRODUCER_QUOTA(
            "negative-producer-quota",
            "producerQuotaBytesPerSec must be a positive integer"),

    NEGATIVE_CONSUMER_QUOTA(
            "negative-consumer-quota",
            "consumerQuotaBytesPerSec must be a positive integer"),

    INVALID_IMAGE_REFERENCE(
            "invalid-image-reference",
            "image must match ^[a-zA-Z0-9/_.-]+:[a-zA-Z0-9_.-]+$ (a registry reference with an explicit tag)");

    private final String ruleId;
    private final String message;

    AdmissionRule(String ruleId, String message) {
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
