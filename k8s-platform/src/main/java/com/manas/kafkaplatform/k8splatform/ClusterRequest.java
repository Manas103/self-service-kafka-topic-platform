package com.manas.kafkaplatform.k8splatform;

/**
 * A declarative request for a self-serve Kafka cluster on Kubernetes: the
 * "20-line request" the resume claim names. Parsed from a plain
 * {@code key: value} text format by {@link ClusterRequestParser}; see
 * {@code examples/payments-cluster.request} for a real one.
 */
public record ClusterRequest(
        String clusterName,
        String namespace,
        int brokers,
        String topic,
        int partitions,
        long retentionMs,
        long producerQuotaBytesPerSec,
        long consumerQuotaBytesPerSec,
        String image
) {
}
