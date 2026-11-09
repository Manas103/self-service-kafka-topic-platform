package com.manas.kafkaplatform.k8splatform.admission;

import com.manas.kafkaplatform.k8splatform.ClusterRequest;

import java.util.ArrayList;
import java.util.List;

/**
 * 40 hand-constructed requests, 5 per {@link AdmissionRule}, each breaking exactly one field
 * off an otherwise-valid baseline request so {@link ClusterRequestValidator} is expected to
 * fail on exactly the rule named, never a different one further down the fixed check order.
 */
public final class SeededCases {

    private SeededCases() {
    }

    private static ClusterRequest baseline() {
        return new ClusterRequest("payments-cluster", "payments", 3, "orders.events", 6,
                3_600_000L, 1_048_576L, 2_097_152L, "apache/kafka:3.7.1");
    }

    public static List<SeededCase> all() {
        List<SeededCase> cases = new ArrayList<>(40);
        int n = 1;

        String[] badClusterNames = {"", "PaymentsCluster", "1payments", "pa", "payments_cluster"};
        for (String bad : badClusterNames) {
            ClusterRequest b = baseline();
            cases.add(new SeededCase("S%02d".formatted(n++),
                    new ClusterRequest(bad, b.namespace(), b.brokers(), b.topic(), b.partitions(),
                            b.retentionMs(), b.producerQuotaBytesPerSec(), b.consumerQuotaBytesPerSec(),
                            b.image()),
                    AdmissionRule.INVALID_CLUSTER_NAME, "clusterName=" + bad));
        }

        String[] badNamespaces = {"", "Payments", "9ops", "ns", "ops_team"};
        for (String bad : badNamespaces) {
            ClusterRequest b = baseline();
            cases.add(new SeededCase("S%02d".formatted(n++),
                    new ClusterRequest(b.clusterName(), bad, b.brokers(), b.topic(), b.partitions(),
                            b.retentionMs(), b.producerQuotaBytesPerSec(), b.consumerQuotaBytesPerSec(),
                            b.image()),
                    AdmissionRule.INVALID_NAMESPACE, "namespace=" + bad));
        }

        int[] badBrokerCounts = {0, 2, 4, 8, -1};
        for (int bad : badBrokerCounts) {
            ClusterRequest b = baseline();
            cases.add(new SeededCase("S%02d".formatted(n++),
                    new ClusterRequest(b.clusterName(), b.namespace(), bad, b.topic(), b.partitions(),
                            b.retentionMs(), b.producerQuotaBytesPerSec(), b.consumerQuotaBytesPerSec(),
                            b.image()),
                    AdmissionRule.INVALID_BROKER_COUNT, "brokers=" + bad));
        }

        String[] badTopics = {"", "orders events", "orders/events", "x".repeat(250), "orders#events"};
        for (String bad : badTopics) {
            ClusterRequest b = baseline();
            cases.add(new SeededCase("S%02d".formatted(n++),
                    new ClusterRequest(b.clusterName(), b.namespace(), b.brokers(), bad, b.partitions(),
                            b.retentionMs(), b.producerQuotaBytesPerSec(), b.consumerQuotaBytesPerSec(),
                            b.image()),
                    AdmissionRule.INVALID_TOPIC_NAME, "topic=" + bad));
        }

        int[] badPartitions = {0, -1, 101, 500, -5};
        for (int bad : badPartitions) {
            ClusterRequest b = baseline();
            cases.add(new SeededCase("S%02d".formatted(n++),
                    new ClusterRequest(b.clusterName(), b.namespace(), b.brokers(), b.topic(), bad,
                            b.retentionMs(), b.producerQuotaBytesPerSec(), b.consumerQuotaBytesPerSec(),
                            b.image()),
                    AdmissionRule.INVALID_PARTITIONS, "partitions=" + bad));
        }

        long[] badProducerQuotas = {0L, -1L, -1024L, -500_000L, -9_999_999L};
        for (long bad : badProducerQuotas) {
            ClusterRequest b = baseline();
            cases.add(new SeededCase("S%02d".formatted(n++),
                    new ClusterRequest(b.clusterName(), b.namespace(), b.brokers(), b.topic(), b.partitions(),
                            b.retentionMs(), bad, b.consumerQuotaBytesPerSec(), b.image()),
                    AdmissionRule.NEGATIVE_PRODUCER_QUOTA, "producerQuotaBytesPerSec=" + bad));
        }

        long[] badConsumerQuotas = {0L, -1L, -2048L, -750_000L, -12_345L};
        for (long bad : badConsumerQuotas) {
            ClusterRequest b = baseline();
            cases.add(new SeededCase("S%02d".formatted(n++),
                    new ClusterRequest(b.clusterName(), b.namespace(), b.brokers(), b.topic(), b.partitions(),
                            b.retentionMs(), b.producerQuotaBytesPerSec(), bad, b.image()),
                    AdmissionRule.NEGATIVE_CONSUMER_QUOTA, "consumerQuotaBytesPerSec=" + bad));
        }

        String[] badImages = {"", "apache/kafka", "apache/kafka:", "Apache/Kafka:3.7.1 ", ":no-name"};
        for (String bad : badImages) {
            ClusterRequest b = baseline();
            cases.add(new SeededCase("S%02d".formatted(n++),
                    new ClusterRequest(b.clusterName(), b.namespace(), b.brokers(), b.topic(), b.partitions(),
                            b.retentionMs(), b.producerQuotaBytesPerSec(), b.consumerQuotaBytesPerSec(),
                            bad),
                    AdmissionRule.INVALID_IMAGE_REFERENCE, "image=" + bad));
        }

        return cases;
    }
}
