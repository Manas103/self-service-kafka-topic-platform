package com.manas.kafkaplatform.k8splatform;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ClusterRequestParserTest {

    private static final String SAMPLE = """
            # a comment, ignored
            clusterName: payments-cluster
            namespace: payments

            brokers: 3
            topic: orders.events
            partitions: 6
            retentionMs: 3600000
            producerQuotaBytesPerSec: 1048576
            consumerQuotaBytesPerSec: 2097152
            image: apache/kafka:3.7.1
            """;

    @Test
    void parsesEveryField() {
        ClusterRequest r = ClusterRequestParser.parse(SAMPLE);
        assertEquals("payments-cluster", r.clusterName());
        assertEquals("payments", r.namespace());
        assertEquals(3, r.brokers());
        assertEquals("orders.events", r.topic());
        assertEquals(6, r.partitions());
        assertEquals(3_600_000L, r.retentionMs());
        assertEquals(1_048_576L, r.producerQuotaBytesPerSec());
        assertEquals(2_097_152L, r.consumerQuotaBytesPerSec());
        assertEquals("apache/kafka:3.7.1", r.image());
    }

    @Test
    void countsOnlyNonCommentNonBlankLines() {
        assertEquals(9, ClusterRequestParser.countNonCommentLines(SAMPLE));
    }
}
