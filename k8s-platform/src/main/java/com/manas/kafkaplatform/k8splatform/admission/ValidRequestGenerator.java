package com.manas.kafkaplatform.k8splatform.admission;

import com.manas.kafkaplatform.k8splatform.ClusterRequest;

import java.util.ArrayList;
import java.util.List;

/**
 * Generates 200 distinct, individually-valid cluster requests by systematically varying every
 * field within its own valid range, always keeping every other field internally consistent.
 * Used to measure {@link ClusterRequestValidator}'s false-rejection rate: every one of these 200
 * is expected to pass.
 */
public final class ValidRequestGenerator {

    private static final int[] VALID_BROKER_COUNTS = {1, 3, 5, 7};
    private static final String[] IMAGES = {"apache/kafka:3.7.1", "apache/kafka:3.7.0", "apache/kafka:3.6.2"};

    private ValidRequestGenerator() {
    }

    public static List<ClusterRequest> generate(int count) {
        List<ClusterRequest> requests = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            requests.add(new ClusterRequest(
                    "team-%03d-cluster".formatted(i),
                    "team-%03d-ns".formatted(i),
                    VALID_BROKER_COUNTS[i % VALID_BROKER_COUNTS.length],
                    "team-%03d.events.v%d".formatted(i, 1 + i % 3),
                    1 + (i % 100),
                    60_000L + i * 1000L,
                    1024L + i * 512L,
                    2048L + i * 512L,
                    IMAGES[i % IMAGES.length]
            ));
        }
        return requests;
    }
}
