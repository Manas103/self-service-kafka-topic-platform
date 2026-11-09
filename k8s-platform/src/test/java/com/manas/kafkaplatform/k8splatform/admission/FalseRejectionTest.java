package com.manas.kafkaplatform.k8splatform.admission;

import com.manas.kafkaplatform.k8splatform.ClusterRequest;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Runs 200 independently generated, individually-valid cluster requests through the admission
 * validator and asserts none are rejected. A validator that rejects everything would trivially
 * pass the seeded-misconfiguration suite, so it must also be shown not to reject good requests.
 */
class FalseRejectionTest {

    @Test
    void noneOfTwoHundredValidRequestsAreRejected() {
        List<ClusterRequest> requests = ValidRequestGenerator.generate(200);
        assertEquals(200, requests.size());
        int rejections = 0;
        StringBuilder failures = new StringBuilder();
        for (ClusterRequest request : requests) {
            try {
                ClusterRequestValidator.validate(request);
            } catch (AdmissionRejectedException e) {
                rejections++;
                failures.append(request.clusterName()).append(" -> ").append(e.getMessage()).append('\n');
            }
        }
        assertEquals(0, rejections, "false rejections among valid requests:\n" + failures);
    }
}
