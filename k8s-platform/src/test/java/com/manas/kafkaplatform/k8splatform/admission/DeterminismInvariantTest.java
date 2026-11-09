package com.manas.kafkaplatform.k8splatform.admission;

import com.manas.kafkaplatform.k8splatform.ClusterRequest;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Every seeded case and every valid request must resolve to the identical outcome every run. */
class DeterminismInvariantTest {

    @Test
    void everySeededCaseFailsTheSameRuleAcrossThreeRuns() {
        List<SeededCase> cases = SeededCases.all();
        for (SeededCase c : cases) {
            AdmissionRule first = outcomeOf(c.request());
            for (int run = 0; run < 3; run++) {
                assertEquals(first, outcomeOf(c.request()), c.id() + " flipped outcome across runs");
            }
        }
    }

    @Test
    void everyValidRequestAcceptsTheSameWayAcrossThreeRuns() {
        List<ClusterRequest> requests = ValidRequestGenerator.generate(200);
        for (ClusterRequest request : requests) {
            boolean first = ClusterRequestValidator.isValid(request);
            for (int run = 0; run < 3; run++) {
                assertEquals(first, ClusterRequestValidator.isValid(request), request.clusterName() + " flipped outcome across runs");
            }
        }
    }

    private AdmissionRule outcomeOf(ClusterRequest request) {
        try {
            ClusterRequestValidator.validate(request);
            return null;
        } catch (AdmissionRejectedException e) {
            return e.rule();
        }
    }
}
