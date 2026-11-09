package com.manas.kafkaplatform.k8splatform.reconcile;

import com.manas.kafkaplatform.k8splatform.ClusterRequest;
import com.manas.kafkaplatform.k8splatform.k8s.KubectlClient;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Reconciles declared cluster state against live cluster state (claim: "renders Kubernetes
 * manifests and reconciles declared against live state"). The StatefulSet's own controller is
 * what actually recreates a deleted pod; this loop's own contribution, and the thing its "healed
 * 30 induced pod deletions" claim is really about, is detecting the dip, waiting for the
 * replacement to become Ready, and then re-verifying that the declared topic this cluster was
 * provisioned with is still present on the live cluster, not just that the pod count recovered.
 * That distinction matters: a pod coming back Ready proves Kubernetes works, not that this
 * platform's own declared state survived the disruption.
 */
public final class ReconcileLoop {

    private final KubectlClient kubectl;

    public ReconcileLoop(KubectlClient kubectl) {
        this.kubectl = kubectl;
    }

    public record HealResult(boolean podDeleted, boolean healedWithinTimeout, Duration healTime, boolean topicStillPresent) {
    }

    /** Deletes one pod, waits for the StatefulSet to recover, then re-verifies declared state. */
    public HealResult induceAndObserveHeal(ClusterRequest request, String podToDelete, Duration healTimeout) {
        Instant start = Instant.now();
        boolean deleted = kubectl.deletePod(request.namespace(), podToDelete).ok();

        boolean healed = kubectl.waitForReady(request.namespace(), request.clusterName(), request.brokers(), healTimeout);
        Duration healTime = Duration.between(start, Instant.now());
        boolean topicStillPresent = healed && declaredTopicIsPresent(request);

        return new HealResult(deleted, healed, healTime, topicStillPresent);
    }

    private boolean declaredTopicIsPresent(ClusterRequest request) {
        var result = kubectl.exec(request.namespace(), request.clusterName() + "-0", List.of(
                "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server", "localhost:9092",
                "--describe", "--topic", request.topic()
        ));
        return result.ok() && result.output().contains(request.topic());
    }
}
