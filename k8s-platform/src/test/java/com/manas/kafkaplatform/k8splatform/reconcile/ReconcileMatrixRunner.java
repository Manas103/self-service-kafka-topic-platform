package com.manas.kafkaplatform.k8splatform.reconcile;

import com.manas.kafkaplatform.k8splatform.ClusterProvisioner;
import com.manas.kafkaplatform.k8splatform.ClusterRequest;
import com.manas.kafkaplatform.k8splatform.k8s.KubectlClient;
import com.manas.kafkaplatform.k8splatform.manifests.K8sManifestRenderer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The real measurement run behind the "reconcile loop healed 30 induced pod deletions with no
 * manual step" claim: provisions a real 3-broker cluster, then deletes one broker pod at a time
 * (round-robin across the three), 30 times, each time waiting for {@link ReconcileLoop} to
 * observe the StatefulSet recover to 3 ready replicas and to re-verify the declared topic is
 * still present, with no manual intervention between trials. Tagged {@code cluster}, excluded
 * from the default run; run explicitly with
 * {@code mvn test -Dtest=ReconcileMatrixRunner -Dsurefire.excludedGroups=}.
 */
class ReconcileMatrixRunner {

    private static final ClusterRequest REQUEST = new ClusterRequest(
            "reconcile-cluster", "k8s-platform-reconcile", 3, "reconcile.events", 3,
            3_600_000L, 1_048_576L, 2_097_152L, "apache/kafka:3.7.1");
    private static final int TRIALS = 30;
    private static final Duration HEAL_TIMEOUT = Duration.ofMinutes(2);

    @Test
    @Tag("cluster")
    void measureThirtyInducedPodDeletionsAgainstARealCluster() {
        KubectlClient kubectl = new KubectlClient();
        assumeTrue(kubectl.apply(K8sManifestRenderer.renderNamespace(REQUEST)).ok(),
                "no reachable Kubernetes cluster; skipping");

        StringBuilder report = new StringBuilder();
        line(report, "=== Self-Serve Kafka Provisioning on Kubernetes -- reconcile-loop benchmark ===");
        line(report, "started: " + Instant.now());

        ClusterProvisioner provisioner = new ClusterProvisioner(kubectl);
        var provisionResult = provisioner.provision(REQUEST, Duration.ofMinutes(6));
        line(report, "initial provisioning: readyWithinTimeout=" + provisionResult.readyWithinTimeout()
                + " topicCreated=" + provisionResult.topicCreated() + " tlsHandshakeOk=" + provisionResult.tlsHandshakeOk());

        ReconcileLoop reconcileLoop = new ReconcileLoop(kubectl);
        int healed = 0;
        int topicSurvived = 0;
        line(report, "");
        line(report, "trial  pod                     healedWithinTimeout  healTimeMs  topicStillPresent");
        for (int i = 0; i < TRIALS; i++) {
            String pod = REQUEST.clusterName() + "-" + (i % REQUEST.brokers());
            var heal = reconcileLoop.induceAndObserveHeal(REQUEST, pod, HEAL_TIMEOUT);
            if (heal.healedWithinTimeout()) healed++;
            if (heal.topicStillPresent()) topicSurvived++;
            line(report, "%-6d %-23s %-20s %-11d %s".formatted(
                    i + 1, pod, heal.healedWithinTimeout(), heal.healTime().toMillis(), heal.topicStillPresent()));
        }

        line(report, "");
        line(report, "-- summary --");
        line(report, "healed within " + HEAL_TIMEOUT.toSeconds() + "s: " + healed + " / " + TRIALS);
        line(report, "declared topic still present after heal: " + topicSurvived + " / " + TRIALS);
        line(report, "finished: " + Instant.now());

        System.out.println(report);
    }

    private void line(StringBuilder sb, String text) {
        sb.append(text).append(System.lineSeparator());
    }
}
