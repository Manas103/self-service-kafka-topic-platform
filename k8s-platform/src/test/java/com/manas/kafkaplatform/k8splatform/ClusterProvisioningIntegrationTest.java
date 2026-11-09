package com.manas.kafkaplatform.k8splatform;

import com.manas.kafkaplatform.k8splatform.k8s.KubectlClient;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Provisions a real 3-broker Kafka cluster on a real Kubernetes cluster (kind, inside WSL2) and
 * asserts every claim this module makes: the StatefulSet reaches 3 ready replicas, the declared
 * topic exists, client quotas are set, and the TLS listener completes a real handshake. Tagged
 * {@code cluster} and excluded from the default run (same convention as {@code HealthBenchmarkRunner}
 * elsewhere in this portfolio); run explicitly with {@code mvn test -Dtest=ClusterProvisioningIntegrationTest -Dsurefire.excludedGroups=}.
 * Skips cleanly if {@code kubectl} cannot reach a cluster.
 */
class ClusterProvisioningIntegrationTest {

    private static final ClusterRequest REQUEST = new ClusterRequest(
            "it-cluster", "k8s-platform-it", 3, "it.events", 3,
            3_600_000L, 1_048_576L, 2_097_152L, "apache/kafka:3.7.1");

    @Test
    @Tag("cluster")
    void provisionsAThreeBrokerClusterWithATopicQuotasAndATlsListener() {
        KubectlClient kubectl = new KubectlClient();
        assumeTrue(kubectl.apply(com.manas.kafkaplatform.k8splatform.manifests.K8sManifestRenderer.renderNamespace(REQUEST)).ok(),
                "no reachable Kubernetes cluster; skipping");

        ClusterProvisioner provisioner = new ClusterProvisioner(kubectl);
        var result = provisioner.provision(REQUEST, Duration.ofMinutes(6));

        assertTrue(result.readyWithinTimeout(), "3-broker StatefulSet did not reach ready: " + result.detail());
        assertTrue(result.topicCreated(), "declared topic was not created");
        assertTrue(result.quotasSet(), "client quotas were not set");
        assertTrue(result.tlsHandshakeOk(), "TLS listener did not complete a handshake");
    }
}
