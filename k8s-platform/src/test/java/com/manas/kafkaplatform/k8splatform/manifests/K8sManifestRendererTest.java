package com.manas.kafkaplatform.k8splatform.manifests;

import com.manas.kafkaplatform.k8splatform.ClusterRequest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class K8sManifestRendererTest {

    private static final ClusterRequest REQUEST = new ClusterRequest(
            "payments-cluster", "payments", 3, "orders.events", 6,
            3_600_000L, 1_048_576L, 2_097_152L, "apache/kafka:3.7.1");

    @Test
    void statefulSetDeclaresTheRequestedReplicaCount() {
        String yaml = K8sManifestRenderer.renderStatefulSet(REQUEST);
        assertTrue(yaml.contains("replicas: 3"));
        assertTrue(yaml.contains("kind: StatefulSet"));
        assertTrue(yaml.contains("image: apache/kafka:3.7.1"));
    }

    @Test
    void statefulSetDeclaresOneControllerQuorumVoterPerBroker() {
        String yaml = K8sManifestRenderer.renderStatefulSet(REQUEST);
        assertTrue(yaml.contains("1@payments-cluster-0.payments-cluster-headless.payments.svc.cluster.local:9093"));
        assertTrue(yaml.contains("2@payments-cluster-1.payments-cluster-headless.payments.svc.cluster.local:9093"));
        assertTrue(yaml.contains("3@payments-cluster-2.payments-cluster-headless.payments.svc.cluster.local:9093"));
    }

    @Test
    void statefulSetAlwaysDeclaresTheSslListenerAndMountsTheTlsSecret() {
        String yaml = K8sManifestRenderer.renderStatefulSet(REQUEST);
        assertTrue(yaml.contains("SSL://:9094"));
        assertTrue(yaml.contains("/etc/kafka/secrets/kafka.keystore.p12"));
        assertTrue(yaml.contains("secretName: payments-cluster-tls"));
    }

    @Test
    void namespaceManifestNamesTheDeclaredNamespace() {
        String yaml = K8sManifestRenderer.renderNamespace(REQUEST);
        assertTrue(yaml.contains("kind: Namespace"));
        assertTrue(yaml.contains("name: payments"));
    }

    @Test
    void tlsSecretCarriesTheBase64EncodedKeystoreAndCaCert() {
        byte[] keystore = "fake-keystore-bytes".getBytes();
        byte[] caCert = "fake-ca-cert-bytes".getBytes();
        String yaml = K8sManifestRenderer.renderTlsSecret(REQUEST, keystore, "changeit", caCert);

        assertTrue(yaml.contains(java.util.Base64.getEncoder().encodeToString(keystore)));
        assertTrue(yaml.contains(java.util.Base64.getEncoder().encodeToString(caCert)));
        assertTrue(yaml.contains("keystore.password: \"changeit\""));
    }

    @Test
    void headlessServiceHasNoClusterIpAndBootstrapServiceDoesHaveOne() {
        String headless = K8sManifestRenderer.renderHeadlessService(REQUEST);
        String bootstrap = K8sManifestRenderer.renderBootstrapService(REQUEST);

        assertTrue(headless.contains("clusterIP: None"));
        assertFalse(bootstrap.contains("clusterIP: None"));
    }
}
