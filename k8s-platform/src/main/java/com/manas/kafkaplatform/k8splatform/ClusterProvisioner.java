package com.manas.kafkaplatform.k8splatform;

import com.manas.kafkaplatform.k8splatform.admission.ClusterRequestValidator;
import com.manas.kafkaplatform.k8splatform.k8s.KubectlClient;
import com.manas.kafkaplatform.k8splatform.manifests.K8sManifestRenderer;
import com.manas.kafkaplatform.k8splatform.tls.SelfSignedCertGenerator;

import java.time.Duration;
import java.util.List;

/**
 * Turns a validated {@link ClusterRequest} into a running Kafka cluster on Kubernetes: renders
 * every manifest, applies them in dependency order, waits for the StatefulSet to report every
 * broker Ready, then provisions the declared topic and client quotas by executing the Kafka CLI
 * tools already bundled in the broker image via {@code kubectl exec}, the same "exec into the
 * cluster rather than expose it to the host" design the reconcile loop's own healing checks use
 * (see the README's honest framing for exactly why).
 */
public final class ClusterProvisioner {

    private static final String KEYSTORE_PASSWORD = "changeit";

    private final KubectlClient kubectl;

    public ClusterProvisioner(KubectlClient kubectl) {
        this.kubectl = kubectl;
    }

    public record ProvisionResult(
            boolean readyWithinTimeout,
            int readyReplicas,
            boolean topicCreated,
            boolean quotasSet,
            boolean tlsHandshakeOk,
            String detail
    ) {
    }

    public ProvisionResult provision(ClusterRequest request, Duration readyTimeout) {
        ClusterRequestValidator.validate(request);

        var tls = SelfSignedCertGenerator.generate(request.clusterName(), KEYSTORE_PASSWORD);

        apply(K8sManifestRenderer.renderNamespace(request));
        apply(K8sManifestRenderer.renderTlsSecret(request, tls.keystoreBytes(), tls.keystorePassword(), tls.caCertBytes()));
        apply(K8sManifestRenderer.renderHeadlessService(request));
        apply(K8sManifestRenderer.renderBootstrapService(request));
        apply(K8sManifestRenderer.renderStatefulSet(request));

        boolean ready = kubectl.waitForReady(request.namespace(), request.clusterName(), request.brokers(), readyTimeout);
        int readyReplicas = kubectl.readyReplicas(request.namespace(), request.clusterName());
        if (!ready) {
            return new ProvisionResult(false, readyReplicas, false, false, false,
                    "StatefulSet did not reach " + request.brokers() + " ready replicas within " + readyTimeout);
        }

        String pod0 = request.clusterName() + "-0";
        boolean topicCreated = createTopic(request, pod0);
        boolean quotasSet = topicCreated && setQuotas(request, pod0);
        boolean tlsHandshakeOk = topicCreated && checkTlsHandshake(request, pod0);

        return new ProvisionResult(true, readyReplicas, topicCreated, quotasSet, tlsHandshakeOk,
                "provisioned " + request.brokers() + "-broker cluster '" + request.clusterName() + "' in namespace '" + request.namespace() + "'");
    }

    private boolean createTopic(ClusterRequest request, String pod) {
        var result = kubectl.exec(request.namespace(), pod, List.of(
                "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server", "localhost:9092",
                "--create", "--if-not-exists",
                "--topic", request.topic(),
                "--partitions", String.valueOf(request.partitions()),
                "--replication-factor", String.valueOf(Math.min(request.brokers(), 3)),
                "--config", "retention.ms=" + request.retentionMs()
        ));
        return result.ok();
    }

    private boolean setQuotas(ClusterRequest request, String pod) {
        var result = kubectl.exec(request.namespace(), pod, List.of(
                "/opt/kafka/bin/kafka-configs.sh",
                "--bootstrap-server", "localhost:9092",
                "--alter",
                "--add-config", "producer_byte_rate=" + request.producerQuotaBytesPerSec()
                        + ",consumer_byte_rate=" + request.consumerQuotaBytesPerSec(),
                "--entity-type", "clients",
                "--entity-default"
        ));
        return result.ok();
    }

    private boolean checkTlsHandshake(ClusterRequest request, String pod) {
        String fqdn = pod + "." + request.clusterName() + "-headless." + request.namespace() + ".svc.cluster.local";
        var result = kubectl.exec(request.namespace(), pod, List.of(
                "/bin/bash", "-c",
                "echo | openssl s_client -connect " + fqdn + ":9094 -CAfile /etc/kafka/secrets/ca.crt 2>&1"
        ));
        return result.ok() && result.output().contains("Verify return code: 0 (ok)");
    }

    private void apply(String manifestYaml) {
        var result = kubectl.apply(manifestYaml);
        if (!result.ok()) {
            throw new IllegalStateException("kubectl apply failed:\n" + result.output());
        }
    }
}
