package com.manas.kafkaplatform.k8splatform.cli;

import com.manas.kafkaplatform.k8splatform.ClusterProvisioner;
import com.manas.kafkaplatform.k8splatform.ClusterRequest;
import com.manas.kafkaplatform.k8splatform.ClusterRequestParser;
import com.manas.kafkaplatform.k8splatform.admission.AdmissionRejectedException;
import com.manas.kafkaplatform.k8splatform.k8s.KubectlClient;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;

/**
 * The real end-to-end demo: a declarative request file to a running, TLS-listening,
 * topic-and-quota-provisioned Kafka cluster on whatever cluster the current kubeconfig context
 * points at. Usage: {@code ProvisionCli <request-file>}.
 */
public final class ProvisionCli {

    private ProvisionCli() {
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("usage: ProvisionCli <request-file>");
            System.exit(2);
        }

        ClusterRequest request;
        try {
            request = ClusterRequestParser.parseFile(Path.of(args[0]));
        } catch (Exception e) {
            System.err.println("failed to parse request file: " + e.getMessage());
            System.exit(1);
            return;
        }

        Instant start = Instant.now();
        ClusterProvisioner provisioner = new ClusterProvisioner(new KubectlClient());
        try {
            var result = provisioner.provision(request, Duration.ofMinutes(5));
            Duration elapsed = Duration.between(start, Instant.now());
            System.out.println("cluster: " + request.clusterName() + " namespace: " + request.namespace());
            System.out.println("ready within timeout: " + result.readyWithinTimeout() + " (" + result.readyReplicas() + "/" + request.brokers() + " ready)");
            System.out.println("topic created: " + result.topicCreated());
            System.out.println("quotas set: " + result.quotasSet());
            System.out.println("TLS handshake ok: " + result.tlsHandshakeOk());
            System.out.println("elapsed: " + elapsed.toMillis() + " ms");
            System.out.println(result.detail());
        } catch (AdmissionRejectedException e) {
            System.err.println("request rejected: " + e.getMessage());
            System.exit(1);
        }
    }
}
