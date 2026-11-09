package com.manas.kafkaplatform.k8splatform.manifests;

import com.manas.kafkaplatform.k8splatform.ClusterRequest;

import java.util.Base64;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Renders the Kubernetes manifests a {@link ClusterRequest} turns into: a Namespace, a Secret
 * holding the TLS keystore, a StatefulSet running the declared number of Kafka brokers in KRaft
 * combined mode with a plaintext listener (used for inter-broker and the controller quorum) and
 * a TLS listener, a headless Service for pod-to-pod discovery, and a ClusterIP bootstrap Service.
 * Every rendered deployment carries the TLS listener; there is no plaintext-only variant, because
 * the claim this module exists to answer ("provisions ... TLS listeners without a ticket") is
 * about every self-served cluster getting one, not an optional extra.
 *
 * <p>Pure string building, no cluster access; {@link com.manas.kafkaplatform.k8splatform.k8s.KubectlClient}
 * is what actually applies what this class renders.
 */
public final class K8sManifestRenderer {

    // A fixed, valid KRaft cluster id: base64url (no padding) of a 16-byte UUID, the exact
    // format kafka-storage.sh's own "random-uuid" generates and "format" requires. Fixed
    // rather than random so every rendered manifest for this module is reproducible.
    private static final String CLUSTER_ID = "SxscfjovTl2cah8uPUxbag";

    private K8sManifestRenderer() {
    }

    public static String renderNamespace(ClusterRequest request) {
        return """
                apiVersion: v1
                kind: Namespace
                metadata:
                  name: %s
                """.formatted(request.namespace());
    }

    public static String renderTlsSecret(ClusterRequest request, byte[] keystoreBytes, String keystorePassword, byte[] caCertBytes) {
        String keystoreBase64 = Base64.getEncoder().encodeToString(keystoreBytes);
        String caCertBase64 = Base64.getEncoder().encodeToString(caCertBytes);
        return """
                apiVersion: v1
                kind: Secret
                metadata:
                  name: %s-tls
                  namespace: %s
                type: Opaque
                data:
                  kafka.keystore.p12: %s
                  ca.crt: %s
                stringData:
                  keystore.password: "%s"
                """.formatted(request.clusterName(), request.namespace(), keystoreBase64, caCertBase64, keystorePassword);
    }

    public static String renderHeadlessService(ClusterRequest request) {
        return """
                apiVersion: v1
                kind: Service
                metadata:
                  name: %s-headless
                  namespace: %s
                spec:
                  clusterIP: None
                  selector:
                    app: %s
                  ports:
                    - name: plaintext
                      port: 9092
                    - name: controller
                      port: 9093
                    - name: ssl
                      port: 9094
                """.formatted(request.clusterName(), request.namespace(), request.clusterName());
    }

    public static String renderBootstrapService(ClusterRequest request) {
        return """
                apiVersion: v1
                kind: Service
                metadata:
                  name: %s-bootstrap
                  namespace: %s
                spec:
                  selector:
                    app: %s
                  ports:
                    - name: plaintext
                      port: 9092
                    - name: ssl
                      port: 9094
                """.formatted(request.clusterName(), request.namespace(), request.clusterName());
    }

    public static String renderStatefulSet(ClusterRequest request) {
        String fqdnSuffix = "%s-headless.%s.svc.cluster.local".formatted(request.clusterName(), request.namespace());
        String quorumVoters = IntStream.range(0, request.brokers())
                .mapToObj(i -> (i + 1) + "@%s-%d.%s:9093".formatted(request.clusterName(), i, fqdnSuffix))
                .collect(Collectors.joining(","));

        return """
                apiVersion: apps/v1
                kind: StatefulSet
                metadata:
                  name: %1$s
                  namespace: %2$s
                spec:
                  serviceName: %1$s-headless
                  replicas: %3$d
                  selector:
                    matchLabels:
                      app: %1$s
                  template:
                    metadata:
                      labels:
                        app: %1$s
                    spec:
                      volumes:
                        - name: tls-secret
                          secret:
                            secretName: %1$s-tls
                      containers:
                        - name: kafka
                          image: %4$s
                          command: ["/bin/bash", "-c"]
                          args:
                            - |
                              export KAFKA_NODE_ID=$(( $(hostname | grep -o '[0-9]*$') + 1 ))
                              export KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://$(hostname).%5$s:9092,SSL://$(hostname).%5$s:9094"
                              exec /etc/kafka/docker/run
                          ports:
                            - containerPort: 9092
                            - containerPort: 9093
                            - containerPort: 9094
                          resources:
                            requests:
                              cpu: "100m"
                              memory: "640Mi"
                            limits:
                              cpu: "500m"
                              memory: "768Mi"
                          volumeMounts:
                            - name: tls-secret
                              mountPath: /etc/kafka/secrets
                              readOnly: true
                          env:
                            - name: KAFKA_HEAP_OPTS
                              value: "-Xmx512m -Xms512m"
                            - name: KAFKA_PROCESS_ROLES
                              value: "broker,controller"
                            - name: KAFKA_LISTENERS
                              value: "PLAINTEXT://:9092,CONTROLLER://:9093,SSL://:9094"
                            - name: KAFKA_LISTENER_SECURITY_PROTOCOL_MAP
                              value: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL"
                            - name: KAFKA_CONTROLLER_LISTENER_NAMES
                              value: "CONTROLLER"
                            - name: KAFKA_INTER_BROKER_LISTENER_NAME
                              value: "PLAINTEXT"
                            - name: KAFKA_CONTROLLER_QUORUM_VOTERS
                              value: "%6$s"
                            - name: CLUSTER_ID
                              value: "%7$s"
                            - name: KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR
                              value: "%8$d"
                            - name: KAFKA_SSL_KEYSTORE_LOCATION
                              value: /etc/kafka/secrets/kafka.keystore.p12
                            - name: KAFKA_SSL_KEYSTORE_TYPE
                              value: PKCS12
                            - name: KAFKA_SSL_CLIENT_AUTH
                              value: none
                            - name: KAFKA_SSL_KEYSTORE_PASSWORD
                              valueFrom:
                                secretKeyRef:
                                  name: %1$s-tls
                                  key: keystore.password
                            - name: KAFKA_SSL_KEY_PASSWORD
                              valueFrom:
                                secretKeyRef:
                                  name: %1$s-tls
                                  key: keystore.password
                """.formatted(
                request.clusterName(), request.namespace(), request.brokers(), request.image(),
                fqdnSuffix, quorumVoters, CLUSTER_ID, Math.min(request.brokers(), 3)
        );
    }
}
