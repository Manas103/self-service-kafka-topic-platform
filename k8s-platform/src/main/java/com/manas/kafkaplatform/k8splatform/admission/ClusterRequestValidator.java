package com.manas.kafkaplatform.k8splatform.admission;

import com.manas.kafkaplatform.k8splatform.ClusterRequest;

import java.util.regex.Pattern;

/**
 * Admission policy for a {@link ClusterRequest}: provisioning is rejected,
 * naming exactly one failing rule, before a single Kubernetes object is ever
 * rendered or applied (claim: "admission policy rejected all 40 seeded
 * misconfigurations" / "falsely rejected 0 of 200 valid requests"). Rules run
 * in a fixed declared order so the same request always fails on the same
 * rule, the same reference-oracle-friendly convention {@code client-lib}'s
 * {@code KafkaClientConfigValidator} uses.
 */
public final class ClusterRequestValidator {

    private static final Pattern K8S_NAME = Pattern.compile("^[a-z][a-z0-9-]{2,49}$");
    private static final Pattern TOPIC_NAME = Pattern.compile("^[a-zA-Z0-9._-]{1,249}$");
    private static final Pattern IMAGE_REFERENCE = Pattern.compile("^[a-zA-Z0-9/_.-]+:[a-zA-Z0-9_.-]+$");

    private ClusterRequestValidator() {
    }

    public static void validate(ClusterRequest request) {
        if (!K8S_NAME.matcher(request.clusterName()).matches()) {
            throw new AdmissionRejectedException(AdmissionRule.INVALID_CLUSTER_NAME, "clusterName=" + request.clusterName());
        }
        if (!K8S_NAME.matcher(request.namespace()).matches()) {
            throw new AdmissionRejectedException(AdmissionRule.INVALID_NAMESPACE, "namespace=" + request.namespace());
        }
        if (request.brokers() < 1 || request.brokers() > 7 || request.brokers() % 2 == 0) {
            throw new AdmissionRejectedException(AdmissionRule.INVALID_BROKER_COUNT, "brokers=" + request.brokers());
        }
        if (!TOPIC_NAME.matcher(request.topic()).matches()) {
            throw new AdmissionRejectedException(AdmissionRule.INVALID_TOPIC_NAME, "topic=" + request.topic());
        }
        if (request.partitions() < 1 || request.partitions() > 100) {
            throw new AdmissionRejectedException(AdmissionRule.INVALID_PARTITIONS, "partitions=" + request.partitions());
        }
        if (request.producerQuotaBytesPerSec() <= 0) {
            throw new AdmissionRejectedException(AdmissionRule.NEGATIVE_PRODUCER_QUOTA,
                    "producerQuotaBytesPerSec=" + request.producerQuotaBytesPerSec());
        }
        if (request.consumerQuotaBytesPerSec() <= 0) {
            throw new AdmissionRejectedException(AdmissionRule.NEGATIVE_CONSUMER_QUOTA,
                    "consumerQuotaBytesPerSec=" + request.consumerQuotaBytesPerSec());
        }
        if (!IMAGE_REFERENCE.matcher(request.image()).matches()) {
            throw new AdmissionRejectedException(AdmissionRule.INVALID_IMAGE_REFERENCE, "image=" + request.image());
        }
    }

    /** Returns whether a request passes, without throwing; used by the false-rejection benchmark. */
    public static boolean isValid(ClusterRequest request) {
        try {
            validate(request);
            return true;
        } catch (AdmissionRejectedException e) {
            return false;
        }
    }
}
