package com.manas.kafkaplatform.k8splatform.admission;

import com.manas.kafkaplatform.k8splatform.ClusterRequest;

/** One row of the reference-oracle table: a request expected to fail exactly one named rule. */
public record SeededCase(String id, ClusterRequest request, AdmissionRule expectedRule, String description) {
}
