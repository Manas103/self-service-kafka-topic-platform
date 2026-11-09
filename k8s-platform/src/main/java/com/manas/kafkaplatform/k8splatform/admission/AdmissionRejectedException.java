package com.manas.kafkaplatform.k8splatform.admission;

/** Always carries the single {@link AdmissionRule} that rejected the request. */
public class AdmissionRejectedException extends RuntimeException {

    private final AdmissionRule rule;

    public AdmissionRejectedException(AdmissionRule rule, String detail) {
        super(rule.ruleId() + ": " + rule.message() + " (" + detail + ")");
        this.rule = rule;
    }

    public AdmissionRule rule() {
        return rule;
    }
}
