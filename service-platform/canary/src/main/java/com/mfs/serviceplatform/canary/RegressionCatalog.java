package com.mfs.serviceplatform.canary;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 41 specs: id 0 is clean (no regression active), ids 1-40 are 5 categories x 8 severities each,
 * ascending. Mirrors the shape of this portfolio's release-health-gate-service-metrics sibling
 * (same 5-category, 8-severity design), reimplemented fresh here with this project's own
 * dependency-free canary service and its own calibrated budget; see README Sibling comparison.
 */
public final class RegressionCatalog {

    public enum Category { LATENCY, ERROR_RATE, AVAILABILITY, SATURATION, RATE_LIMIT }

    public record Spec(int id, Category category, int severity) {
        /** Extra sleep added to every /work response, milliseconds. */
        public long latencyExtraMillis() {
            return category == Category.LATENCY ? severity * 8L : 0;
        }

        /** Probability [0,1] a request returns 500 (a business-logic error). */
        public double errorProbability() {
            return category == Category.ERROR_RATE ? severity * 0.015 : 0;
        }

        /** Probability [0,1] a request returns 503 (the dependency this service calls is down). */
        public double unavailableProbability() {
            return category == Category.AVAILABILITY ? severity * 0.015 : 0;
        }

        /** Probability [0,1] a request returns 429 (rate-limited upstream). */
        public double rateLimitProbability() {
            return category == Category.RATE_LIMIT ? severity * 0.08 : 0;
        }

        /** Extra time the in-flight slot is held open AFTER the response is sent, milliseconds. */
        public long saturationHoldMillis() {
            return category == Category.SATURATION ? severity * 250L : 0;
        }
    }

    private static final Map<Integer, Spec> BY_ID = new LinkedHashMap<>();

    static {
        BY_ID.put(0, new Spec(0, null, 0));
        int id = 1;
        for (Category cat : Category.values()) {
            for (int severity = 1; severity <= 8; severity++) {
                BY_ID.put(id, new Spec(id, cat, severity));
                id++;
            }
        }
        if (BY_ID.size() != 41) {
            throw new IllegalStateException("expected 41 specs (1 clean + 40 regressions), found " + BY_ID.size());
        }
    }

    private RegressionCatalog() {
    }

    public static Spec byId(int id) {
        Spec spec = BY_ID.get(id);
        if (spec == null) {
            throw new IllegalArgumentException("unknown regression id " + id);
        }
        return spec;
    }

    public static int count() {
        return BY_ID.size();
    }
}
