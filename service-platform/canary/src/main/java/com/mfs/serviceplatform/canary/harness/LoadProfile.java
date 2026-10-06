package com.mfs.serviceplatform.canary.harness;

/** One fixed load profile, reused identically for every regression and every clean run so every
 * comparison is apples to apples (same precedent as release-health-gate-service-metrics). */
public record LoadProfile(int requestCount, int concurrency, long perRequestTimeoutMillis) {
    public static final LoadProfile FIXED = new LoadProfile(90, 25, 800);
}
