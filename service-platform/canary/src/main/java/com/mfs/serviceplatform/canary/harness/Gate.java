package com.mfs.serviceplatform.canary.harness;

import java.util.ArrayList;
import java.util.List;

/**
 * The budget, the measurement it is checked against, and the pure-function gate itself, in one
 * file: {@link #evaluate} takes numbers in and returns a decision out, no I/O, so every indicator
 * crossing is independently unit-testable without starting a service or replaying load.
 */
public final class Gate {

    public enum Indicator { RATE, ERRORS, AVAILABILITY, DURATION_P99, SATURATION }

    /** Per-indicator budget. A run blocks if any measured value crosses its threshold. */
    public record Budget(double minRatePerSec, double maxErrorRate, double minAvailability,
                          double maxDurationP99Millis, double maxInflight) {
        /** Calibrated against this project's own measured clean-run baseline; see README Findings. */
        public static final Budget DEFAULT = new Budget(80.0, 0.02, 0.98, 60.0, 30.0);
    }

    public record RunMeasurement(double ratePerSec, double errorRate, double availability,
                                  double durationP99Millis, double maxInflightObserved) {
    }

    public record GateResult(boolean blocked, List<Indicator> movedIndicators) {
        public static GateResult pass() {
            return new GateResult(false, List.of());
        }
    }

    private Gate() {
    }

    public static GateResult evaluate(RunMeasurement m, Budget b) {
        List<Indicator> moved = new ArrayList<>();
        if (m.ratePerSec() < b.minRatePerSec()) {
            moved.add(Indicator.RATE);
        }
        if (m.errorRate() > b.maxErrorRate()) {
            moved.add(Indicator.ERRORS);
        }
        if (m.availability() < b.minAvailability()) {
            moved.add(Indicator.AVAILABILITY);
        }
        if (m.durationP99Millis() > b.maxDurationP99Millis()) {
            moved.add(Indicator.DURATION_P99);
        }
        if (m.maxInflightObserved() > b.maxInflight()) {
            moved.add(Indicator.SATURATION);
        }
        return moved.isEmpty() ? GateResult.pass() : new GateResult(true, moved);
    }
}
