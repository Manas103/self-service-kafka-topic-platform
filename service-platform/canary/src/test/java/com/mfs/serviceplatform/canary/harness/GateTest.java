package com.mfs.serviceplatform.canary.harness;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class GateTest {

    private static final Gate.Budget B = Gate.Budget.DEFAULT;

    @Test
    void cleanMeasurementPasses() {
        Gate.RunMeasurement m = new Gate.RunMeasurement(100, 0.0, 1.0, 20.0, 10.0);
        assertFalse(Gate.evaluate(m, B).blocked());
    }

    @Test
    void errorRateAboveBudgetBlocksAndNamesErrors() {
        Gate.RunMeasurement m = new Gate.RunMeasurement(100, 0.05, 1.0, 20.0, 10.0);
        Gate.GateResult r = Gate.evaluate(m, B);
        assertTrue(r.blocked());
        assertTrue(r.movedIndicators().contains(Gate.Indicator.ERRORS));
    }

    @Test
    void availabilityBelowBudgetBlocksAndNamesAvailability() {
        Gate.RunMeasurement m = new Gate.RunMeasurement(100, 0.0, 0.90, 20.0, 10.0);
        Gate.GateResult r = Gate.evaluate(m, B);
        assertTrue(r.blocked());
        assertTrue(r.movedIndicators().contains(Gate.Indicator.AVAILABILITY));
    }

    @Test
    void durationAboveBudgetBlocksAndNamesDuration() {
        Gate.RunMeasurement m = new Gate.RunMeasurement(100, 0.0, 1.0, 999.0, 10.0);
        Gate.GateResult r = Gate.evaluate(m, B);
        assertTrue(r.blocked());
        assertTrue(r.movedIndicators().contains(Gate.Indicator.DURATION_P99));
    }

    @Test
    void saturationAboveBudgetBlocksAndNamesSaturation() {
        Gate.RunMeasurement m = new Gate.RunMeasurement(100, 0.0, 1.0, 20.0, 999.0);
        Gate.GateResult r = Gate.evaluate(m, B);
        assertTrue(r.blocked());
        assertTrue(r.movedIndicators().contains(Gate.Indicator.SATURATION));
    }

    @Test
    void rateBelowBudgetBlocksAndNamesRate() {
        Gate.RunMeasurement m = new Gate.RunMeasurement(1.0, 0.0, 1.0, 20.0, 10.0);
        Gate.GateResult r = Gate.evaluate(m, B);
        assertTrue(r.blocked());
        assertTrue(r.movedIndicators().contains(Gate.Indicator.RATE));
    }

    @Test
    void multipleSimultaneousCrossingsAreAllNamed() {
        Gate.RunMeasurement m = new Gate.RunMeasurement(100, 0.05, 0.90, 20.0, 10.0);
        Gate.GateResult r = Gate.evaluate(m, B);
        assertTrue(r.blocked());
        assertEquals(2, r.movedIndicators().size());
    }

    @Test
    void valueExactlyAtBudgetBoundaryDoesNotBlock() {
        Gate.RunMeasurement m = new Gate.RunMeasurement(B.minRatePerSec(), B.maxErrorRate(),
                B.minAvailability(), B.maxDurationP99Millis(), B.maxInflight());
        assertFalse(Gate.evaluate(m, B).blocked(), "strictly below/above the budget is the rule, not at-or-beyond");
    }
}
