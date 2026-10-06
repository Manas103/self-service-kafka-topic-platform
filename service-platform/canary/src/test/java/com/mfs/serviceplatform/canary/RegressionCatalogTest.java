package com.mfs.serviceplatform.canary;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RegressionCatalogTest {

    @Test
    void has41SpecsTotal() {
        assertEquals(41, RegressionCatalog.count());
    }

    @Test
    void idZeroIsClean() {
        RegressionCatalog.Spec clean = RegressionCatalog.byId(0);
        assertNull(clean.category());
        assertEquals(0, clean.latencyExtraMillis());
        assertEquals(0, clean.errorProbability());
        assertEquals(0, clean.unavailableProbability());
        assertEquals(0, clean.rateLimitProbability());
        assertEquals(0, clean.saturationHoldMillis());
    }

    @Test
    void everyNonCleanIdResolvesToExactlyOneCategory() {
        for (int id = 1; id <= 40; id++) {
            RegressionCatalog.Spec spec = RegressionCatalog.byId(id);
            assertNotNull(spec.category(), "id " + id + " must have a category");
            assertTrue(spec.severity() >= 1 && spec.severity() <= 8, "id " + id + " severity out of range");
        }
    }

    @Test
    void unknownIdThrows() {
        assertThrows(IllegalArgumentException.class, () -> RegressionCatalog.byId(999));
    }

    @Test
    void onlyTheMatchingCategorysEffectIsNonZero() {
        RegressionCatalog.Spec latency = RegressionCatalog.byId(1); // LATENCY severity 1
        assertTrue(latency.latencyExtraMillis() > 0);
        assertEquals(0, latency.errorProbability());
        assertEquals(0, latency.unavailableProbability());
        assertEquals(0, latency.rateLimitProbability());
        assertEquals(0, latency.saturationHoldMillis());
    }
}
