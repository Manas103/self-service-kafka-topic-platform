package com.mfs.serviceplatform.canary;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ReadsMetricsTest {

    @Test
    void p99OfKnownValuesIsTheExpectedPercentile() {
        ReadsMetrics m = new ReadsMetrics();
        for (int i = 1; i <= 100; i++) {
            m.onRequestStart();
            m.onRequestEnd(i, false);
        }
        assertEquals(99.0, m.p99DurationMillis(), 0.01);
    }

    @Test
    void errorsAreCountedOnlyWhenFlagged() {
        ReadsMetrics m = new ReadsMetrics();
        m.onRequestStart();
        m.onRequestEnd(5, true);
        m.onRequestStart();
        m.onRequestEnd(5, false);
        assertEquals(2, m.requestsTotal());
        assertEquals(1, m.errorsTotal());
    }

    @Test
    void resetWindowClearsCountersButNotInflight() {
        ReadsMetrics m = new ReadsMetrics();
        m.onRequestStart(); // never finished: simulates a request still in flight at reset time
        m.onRequestStart();
        m.onRequestEnd(5, true);

        m.resetWindow();

        assertEquals(0, m.requestsTotal());
        assertEquals(0, m.errorsTotal());
        assertEquals(1, m.inflight(), "a request still open at reset time must not be dropped from inflight");
    }

    @Test
    void drainInflightWaitsForReleaseThenReturnsTrue() throws InterruptedException {
        ReadsMetrics m = new ReadsMetrics();
        m.onRequestStart();
        Thread releaser = new Thread(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
            }
            m.releaseInflight();
        });
        releaser.start();

        boolean drained = m.drainInflight(2000);
        assertTrue(drained);
        assertEquals(0, m.inflight());
        releaser.join();
    }

    @Test
    void drainInflightTimesOutIfNeverReleased() {
        ReadsMetrics m = new ReadsMetrics();
        m.onRequestStart();
        assertFalse(m.drainInflight(100));
    }
}
