package com.mfs.serviceplatform.canary;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The five READS indicators (Rate, Errors, Availability, Duration, Saturation), backed by plain
 * atomics and a sample list, with no metrics-library dependency. Rate and availability are
 * reported by the load replayer itself from what it directly observed (see
 * {@code LoadReplayer}); this class is the server-side half: errors, duration and saturation,
 * the three indicators that depend on server-internal state a client cannot observe directly.
 */
public final class ReadsMetrics {

    private final AtomicLong requestsTotal = new AtomicLong();
    private final AtomicLong errorsTotal = new AtomicLong();
    private final AtomicInteger inflight = new AtomicInteger();
    private final List<Long> durationsMillis = new CopyOnWriteArrayList<>();

    public void onRequestStart() {
        requestsTotal.incrementAndGet();
        inflight.incrementAndGet();
    }

    /** Records the outcome of a finished request, but does not release its inflight slot;
     * call {@link #releaseInflight} separately once the slot should actually free up, which a
     * SATURATION regression deliberately delays past when the response was already sent. */
    public void onRequestFinish(long durationMillis, boolean wasError) {
        durationsMillis.add(durationMillis);
        if (wasError) {
            errorsTotal.incrementAndGet();
        }
    }

    public void releaseInflight() {
        inflight.decrementAndGet();
    }

    /** The normal (no saturation hold) path: record the outcome and release the slot together. */
    public void onRequestEnd(long durationMillis, boolean wasError) {
        onRequestFinish(durationMillis, wasError);
        releaseInflight();
    }

    public long requestsTotal() {
        return requestsTotal.get();
    }

    public long errorsTotal() {
        return errorsTotal.get();
    }

    public int inflight() {
        return inflight.get();
    }

    public double p99DurationMillis() {
        List<Long> snapshot = new ArrayList<>(durationsMillis);
        if (snapshot.isEmpty()) {
            return 0;
        }
        Collections.sort(snapshot);
        int idx = (int) Math.min(snapshot.size() - 1, Math.ceil(snapshot.size() * 0.99) - 1);
        return snapshot.get(Math.max(idx, 0));
    }

    /**
     * Resets request/error/duration counters for a new measurement window, but deliberately does
     * NOT reset inflight to zero: whatever is genuinely still in flight at reset time is carried
     * forward, so a request spanning the reset boundary is not silently dropped from the count.
     * See README Findings for why this matters and {@link #drainInflight} for the fix it needs.
     */
    public void resetWindow() {
        requestsTotal.set(0);
        errorsTotal.set(0);
        durationsMillis.clear();
    }

    /** Blocks (bounded) until inflight genuinely reaches zero, so a leaked slot from a prior
     * regression (e.g. SATURATION, which deliberately holds a slot open past its response)
     * cannot bleed into the next window's measurement. */
    public boolean drainInflight(long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (inflight.get() > 0 && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return inflight.get() == 0;
    }
}
