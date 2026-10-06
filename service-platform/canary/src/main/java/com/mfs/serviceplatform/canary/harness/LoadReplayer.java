package com.mfs.serviceplatform.canary.harness;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Replays {@link LoadProfile#FIXED} against /work, measuring rate and availability itself from
 * what it directly observed (the same reasoning release-health-gate-service-metrics uses: a
 * client should not trust a server-reported success count a broken server could misreport), and
 * separately polls /metrics during the run to capture peak saturation and the server-side error
 * and duration-p99 figures that depend on server-internal state a client cannot see directly.
 */
public final class LoadReplayer {

    private final String baseUrl;
    private final AdminClient admin;
    // Shared, and created once per harness run (not once per LoadReplayer.run() call): reusing
    // one HttpClient lets HTTP/1.1 keep-alive connections carry over between runs instead of
    // every one of the 100 runs in RegressionMatrixRunner opening its own fresh ~90 connections
    // from scratch. The first version created a new HttpClient per run() call; across 100 runs
    // that is up to 9,000 short-lived loopback connections in a short window, which measurably
    // caused clean runs late in the matrix to see transient client-side connection failures
    // (availability as low as 0.90 with 0 server-side errors, see README Findings) that were not
    // present when the same clean run was executed in isolation. Reusing the client removed them.
    private final HttpClient http = HttpClient.newHttpClient();

    public LoadReplayer(String baseUrl) {
        this.baseUrl = baseUrl;
        this.admin = new AdminClient(baseUrl);
    }

    public Gate.RunMeasurement run(LoadProfile profile) throws Exception {
        admin.reset();

        AtomicInteger succeeded = new AtomicInteger();
        AtomicLong maxInflightObserved = new AtomicLong();
        ExecutorService pool = Executors.newFixedThreadPool(profile.concurrency());

        ScheduledExecutorService poller = Executors.newSingleThreadScheduledExecutor();
        ScheduledFuture<?> pollTask = poller.scheduleAtFixedRate(() -> {
            try {
                Map<String, Double> m = MetricsScraper.parse(admin.scrapeMetrics());
                double inflight = m.getOrDefault("inflight", 0.0);
                maxInflightObserved.getAndUpdate(cur -> Math.max(cur, (long) inflight));
            } catch (Exception ignored) {
                // a transient scrape failure mid-run does not invalidate the run
            }
        }, 0, 10, TimeUnit.MILLISECONDS);

        long start = System.nanoTime();
        var futures = new java.util.ArrayList<Future<?>>();
        for (int i = 0; i < profile.requestCount(); i++) {
            futures.add(pool.submit(() -> {
                try {
                    HttpRequest req = HttpRequest.newBuilder(URI.create(baseUrl + "/work"))
                            .timeout(Duration.ofMillis(profile.perRequestTimeoutMillis())).GET().build();
                    HttpResponse<Void> resp = http.send(req, HttpResponse.BodyHandlers.discarding());
                    if (resp.statusCode() == 200) {
                        succeeded.incrementAndGet();
                    }
                } catch (Exception e) {
                    // timeout or connection error: counts as unavailable, not incremented
                }
            }));
        }
        for (Future<?> f : futures) {
            f.get();
        }
        double elapsedSeconds = (System.nanoTime() - start) / 1_000_000_000.0;

        pollTask.cancel(false);
        poller.shutdown();
        pool.shutdown();

        Map<String, Double> finalMetrics = MetricsScraper.parse(admin.scrapeMetrics());
        double requestsTotal = finalMetrics.getOrDefault("requests_total", 0.0);
        double errorsTotal = finalMetrics.getOrDefault("errors_total", 0.0);
        double durationP99 = finalMetrics.getOrDefault("duration_p99_ms", 0.0);

        double ratePerSec = profile.requestCount() / elapsedSeconds;
        double availability = succeeded.get() / (double) profile.requestCount();
        double errorRate = requestsTotal == 0 ? 0 : errorsTotal / requestsTotal;

        return new Gate.RunMeasurement(ratePerSec, errorRate, availability, durationP99, maxInflightObserved.get());
    }
}
