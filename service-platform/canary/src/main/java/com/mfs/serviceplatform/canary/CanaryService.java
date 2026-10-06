package com.mfs.serviceplatform.canary;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Executors;

/**
 * A small, dependency-free (plain {@code com.sun.net.httpserver.HttpServer}) HTTP service whose
 * one real endpoint, {@code /work}, does a fixed amount of real work plus whichever of the 40
 * seeded regressions is active, selected through {@code /admin/regression/{id}}. {@code /metrics}
 * exports the five READS indicators as plain text, scraped by {@code MetricsScraper}.
 */
public final class CanaryService {

    private static final long BASE_WORK_MILLIS = 5;

    private final ReadsMetrics metrics = new ReadsMetrics();
    private final RegressionState regression = new RegressionState();
    private final Random rng = new Random();
    private HttpServer server;

    public int start(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        server.setExecutor(Executors.newFixedThreadPool(64));
        server.createContext("/work", this::handleWork);
        server.createContext("/metrics", this::handleMetrics);
        server.createContext("/admin/regression", this::handleSetRegression);
        server.createContext("/admin/reset", this::handleReset);
        server.start();
        return server.getAddress().getPort();
    }

    public void stop() {
        server.stop(0);
    }

    private void handleWork(HttpExchange ex) throws IOException {
        long start = System.currentTimeMillis();
        metrics.onRequestStart();
        // Everything from here on must releaseInflight() exactly once, even if the client
        // disconnects or times out mid-response and sendResponseHeaders/write throws: the first
        // measured run of this gate found max_inflight pinned around 144-152 on every clean run
        // (budget 30), root-caused to exactly this path leaking a slot per failed write with no
        // finally, which never self-heals because resetWindow() deliberately does not zero
        // inflight (see its own Javadoc) and a leaked slot just carries forward into every later
        // run, accumulating across all 40 regressions before the clean runs were even measured.
        try {
            RegressionCatalog.Spec spec = regression.active();

            sleepQuietly(BASE_WORK_MILLIS + spec.latencyExtraMillis());

            int status = 200;
            double roll = rng.nextDouble();
            if (roll < spec.errorProbability()) {
                status = 500;
            } else if (roll < spec.errorProbability() + spec.unavailableProbability()) {
                status = 503;
            } else if (roll < spec.errorProbability() + spec.unavailableProbability() + spec.rateLimitProbability()) {
                status = 429;
            }

            byte[] body = ("status=" + status).getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(status, body.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(body);
            }

            long duration = System.currentTimeMillis() - start;
            metrics.onRequestFinish(duration, status >= 400);

            long hold = spec.saturationHoldMillis();
            if (hold > 0) {
                sleepQuietly(hold);
            }
        } finally {
            metrics.releaseInflight();
        }
    }

    private void handleMetrics(HttpExchange ex) throws IOException {
        String body = String.format(Locale.ROOT,
                "requests_total %d%nerrors_total %d%nduration_p99_ms %.1f%ninflight %d%n",
                metrics.requestsTotal(), metrics.errorsTotal(), metrics.p99DurationMillis(), metrics.inflight());
        byte[] data = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "text/plain");
        ex.sendResponseHeaders(200, data.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(data);
        }
    }

    private void handleSetRegression(HttpExchange ex) throws IOException {
        String path = ex.getRequestURI().getPath();
        int id = Integer.parseInt(path.substring(path.lastIndexOf('/') + 1));
        regression.set(id);
        ex.sendResponseHeaders(200, -1);
    }

    private void handleReset(HttpExchange ex) throws IOException {
        boolean drained = metrics.drainInflight(5000);
        metrics.resetWindow();
        ex.sendResponseHeaders(drained ? 200 : 500, -1);
    }

    private static void sleepQuietly(long millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public ReadsMetrics metricsForTest() {
        return metrics;
    }
}
