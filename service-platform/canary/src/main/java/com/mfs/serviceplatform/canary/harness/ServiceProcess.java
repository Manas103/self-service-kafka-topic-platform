package com.mfs.serviceplatform.canary.harness;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Starts the canary service jar as a child process bound to port 0, reads the OS-assigned port
 * off its own stdout, and holds the Process handle so this same harness run is the only thing
 * that stops it, by the PID it holds.
 */
public final class ServiceProcess implements AutoCloseable {

    private static final Pattern PORT_PATTERN = Pattern.compile("listening on port (\\d+)");

    private final Process process;
    private final String baseUrl;

    private ServiceProcess(Process process, String baseUrl) {
        this.process = process;
        this.baseUrl = baseUrl;
    }

    public static ServiceProcess start(Path canaryJar) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("java", "-jar", canaryJar.toAbsolutePath().toString(), "0");
        pb.redirectErrorStream(true);
        Process process = pb.start();

        AtomicReference<Integer> port = new AtomicReference<>();
        Thread logPump = new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    Matcher m = PORT_PATTERN.matcher(line);
                    if (m.find()) {
                        port.set(Integer.parseInt(m.group(1)));
                    }
                }
            } catch (IOException ignored) {
                // stream closed when the process exits, expected
            }
        }, "canary-service-log-pump");
        logPump.setDaemon(true);
        logPump.start();

        long deadline = System.currentTimeMillis() + 30_000;
        while (port.get() == null && System.currentTimeMillis() < deadline) {
            if (!process.isAlive()) {
                throw new IllegalStateException("canary service process exited before it started listening");
            }
            Thread.sleep(50);
        }
        if (port.get() == null) {
            process.destroyForcibly();
            throw new IllegalStateException("canary service did not report a listening port within 30s");
        }

        String baseUrl = "http://127.0.0.1:" + port.get();
        waitForHealth(baseUrl);
        return new ServiceProcess(process, baseUrl);
    }

    private static void waitForHealth(String baseUrl) throws InterruptedException {
        HttpClient client = HttpClient.newHttpClient();
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            try {
                HttpRequest req = HttpRequest.newBuilder(URI.create(baseUrl + "/metrics"))
                        .timeout(Duration.ofSeconds(2)).GET().build();
                HttpResponse<Void> resp = client.send(req, HttpResponse.BodyHandlers.discarding());
                if (resp.statusCode() == 200) {
                    return;
                }
            } catch (Exception ignored) {
                // not up yet
            }
            Thread.sleep(50);
        }
        throw new IllegalStateException("canary service never became healthy");
    }

    public String baseUrl() {
        return baseUrl;
    }

    public long pid() {
        return process.pid();
    }

    @Override
    public void close() {
        if (process.isAlive()) {
            process.destroy();
            try {
                if (!process.waitFor(10, TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                }
            } catch (InterruptedException e) {
                process.destroyForcibly();
                Thread.currentThread().interrupt();
            }
        }
    }
}
