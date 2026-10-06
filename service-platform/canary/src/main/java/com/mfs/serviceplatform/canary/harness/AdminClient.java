package com.mfs.serviceplatform.canary.harness;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/** Talks to the canary service's /admin surface: select a regression, reset the window. */
public final class AdminClient {
    private final HttpClient client = HttpClient.newHttpClient();
    private final String baseUrl;

    public AdminClient(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public void setRegression(int id) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(baseUrl + "/admin/regression/" + id))
                .timeout(Duration.ofSeconds(5)).POST(HttpRequest.BodyPublishers.noBody()).build();
        client.send(req, HttpResponse.BodyHandlers.discarding());
    }

    public void reset() throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(baseUrl + "/admin/reset"))
                .timeout(Duration.ofSeconds(10)).POST(HttpRequest.BodyPublishers.noBody()).build();
        client.send(req, HttpResponse.BodyHandlers.discarding());
    }

    public String scrapeMetrics() throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(baseUrl + "/metrics"))
                .timeout(Duration.ofSeconds(5)).GET().build();
        return client.send(req, HttpResponse.BodyHandlers.ofString()).body();
    }
}
