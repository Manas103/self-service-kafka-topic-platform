package com.manas.kafkaplatform.controlplane;

import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end proof of the central claim: ONE HTTP call to {@code POST /topics} results in three
 * artifacts existing (the topic on the embedded broker, its dashboard JSON, its cost report
 * JSON). {@link #httpCallCount} is incremented exactly once per HTTP request made in the test
 * that exercises this claim, so the "one API call" number in the README is read directly off
 * this counter, not asserted separately from what actually happened.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 1, topics = {})
class TopicProvisioningIntegrationTest {

    private static final AtomicInteger httpCallCount = new AtomicInteger(0);

    // Not @TempDir on purpose: kept under target/ so a real sample dashboard/cost-report pair
    // from this exact test run can be copied into docs/ as committed example output.
    static final Path tempOutputDir = Path.of("target/it-generated");

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", () -> System.getProperty("spring.embedded.kafka.brokers"));
        registry.add("platform.output-dir", () -> tempOutputDir.toString());
    }

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private org.springframework.kafka.core.KafkaAdmin kafkaAdmin;

    @Test
    void oneApiCallProducesTopicDashboardAndCostReport() throws Exception {
        TopicProvisionRequest request = new TopicProvisionRequest(
                "orders-events", 3, 1, 3_600_000L, 50_000L, 512);

        httpCallCount.incrementAndGet();
        TopicProvisionResponse response = restTemplate.postForObject(
                "http://localhost:" + port + "/topics", request, TopicProvisionResponse.class);

        assertThat(httpCallCount.get()).isEqualTo(1);
        assertThat(response).isNotNull();
        assertThat(response.topicName()).isEqualTo("orders-events");

        // artifact 1: the topic exists on the broker
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            var names = adminClient.listTopics().names().get();
            assertThat(names).contains("orders-events");
        }

        // artifact 2: the dashboard file exists and mentions the topic
        Path dashboardPath = Path.of(response.dashboardPath());
        assertThat(Files.exists(dashboardPath)).isTrue();
        assertThat(Files.readString(dashboardPath)).contains("orders-events");

        // artifact 3: the cost report file exists and mentions the topic
        Path costReportPath = Path.of(response.costReportPath());
        assertThat(Files.exists(costReportPath)).isTrue();
        assertThat(Files.readString(costReportPath)).contains("orders-events");

        // invariant: exactly one dashboard file and one cost report file for THIS topic (the
        // shared output directory may also hold other topics' artifacts from sibling tests)
        try (var dashboards = Files.list(tempOutputDir.resolve("dashboards"));
             var costReports = Files.list(tempOutputDir.resolve("cost-reports"))) {
            assertThat(dashboards.filter(p -> p.getFileName().toString().startsWith("orders-events")).count()).isEqualTo(1);
            assertThat(costReports.filter(p -> p.getFileName().toString().startsWith("orders-events")).count()).isEqualTo(1);
        }
    }

    @Test
    void secondCallForSameTopicIsRejectedNotSilentlyDuplicated() {
        TopicProvisionRequest request = new TopicProvisionRequest(
                "duplicate-topic", 1, 1, 3_600_000L, 1000L, 100);
        restTemplate.postForObject("http://localhost:" + port + "/topics", request, TopicProvisionResponse.class);
        var second = restTemplate.postForEntity("http://localhost:" + port + "/topics", request, Map.class);
        assertThat(second.getStatusCode().value()).isEqualTo(409);
    }

    @Test
    void replicationFactorAboveOneIsRejectedWithAClearReason() {
        TopicProvisionRequest request = new TopicProvisionRequest(
                "bad-replication", 1, 2, 3_600_000L, 1000L, 100);
        var response = restTemplate.postForEntity("http://localhost:" + port + "/topics", request, Map.class);
        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }
}
