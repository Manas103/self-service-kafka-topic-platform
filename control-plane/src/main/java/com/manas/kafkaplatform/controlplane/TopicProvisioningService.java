package com.manas.kafkaplatform.controlplane;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.manas.kafkaplatform.controlplane.cost.StorageCostEstimator;
import com.manas.kafkaplatform.controlplane.cost.StorageCostReport;
import com.manas.kafkaplatform.controlplane.dashboard.DashboardTemplateRenderer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

/**
 * The one call that provisions a topic: creates it on the broker, renders its default dashboard,
 * and writes its storage-cost report, all as a side effect of a single {@code provisionTopic}
 * invocation. See {@link com.manas.kafkaplatform.controlplane.web.TopicProvisioningController}
 * for the HTTP boundary.
 */
@Service
public class TopicProvisioningService {

    private static final Pattern TOPIC_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9._-]{1,249}$");

    private final KafkaAdmin kafkaAdmin;
    private final DashboardTemplateRenderer dashboardTemplateRenderer;
    private final StorageCostEstimator storageCostEstimator;
    private final ObjectMapper objectMapper;
    private final Path outputDir;

    public TopicProvisioningService(KafkaAdmin kafkaAdmin,
                                     DashboardTemplateRenderer dashboardTemplateRenderer,
                                     StorageCostEstimator storageCostEstimator,
                                     ObjectMapper objectMapper,
                                     @Value("${platform.output-dir:generated}") String outputDir) {
        this.kafkaAdmin = kafkaAdmin;
        this.dashboardTemplateRenderer = dashboardTemplateRenderer;
        this.storageCostEstimator = storageCostEstimator;
        this.objectMapper = objectMapper.copy().enable(com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT);
        this.outputDir = Path.of(outputDir);
    }

    public TopicProvisionResponse provisionTopic(TopicProvisionRequest request) {
        String topicName = request.topicName();
        if (topicName == null || !TOPIC_NAME_PATTERN.matcher(topicName).matches()) {
            throw new IllegalArgumentException("topicName must match " + TOPIC_NAME_PATTERN.pattern());
        }
        if (request.partitions() < 1) {
            throw new IllegalArgumentException("partitions must be >= 1");
        }
        if (request.replicationFactor() != 1) {
            // This platform runs a single-broker embedded Kafka; a single broker cannot host a
            // replication factor above 1. Rejecting explicitly here beats letting the broker
            // fail the create with a less legible error.
            throw new IllegalArgumentException(
                    "replicationFactor must be 1 in this single-broker embedded environment, got "
                            + request.replicationFactor());
        }

        createTopicOnBroker(topicName, request.partitions(), request.replicationFactor(), request.retentionMs());

        String dashboardJson = dashboardTemplateRenderer.render(topicName, request.partitions(), request.retentionMs());
        Path dashboardPath = writeFile(outputDir.resolve("dashboards").resolve(topicName + "-dashboard.json"), dashboardJson);

        StorageCostReport costReport = storageCostEstimator.estimate(
                topicName, request.partitions(), request.replicationFactor(), request.retentionMs(),
                request.estimatedMessagesPerDay(), request.estimatedAvgMessageSizeBytes());
        Path costReportPath = writeFile(outputDir.resolve("cost-reports").resolve(topicName + "-cost-report.json"),
                writeValueAsString(costReport));

        return new TopicProvisionResponse(
                topicName, request.partitions(), request.replicationFactor(), request.retentionMs(),
                dashboardPath.toString(), costReportPath.toString(),
                costReport.estimatedMonthlyCostUsd(), Instant.now());
    }

    private void createTopicOnBroker(String topicName, int partitions, int replicationFactor, long retentionMs) {
        NewTopic newTopic = new NewTopic(topicName, partitions, (short) replicationFactor)
                .configs(Map.of(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(retentionMs)));
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            adminClient.createTopics(List.of(newTopic)).all().get(30, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                throw new TopicAlreadyExistsException(topicName);
            }
            throw new IllegalStateException("failed to create topic " + topicName, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while creating topic " + topicName, e);
        } catch (TimeoutException e) {
            throw new IllegalStateException("timed out creating topic " + topicName, e);
        }
    }

    private Path writeFile(Path path, String content) {
        try {
            Files.createDirectories(path.getParent());
            Files.writeString(path, content);
            return path;
        } catch (IOException e) {
            throw new UncheckedIOException("could not write " + path.toAbsolutePath(), e);
        }
    }

    private String writeValueAsString(StorageCostReport report) {
        try {
            return objectMapper.writeValueAsString(report);
        } catch (Exception e) {
            throw new IllegalStateException("could not serialize storage cost report", e);
        }
    }
}
