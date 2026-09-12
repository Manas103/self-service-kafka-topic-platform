package com.manas.kafkaplatform.controlplane.dashboard;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Renders the single Grafana dashboard JSON template (checked in once, at
 * {@code grafana/templates/topic-dashboard-template.json}) into one concrete dashboard file per
 * provisioned topic, by substituting {@code {{TOPIC_NAME}}}, {@code {{PARTITIONS}}} and
 * {@code {{RETENTION_MS}}} placeholders. No live Grafana server is involved anywhere in this
 * repo; a rendered file is the deliverable, and it is valid Grafana dashboard JSON that a human
 * could import by hand.
 */
@Component
public class DashboardTemplateRenderer {

    private final Path templatePath;

    public DashboardTemplateRenderer(
            @Value("${platform.grafana.template-path:../grafana/templates/topic-dashboard-template.json}") String templatePath) {
        this.templatePath = Path.of(templatePath);
    }

    public String render(String topicName, int partitions, long retentionMs) {
        String template = readTemplate();
        return template
                .replace("{{TOPIC_NAME}}", topicName)
                .replace("{{PARTITIONS}}", String.valueOf(partitions))
                .replace("{{RETENTION_MS}}", String.valueOf(retentionMs));
    }

    private String readTemplate() {
        try {
            return Files.readString(templatePath, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("could not read dashboard template at " + templatePath.toAbsolutePath(), e);
        }
    }
}
