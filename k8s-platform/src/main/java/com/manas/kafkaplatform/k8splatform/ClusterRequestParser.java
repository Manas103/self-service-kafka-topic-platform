package com.manas.kafkaplatform.k8splatform;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Parses the plain {@code key: value} declarative request format (comments
 * with {@code #}, blank lines ignored) into a {@link ClusterRequest}.
 * Unknown keys are ignored rather than rejected here; {@link ClusterRequestValidator}
 * is where a request is accepted or rejected, this parser only reads it.
 */
public final class ClusterRequestParser {

    private ClusterRequestParser() {
    }

    public static ClusterRequest parse(String text) {
        Map<String, String> fields = new HashMap<>();
        for (String rawLine : text.lines().toList()) {
            String line = rawLine.strip();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            int colon = line.indexOf(':');
            if (colon < 0) {
                continue;
            }
            fields.put(line.substring(0, colon).strip(), line.substring(colon + 1).strip());
        }

        return new ClusterRequest(
                fields.getOrDefault("clusterName", ""),
                fields.getOrDefault("namespace", ""),
                parseIntOr(fields.get("brokers"), -1),
                fields.getOrDefault("topic", ""),
                parseIntOr(fields.get("partitions"), -1),
                parseLongOr(fields.get("retentionMs"), 86_400_000L),
                parseLongOr(fields.get("producerQuotaBytesPerSec"), -1L),
                parseLongOr(fields.get("consumerQuotaBytesPerSec"), -1L),
                fields.getOrDefault("image", "")
        );
    }

    public static ClusterRequest parseFile(Path path) {
        try {
            return parse(Files.readString(path));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Non-comment, non-blank lines, the measure the "20-line request" claim uses. */
    public static int countNonCommentLines(String text) {
        List<String> lines = text.lines().toList();
        int count = 0;
        for (String rawLine : lines) {
            String line = rawLine.strip();
            if (!line.isEmpty() && !line.startsWith("#")) {
                count++;
            }
        }
        return count;
    }

    private static int parseIntOr(String s, int fallback) {
        if (s == null) return fallback;
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static long parseLongOr(String s, long fallback) {
        if (s == null) return fallback;
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

}
