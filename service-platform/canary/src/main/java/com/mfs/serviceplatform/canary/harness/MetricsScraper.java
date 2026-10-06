package com.mfs.serviceplatform.canary.harness;

import java.util.HashMap;
import java.util.Map;

/** Parses the plain-text {@code key value} lines {@code CanaryService}'s /metrics emits. */
public final class MetricsScraper {
    private MetricsScraper() {
    }

    public static Map<String, Double> parse(String body) {
        Map<String, Double> out = new HashMap<>();
        for (String line : body.split("\n")) {
            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }
            int sp = line.indexOf(' ');
            if (sp < 0) {
                continue;
            }
            out.put(line.substring(0, sp), Double.parseDouble(line.substring(sp + 1)));
        }
        return out;
    }
}
