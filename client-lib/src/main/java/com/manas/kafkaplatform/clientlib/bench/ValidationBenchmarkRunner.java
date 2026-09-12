package com.manas.kafkaplatform.clientlib.bench;

import com.manas.kafkaplatform.clientlib.ConfigKind;
import com.manas.kafkaplatform.clientlib.ConfigValidationException;
import com.manas.kafkaplatform.clientlib.KafkaClientConfigValidator;
import com.manas.kafkaplatform.clientlib.SeededCase;
import com.manas.kafkaplatform.clientlib.SeededCases;
import com.manas.kafkaplatform.clientlib.ValidConfigGenerator;

import java.util.List;
import java.util.Map;

/**
 * Prints a reproducible, human-readable run of either the 40 seeded misconfigurations or the
 * 200 generated valid configurations against {@link KafkaClientConfigValidator}. Raw stdout from
 * this class, redirected to a file, is what is committed under {@code docs/}. Run with:
 *
 * <pre>
 * mvn -q -pl client-lib exec:java -Dexec.mainClass=com.manas.kafkaplatform.clientlib.bench.ValidationBenchmarkRunner -Dexec.args=seeded
 * mvn -q -pl client-lib exec:java -Dexec.mainClass=com.manas.kafkaplatform.clientlib.bench.ValidationBenchmarkRunner -Dexec.args=valid
 * </pre>
 */
public final class ValidationBenchmarkRunner {

    private ValidationBenchmarkRunner() {
    }

    public static void main(String[] args) {
        String mode = args.length > 0 ? args[0] : "seeded";
        if (mode.equals("seeded")) {
            runSeeded();
        } else if (mode.equals("valid")) {
            runValid();
        } else {
            System.err.println("unknown mode: " + mode + " (expected 'seeded' or 'valid')");
            System.exit(2);
        }
    }

    private static void runSeeded() {
        List<SeededCase> cases = SeededCases.all();
        int rejectedNamingCorrectRule = 0;
        System.out.println("seeded misconfiguration run: " + cases.size() + " cases");
        System.out.println("case_id  kind      expected_rule                 outcome");
        for (SeededCase c : cases) {
            String outcome;
            try {
                validate(c.kind(), c.config());
                outcome = "NOT_REJECTED (unexpected: config was accepted)";
            } catch (ConfigValidationException e) {
                boolean correctRule = e.rule() == c.expectedRule();
                if (correctRule) {
                    rejectedNamingCorrectRule++;
                }
                outcome = "REJECTED rule=" + e.rule().ruleId() + (correctRule ? " (matches expected)" : " (MISMATCH, expected " + c.expectedRule().ruleId() + ")");
            }
            System.out.printf("%-8s %-9s %-29s %s%n", c.id(), c.kind(), c.expectedRule().ruleId(), outcome);
        }
        System.out.println();
        System.out.println("result: " + rejectedNamingCorrectRule + "/" + cases.size()
                + " rejected naming the correct failing rule");
    }

    private static void runValid() {
        List<Map<String, String>> producerConfigs = ValidConfigGenerator.generateProducerConfigs(100);
        List<Map<String, String>> consumerConfigs = ValidConfigGenerator.generateConsumerConfigs(100);
        int total = producerConfigs.size() + consumerConfigs.size();
        int falseRejections = 0;

        System.out.println("valid-configuration run: " + total + " configs (100 producer, 100 consumer)");
        for (int i = 0; i < producerConfigs.size(); i++) {
            Map<String, String> cfg = producerConfigs.get(i);
            try {
                KafkaClientConfigValidator.validateProducerConfig(cfg);
            } catch (ConfigValidationException e) {
                falseRejections++;
                System.out.println("FALSE REJECTION producer[" + i + "] client.id=" + cfg.get("client.id")
                        + " rule=" + e.rule().ruleId());
            }
        }
        for (int i = 0; i < consumerConfigs.size(); i++) {
            Map<String, String> cfg = consumerConfigs.get(i);
            try {
                KafkaClientConfigValidator.validateConsumerConfig(cfg);
            } catch (ConfigValidationException e) {
                falseRejections++;
                System.out.println("FALSE REJECTION consumer[" + i + "] group.id=" + cfg.get("group.id")
                        + " rule=" + e.rule().ruleId());
            }
        }
        System.out.println();
        System.out.println("result: " + falseRejections + "/" + total + " false rejections");
    }

    private static void validate(ConfigKind kind, Map<String, String> config) {
        if (kind == ConfigKind.PRODUCER) {
            KafkaClientConfigValidator.validateProducerConfig(config);
        } else {
            KafkaClientConfigValidator.validateConsumerConfig(config);
        }
    }
}
