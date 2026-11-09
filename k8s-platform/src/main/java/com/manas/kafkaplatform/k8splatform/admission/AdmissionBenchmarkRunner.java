package com.manas.kafkaplatform.k8splatform.admission;

import com.manas.kafkaplatform.k8splatform.ClusterRequest;

import java.util.List;

/**
 * Prints a reproducible, human-readable run of either the 40 seeded misconfigurations or the
 * 200 generated valid requests against {@link ClusterRequestValidator}. Raw stdout from this
 * class, redirected to a file, is what is committed under {@code docs/}. Run with:
 *
 * <pre>
 * mvn -q -pl k8s-platform exec:java -Dexec.mainClass=com.manas.kafkaplatform.k8splatform.admission.AdmissionBenchmarkRunner -Dexec.args=seeded
 * mvn -q -pl k8s-platform exec:java -Dexec.mainClass=com.manas.kafkaplatform.k8splatform.admission.AdmissionBenchmarkRunner -Dexec.args=valid
 * </pre>
 */
public final class AdmissionBenchmarkRunner {

    private AdmissionBenchmarkRunner() {
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
        System.out.println("case_id  expected_rule                  outcome");
        for (SeededCase c : cases) {
            String outcome;
            try {
                ClusterRequestValidator.validate(c.request());
                outcome = "NOT_REJECTED (unexpected: request was accepted)";
            } catch (AdmissionRejectedException e) {
                boolean correctRule = e.rule() == c.expectedRule();
                if (correctRule) {
                    rejectedNamingCorrectRule++;
                }
                outcome = "REJECTED rule=" + e.rule().ruleId() + (correctRule ? " (matches expected)" : " (MISMATCH, expected " + c.expectedRule().ruleId() + ")");
            }
            System.out.printf("%-8s %-30s %s%n", c.id(), c.expectedRule().ruleId(), outcome);
        }
        System.out.println();
        System.out.println("result: " + rejectedNamingCorrectRule + "/" + cases.size()
                + " rejected naming the correct failing rule");
    }

    private static void runValid() {
        List<ClusterRequest> requests = ValidRequestGenerator.generate(200);
        int falseRejections = 0;

        System.out.println("valid-request run: " + requests.size() + " requests");
        for (int i = 0; i < requests.size(); i++) {
            ClusterRequest request = requests.get(i);
            try {
                ClusterRequestValidator.validate(request);
            } catch (AdmissionRejectedException e) {
                falseRejections++;
                System.out.println("FALSE REJECTION request[" + i + "] clusterName=" + request.clusterName()
                        + " rule=" + e.rule().ruleId());
            }
        }
        System.out.println();
        System.out.println("result: " + falseRejections + "/" + requests.size() + " false rejections");
    }
}
