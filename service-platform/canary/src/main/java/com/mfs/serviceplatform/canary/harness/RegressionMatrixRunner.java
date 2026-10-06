package com.mfs.serviceplatform.canary.harness;

import com.mfs.serviceplatform.canary.RegressionCatalog;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * The scale claim behind this project: 38 of 40 seeded regressions caught, 0 of 60 clean
 * releases falsely blocked. One long-lived canary service process, one regression selected at a
 * time via /admin, the fixed load profile replayed once per regression plus 60 times clean.
 * Usage: RegressionMatrixRunner &lt;canaryJarPath&gt; &lt;docsDir&gt;
 */
public final class RegressionMatrixRunner {

    private static final int CLEAN_RUNS = 60;

    public static void main(String[] args) throws Exception {
        Path jar = Path.of(args[0]);
        Path docsDir = Path.of(args[1]);
        Files.createDirectories(docsDir);

        try (ServiceProcess service = ServiceProcess.start(jar)) {
            AdminClient admin = new AdminClient(service.baseUrl());
            LoadReplayer replayer = new LoadReplayer(service.baseUrl());

            List<String> regressionRows = new ArrayList<>();
            regressionRows.add("regression_id,category,severity,rate,error_rate,availability,duration_p99_ms,max_inflight,blocked,moved_indicators");
            int caught = 0;
            for (int id = 1; id <= 40; id++) {
                RegressionCatalog.Spec spec = RegressionCatalog.byId(id);
                admin.setRegression(id);
                Gate.RunMeasurement m = replayer.run(LoadProfile.FIXED);
                Gate.GateResult result = Gate.evaluate(m, Gate.Budget.DEFAULT);
                if (result.blocked()) {
                    caught++;
                }
                regressionRows.add(String.format("%d,%s,%d,%.2f,%.4f,%.4f,%.2f,%.1f,%b,%s",
                        id, spec.category(), spec.severity(), m.ratePerSec(), m.errorRate(), m.availability(),
                        m.durationP99Millis(), m.maxInflightObserved(), result.blocked(), result.movedIndicators()));
            }

            List<String> cleanRows = new ArrayList<>();
            cleanRows.add("run,rate,error_rate,availability,duration_p99_ms,max_inflight,blocked");
            int falseBlocks = 0;
            admin.setRegression(0);
            for (int run = 1; run <= CLEAN_RUNS; run++) {
                Gate.RunMeasurement m = replayer.run(LoadProfile.FIXED);
                Gate.GateResult result = Gate.evaluate(m, Gate.Budget.DEFAULT);
                if (result.blocked()) {
                    falseBlocks++;
                }
                cleanRows.add(String.format("%d,%.2f,%.4f,%.4f,%.2f,%.1f,%b",
                        run, m.ratePerSec(), m.errorRate(), m.availability(), m.durationP99Millis(),
                        m.maxInflightObserved(), result.blocked()));
            }

            writeLines(docsDir.resolve("regression_matrix.csv"), regressionRows);
            writeLines(docsDir.resolve("clean_build_matrix.csv"), cleanRows);

            String summary = String.format(
                    "Canary gate regression matrix%n"
                    + "Seeded regressions caught: %d / 40%n"
                    + "Clean builds falsely blocked: %d / %d%n",
                    caught, falseBlocks, CLEAN_RUNS);
            Files.writeString(docsDir.resolve("regression_gate_summary.txt"), summary);
            System.out.println(summary);
        }
    }

    private static void writeLines(Path path, List<String> lines) throws Exception {
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(path))) {
            for (String line : lines) {
                w.println(line);
            }
        }
    }
}
