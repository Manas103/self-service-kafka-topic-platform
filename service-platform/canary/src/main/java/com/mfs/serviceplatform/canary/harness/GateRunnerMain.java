package com.mfs.serviceplatform.canary.harness;

/**
 * One real gate run: starts a fresh canary service, selects a regression (0 = clean), replays
 * the fixed load profile once, evaluates the gate, prints the result, exits 0 (pass) or 1
 * (blocked). This is what {@code provisionctl} invokes before declaring a rollout complete.
 * Usage: GateRunnerMain &lt;canaryJarPath&gt; &lt;regressionId&gt;
 */
public final class GateRunnerMain {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("usage: GateRunnerMain <canaryJarPath> <regressionId>");
            System.exit(2);
        }
        java.nio.file.Path jar = java.nio.file.Path.of(args[0]);
        int regressionId = Integer.parseInt(args[1]);

        boolean blocked;
        try (ServiceProcess service = ServiceProcess.start(jar)) {
            AdminClient admin = new AdminClient(service.baseUrl());
            admin.setRegression(regressionId);

            LoadReplayer replayer = new LoadReplayer(service.baseUrl());
            Gate.RunMeasurement measurement = replayer.run(LoadProfile.FIXED);
            Gate.GateResult result = Gate.evaluate(measurement, Gate.Budget.DEFAULT);

            System.out.printf("regression=%d rate=%.1f/s errorRate=%.3f availability=%.3f p99=%.1fms maxInflight=%.1f%n",
                    regressionId, measurement.ratePerSec(), measurement.errorRate(), measurement.availability(),
                    measurement.durationP99Millis(), measurement.maxInflightObserved());

            blocked = result.blocked();
            System.out.println(blocked ? "GATE BLOCKED: " + result.movedIndicators() : "GATE PASSED");
        }
        // System.exit is outside the try-with-resources block on purpose: calling it inside
        // short-circuits the JVM before ServiceProcess.close() runs, which is exactly how the
        // very first version of this class leaked its child canary-service processes.
        System.exit(blocked ? 1 : 0);
    }
}
