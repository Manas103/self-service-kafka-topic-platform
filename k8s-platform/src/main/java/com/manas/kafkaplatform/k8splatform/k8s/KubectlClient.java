package com.manas.kafkaplatform.k8splatform.k8s;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * A thin wrapper around the real {@code kubectl} binary, run as a child process, rather than a
 * Kubernetes client SDK: the surface this control plane needs (apply, exec, delete, read a
 * handful of status fields) is small enough that a hand-written wrapper is honest about exactly
 * what it does, the same tradeoff {@code service-platform/controlplane}'s raw Docker Engine API
 * client makes in this same portfolio for a different API. Every method here runs a real
 * {@code kubectl} against whichever cluster the current kubeconfig context points at.
 */
public final class KubectlClient {

    public record CommandResult(int exitCode, String output) {
        public boolean ok() {
            return exitCode == 0;
        }
    }

    public CommandResult apply(String manifestYaml) {
        try {
            Path tmp = Files.createTempFile("k8s-platform-apply-", ".yaml");
            Files.writeString(tmp, manifestYaml, StandardCharsets.UTF_8);
            try {
                return run(List.of("kubectl", "apply", "-f", tmp.toString()));
            } finally {
                Files.deleteIfExists(tmp);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public CommandResult exec(String namespace, String pod, List<String> command) {
        List<String> full = new ArrayList<>(List.of("kubectl", "exec", "-n", namespace, pod, "--"));
        full.addAll(command);
        return run(full);
    }

    public CommandResult deletePod(String namespace, String pod) {
        return run(List.of("kubectl", "delete", "pod", "-n", namespace, pod, "--wait=false"));
    }

    public int readyReplicas(String namespace, String statefulSetName) {
        CommandResult result = run(List.of("kubectl", "get", "statefulset", "-n", namespace, statefulSetName,
                "-o", "jsonpath={.status.readyReplicas}"));
        if (!result.ok() || result.output().isBlank()) {
            return 0;
        }
        try {
            return Integer.parseInt(result.output().strip());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    /** Polls readyReplicas until it reaches {@code desired} or the timeout elapses. */
    public boolean waitForReady(String namespace, String statefulSetName, int desired, Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (readyReplicas(namespace, statefulSetName) >= desired) {
                return true;
            }
            sleep(Duration.ofSeconds(3));
        }
        return readyReplicas(namespace, statefulSetName) >= desired;
    }

    private void sleep(Duration d) {
        try {
            Thread.sleep(d.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private CommandResult run(List<String> command) {
        try {
            Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            int exit = process.waitFor();
            return new CommandResult(exit, output);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted running " + command, e);
        }
    }
}
