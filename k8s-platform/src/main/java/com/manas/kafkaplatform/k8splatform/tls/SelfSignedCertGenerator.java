package com.manas.kafkaplatform.k8splatform.tls;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Generates a self-signed TLS key pair for a cluster's TLS listener, using the JDK's own
 * {@code keytool} (shipped with every JDK, no third-party dependency) rather than a Java
 * cryptography library: provisioning one cluster is a few seconds of process startup, not a
 * hot path, so the simplicity of shelling out to a tool every JDK already carries won this
 * tradeoff. Not a real certificate authority; one self-signed cert per cluster, the same cert
 * trusted directly as its own CA, which is exactly enough to prove the TLS listener itself
 * works, not to prove identity to an external party.
 */
public final class SelfSignedCertGenerator {

    public record TlsMaterial(byte[] keystoreBytes, String keystorePassword, byte[] caCertBytes) {
    }

    private SelfSignedCertGenerator() {
    }

    public static TlsMaterial generate(String commonName, String keystorePassword) {
        try {
            Path dir = Files.createTempDirectory("k8s-platform-tls-");
            Path keystorePath = dir.resolve("kafka.keystore.p12");
            Path certPath = dir.resolve("ca.crt");

            run(List.of("keytool", "-genkeypair",
                    "-keystore", keystorePath.toString(),
                    "-storetype", "PKCS12",
                    "-storepass", keystorePassword,
                    "-keypass", keystorePassword,
                    "-alias", "kafka",
                    "-dname", "CN=" + commonName + ",OU=k8s-platform,O=manas,L=local,ST=local,C=US",
                    "-keyalg", "RSA",
                    "-keysize", "2048",
                    "-validity", "3650"));

            run(List.of("keytool", "-exportcert", "-rfc",
                    "-keystore", keystorePath.toString(),
                    "-storepass", keystorePassword,
                    "-alias", "kafka",
                    "-file", certPath.toString()));

            byte[] keystoreBytes = Files.readAllBytes(keystorePath);
            byte[] caCertBytes = Files.readAllBytes(certPath);
            deleteQuietly(keystorePath);
            deleteQuietly(certPath);
            deleteQuietly(dir);

            return new TlsMaterial(keystoreBytes, keystorePassword, caCertBytes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void run(List<String> command) {
        try {
            Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
            String output = new String(process.getInputStream().readAllBytes());
            int exit = process.waitFor();
            if (exit != 0) {
                throw new IllegalStateException("command failed (" + exit + "): " + command + "\n" + output);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted running " + command, e);
        }
    }

    private static void deleteQuietly(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException ignored) {
            // best-effort cleanup of a temp file; not worth failing provisioning over
        }
    }
}
