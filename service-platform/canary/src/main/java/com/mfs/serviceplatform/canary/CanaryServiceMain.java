package com.mfs.serviceplatform.canary;

/**
 * Entry point for the canary service as a standalone process. Binds to an OS-assigned port
 * (0 unless overridden) and prints it on the line {@code ServiceProcess} parses.
 */
public final class CanaryServiceMain {
    public static void main(String[] args) throws Exception {
        int requestedPort = args.length > 0 ? Integer.parseInt(args[0]) : 0;
        CanaryService service = new CanaryService();
        int port = service.start(requestedPort);
        System.out.println("Canary service listening on port " + port);
    }
}
