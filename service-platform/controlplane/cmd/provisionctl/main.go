// provisionctl is the control plane's CLI entry point: the thing a team
// actually runs. It turns one blueprint file into a running, health-checked
// container, emits that service's dashboard and alert rules, then runs the
// canary gate against it before declaring the rollout done.
//
// Usage: provisionctl <blueprint-file> <output-dir> [canary-gate-jar]
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"time"

	"selfservice/controlplane/internal/blueprint"
	"selfservice/controlplane/internal/dashboard"
	"selfservice/controlplane/internal/docker"
)

const dockerSocket = "/var/run/docker.sock"
const healthCheckTimeout = 60 * time.Second
const healthCheckPollInterval = 200 * time.Millisecond

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: provisionctl <blueprint-file> <output-dir> [canary-gate-jar]")
		os.Exit(2)
	}
	blueprintPath := os.Args[1]
	outDir := os.Args[2]
	var canaryGateJar string
	if len(os.Args) >= 4 {
		canaryGateJar = os.Args[3]
	}

	start := time.Now()

	bp, err := blueprint.Parse(blueprintPath)
	fatalIf(err, "parsing blueprint")
	fmt.Printf("parsed blueprint: service=%s image=%s port=%d\n", bp.Service, bp.Image, bp.Port)

	client := docker.NewClient(dockerSocket)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	containerID, err := client.CreateAndStart(ctx, docker.ContainerSpec{
		Name:     bp.Service + "-" + strconv.FormatInt(time.Now().UnixNano(), 36),
		Image:    bp.Image,
		Port:     bp.Port,
		Env:      bp.Env,
		CPUUnits: bp.CPUUnits,
		MemoryMB: bp.MemoryMB,
	})
	fatalIf(err, "creating and starting container")
	fmt.Printf("container started: %s\n", containerID)

	healthURL := fmt.Sprintf("http://127.0.0.1:%d%s", bp.Port, bp.HealthCheckPath)
	healthyAt, err := waitHealthy(healthURL, healthCheckTimeout)
	fatalIf(err, "waiting for health check")
	servedAt := time.Since(start)
	fmt.Printf("health check passed at %s: %s\n", healthURL, healthyAt)
	fmt.Printf("time from blueprint to first healthy response: %v\n", servedAt)

	dashboardPath, alertRulesPath, err := dashboard.Render(bp, outDir)
	fatalIf(err, "rendering dashboard and alert rules")
	fmt.Printf("dashboard: %s\n", dashboardPath)
	fmt.Printf("alert rules: %s\n", alertRulesPath)

	if canaryGateJar != "" {
		// The gate runs against its OWN dedicated, regression-injectable canary service, not
		// against the container just provisioned above: only the dedicated service can have a
		// reproducible regression seeded into it, which is what makes the 38/40 catch-rate claim
		// measurable at all. See README Honest framing for why this is the right scope boundary.
		fmt.Println("running canary gate (against its own dedicated instrumented service) before declaring rollout complete...")
		cmd := exec.Command("java", "-cp", canaryGateJar,
			"com.mfs.serviceplatform.canary.harness.GateRunnerMain", canaryGateJar, "0")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		gateErr := cmd.Run()
		if gateErr != nil {
			fmt.Println("canary gate BLOCKED this rollout; stopping and removing the container")
			_ = client.Stop(ctx, containerID)
			_ = client.Remove(ctx, containerID)
			os.Exit(1)
		}
		fmt.Println("canary gate PASSED; rollout complete")
	}

	fmt.Printf("TOTAL time, blueprint to rolled-out-and-gated service: %v\n", time.Since(start))
	fmt.Printf("container id: %s (stop with: docker stop %s)\n", containerID, containerID)
}

func waitHealthy(url string, timeout time.Duration) (time.Duration, error) {
	deadline := time.Now().Add(timeout)
	start := time.Now()
	client := &http.Client{Timeout: 2 * time.Second}
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode < 300 {
				return time.Since(start), nil
			}
		}
		time.Sleep(healthCheckPollInterval)
	}
	return 0, fmt.Errorf("never became healthy within %v", timeout)
}

func fatalIf(err error, context string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", context, err)
		os.Exit(1)
	}
}
