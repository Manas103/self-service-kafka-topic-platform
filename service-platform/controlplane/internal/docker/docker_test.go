package docker

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"
)

const testSocket = "/var/run/docker.sock"

func skipIfDockerUnreachable(t *testing.T) {
	t.Helper()
	conn, err := net.DialTimeout("unix", testSocket, 500*time.Millisecond)
	if err != nil {
		t.Skipf("Docker is not reachable at %s: %v", testSocket, err)
	}
	conn.Close()
}

// TestCreateStartStopRemoveAgainstARealDockerDaemon is the claim this
// package exists for: a real container, created and started through the
// real Docker Engine API over the real Unix socket, answering a real HTTP
// health check, then torn down by the ID this test itself holds.
func TestCreateStartStopRemoveAgainstARealDockerDaemon(t *testing.T) {
	skipIfDockerUnreachable(t)

	client := NewClient(testSocket)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	hostPort := 18099
	name := fmt.Sprintf("idqg-docker-test-%d", time.Now().UnixNano())
	id, err := client.CreateAndStart(ctx, ContainerSpec{
		Name: name, Image: "nginx:1.27-alpine", Port: 80, HostPort: hostPort,
		Env: map[string]string{"LOG_LEVEL": "info"}, CPUUnits: 256, MemoryMB: 256,
	})
	if err != nil {
		t.Fatalf("CreateAndStart: %v", err)
	}
	defer func() {
		_ = client.Stop(ctx, id)
		_ = client.Remove(ctx, id)
	}()

	if id == "" {
		t.Fatal("expected a non-empty container ID")
	}

	url := fmt.Sprintf("http://127.0.0.1:%d/", hostPort)
	deadline := time.Now().Add(20 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				lastErr = nil
				break
			}
		}
		lastErr = err
		time.Sleep(200 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("container never answered a health check: %v", lastErr)
	}

	if err := client.Stop(ctx, id); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := client.Remove(ctx, id); err != nil {
		t.Fatalf("Remove: %v", err)
	}
}
