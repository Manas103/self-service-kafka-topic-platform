package blueprint

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTempBlueprint(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.blueprint")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestParsesAllFieldsAndEnv(t *testing.T) {
	path := writeTempBlueprint(t, `service: payments-api
image: nginx:1.27-alpine
port: 80
healthCheckPath: /
replicas: 2
cpu: 256
memoryMb: 512
env.LOG_LEVEL: info
env.REGION: us-east-1
team: payments
alertEmail: oncall@example.com
maxUnhealthySeconds: 30
`)
	bp, err := Parse(path)
	if err != nil {
		t.Fatal(err)
	}
	if bp.Service != "payments-api" || bp.Image != "nginx:1.27-alpine" || bp.Port != 80 {
		t.Fatalf("core fields wrong: %+v", bp)
	}
	if bp.Replicas != 2 || bp.CPUUnits != 256 || bp.MemoryMB != 512 {
		t.Fatalf("resource fields wrong: %+v", bp)
	}
	if bp.Env["LOG_LEVEL"] != "info" || bp.Env["REGION"] != "us-east-1" {
		t.Fatalf("env fields wrong: %+v", bp.Env)
	}
	if bp.Team != "payments" || bp.AlertEmail != "oncall@example.com" || bp.MaxUnhealthySeconds != 30 {
		t.Fatalf("ownership fields wrong: %+v", bp)
	}
}

func TestDefaultsReplicasToOne(t *testing.T) {
	path := writeTempBlueprint(t, "service: s\nimage: i\nport: 80\n")
	bp, err := Parse(path)
	if err != nil {
		t.Fatal(err)
	}
	if bp.Replicas != 1 {
		t.Fatalf("expected default replicas 1, got %d", bp.Replicas)
	}
}

func TestRejectsUnknownKey(t *testing.T) {
	path := writeTempBlueprint(t, "service: s\nimage: i\nport: 80\nbogusKey: x\n")
	if _, err := Parse(path); err == nil {
		t.Fatal("expected an error for an unknown key, got nil")
	}
}

func TestRejectsMissingRequiredField(t *testing.T) {
	path := writeTempBlueprint(t, "service: s\nimage: i\n")
	if _, err := Parse(path); err == nil {
		t.Fatal("expected an error for a missing required field (port), got nil")
	}
}

func TestIgnoresBlankLinesAndComments(t *testing.T) {
	path := writeTempBlueprint(t, "# a comment\nservice: s\n\nimage: i\nport: 80\n")
	bp, err := Parse(path)
	if err != nil {
		t.Fatal(err)
	}
	if bp.Service != "s" {
		t.Fatalf("expected service s, got %q", bp.Service)
	}
}
