package dashboard

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"selfservice/controlplane/internal/blueprint"
)

func TestRenderWritesOneDashboardAndOneAlertRulesFilePerService(t *testing.T) {
	dir := t.TempDir()
	bp := &blueprint.Blueprint{
		Service: "orders-api", Team: "orders", AlertEmail: "oncall@example.com",
	}

	dashPath, alertPath, err := Render(bp, dir)
	if err != nil {
		t.Fatal(err)
	}

	matches, _ := filepath.Glob(filepath.Join(dir, "orders-api-*"))
	if len(matches) != 2 {
		t.Fatalf("expected exactly 2 files for this service, got %d: %v", len(matches), matches)
	}

	var dash dashboardDoc
	raw, err := os.ReadFile(dashPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &dash); err != nil {
		t.Fatalf("dashboard is not valid JSON: %v", err)
	}
	if len(dash.Panels) != 5 {
		t.Fatalf("expected 5 panels (rate, errors, availability, duration, saturation), got %d", len(dash.Panels))
	}

	var alerts alertRulesDoc
	raw, err = os.ReadFile(alertPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &alerts); err != nil {
		t.Fatalf("alert rules file is not valid JSON: %v", err)
	}
	if len(alerts.Rules) != 4 {
		t.Fatalf("expected 4 alert rules, got %d", len(alerts.Rules))
	}
	for _, r := range alerts.Rules {
		if r.NotifyEmail != bp.AlertEmail {
			t.Fatalf("alert rule %s does not route to the blueprint's own alertEmail", r.Name)
		}
	}
}

func TestRenderIsIdempotentPerServiceNotCumulative(t *testing.T) {
	dir := t.TempDir()
	bp := &blueprint.Blueprint{Service: "a", Team: "t", AlertEmail: "e@example.com"}
	if _, _, err := Render(bp, dir); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Render(bp, dir); err != nil {
		t.Fatal(err)
	}
	matches, _ := filepath.Glob(filepath.Join(dir, "a-*"))
	if len(matches) != 2 {
		t.Fatalf("re-rendering the same service must still leave exactly 2 files, got %d", len(matches))
	}
}
