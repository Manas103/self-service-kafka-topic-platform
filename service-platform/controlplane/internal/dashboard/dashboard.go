// Package dashboard renders a Grafana-schema dashboard JSON and an alert
// rules file per provisioned service, the same "provisioning and
// observability are one action, not two follow-up tickets" idea
// client-lib's sibling control plane already uses for topics.
package dashboard

import (
	"encoding/json"
	"os"
	"path/filepath"

	"selfservice/controlplane/internal/blueprint"
)

type panel struct {
	Title string `json:"title"`
	Type  string `json:"type"`
	Expr  string `json:"expr"`
}

type dashboardDoc struct {
	Title  string  `json:"title"`
	Tags   []string `json:"tags"`
	Panels []panel `json:"panels"`
}

type alertRule struct {
	Name      string  `json:"name"`
	Indicator string  `json:"indicator"`
	Operator  string  `json:"operator"`
	Threshold float64 `json:"threshold"`
	NotifyEmail string `json:"notifyEmail"`
}

type alertRulesDoc struct {
	Service string      `json:"service"`
	Rules   []alertRule `json:"rules"`
}

// Render writes <outDir>/<service>-dashboard.json and
// <outDir>/<service>-alert-rules.json, the same side effect the topic
// control plane's DashboardTemplateRenderer and StorageCostEstimator
// produce per topic. Both are real, parseable JSON files on disk, not
// pushed to a live Grafana/alerting backend (see README Honest framing).
func Render(bp *blueprint.Blueprint, outDir string) (dashboardPath, alertRulesPath string, err error) {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return "", "", err
	}

	dash := dashboardDoc{
		Title: bp.Service + " (" + bp.Team + ")",
		Tags:  []string{"service-platform", bp.Service, bp.Team},
		Panels: []panel{
			{Title: "Request rate", Type: "graph", Expr: `rate(` + bp.Service + `_requests_total[1m])`},
			{Title: "Error rate", Type: "graph", Expr: `rate(` + bp.Service + `_errors_total[1m])`},
			{Title: "Availability", Type: "stat", Expr: `1 - (rate(` + bp.Service + `_errors_total[5m]) / rate(` + bp.Service + `_requests_total[5m]))`},
			{Title: "p99 duration (ms)", Type: "graph", Expr: bp.Service + `_duration_p99_ms`},
			{Title: "Saturation (in-flight)", Type: "graph", Expr: bp.Service + `_inflight`},
		},
	}
	dashboardPath = filepath.Join(outDir, bp.Service+"-dashboard.json")
	if err := writeJSON(dashboardPath, dash); err != nil {
		return "", "", err
	}

	alerts := alertRulesDoc{
		Service: bp.Service,
		Rules: []alertRule{
			{Name: "error-rate-high", Indicator: "errors", Operator: ">", Threshold: 0.02, NotifyEmail: bp.AlertEmail},
			{Name: "availability-low", Indicator: "availability", Operator: "<", Threshold: 0.995, NotifyEmail: bp.AlertEmail},
			{Name: "latency-p99-high", Indicator: "duration_p99_ms", Operator: ">", Threshold: 500, NotifyEmail: bp.AlertEmail},
			{Name: "saturation-high", Indicator: "inflight", Operator: ">", Threshold: 30, NotifyEmail: bp.AlertEmail},
		},
	}
	alertRulesPath = filepath.Join(outDir, bp.Service+"-alert-rules.json")
	if err := writeJSON(alertRulesPath, alerts); err != nil {
		return "", "", err
	}
	return dashboardPath, alertRulesPath, nil
}

func writeJSON(path string, v interface{}) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

// Clean is a small helper the test suite uses to assert exactly one
// dashboard/alert-rules pair exists per service, the same per-service (not
// per-directory) invariant the topic control plane's Findings fixed.
func Clean(outDir, service string) error {
	matches, _ := filepath.Glob(filepath.Join(outDir, service+"-*"))
	for _, m := range matches {
		if err := os.Remove(m); err != nil {
			return err
		}
	}
	return nil
}
