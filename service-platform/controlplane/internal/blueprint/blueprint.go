// Package blueprint parses the one artifact a team actually writes: a plain
// key:value text file, no YAML/JSON library dependency, deliberately small
// enough that the whole claim ("a 12-line service request into a running
// container") is checkable by counting lines in examples/payments-api.blueprint.
package blueprint

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Blueprint is the one declarative input the control plane needs to run a
// service: what image to run, how to reach it, how to tell it is healthy,
// and who owns it (for the dashboard and alert rules this blueprint emits).
type Blueprint struct {
	Service             string
	Image                string
	Port                int
	HealthCheckPath      string
	Replicas             int
	CPUUnits             int
	MemoryMB             int
	Env                  map[string]string
	Team                 string
	AlertEmail           string
	MaxUnhealthySeconds  int
}

// Parse reads a blueprint file: one "key: value" pair per line, blank lines
// and lines starting with "#" ignored, "env.NAME: value" lines collected
// into Env. Unknown keys are rejected so a typo fails loudly rather than
// silently falling back to a zero value.
func Parse(path string) (*Blueprint, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening blueprint: %w", err)
	}
	defer f.Close()

	bp := &Blueprint{Env: map[string]string{}}
	lineNo := 0
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		idx := strings.Index(line, ":")
		if idx < 0 {
			return nil, fmt.Errorf("line %d: expected \"key: value\", got %q", lineNo, line)
		}
		key := strings.TrimSpace(line[:idx])
		value := strings.TrimSpace(line[idx+1:])

		if strings.HasPrefix(key, "env.") {
			bp.Env[strings.TrimPrefix(key, "env.")] = value
			continue
		}

		var parseErr error
		switch key {
		case "service":
			bp.Service = value
		case "image":
			bp.Image = value
		case "port":
			bp.Port, parseErr = strconv.Atoi(value)
		case "healthCheckPath":
			bp.HealthCheckPath = value
		case "replicas":
			bp.Replicas, parseErr = strconv.Atoi(value)
		case "cpu":
			bp.CPUUnits, parseErr = strconv.Atoi(value)
		case "memoryMb":
			bp.MemoryMB, parseErr = strconv.Atoi(value)
		case "team":
			bp.Team = value
		case "alertEmail":
			bp.AlertEmail = value
		case "maxUnhealthySeconds":
			bp.MaxUnhealthySeconds, parseErr = strconv.Atoi(value)
		default:
			return nil, fmt.Errorf("line %d: unknown blueprint key %q", lineNo, key)
		}
		if parseErr != nil {
			return nil, fmt.Errorf("line %d: %w", lineNo, parseErr)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if bp.Service == "" || bp.Image == "" || bp.Port == 0 {
		return nil, fmt.Errorf("blueprint missing required field: service, image and port are mandatory")
	}
	if bp.Replicas == 0 {
		bp.Replicas = 1
	}
	return bp, nil
}
