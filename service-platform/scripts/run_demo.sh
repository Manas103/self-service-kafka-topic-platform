#!/bin/bash
# Builds the Go control plane and the Java canary gate, then runs one real end-to-end
# provisioning demo: a 12-line blueprint becomes a running, health-checked, real Docker
# container (nginx, local Docker Engine API, no AWS account in this build; see README Honest
# framing), then the canary gate runs against its own dedicated instrumented service before the
# rollout is declared complete. Run from Windows: wsl.exe -d Ubuntu-22.04 -- bash "<this script>"
set -e
SRC="/mnt/c/Users/Manas/Downloads/Auto Job Applier/Total Job Application process flow/projects/self-service-kafka-topic-platform/service-platform"
WORK="$HOME/build/selfservice-platform"

rm -rf "$WORK"
mkdir -p "$WORK"
rsync -a --exclude target --exclude docs "$SRC/" "$WORK/"
mkdir -p "$WORK/docs"

echo "=== Go: vet, build, test ==="
cd "$WORK/controlplane"
go vet ./...
go build -o /tmp/provisionctl ./cmd/provisionctl
go test ./... -v > /tmp/go_test_output.txt 2>&1
cat /tmp/go_test_output.txt
cp /tmp/go_test_output.txt "$SRC/docs/go_test_output.txt"

echo "=== Java: canary gate build and test ==="
cd "$WORK/canary"
mvn -q test > /tmp/canary_test.log 2>&1
echo "canary tests: exit $?"
mvn -q package -DskipTests > /tmp/canary_package.log 2>&1
cp target/canary-gate.jar /tmp/canary-gate.jar

echo "=== End-to-end provisioning demo ==="
mkdir -p /tmp/provisioning_demo_out
/tmp/provisionctl "$WORK/examples/payments-api.blueprint" /tmp/provisioning_demo_out /tmp/canary-gate.jar \
  2>&1 | tee /tmp/provisioning_demo_output.txt

cp /tmp/provisioning_demo_output.txt "$SRC/docs/"
cp /tmp/provisioning_demo_out/*.json "$SRC/docs/" 2>/dev/null || true

echo "=== DONE ==="
