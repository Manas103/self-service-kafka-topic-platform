#!/bin/bash
set -e
SRC="/mnt/c/Users/Manas/Downloads/Auto Job Applier/Total Job Application process flow/projects/self-service-kafka-topic-platform/service-platform/controlplane"
WORK="$HOME/build/selfservice-controlplane"

rm -rf "$WORK"
mkdir -p "$WORK"
rsync -a "$SRC/" "$WORK/"
cd "$WORK"

echo "=== go vet ==="
go vet ./...

echo "=== go build ==="
go build -o /tmp/provisionctl ./cmd/provisionctl

echo "=== go test ==="
go test ./... -v 2>&1 | tee /tmp/go_test_output.txt

echo "=== DONE ==="
