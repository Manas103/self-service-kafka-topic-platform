#!/usr/bin/env bash
# Runs the real end-to-end demo inside WSL2, against a kind cluster. Not run by this build's
# measured claims (see the README's Honest framing for why); this script is how to run it
# yourself against a real cluster.
set -euo pipefail
cd "$(dirname "$0")/.."

kind get clusters | grep -qx "k8s-platform-demo" || kind create cluster --name k8s-platform-demo
kubectl config use-context kind-k8s-platform-demo

mvn -q package -DskipTests
mvn -q exec:java -Dexec.mainClass=com.manas.kafkaplatform.k8splatform.cli.ProvisionCli \
    -Dexec.args="examples/payments-cluster.request"
