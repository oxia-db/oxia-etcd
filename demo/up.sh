#!/usr/bin/env bash
# Starts Oxia, the oxia-etcd gateway, and a kind cluster that uses the gateway as its etcd.
set -euo pipefail
cd "$(dirname "$0")/.."

IMAGE=${IMAGE:-oxia/oxia-etcd:dev}
OXIA_IMAGE=${OXIA_IMAGE:-oxia/oxia:0.17.1}

# The containers share kind's network, so that the cluster's node reaches the gateway by name
docker network inspect kind >/dev/null 2>&1 || docker network create kind >/dev/null

echo "==> Building ${IMAGE}"
docker build -q -t "${IMAGE}" . >/dev/null

echo "==> Starting Oxia"
docker rm -f oxia oxia-etcd >/dev/null 2>&1 || true
docker run -d --name oxia --network kind "${OXIA_IMAGE}" oxia standalone >/dev/null

echo "==> Starting the oxia-etcd gateway"
docker run -d --name oxia-etcd --network kind -p 2379:2379 "${IMAGE}" \
  --oxia oxia:6648 --cluster oxia-etcd-demo --advertise-client-url http://oxia-etcd:2379 >/dev/null
until curl -sf http://localhost:2379/health >/dev/null; do sleep 1; done
curl -s http://localhost:2379/version; echo

echo "==> Creating the kind cluster"
kind create cluster --config demo/kind.yaml
kubectl --context kind-oxia-etcd-demo get nodes
