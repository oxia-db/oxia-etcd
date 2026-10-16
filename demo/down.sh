#!/usr/bin/env bash
# Removes the demo cluster, the gateway and Oxia.
set -uo pipefail
kind delete cluster --name oxia-etcd-demo
docker rm -f oxia-etcd oxia
