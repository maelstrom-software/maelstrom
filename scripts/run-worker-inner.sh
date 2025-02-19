#!/usr/bin/env bash

set -ex

export ACTIONS_RUNTIME_TOKEN=$1
export ACTIONS_RESULTS_URL=$2
WORKER_ARGS="--artifact-transfer-strategy github --broker-connection github"

cargo run --release --bin maelstrom-worker -- --broker=0.0.0.0:0 $WORKER_ARGS || true
