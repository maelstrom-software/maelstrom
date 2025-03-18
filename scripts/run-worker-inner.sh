#!/usr/bin/env bash

set -ex

export MAELSTROM_WORKER_BROKER_CONNECTION="github"
export MAELSTROM_WORKER_GITHUB_ACTIONS_TOKEN=$1
export MAELSTROM_WORKER_GITHUB_ACTIONS_URL=$2

cargo run --release --bin maelstrom-worker || true
