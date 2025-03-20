#!/usr/bin/env bash

set -ex

export MAELSTROM_BROKER_CLUSTER_COMMUNICATION_STRATEGY="github"
export MAELSTROM_BROKER_GITHUB_ACTIONS_TOKEN=$1
export MAELSTROM_BROKER_GITHUB_ACTIONS_URL=$2

exec cargo run --release --bin maelstrom-broker
