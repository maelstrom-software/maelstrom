#!/usr/bin/env bash

set -ex

cargo build --release
py/protobuf_compile.sh

BROKER_ARGS=""
WORKER_ARGS=""
CARGO_ARGS="--profile=release"
PYTEST_ARGS=""
START_WORKER=1

if [[ $# -gt 0 ]]; then
    export MAELSTROM_GITHUB_ACTIONS_TOKEN=$1
    export MAELSTROM_GITHUB_ACTIONS_URL=$2
    export MAELSTROM_CLUSTER_COMMUNICATION_STRATEGY=github
    echo "using github for artifact transfer"
    cargo run --release --bin maelstrom-broker -- $BROKER_ARGS &
else 
    TEMPFILE=$(mktemp --tmpdir run-tests-on-maelstrom-broker-stderr.XXXXXX)
    cargo run --release --bin maelstrom-broker -- $BROKER_ARGS 2> >(tee "$TEMPFILE" >&2) &
    PORT=$( \
    	tail -f "$TEMPFILE" \
    	| awk '/\<addr: / { print $0; exit}' \
    	| sed -Ee 's/^.*\baddr: [^,]*:([0-9]+),.*$/\1/' \
    )
    cargo run --release --bin maelstrom-worker -- --broker=localhost:$PORT $WORKER_ARGS &
    CARGO_ARGS="$CARGO_ARGS --broker=localhost:$PORT"
    PYTEST_ARGS="$PYTEST_ARGS --broker=localhost:$PORT"
fi

set +e

cargo run --release --bin cargo-maelstrom -- $CARGO_ARGS
CARGO_MAELSTROM_STATUS=$?

cargo run --release --bin maelstrom-pytest -- $PYTEST_ARGS
MAELSTROM_PYTEST_STATUS=$?

cargo run --release --bin maelstrom-admin stop

if [[ -v TEMPFILE ]]; then
    rm "$TEMPFILE"
fi

set -e

if [[ $CARGO_MAELSTROM_STATUS != 0 ]]; then
    exit $CARGO_MAELSTROM_STATUS
fi

if [[ $MAELSTROM_PYTEST_STATUS != 0 ]]; then
    exit $MAELSTROM_PYTEST_STATUS
fi

cargo test --release --doc
