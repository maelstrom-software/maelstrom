#!/usr/bin/env bash

set -ex

cargo build --release
py/protobuf_compile.sh

BROKER_ARGS=""
WORKER_ARGS=""
CARGO_ARGS="--profile=release"
PYTEST_ARGS=""

if [[ $# -gt 0 ]]; then
    export ACTIONS_RUNTIME_TOKEN=$1
    export ACTIONS_RESULTS_URL=$2
    BROKER_ARGS="$BROKER_ARGS --artifact-transfer-strategy github"
    WORKER_ARGS="$WORKER_ARGS --artifact-transfer-strategy github"
    CARGO_ARGS="$CARGO_ARGS --artifact-transfer-strategy github"
    PYTEST_ARGS="$PYTEST_ARGS --artifact-transfer-strategy github"
    echo "using github for artifact transfer"
fi

set +e

TEMPFILE=$(mktemp --tmpdir run-tests-on-maelstrom-broker-stderr.XXXXXX)
cargo run --release --bin maelstrom-broker -- $BROKER_ARGS 2> >(tee "$TEMPFILE" >&2) &
BROKER_PID=$!
PORT=$( \
	tail -f "$TEMPFILE" \
	| awk '/\<addr: / { print $0; exit}' \
	| sed -Ee 's/^.*\baddr: [^,]*:([0-9]+),.*$/\1/' \
)
cargo run --release --bin maelstrom-worker -- --broker=localhost:$PORT $WORKER_ARGS &

cargo run --release --bin cargo-maelstrom -- --broker=localhost:$PORT $CARGO_ARGS
CARGO_MAELSTROM_STATUS=$?

cargo run --release --bin maelstrom-pytest -- --broker=localhost:$PORT $PYTEST_ARGS
MAELSTROM_PYTEST_STATUS=$?

kill -9 $BROKER_PID
rm "$TEMPFILE"
set -e

if [ $CARGO_MAELSTROM_STATUS != 0 ]; then
	exit $CARGO_MAELSTROM_STATUS
fi

if [ $MAELSTROM_PYTEST_STATUS != 0 ]; then
	exit $MAELSTROM_PYTEST_STATUS
fi

cargo test --release --doc
