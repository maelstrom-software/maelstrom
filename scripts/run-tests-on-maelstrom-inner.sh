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
    export MAELSTROM_BROKER_CONNECTION=github
    BROKER_ARGS="$BROKER_ARGS --artifact-transfer-strategy github"
    START_WORKER=0
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
if [[ $START_WORKER -gt 0 ]]; then
    cargo run --release --bin maelstrom-worker -- --broker=localhost:$PORT $WORKER_ARGS &
fi

cargo run --release --bin cargo-maelstrom -- --broker=localhost:$PORT $CARGO_ARGS
CARGO_MAELSTROM_STATUS=$?

cargo run --release --bin maelstrom-pytest -- --broker=localhost:$PORT $PYTEST_ARGS
MAELSTROM_PYTEST_STATUS=$?

kill -15 $BROKER_PID
rm "$TEMPFILE"
set -e

if [ $CARGO_MAELSTROM_STATUS != 0 ]; then
	exit $CARGO_MAELSTROM_STATUS
fi

if [ $MAELSTROM_PYTEST_STATUS != 0 ]; then
	exit $MAELSTROM_PYTEST_STATUS
fi

cargo test --release --doc
