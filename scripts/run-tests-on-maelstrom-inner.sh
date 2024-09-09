#!/usr/bin/env bash

set -ex

cargo build --release
py/protobuf_compile.sh

set +e

TEMPFILE=$(mktemp --tmpdir run-tests-on-maelstrom-broker-stderr.XXXXXX)
cargo run --release --bin maelstrom-broker 2> >(tee "$TEMPFILE" >&2) &
BROKER_PID=$!
PORT=$( \
	tail -f "$TEMPFILE" \
	| awk '/\<addr: / { print $0; exit}' \
	| sed -Ee 's/^.*\baddr: [^,]*:([0-9]+),.*$/\1/' \
)
cargo run --release --bin maelstrom-worker -- --broker=localhost:$PORT &

cargo run --release --bin cargo-maelstrom -- --broker=localhost:$PORT --profile=release
CARGO_MAELSTROM_STATUS=$?

cargo run --release --bin maelstrom-pytest -- --broker=localhost:$PORT
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
