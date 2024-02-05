#!/usr/bin/env bash

set -x

echo $PATH
which cargo

if ! cargo build --release; then
	exit $!
fi

TEMPFILE=$(mktemp --tmpdir run-tests-on-maelstrom-broker-stderr.XXXXXX)
cargo run --release --bin maelstrom-broker 2> >(tee "$TEMPFILE" >&2) &
BROKER_PID=$!
PORT=$( \
	tail -f "$TEMPFILE" \
	| awk '/\<addr: / { print $0; exit}' \
	| sed -Ee 's/^.*\baddr: [^,]*:([0-9]+),.*$/\1/' \
)
cargo run --release --bin maelstrom-worker -- --broker=localhost:$PORT &
cargo run --release --bin cargo-maelstrom -- --broker=localhost:$PORT run
CARGO_MAELSTROM_STATUS=$?
kill -9 $BROKER_PID
rm "$TEMPFILE"
if [ $CARGO_MAELSTROM_STATUS != 0 ]; then
	exit $CARGO_MAELSTROM_STATUS
fi

if ! cargo test --doc; then
	exit $!
fi
