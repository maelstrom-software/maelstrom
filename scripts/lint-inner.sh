#!/usr/bin/env bash

set -ex

cargo-fmt --check
cargo-clippy clippy --all-targets -- -Dwarnings
cargo-clippy clippy -p maelstrom-web --target wasm32-unknown-unknown -- -Dwarnings
cargo xtask publish --lint
cargo check --all-targets

# Build every version of the book, the same way the documentation CI job does.
docs_dir=$(mktemp -d)
trap 'rm -rf "$docs_dir"' EXIT
(cd doc/book && ./build-all-versions.sh "$docs_dir")
#(cd doc/book && mdbook test)
