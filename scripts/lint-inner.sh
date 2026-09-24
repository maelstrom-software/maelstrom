#!/usr/bin/env bash

set -ex

cargo-fmt --check
cargo-clippy clippy --all-targets -- -Dwarnings
cargo-clippy clippy -p maelstrom-web --target wasm32-unknown-unknown -- -Dwarnings
cargo xtask publish --lint
cargo check --all-targets
#(cd doc/book && mdbook test)
