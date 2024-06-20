#!/usr/bin/env bash

set -ex

cargo-fmt --check
cargo-clippy -Dwarnings
cargo xtask publish --lint
cargo check --all-targets
(cd doc/book && mdbook test)
