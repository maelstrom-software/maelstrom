#!/usr/bin/env bash

set -ex

cargo fmt --check
cargo clippy -- --deny warnings
cargo xtask publish --lint
cargo check --all-targets
(cd site && mdbook test)
(cd doc/book && mdbook test)
