#!/usr/bin/env bash

set -ex

cargo fmt --check
cargo clippy -- --deny warnings
cargo xtask publish --lint
