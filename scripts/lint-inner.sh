#!/usr/bin/env bash

set -ex

cargo fmt --check
cargo build --lib --package maelstrom-web --target wasm32-unknown-unknown
cargo clippy
