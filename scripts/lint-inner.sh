#!/usr/bin/env bash

set -ex

cargo fmt --check
cargo build
cargo clippy
