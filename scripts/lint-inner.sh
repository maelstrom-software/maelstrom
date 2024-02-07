#!/usr/bin/env bash

set -ex

env
sleep 10

cargo fmt --check
cargo clippy -- --deny warnings
