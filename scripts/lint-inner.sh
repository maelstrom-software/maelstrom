#!/usr/bin/env bash

set -ex

env

cargo fmt --check
cargo clippy -- --deny warnings
