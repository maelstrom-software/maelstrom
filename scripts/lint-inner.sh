#!/usr/bin/env bash

set -ex

cargo fmt --check
cargo clippy
