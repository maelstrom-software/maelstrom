#!/usr/bin/env bash

set -ex

echo $PATH

cargo fmt --check
cargo clippy
