#!/usr/bin/env bash

echo $PATH

set -ex

cargo fmt --check
cargo clippy
