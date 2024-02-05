#!/usr/bin/env bash

set -ex

echo $PATH
which cargo

cargo fmt --check
cargo clippy
