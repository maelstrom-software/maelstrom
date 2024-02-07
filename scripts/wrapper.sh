#!/usr/bin/env bash

set -ex

nix develop --ignore-environment --keep TERM --command ${0%%.sh}-inner.sh
