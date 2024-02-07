#!/usr/bin/env bash

exec nix develop --ignore-environment --keep TERM --command ${0%%.sh}-inner.sh
