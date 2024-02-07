#!/usr/bin/env bash

set -ex

if [ -e ".cargo/bin/cargo" ]; then
	echo "There is a cargo binary in .cargo/bin/cargo!" 1>&2
fi

nix develop --ignore-environment --keep TERM --command ${0%%.sh}-inner.sh
