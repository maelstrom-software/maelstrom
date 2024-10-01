#!/bin/bash

set -ex

if [ -z "$1" ] || [ -z "$2" ]
then
    echo "args: <gsheets-service-account-json> <gh-token>"
    exit 1
fi

export SERVICE_ACCOUNT_JSON=$1
echo "$2" | gh auth login --with-token
cargo xtask pull-stats
