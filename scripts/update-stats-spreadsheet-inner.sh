#!/bin/bash

set -ex

if [ -z "$1" ] || [ -z "$2" ]
then
    echo "args: <gsheets-service-account-json> <gh-token>"
    exit 1
fi

export SERVICE_ACCOUNT_JSON=$1
export GH_TOKEN=$2
cargo xtask pull-stats
