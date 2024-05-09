#!/bin/bash
set -ex

mkdir -p target/py
python3 -m grpc_tools.protoc \
    -Icrates/maelstrom-client-base/src/ \
    --python_out=target/py --pyi_out=target/py \
    --grpc_python_out=target/py \
    crates/maelstrom-client-base/src/items.proto

sed -i 's/^import .*_pb2 as/from . \0/' target/py/*.py
