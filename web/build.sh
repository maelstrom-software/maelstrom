#!/bin/bash

set -ex

pushd web
wasm-pack build --release --target web
popd

cp web/www/* web/pkg/
tar --create --directory web/pkg . --file target/web.tar
