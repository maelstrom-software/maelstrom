#!/bin/bash

set -ex

pushd crates/meticulous-web
wasm-pack build --release --target web
popd

cp crates/meticulous-web/www/* crates/meticulous-web/pkg/
tar --create --directory crates/meticulous-web/pkg . --file target/web.tar
