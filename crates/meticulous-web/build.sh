#!/bin/bash

set -ex

pkg_dir=target/wasm_pkg/
mkdir -p $pkg_dir

pushd crates/meticulous-web
wasm-pack build --release --target web --out-dir ../../$pkg_dir
popd

cp crates/meticulous-web/www/* $pkg_dir
tar --create --directory $pkg_dir . --file target/web.tar
