#!/bin/bash

set -ex

profile=$1

pushd crates/meticulous-web

target=wasm32-unknown-unknown
build_dir=../../target
pkg_dir=$build_dir/$profile/wasm_pkg/
mkdir -p $pkg_dir

cargo build --lib --target $target --profile $profile

wasm-bindgen \
    $build_dir/$target/$profile/meticulous_web.wasm \
    --out-dir $pkg_dir \
    --typescript \
    --target web

if [ "$profile" = "wasm_release" ]; then
    wasm-opt \
        $pkg_dir/meticulous_web_bg.wasm \
        -o $pkg_dir/meticulous_web_bg.wasm-opt.wasm \
        -O

    mv $pkg_dir/meticulous_web_bg.wasm-opt.wasm $pkg_dir/meticulous_web_bg.wasm

    # Add this file so we can tell we ran the optimizations
    touch $pkg_dir/.wasm-opt
fi

cp www/* $pkg_dir
tar --create --directory $pkg_dir . --file $build_dir/$profile/web.tar

popd
