# Installing cargo-maelstrom

Keep in mind that `cargo-maelstrom` only works on Linux.

Once installed you should be able to invoke it by running `cargo maelstrom`

## Installing from Pre-Built Binary

The easiest way to install it is using [cargo-binstall](https://github.com/cargo-bins/cargo-binstall).

```bash
cargo binstall cargo-maelstrom
```

This will install a pre-built binary from the [github releases page](https://github.com/maelstrom-software/maelstrom/releases).

If you don't have `cargo-binstall`, you can always download the binaries manually. If we don't have
a binary for your specific architecture, you can compile from source.

## Compiling from Source

First make sure you've installed [Rust](https://www.rust-lang.org/tools/install).

Then use cargo to download, and compile `cargo-maelstrom` from source

```bash
export GITHUB_URL="https://github.com/maelstrom-software/maelstrom.git"
cargo install --git $GITHUB_URL cargo-maelstrom
```
