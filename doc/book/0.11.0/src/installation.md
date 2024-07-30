# Installation

Maelstrom consists of a number of different programs. These are covered in more
depth in [this chapter](programs.md). If you just want to give Maelstrom
test-ride, you'll probably just want to install a test runner like
[`cargo-maelstrom`](cargo-maelstrom.md),
[`maelstrom-go-test`](go-test.md), or
[`maelstrom-pytest`](pytest.md).

The installation process is virtual identical for all programs. We'll
demonstrate how to install all the binaries in the following sections. You can
pick and choose which ones you actual want to install.

Maelstrom currently only supports Linux.

## Installing From Pre-Built Binaries

The easiest way to install Maelstrom binaries is to use
[cargo-binstall](https://github.com/cargo-bins/cargo-binstall), which allows
you to pick and choose the binaries you want to install:

```bash
cargo binstall maelstrom-run
cargo binstall cargo-maelstrom
cargo binstall maelstrom-go-test
cargo binstall maelstrom-pytest
cargo binstall maelstrom-broker
cargo binstall maelstrom-worker
```

These commands retrieve the pre-built binaries from the [Maelstrom GitHub
release page](https://github.com/maelstrom-software/maelstrom/releases). If you
don't have `cargo-binstall`, you can directly install the pre-built binaries by
simply untarring the release artifacts. For example:

```bash
wget -q -O - https://github.com/maelstrom-software/maelstrom/releases/latest/download/cargo-maelstrom-x86_64-unknown-linux-gnu.tgz | tar xzf -
```

This will download and extract the latest release of `cargo-maelstrom` for
Linux on the x86-64 architecture.

## Installing Using Nix

Maelstrom includes a `nix.flake` file, so you can install all Maelstrom binaries with `nix profile install`:

```bash
nix profile install github:maelstrom-software/maelstrom
```

The Nix flake doesn't currently support installing individual binaries.

## Installing From Source With `cargo install`

Maelstrom binaries can be built from source using [`cargo
install`](https://doc.rust-lang.org/cargo/commands/cargo-install.html):

```bash
cargo install maelstrom-run
cargo install cargo-maelstrom
cargo install maelstrom-go-test
cargo install maelstrom-pytest
cargo install maelstrom-worker
```

However, `maelstrom-broker` requires some extra dependencies be installed
before it can be built from source:

```bash
rustup target add wasm32-unknown-unknown
cargo install wasm-opt
cargo install maelstrom-broker
```
