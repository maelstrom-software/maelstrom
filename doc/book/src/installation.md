# Installation

## Requirements

Maelstrom currently only supports Linux.

...presumably there are also kernel version requirements?

## Installed Binaries

Maelstrom consists of the following binaries:
  - [`cargo-maelstrom`](cargo-maelstrom.md): A Rust test runner. This can be run in
    standalone mode &mdash; where tests will be executed on the local machine
    &mdash; or in clustered mode. In standalone mode, no other Maelstrom
    binaries need be installed, but in clustered mode, there must be a broker
    and some workers available on the network.
  - [`maelstrom-broker`](broker.md): The Maelstrom cluster broker. This component is
    responsible for scheduling work onto nodes in the cluster. There must be one of
    these per Maelstrom cluster.
  - [`maelstrom-worker`](worker.md): The Maelstrom cluster worker. This must be
    installed on the machines in the cluster that will actually run jobs
    (i.e. tests).
  - [`maelstrom-run`](run.md): A Maelstrom client for running arbitrary commands on a
    Maelstrom cluster. While this binary can run in standalone mode, it's only
    necessary in clustered mode.

## Installing From Pre-Built Binaries

The easiest way to install Maelstrom binaries is to use
[cargo-binstall](https://github.com/cargo-bins/cargo-binstall), which allows you to
pick and choose the binaries you want to install:

```bash
cargo binstall cargo-maelstrom
cargo binstall maelstrom-worker
cargo binstall maelstrom-broker
cargo binstall maelstrom-run
```

These commands retrieve the pre-built binaries from the [Maelstrom GitHub
release page](https://github.com/maelstrom-software/maelstrom/releases). If you
don't have `cargo-binstall`, you can directly install the pre-built binaries by
simply untarring the release artifacts. For example:

```bash
wget -q -O - https://github.com/maelstrom-software/maelstrom/releases/latest/download/cargo-maelstrom-x86_64-unknown-linux-gnu.tgz | tar xzf -
```

This will download and extract the latest release of `cargo-maelstrom` for Linux on the x86-64 architecture.

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
cargo install cargo-maelstrom
cargo install maelstrom-worker
cargo install maelstrom-run
```

However, `maelstrom-broker` requires some extra dependencies be installed
before it can be built from source:

```bash
rustup target add wasm32-unknown-unknown
cargo install wasm-opt
cargo install maelstrom-broker
```
