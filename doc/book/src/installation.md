# Installation

Maelstrom consists of the following binaries:
  - [`cargo-maelstrom`](cargo-maelstrom.md): An alternative Rust test runner. This can be run in
    standalone mode &mdash; where tests will be executed on the local machine
    &mdash; or in clustered mode. In standalone mode, no other Maelstrom
    binaries need be installed, but in clustered mode, there must be a broker
    and some workers available on the network.
  - [`maelstrom-broker`](broker.md): The Maelstrom cluster scheduler. There must be one of
    these per Maelstrom cluster.
  - [`maelstrom-worker`](worker.md): The Maelstrom cluster worker. This must be
    installed on the machines in the cluster that will actually run jobs
    (tests).
  - [`maelstrom-run`](run.md): A Maelstrom client for running arbitrary commands on a
    Maelstrom cluster. While this binary can run in standalone mode, it's only really
    useful in clustered mode.

The installation process for all binaries is virtually identical. We'll show
how to install all the binaries in the following sections. You can pick and
choose which ones you actually want to install.

Maelstrom only supports Linux currently.

## Installing From Pre-Built Binaries

The easiest way to install Maelstrom binaries is to use
[cargo-binstall](https://github.com/cargo-bins/cargo-binstall):

```bash
cargo binstall cargo-maelstrom
cargo binstall maelstrom-worker
cargo binstall maelstrom-broker
cargo binstall maelstrom-run
```

These commands retrieve the pre-built binaries from the [Maelstrom GitHub
release page](https://github.com/maelstrom-software/maelstrom/releases). If you
don't have `cargo-binstall`, you can just manually install the binaries from the
releases page. For example:

```bash
wget -q -O - https://github.com/maelstrom-software/maelstrom/releases/latest/download/cargo-maelstrom-x86_64-unknown-linux-gnu.tgz | tar xzf -
```

This will download and extract the latest release of `cargo-maelstrom` for x86 Linux.

## Installing Using Nix

We have a `nix.flake` file, so you can install all Maelstrom binaries with something like:

```bash
nix profile install github:maelstrom-software/maelstrom
```

Our Nix flake doesn't currently have the ability to install individual binaries yet.

## Installing From Source With `cargo install`

Maelstrom binaries can be built from source using [`cargo install`]:

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

Now we can compile the broker

```bash
export GITHUB_URL="https://github.com/maelstrom-software/maelstrom.git"
cargo install --git $GITHUB_URL maelstrom-broker
```
