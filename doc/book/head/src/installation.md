# Installation

Maelstrom consists of a number of different programs. These are covered in more
depth in [this chapter](programs.md). If you just want to give Maelstrom
test-drive, you can start by installing a test runner like
[`cargo-maelstrom`](cargo-maelstrom.md), [`maelstrom-go-test`](go-test.md), or
[`maelstrom-pytest`](pytest.md), and then later add daemon programs for
clustering.

The installation process is virtual identical for all programs. We'll
demonstrate how to install all the binaries in the following sections. You can
pick and choose which ones you actual want to install.

Maelstrom currently only supports Linux.

## Installing From Pre-Built Binaries

The easiest way to install Maelstrom binaries is to use
[cargo-binstall](https://github.com/cargo-bins/cargo-binstall), which allows
you to pick and choose the binaries you want to install:

```bash
cargo binstall cargo-maelstrom
cargo binstall maelstrom-go-test
cargo binstall maelstrom-pytest
cargo binstall maelstrom-run
cargo binstall maelstrom-broker
cargo binstall maelstrom-worker
cargo binstall maelstrom-admin
```

These commands retrieve the pre-built binaries from the [Maelstrom GitHub
release page](https://github.com/maelstrom-software/maelstrom/releases).

If you don't have `cargo-binstall`, you can directly install the pre-built
binaries by simply untarring the release artifacts. For example:

```bash
wget -q -O - https://github.com/maelstrom-software/maelstrom/releases/latest/download/cargo-maelstrom-x86_64-unknown-linux-gnu.tgz | tar xzf -
```

This will download and extract the latest release of `cargo-maelstrom` for
Linux on the x86-64 architecture.

The pre-built binaries have shared-library dependencies on libc and OpenSSL.

## Installing on Arch Linux

[David Runge](https://github.com/dvzrv) graciously maintains the Arch Linux
[packages for
Maelstrom](https://archlinux.org/packages/?sort=&q=maelstrom&maintainer=dvzrv).
These can be installed using [`pacman`](https://wiki.archlinux.org/title/Pacman):

```bash
pacman -S cargo-maelstrom
pacman -S maelstrom-go-test
pacman -S maelstrom-pytest
pacman -S maelstrom-run
pacman -S maelstrom-broker
pacman -S maelstrom-worker
pacman -S maelstrom-admin
```

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
cargo install maelstrom-broker
cargo install maelstrom-admin
```

There is an optional `web-ui` feature for `maelstrom-broker`, which is turned
off by default. With this feature enabled, `maelstrom-broker` will serve a
basic web UI dashboard. To enable it, some extra dependencies must be
installed:

```bash
rustup target add wasm32-unknown-unknown
cargo install wasm-opt
cargo install maelstrom-broker --features=web-ui
```
