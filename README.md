![Maelstrom Logo (Dark Compatible)](https://github.com/maelstrom-software/maelstrom/assets/146376379/7b46a1c1-e67f-412a-b618-42f7e2c25139)

[![CI](https://github.com/maelstrom-software/maelstrom/actions/workflows/ci.yml/badge.svg)](https://github.com/maelstrom-software/maelstrom/actions/workflows/ci.yml)

| crate            | badges                                                                                                                                       |
| ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------  |
| cargo-maelstrom  | [![cargo-maelstrom](https://img.shields.io/crates/v/cargo-maelstrom.svg)](https://crates.io/crates/cargo-maelstrom) [![maelstrom-client](https://img.shields.io/docsrs/cargo-maelstrom)](https://docs.rs/cargo-maelstrom)     |
| maelstrom-run    | [![maelstrom-run](https://img.shields.io/crates/v/maelstrom-run.svg)](https://crates.io/crates/maelstrom-run) [![maelstrom-client](https://img.shields.io/docsrs/maelstrom-run)](https://docs.rs/maelstrom-run)               |
| maelstrom-client | [![maelstrom-client](https://img.shields.io/crates/v/maelstrom-client.svg)](https://crates.io/crates/maelstrom-client) [![maelstrom-client](https://img.shields.io/docsrs/maelstrom-client)](https://docs.rs/maelstrom-client)|
| maelstrom-broker | [![maelstrom-broker](https://img.shields.io/crates/v/maelstrom-broker.svg)](https://crates.io/crates/maelstrom-broker) [![maelstrom-client](https://img.shields.io/docsrs/maelstrom-broker)](https://docs.rs/maelstrom-broker)|
| maelstrom-worker | [![maelstrom-worker](https://img.shields.io/crates/v/maelstrom-worker.svg)](https://crates.io/crates/maelstrom-worker) [![maelstrom-client](https://img.shields.io/docsrs/maelstrom-worker)](https://docs.rs/maelstrom-worker)|


Maelstrom is an extremely fast Rust test runner built on top of a
general-purpose clustered job runner. Maelstrom packages your Rust tests into
hermetic micro-containers, then distributes them to be run on an arbitrarily
large cluster of test-runners. You should use Maelstrom to run your tests
because:

* It's easy. Maelstrom functions as a drop-in replacement for cargo-test, so in most cases, it just works.
* It's reliable. Maelstrom runs every test hermetically in its own lightweight container and runs each test independently, eliminating confusing test errors caused by inter-test or test-environment dependencies.
* It's scalable. Add more worker machines to linearly improve test throughput.
* It works everywhere. Maelstrom workers are rootless containers so you can run them securely, anywhere.
* It's clean. Maelstrom has a home-grown container implementation (not relying on docker or RunC), optimized to be low-overhead and start quickly.
* It's Rusty. The whole project is built in Rust.

Maelstrom is currently available for Rust on Linux. C++, Typescript, Python, and Java are coming soon.

See the book for more information:
[Maelstrom Book](https://maelstrom-software.com/book/)

# Design

![Architecture](https://github.com/maelstrom-software/maelstrom/assets/146376379/07209c96-b529-45b6-a215-8c0c1a713795)

Maelstrom is split up into a few different pieces of software.

* The Broker. This is the central brain of the clustered job runner. Clients and Workers connect to it.
* The Worker. There are one or many instances of these. This is what runs the actual job (or test.)
* The Client. There are one or many instances of these. This is what connects to the broker and submits jobs.
* cargo-maelstrom. This is our cargo test replacement which submits tests as jobs by acting as a client.

# Getting Started

## Installing cargo-maelstrom

Keep in mind that `cargo-maelstrom` only works on Linux.

Once installed you should be able to invoke it by running `cargo maelstrom`

### Installing from Pre-Built Binary

The easiest way to install it is using [cargo-binstall](https://github.com/cargo-bins/cargo-binstall).

```bash
cargo binstall cargo-maelstrom
```

This will install a pre-built binary from the [github releases page](https://github.com/maelstrom-software/maelstrom/releases).

If you don't have `cargo-binstall`, you can always download the binaries manually. If we don't have
a binary for your specific architecture, you can compile from source.

### Compiling from Source

First make sure you've installed [Rust](https://www.rust-lang.org/tools/install).

Then use cargo to download, and compile `cargo-maelstrom` from source

```bash
export GITHUB_URL="https://github.com/maelstrom-software/maelstrom.git"
cargo install --git $GITHUB_URL cargo-maelstrom
```

## Installing Clustered Job Runner

This covers setting up the clustered job runner. This is split into two
different parts.

- **The Broker**. This is the brains of the clustered job runner, clients and
  workers connect to it.
- **The Worker**. There are one or many of these running on the same or different
  machines from each other and the broker.

The broker and the worker only work on Linux.

### Installing from Pre-Built Binary

The easiest way to install both is using [cargo-binstall](https://github.com/cargo-bins/cargo-binstall).

```bash
cargo binstall maelstrom-broker maelstrom-worker
```

This will install a pre-built binary from the [github releases page](https://github.com/maelstrom-software/maelstrom/releases).

If you don't have `cargo-binstall`, you can always download the binaries manually. If we don't have
a binary for your specific architecture, you can compile from source.


### Compiling the Broker from Source

We will use cargo to compile and install the broker, but first we need to install some dependencies.
First make sure you've installed
[Rust](https://www.rust-lang.org/tools/install). Then install these other
required things.

```bash
rustup target add wasm32-unknown-unknown
cargo install wasm-opt
```

Now we can compile the broker

```bash
export GITHUB_URL="https://github.com/maelstrom-software/maelstrom.git"
cargo install --git $GITHUB_URL maelstrom-broker
```

### Compiling the Worker from Source

First make sure you've installed [Rust](https://www.rust-lang.org/tools/install).

Install the worker with

```bash
export GITHUB_URL="https://github.com/maelstrom-software/maelstrom.git"
cargo install --git $GITHUB_URL maelstrom-worker
```

### Configuring the Worker and Broker
See the [installation section of the book](https://maelstrom-software.com/book/install/clustered_job_runner.html)

# Licensing

This project is available under the terms of either the Apache 2.0 license or the MIT license.
