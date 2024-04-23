![Maelstrom Logo (Dark Compatible)](https://github.com/maelstrom-software/maelstrom/assets/146376379/7b46a1c1-e67f-412a-b618-42f7e2c25139)

![Crates.io](https://img.shields.io/crates/v/cargo-maelstrom)
[![CI](https://github.com/maelstrom-software/maelstrom/actions/workflows/ci.yml/badge.svg)](https://github.com/maelstrom-software/maelstrom/actions/workflows/ci.yml)
[![Discord](https://img.shields.io/discord/1197610263147462736)](https://discord.gg/WGacKK5eZz)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Maelstrom is a Rust test runner, built on top of a general-purpose
clustered job runner. Maelstrom packages your Rust tests into hermetic
micro-containers, then distributes them to be run on an arbitrarily large
cluster of test-runners, or locally on your machine. You might use
Maelstrom to run your tests because:

* It's easy. Maelstrom functions as a drop-in replacement for `cargo test`, so in
  most cases, it just works.
* It's reliable. Maelstrom runs every test hermetically in its own lightweight
  container, eliminating confusing errors caused by inter-test or implicit
  test-environment dependencies.
* It's scalable. Maelstrom can be run as a cluster. You can add more worker machines to
  linearly improve test throughput.
* It's fast. In most cases, Maelstrom is faster than `cargo test`, even
  without adding clustering.
* It's clean. Maelstrom has a from-scratch, rootless container implementation
  (not relying on docker or RunC), optimized to be low-overhead and start
  quickly.
* It's Rusty. The whole project is written in Rust.

We started with a Rust test runner, but Maelstrom's underlying job
execution system is general-purpose. We will add support for other languages' test frameworks throughout 2024. We have also provided tools for
adventurous users to run arbitrary jobs, either using a command-line tool or a gRPC-based SDK.

The project is currently Linux-only (x86 and ARM), as it relies on namespaces
to implement containers.

See the book for more information:
[Maelstrom Book](https://maelstrom-software.com/book/)

# Getting Started



## Installing `cargo-maelstrom`

To run your tests using Maelstrom, you need the `cargo-maelstrom`
binary. The easiest way to get it is using
[cargo-binstall](https://github.com/cargo-bins/cargo-binstall):

```bash
cargo binstall cargo-maelstrom
```

This will install a pre-built binary from the [github releases page](https://github.com/maelstrom-software/maelstrom/releases).

If you don't have `cargo-binstall`, you can download the binary manually.

Check out [the book](https://maelstrom-software.com/book/install/cargo_maelstrom.html) for more ways to get `cargo-maelstrom`.

## Running `cargo-maelstrom`

To run your Rust tests, use `cargo-maelstrom`:

```bash
cargo maelstrom
```

This runs in "standalone" mode, meaning all tests are run locally. Each test is
run in its own container, configured with a few common dependencies. It may work for your project without any further configuration.

If some tests fail, however, it likely means those tests have dependencies on
their execution environment that aren't packaged in their containers.
You can remedy this by adding directives to the `maelstrom-test.toml` file. To
do this, run:

```bash
cargo maelstrom --init
```

Then edit the created `maelstrom-test.toml` file as described [in the book](https://maelstrom-software.com/book/cargo_maelstrom/execution_environment.html).

## Setting Up a Cluster

To get even more out of Maelstrom, you can set up a cluster to run your tests on.
You will need to run one copy of the broker (`maelstrom-broker`) somewhere, and
one copy of the worker (`maelstrom-worker`) on each node of the cluster.

You can install these using [multiple
methods](https://maelstrom-software.com/book/install/clustered_job_runner.html),
including `cargo-binstall`:

```bash
cargo binstall maelstrom-worker maelstrom-broker
```

Then you can start the broker:

```bash
maelstrom-broker --port=1234
```

Then a few workers:

```bash
maelstrom-worker --broker=broker-host:1234
```

And then run `cargo-maelstrom` against the cluster:

```bash
cargo maelstrom --broker=broker-host:1234
```

# Learn More

Find our complete documentation in the [book] (https://maelstrom-software.com/book/).

# Licensing

This project is available under the terms of either the Apache 2.0 license or the MIT license.
