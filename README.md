![Maelstrom Logo (Dark Compatible)](https://github.com/maelstrom-software/maelstrom/assets/146376379/7b46a1c1-e67f-412a-b618-42f7e2c25139)

[![CI](https://github.com/maelstrom-software/maelstrom/actions/workflows/ci.yml/badge.svg)](https://github.com/maelstrom-software/maelstrom/actions/workflows/ci.yml)
[![Discord](https://img.shields.io/discord/1197610263147462736)](https://discord.gg/WGacKK5eZz)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Maelstrom is a fast Rust test runner built on top of a general-purpose
clustered job runner. Maelstrom packages your Rust tests into hermetic
micro-containers, then distributes them to be run on an arbitrarily large
cluster of test-runners, or locally on your machine. You might want to use
Maelstrom to run your tests because:

* It's easy. Maelstrom functions as a drop-in replacement for `cargo test`, so in
  most cases, it just works.
* It's reliable. Maelstrom runs every test hermetically in its own lightweight
  container, eliminating confusing test errors caused by inter-test or implicit
  test-environment dependencies.
* It's scalable. Maelstrom can be run as a cluster. Add more worker machines to
  linearly improve test throughput.
* It's fast. On most projects it's significantlyfaster than `cargo test`, even
  without adding clustering.
* It's clean. Maelstrom has a from-scratch, rootless container implementation
  (not relying on docker or RunC), optimized to be low-overhead and start
  quickly.
* It's Rusty. The whole project is written in Rust.

We've initially focussed on Rust tests, but Maelstrom's underlying job
execution system is general-purpose. In the near future, we will add support
for other languages' test frameworks. Additionally, we've provided tools for
adventurous users to run arbitrary jobs, either using a command-line tool, or
using a gRPC-based SDK.

The project is currently Linux-only (x86 and ARM), as it relies on namespaces
to implement containers.

See the book for more information:
[Maelstrom Book](https://maelstrom-software.com/book/)

# Installing `cargo-maelstrom`

To run your tests using Maelstrom, you're going to need the `cargo-maelstrom`
binary. The easiest way to get it is using
[cargo-binstall](https://github.com/cargo-bins/cargo-binstall):

```bash
cargo binstall cargo-maelstrom
```

This will install a pre-built binary from the [github releases page](https://github.com/maelstrom-software/maelstrom/releases).

If you don't have `cargo-binstall`, you can always download the binary manually.

You can learn about more ways to get `cargo-maelstrom` [in the book](https://maelstrom-software.com/book/install/cargo_maelstrom.html).

# Running `cargo-maelstrom`

To run your Rust tests just use `cargo-maelstrom`:

```bash
cargo maelstrom
```

This runs in "standalone" mode, meaning all tests are run locally. Each test is
run in its own container configured with a few common dependencies. It may work
straight out of the box for your project.

If some tests fail, it probably means those tests have some dependencies on
their execution environment that aren't being packaged in their containers.
This can be remedied by adding directives to the `maelstrom-test.toml` file. To
do this, run:

```bash
cargo maelstrom --init
```

Then edit the created `maelstrom-test.toml` file as described [in the book](https://maelstrom-software.com/book/cargo_maelstrom/execution_environment.html).

# Setting Up a Cluster

To get more out of Maelstrom, you can set up a cluster to run your tests on.
You'll need to run one copy of the broker (`maelstrom-broker`) somewhere, plus
one copy of the worker (`maelstrom-worker`) on each node of the cluster.

These can be installed using [many
methods](https://maelstrom-software.com/book/install/clustered_job_runner.html),
including `cargo-binstall`:

```bash
cargo binstall maelstrom-worker maelstrom-broker
```

You can now start the broker:

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

# Learning More

There is much more documentation in the [book] (https://maelstrom-software.com/book/).

# Licensing

This project is available under the terms of either the Apache 2.0 license or the MIT license.
