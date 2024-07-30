![Maelstrom Logo (Dark Compatible)](https://github.com/maelstrom-software/maelstrom/assets/146376379/7b46a1c1-e67f-412a-b618-42f7e2c25139)

![Crates.io](https://img.shields.io/crates/v/cargo-maelstrom)
[![Discord](https://img.shields.io/discord/1197610263147462736)](https://discord.gg/WGacKK5eZz)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![CI](https://github.com/maelstrom-software/maelstrom/actions/workflows/ci.yml/badge.svg)](https://github.com/maelstrom-software/maelstrom/actions/workflows/ci.yml)

Maelstrom is a suite of tools for running tests in hermetic micro-containers
locally on your machine or distributed across arbitrarily large clusters.
Maelstrom currently has test runners for Rust, Go, and Python, with more on the
way. You might use Maelstrom to run your tests because:

* **It's easy.** Maelstrom provides drop-in replacements for `cargo test`, `go
  test`, and `pytest`. In most cases, it just works with your existing tests
  with minimal configuration.
* **It's reliable.** Maelstrom runs every test hermetically in its own lightweight
  container, eliminating confusing errors caused by inter-test or implicit
  test-environment dependencies.
* **It's scalable.** Maelstrom can be run as a cluster. You can add more worker
  machines to linearly increase test throughput.
* **It's clean.** Maelstrom has built a rootless container implementation (not
  relying on Docker or RunC) from scratch, in Rust, optimized to be
  low-overhead and start quickly.
* **It's fast.** In most cases, Maelstrom is faster than `cargo test` or `go
  test`, even without using clustering. Maelstrom’s test-per-process model is
  inherently slower than `pytest`’s shared-process model, but Maelstrom
  provides test isolation at a low performance cost.

While our focus thus far has been on running tests, Maelstrom's underlying
job execution system is general-purpose. We provide a command
line utility to run arbitrary commands, as well a gRPC-based API and Rust
bindings for programmatic access and control.

The project is currently Linux-only (x86 and ARM), as it relies on namespaces
to implement containers.

See the [book](https://maelstrom-software.com/doc/book/latest/) for more information.

# Getting Started

## Installing Pre-Built Binaries

To run your tests using Maelstrom, you need a test runner binary.
The easiest way to get it is using
[cargo-binstall](https://github.com/cargo-bins/cargo-binstall):

For Rust tests:

```bash
cargo binstall cargo-maelstrom
```

For Go tests:

```bash
cargo binstall maelstrom-go-test
```

For Python tests:

```bash
cargo binstall maelstrom-pytest
```

This will install a pre-built binary from the [github releases page](https://github.com/maelstrom-software/maelstrom/releases).

If you don't have `cargo-binstall`, you can download the binaries manually.

Check out the [book](https://maelstrom-software.com/doc/book/latest/installation.html) for more ways to get Maelstrom.

## Running `cargo-maelstrom`

To run your Rust tests, use `cargo-maelstrom`:

```bash
cargo maelstrom
```

This runs in "standalone" mode, meaning all tests are run locally. Each test is
run in its own container, configured with a few common dependencies. It may
work for your project without any further configuration.

If some tests fail, however, it likely means those tests have dependencies on
their execution environment that aren't packaged in their containers.
You can remedy this by adding directives to the `cargo-maelstrom.toml` file. To
do this, run:

```bash
cargo maelstrom --init
```

Then edit the created `cargo-maelstrom.toml` file as described in the
[book](https://maelstrom-software.com/doc/book/latest/cargo-maelstrom/spec.html).

## Running `maelstrom-go-test`

To run your Go tests, use `maelstrom-go-test`:

```bash
maelstrom-go-test
```

This runs in "standalone" mode, meaning all tests are run locally. Each test is
run in its own container, configured with a few common dependencies. It may
work for your project without any further configuration.

If some tests fail, however, it likely means those tests have dependencies on
their execution environment that aren't packaged in their containers. You can
remedy this by adding directives to the `maelstrom-go-test.toml` file. To do
this, run:

```bash
maelstrom-go-test --init
```

Then edit the created `maelstrom-go-test.toml` file as described in the
[book](https://maelstrom-software.com/doc/book/latest/go-test/spec.html).

## Running `maelstrom-pytest`

Before running tests, we need to do a little setup.

## Choosing a Python Image
First generate a `maelstrom-pytest.toml` file
```bash
maelstrom-pytest --init
```

Then update the file to include a python image
```toml
[[directives]]
image = "docker://python:3.11-slim"
```
This example installs an [image from Docker](https://hub.docker.com/_/python)

## Including Your Project Python Files
So that your tests can be run from the container, your project's python must be included.
```toml
added_layers = [ { glob = "**.py" } ]
```
This example just adds all files with a `.py` extension. You may also need to include `.pyi` files
or other files.

## Including `pip` Packages
If you have an image named "python", `maelstrom-pytest` will automatically include pip packages for
you as part of the container. It expects to read these packages from a `test-requirements.txt` file
in your project directory. This needs to at a minimum include the `pytest` package

`test-requirements.txt`
```
pytest==8.1.1
```

Now we are ready to try to run tests. Just invoke `maelstrom-pytest`:

```bash
maelstrom-pytest
```

This runs in "standalone" mode, meaning all tests are run locally. Each test is run in its own
container.

## Setting Up a Cluster

To get even more out of Maelstrom, you can set up a cluster to run your tests on.
You will need to run one copy of the broker (`maelstrom-broker`) somewhere, and
one copy of the worker (`maelstrom-worker`) on each node of the cluster.

You can install these using [multiple
methods](https://maelstrom-software.com/doc/book/latest/installation.html),
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

And then run `cargo-maelstrom` or `maelstrom-pytest` against the cluster:

```bash
cargo maelstrom --broker=broker-host:1234
maelstrom-pytest --broker=broker-host:1234
```

# Learn More

Find our complete documentation in the [book](https://maelstrom-software.com/doc/book/latest/).

# Licensing

This project is available under the terms of either the Apache 2.0 license or the MIT license.
