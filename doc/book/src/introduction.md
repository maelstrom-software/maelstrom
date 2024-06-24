# What is Maelstrom?

Maelstrom is a suite of tools for running tests in hermetic micro-containers
locally on your machine or distributed across arbitrarily large clusters.
Maelstrom currently has test runners for Rust and Python, with more on the
way.

* It's easy. Maelstrom provides a drop-in replacement for `cargo test`, and a
  pytest plugin. In most cases, it just works with your existing tests without
  any configuration.
* It's reliable. Maelstrom runs every test hermetically in its own lightweight
  container, eliminating confusing errors caused by inter-test or implicit
  test-environment dependencies.
* It's scalable. Maelstrom can be run as a cluster. You can add more worker machines to
  linearly increase test throughput.
* It's fast. In most cases, Maelstrom is faster than `cargo test`, even
  without using clustering.
* It's clean. Maelstrom has built a rootless container implementation (not
  relying on Docker or RunC) from scratch, in Rust, optimized to be
  low-overhead and start quickly.

While our main focus thus far has been on running tests, Maelstrom's underlying
job execution system is general-purpose. We provide a general-purpose command
line utility to run arbitrary commands, as well a gRPC-based API and Rust
bindings for programmatic access and control.

The project is currently Linux-only (x86 and ARM), as it relies on namespaces
to implement containers.

## Structure of This Book

This book will start out covering how to [install](installation.md) Maelstrom.
Next, it will cover [common concepts](common.md) that are applicable to all
Maelstrom components. After that, there are in-depth sections for each of the
four binaries: [`cargo-maelstrom`](cargo-maelstrom.md),
[`maelstrom-broker`](broker.md), [`maelstrom-worker`](worker.md), and
[`maelstrom-run`](run.md).

There is no documentation yet for the gRPC API. Contact us if you're interested
in using it, and we'll help get you started.
