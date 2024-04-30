# What is Maelstrom?

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
  linearly increase test throughput.
* It's fast. In most cases, Maelstrom is faster than `cargo test`, even
  without using clustering.
* It's clean. Maelstrom has a from-scratch, rootless container implementation
  (not relying on docker or RunC), optimized to be low-overhead and start
  quickly.
* It's Rusty. The whole project is written in Rust.

We started with a Rust test runner, but Maelstrom's underlying job execution
system is general-purpose. We will add support for other languages' test
frameworks in the near future. We have also provided tools for adventurous users
to run arbitrary jobs, either using a command-line tool or a gRPC-based SDK.

The project is currently Linux-only (x86 and ARM), as it relies on namespaces
to implement containers.

## Structure of This Book

This book will start out covering how to [install](installation.md) Maelstrom.
Next, it will cover [common concepts](common.md) that are applicable to all
Maelstrom components. After that, there are in-depth sections for each of the
four binaries: [`cargo-maelstrom`](cargo-maelstrom.md),
[`maelstrom-broker`](broker.md), [`maelstrom-worker`](worker.md), and
[`maelstrom-run`](run.md).
