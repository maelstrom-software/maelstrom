# What is Maelstrom?

Maelstrom is a suite of tools for running tests in isolated micro-containers
locally on your machine or distributed across arbitrarily large clusters.
Maelstrom currently has test runners for Rust, Go, and Python, with more on the
way. You might use Maelstrom to run your tests because:

* It's easy. Maelstrom provides drop-in replacements for `cargo test`, `go test`, and
  `pytest`. In most cases, it just works with your existing tests with minimal
  configuration.
* It's reliable. Maelstrom runs every test isolated in its own lightweight
  container, eliminating confusing errors caused by inter-test or implicit
  test-environment dependencies.
* It's scalable. Maelstrom can be run as a cluster. You can add more worker machines to
  linearly increase test throughput.
* It's clean. Maelstrom has built a rootless container implementation (not
  relying on Docker or RunC) from scratch, in Rust, optimized to be
  low-overhead and start quickly.
* It's fast. In most cases, Maelstrom is faster than `cargo test` or `go test`,
  even without using clustering. Maelstrom’s test-per-process model is inherently
  slower than Pytest’s shared-process model, but Maelstrom provides test
  isolation at a low performance cost.

While our focus thus far has been on running tests, Maelstrom's underlying job
execution system is general-purpose. We provide a command line utility to run
arbitrary commands, as well a gRPC-based API and Rust bindings for programmatic
access and control.

The project is currently Linux-only (x86 and ARM), as it relies on namespaces
to implement containers.

## Structure of This Book

This book will start out covering how to [install](installation.md) Maelstrom.
Next, it will cover [common concepts](common.md) that are applicable to all
Maelstrom components, and [other concepts](client-specific-concepts.md) that
are specific to all Maelstrom clients. After that, there are in-depth chapters
for each of the six binaries: [`cargo-maelstrom`](cargo-maelstrom.md),
[`maelstrom-go-test`](go-test.md), [`maelstrom-pytest`](pytest.md),
[`maelstrom-run`](run.md), [`maelstrom-broker`](broker.md), and
[`maelstrom-worker`](worker.md).

There is no documentation yet for the gRPC API or the Rust bindings. Contact us
if you're interested in using them, and we'll help get you started.
