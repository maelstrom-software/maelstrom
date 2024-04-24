# What is Maelstrom?
Maelstrom is a tool for encapsulating and running large collections of jobs
both locally and on clusters. We've initally focussed on running Rust tests as
jobs. In the near future, we hope to add support for other languages' test
frameworks. Additionally, we've provided tools for adventurous users to run
arbitrary jobs, either using a command-line tool, or using a gRPC-based SDK.

In other words, today Maelstrom is an alternative Rust test runner.

We've put an initial emphasis on running tests as jobs, but jobs can be
any program. Today, we only have fleshed-out support for Rust tests, but in the
future we hope to add other languages' test frameworks.

Practially, Maelstrom is a test runner for Rust projects that:

Maelstrom is a job execution framework with an emphasis on running tests.
Currently, the main ephasis is running Rust tests, but we hope to bring support
more languages' test frameworks

Maelstrom is a Rust test runner built on top of a general-purpose clustered job
runner.

It can be used as an alternate way to run tests for your Rust project that uses
cargo. It provides some advantages over using plain cargo alone.

- Parallelization. Many more tests are run in parallel.
- Distributed. Compute from many machines can be utilized.
- Isolation. Tests are run in their own lightweight containers.

Maelstrom itself is split up into a few different pieces of software.

- **The Broker**. This is the central brain of the clustered job runner. Clients
  and Workers connect to it.
- **The Worker**. There are one or many instances of these. This is what runs
  the actual job (or test.)
- **The Client**. There are one or many instances of these. This is what
  connects to the broker and submits jobs.
- [`cargo-maelstrom`](./cargo_maelstrom.md). This is our cargo test
  replacement which submits tests as jobs by acting as a client.

# What will this book cover?
This guide will attempt to cover the following topics:

- Basic Install. How do you install and configure this for your own projects.
  Both setting up the clustered job runner and using cargo-maelstrom
- `cargo-maelstrom` configuration. Sometimes extra configuration is needed to make
  tests run successfully, this will cover how to do that.
- Clustered job runner management. How clustered job runner works, how to
  configured it, and how to use the
    [web UI](./clustered_job_runner_management/web_ui.md).
