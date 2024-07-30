# `maelstrom-go-test`

`maelstrom-go-test` is a replacement for `go test` which runs tests in
lightweight containers, either locally or on a distributed cluster. Since each
test runs in its own container, it is isolated from the computer it is running
on and from other tests.

For a lot of projects, `maelstrom-go-test` will run all tests successfully
right out of the box. Some tests, though, have external dependencies that cause
them to fail when run in `maelstrom-go-test`'s default, stripped-down
containers. When this happens, it's usually pretty easy to configure
`maelstrom-go-test` so that it invokes the test in a container that contains
all of the necessary dependencies. The [Job
Specification](go-test/spec.md) chapter goes into detail about how to
do so.

# Running Tests

As described [here](dirs.html#project-directory), `maelstrom-go-test` finds the
project directory and then proceding up the directory tree until a `go.mod`
file is found. This means that `maelstrom-go-test` will run all the tests for
the main module, even if it is invoked in a subdirectory of the module. If you
want to restrict the tests to run, use the [`--include` and `--exclude`
options](go-test/cli.md#include-and-exclude).

Similar to `go test`, `maelstrom-go-test` won't run any tests from nested
modules. For these you need to change to the root or a sub-directory of the
module-root for these modules.

Currently we don't support go's test caching, coverage instrumentation,
profiling, or benchmarking.

We run fuzz tests and examples in the same way that `go test` does (without any
special options). If you have corpus files you need to make sure you copy them
into the test container using the [Job Specification](go-test/spec.md).

`maelstrom-go-test` doesn't currently do fuzzing beyond the included corpus
entries.
