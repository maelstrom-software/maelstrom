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
