# cargo-maelstrom

`cargo-maelstrom` is a replacement for `cargo test` which runs tests in
lightweight containers, either locally or on a distributed cluster.
Since each test runs in its own container, it is isolated from computer it is
running on and from other tests.

`cargo-maelstrom` is designed to be run as a [custom Cargo
subcommand](https://doc.rust-lang.org/book/ch14-05-extending-cargo.html). One
can either run it as `cargo-maelstrom` or as `cargo maelstrom`.

For a lot of projects, `cargo-maelstrom` will run all tests successfully, right
out of the box. Some tests, though, have external dependencies that cause them
to fail when run in `cargo-maelstrom`'s default, stripped-down containers. When
this happens, it's usually pretty easy to configure `cargo-maelstrom` so that
it invokes the test in a container that contains all of the necessary
dependencies. The [Job Specification](cargo-maelstrom/spec.md) chaper goes into
detail about how to do so.
