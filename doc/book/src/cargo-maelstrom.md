# cargo-maelstrom

cargo-maelstrom is a replacement for `cargo test` which will run tests as jobs
on a distributed clustered job runner. Each test runs in a lightweight container
where it is isolated from computer it is running on and other tests.

Running your tests using it can be as simple as running `cargo maelstrom`
instead of `cargo test`, (see [Running
Tests](./cargo_maelstrom/running_tests.md) for details) but due to the tests
running in a very isolated environment by default, there can be some
configuration required to make all your tests pass (see
[Configuration](./cargo_maelstrom/configuration.md).)

# Running Tests

In order to run tests we will need to have a clustered job runner running
somewhere first. (See [Installing Clustered Job
Runner](../install/clustered_job_runner.md))

Also ensure you've installed `cargo-maelstrom` (See [Installing
cargo-maelstrom](../install/cargo_maelstrom.md).)

We need to provide the address of the broker to cargo-maelstrom. This can be
done via the command line by passing `--broker`. Since this is required every
time the command is run, it is easier to provide it via the configuration file.
See [Configuration](./configuration.md) for more details.

Create a file in `~/.config/maelstrom/cargo-maelstrom/config.toml` and put the
following contents

```toml
broker = "<broker-address>:<broker-port>"
```

Replace `<broker-address>` with the hostname or IP of the machine the broker is
running on. Replace `<broker-port>` with the port you provided to the broker via
the `--port` CLI option. (In the book we chose port 9001 for this)

Now we should be able to build and run tests just by running `cargo maelstrom`

With no other arguments it will attempt to run all the tests in your project.

The first time you run it you may notice that the progress bar is not all that
useful, this is because it has no way of predicting how many tests will run.
This should be improved with subsequent invocations. See [Test
Listing](#test-listing)

# Terminal Output
By default `cargo-maelstrom` prints the name of the tests that have been
completed to stdout. It also displays four progress bars indicating the state of
jobs. See [Job States](../job-states.md).

When tests fail, their stderr is printed out after the name (also to stdout.)

Before the progress bars there is a "spinner" which provides insight into what
the test job enqueuing thread is doing. It disappears once all tests are
enqueued.

Before running tests, `cargo-maelstrom` always invokes `cargo` to build the
tests before running them. This happens in parallel with enqueuing the tests to
run. If the build fails for some reason, `cargo-maelstrom` will abruptly stop
and print out the build output.

Providing the `--quiet` flag will show only a single progress bar and provide no
other output.

If stdout isn't a TTY, no progress bars are displayed, and color is disabled.
