# Running Tests

In order to run tests we will need to have a clustered job runner running
somewhere first. (See [Installing Clustered Job
Runner](../install/clustered_job_runner.md))

Also ensure you've installed `cargo-maelstrom` (See [Installing
cargo-maelstrom](../install/cargo_maelstrom.md).)

We need to provide the address of the broker to cargo-maelstrom. This can be done
via the command line by passing `--broker`, but since you have to provide it
every time it is easer to provide it via the configuration file.

Create a file in `.config/cargo-maelstrom.toml` and put the following contents

```toml
broker = "<broker-address>:<broker-port>"
```

Replace `<broker-address>` with the hostname or IP of the machine the broker is
running on. Replace `<broker-port>` with the port you provided to the broker via
the `--port` CLI option. (In the book we chose port 9001 for this)

Now we should be able to build and run tests just by running `cargo metest`

With no arguments it attempt to run all the tests in your project.

The first time you run it you may notice that the progress bar is not all that
useful, this is because it has no way of predicting how many tests will run.
This should be improved with subsequent invocations. See [Test
Listing](#test-listing)

# Terminal Output
By default `cargo-maelstrom` prints the name of the tests that have been completed
to stdout. It also displays four progress bars indicating the state of jobs. See
[Job States](../clustered_job_runner_management/job_states.md).

When tests fail, their stderr is printed out after the name (also to stdout.)

Before the progress bars there is a "spinner" which provides insight into what
the test job enqueuing thread is doing. It disappears once all tests are
enqueued.

Before running tests, `cargo-maelstrom` always runs invokes `cargo` to build the
tests before running them. This happens in parallel with enqueuing the tests to
run. If the build fails for some reason, `cargo-maelstrom` will abruptly stop and
print out the build output.

Providing the `--quiet` flag will show only a single progress bar and provide no
other output.

If stdout isn't a TTY, no progress bars are displayed, and color is disabled.

# Caching

`cargo-maelstrom` caches some things in the `target/` directory, these things are
covered below. It also caches some things related to containers (not in the
`target/` directory) and that is covered in [Using Container
Images](../cargo_maelstrom/using_container_images.md).

## `.tar` files
As part of running tests as jobs in the clustered job runner, `cargo-maelstrom`
must create certain temporary `.tar` files. It stores these files alongside the
build artifacts that `cargo` produces. At the moment nothing cleans up stale
ones.

## Test Listing
A listing of all the tests in your project that `cargo-maelstrom` found is stored
in `target/maelstrom-test-listing.toml` file. This listing is used to predict the
amount of tests that will be run with subsequent invocations.

## File Digests
Files uploaded to the broker are identified via a hash of the file contents.
Calculating these hashes can be time consuming so `cargo-maelstrom` caches this
information. This can be found in `target/meticulous-cached-digests.toml`. The
cache works by recording the hash and the mtime of the file from when the hash
was calculated. If the file has a different mtime than what is recorded, the
entry is known to be stale.
