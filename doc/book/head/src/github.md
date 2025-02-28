# Experimental GitHub Integration

Version 0.13.0 provides experimental integration with GitHub. We hope to refine
and improve this integration in the next few releases. If you're feeling
adventurous, please give it a shot.

Currently, GitHub mode is only meant to run in [GitHub
actions](https://github.com/features/actions), where Maelstrom test runners
(`cargo-maelstrom`, `maelstrom-go-test`, `maelstrom-pytest),
`maelstrom-broker`, and the workers (`maelstrom-worker`) are all running inside
of a GitHub action. This mode allows you to run your CI tests with parallel
workers. What it doesn't do is allow you to use your GitHub workers to run
tests ad hoc from your development machine.

## How It Works

With GitHub actions, we want to create a GitHub job for each worker (running on
its own GitHub worker), along with a GitHub job for the test runner. We want
these jobs to run in parallel so that they can form a cluster to accelerate the
test execution.

The challenge is to get these jobs to talk to each other. GitHub doesn't
provide a way for jobs to talk to each other using normal TCP/IP.

There are two parts of Maelstrom cluster communication. The first is
coordination and metadata transfer. This is the protocol where test runner
communicates with the broker to enqueue jobs to be run, and where the broker
communicates with workers to schedule jobs. It is generally pretty
low-bandwidth, as no container layers (what we call artifacts) are transferred
here.

The second is artifact transfer. This how we distribute container layers
between test runners, the broker, and workers. This can be pretty bandwidth
intensive.

In our experimental GitHub support, we currently use GitHub's artifact store
for both types of communication.

### Cluster Coordination

To deal with cluster communication, we create a sort of stream abstraction on
top of GitHub's artifacts. For each connection, there are two artifacts: one
for each direction. One side appends to one artifact and reads from the other,
and vice versa.

### Artifacts

The use of GitHub's artifact store for Maelstrom artifacts is pretty natural.
We just create artifacts in the store named by their SHA-256 checksums.

## Invocation

To use the GitHub support, you need to have the `ACTIONS_RUNTIME_TOKEN` and
`ACTIONS_RESULTS_URL` environment variables exposed from GitHub. We use the
[`crazy-max/ghaction-github-runtime@v3`](https://github.com/crazy-max/ghaction-github-runtime)
GitHub action to achieve this.

### `maelstrom-broker`

`maelstrom-broker` needs to be run with the environment variables above
exposed, and with the `--artifact-transfer-strategy github` hidden command-line
option.

Currently, `maelstrom-broker` and the test runner need to be run inside the
same GitHub job and use sockets to communicate. Only broker-worker
communication can currently go over GitHub artifacts.

In our CI, we run `maelstrom-broker` in the background like this:

```bash
TEMPFILE=$(mktemp)
maelstrom-broker --artifact-transfer-strategy github 2> >(tee "$TEMPFILE" >&2) &
BROKER_PID=$!
BROKER_PORT=$( \
	tail -f "$TEMPFILE" \
	| awk '/\<addr: / { print $0; exit}' \
	| sed -Ee 's/^.*\baddr: [^,]*:([0-9]+),.*$/\1/' \
)
```

This gives us `BROKER_PORT` to pass to the test runner and `BROKER_PID` to kill
the broker at the end of the GitHub job.

### Test Runner

The test runner needs to be run with the environment variables above exposed,
and with the `--artifact-transfer-strategy github` hidden command-line option.
As discussed [above](#maelstrom-broker), it also needs to be used in the same
GitHub job as the broker. Continuing the example from above, we run it like
this:

```bash
cargo-maelstrom --broker=localhost:$BROKER_PORT --artifact-transfer-strategy github
maelstrom-go-test --broker=localhost:$BROKER_PORT --artifact-transfer-strategy github
maelstrom-pytest --broker=localhost:$BROKER_PORT --artifact-transfer-strategy github
```

### `maelstrom-worker`

The whole point of doing all of this is to run each `maelstrom-worker` in its
own GitHub job. We currently use something like this in our github workflow:

```yaml
  run_worker:
    strategy:
      matrix:
        worker_number: [1, 2, 3, 4]
    name: Run Worker
```

`maelstrom-worker` needs to be run with the environment variables above
exposed, and with the `--artifact-transfer-strategy github` and
`--broker-connection github` options. This is the whole invocation:

```bash
maelstrom-worker --broker=0.0.0.0:0 --artifact-transfer-strategy github --broker-connection github || true
```

The `--broker` config value must currently be specified, even though it's
ignored in this case. We also ignore the exit value from the worker, as it's
not an error when the broker eventually disconnects.

## Examples

Maelstrom's own uses may be helpful examples:
  - [This](https://github.com/maelstrom-software/maelstrom/blob/main/.github/workflows/ci.yml)
    is our `ci.yml`.
  - [This](https://github.com/maelstrom-software/maelstrom/blob/main/scripts/run-tests-on-maelstrom-inner.sh)
    is the script we use to execute the broker and run our tests.
  - [This](https://github.com/maelstrom-software/maelstrom/blob/main/scripts/run-worker-inner.sh)
    is the script we use to execute the worker.
