# Experimental GitHub Integration

Version 0.13.0 provides experimental integration with GitHub. We hope to refine
and improve this integration in the next few releases. If you're feeling
adventurous, please give it a shot.

Currently, GitHub mode is only meant to run in [GitHub
actions](https://github.com/features/actions), where Maelstrom test runners
(`cargo-maelstrom`, `maelstrom-go-test`, `maelstrom-pytest),
`maelstrom-broker`, and the workers (`maelstrom-worker`) are all running inside
of a GitHub action. This mode allows you to run your CI tests with parallel
workers. What it doesn't do (yet) is allow you to use your GitHub workers to
run tests ad hoc from your development machine.

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

The best way to learning how to use this feature is by looking at [this
workflow](https://github.com/maelstrom-software/maelstrom-examples/blob/main/.github/workflows/ci-base.yml)
in the the [Maelstrom examples
repository](https://github.com/maelstrom/software/maelstrom-examples).

You will see that one GitHub job is used to run the broker and all of the test
runners, while worker are run in parallel in other jobs.

We will package this up into a re-usable GitHub action in the near future.
