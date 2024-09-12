# Programs

Maelstrom comprises a number of different programs, split into two main
categories: clients and daemons.

## Clients

Clients include the test runners &mdash;
[`cargo-maelstrom`](cargo-maelstrom.md), [`maelstrom-go-test`](go-test.md), and
[`maelstrom-pytest`](pytest.md) &mdash; plus the CLI tool
[`maelstrom-run`](run.md).

### Test Runners

Test runners are the glue between their associated test frameworks (Cargo, Go,
Pytest, etc.) and the Maelstrom system. Test runners know how to build test
binaries (if applicable), what dependencies to package up into containers to
run the test binaries, and how to execute individual tests using the test
binaries. They then use this information to build tests, execute them on the
Maelstrom system, and then collect and present the results.

We currently have three test runners, but we're working to add more quickly.
Please let us know if there is a specific test framework you are interested in,
and we'll work to prioritize it.

### `maelstrom-run`

In addition to the test runners, there is also a general-purpose CLI for
running arbitrary jobs in the Maelstrom environment: `maelstrom-run`.
This program can be useful in a few different scenarios.

First, it can be used to explore and debug containers used by tests. Sometimes,
if you can't figure out why a test fails in a container but succeeds otherwise,
it's useful to enter the container environment and poke around. You can try
running test manually, exploring the directory structure, etc. This is done by
running `maelstrom-run --tty`. In this scenario, it's very similar in feel to
`docker exec -it`. 

Second, it can be used in a script to execute arbitrary programs on the
cluster. Let's say you had a Monte Cargo simulation program and you wanted to
run it thousands of times, in parallel, on your Maelstrom cluster. You could
use `maelstrom-run` to do this easily, either from the command-line or from a
script.

## Daemons

Unless configured otherwise, Maelstrom clients will execute jobs locally, in
[standalone mode](local-worker.md). Put another way: clustering is completely
optional. While standalone mode can be useful in certain applications,
Maelstrom becomes even more powerful when jobs are executed on a cluster. The
Maelstrom daemon programs are used to create Maelstrom clusters.

### `maelstrom-worker`

Each Maelstrom cluster must have at least one worker. Workers are where jobs
are actually executed. The worker executes each jobs in its own rootless
container. Our custom-built container implementation ensures that there is very
little overhead for container startup or teardown.

### `maelstrom-broker`

Each Maelstrom cluster has exactly one broker. The broker coordinates between
clients and workers. It caches artifacts, and schedules jobs on the workers.

The broker should be run on a machine that has good network connectivity with
both the workers and clients, and which has a reasonably large amount of disk
space available for caching artifacts.

## Summary

You'll probably be mostly interested in a specific Maelstrom client: the test
runner for your test framework. You may also be interested in the
general-purpose `maelstrom-run` client, either for scripting against the
cluster, or for exploring the containers used by your tests.

If you have access to multiple machines and want to build a Maelstrom cluster,
you'll need to install one instance of the broker daemon and as many instances
of the worker daemon as you have available machines.
