# GitHub Integration

In v0.13.0, we introduced integration with GitHub. We hope to refine and
improve this integration in the next few releases. Please give it a try and let
us know what your experience is.

Currently, GitHub mode is only meant to run in [GitHub
actions](https://github.com/features/actions), where Maelstrom test runners
(`cargo-maelstrom`, `maelstrom-go-test`, `maelstrom-pytest`),
`maelstrom-broker`, and the workers (`maelstrom-worker`) are all running inside
of a single GitHub workflow. This mode allows you to run your CI tests with
parallel workers. What it doesn't do (yet) is allow you to use your GitHub
workers to run tests ad hoc from your development machine.

## How It Works

(In this section, when we talk about "jobs", we're referring to the [GitHub
Actions
concept](https://docs.github.com/en/actions/about-github-actions/understanding-github-actions#jobs),
not the [Maelstrom concept](jobs.md).)

Inside of a workflow, you start multiple, parallel jobs. One of those jobs runs
the broker, and the rest run worker instances. These jobs form a Maelstrom
cluster that you can then use from any number of other jobs in the workflow.

When a Maelstrom test runner is run from a GitHub job inside the workflow,
instead of running the test on the job's runner, the test is passed to one of
the Maelstrom workers to run. In this way, the tests all run in parallel in
multiple jobs, on separate runners.

When all of the jobs that require the Maelstrom cluster have completed, you
stop the Maelstrom cluster with [`maelstrom-admin`](admin.md), which is
provided by the
[`maelstrom-admin-action`](https://github.com/maelstrom-software/maelstrom-admin-action).

The main implementation challenge we face is that GitHub doesn't allow jobs
to communicate directly with each other using normal TCP/IP. To work around
this, we use the [artifact
store](https://docs.github.com/en/actions/writing-workflows/choosing-what-your-workflow-does/storing-and-sharing-data-from-a-workflow).

## Broker Job

You must start one instance of the broker on its own job. This should be
started at the beginning of the workflow. It will be terminated when the
Maelstrom cluster is stopped, as discussed below.

The broker job is run using the
[`maelstrom-broker-action`](https://github.com/maelstrom-software/maelstrom-broker-action):

```yml
jobs:
  # [Other jobs...]

  maelstrom-broker:
    name: Maelstrom Broker
    runs-on: ubuntu-24.04
    steps:
    - name: Install and Run Maelstrom Broker
      uses: maelstrom-software/maelstrom-broker-action@v1
```

## Worker Jobs

You must start some number of worker instances on their own jobs. These should
be started at the beginning of the workflow as well. They will be terminated
when the broker is stopped.

A worker job is run using the
[`maelstrom-broker-action`](https://github.com/maelstrom-software/maelstrom-broker-action).
It's recommended that you use a matrix strategy to start a number of them:

```yml
jobs:
  # [Other jobs...]

  maelstrom-worker:
    strategy:
      matrix:
        worker-number: [1, 2, 3, 4]
    name: Maelstrom Worker ${{ matrix.worker-number }}
    runs-on: ubuntu-24.04
    steps:
    - name: Install and Run Maelstrom Worker
      uses: maelstrom-software/maelstrom-worker-action@v1
```

The worker can be run on an Ubuntu x86-64 image (e.g. `ubuntu-24.04`), or an
Ubuntu AArch64 image (e.g. `ubuntu-24.04-arm`). The architecture must match the
architecture of the client jobs.

## Client Jobs

You can run any number of client jobs: jobs that run Maelstrom clients that
talk to the workflow's Maelstrom cluster. You set that up like this:

```yml
jobs:
  # [Other jobs...]

  run-tests-1:
    name: Run Tests 1
    runs-on: ubuntu-24.04
    steps:
    - name: Check Out Repository
      uses: actions/checkout@v4
    - name: Install and Configure cargo-maelstrom
      uses: maelstrom-software/cargo-maelstrom-action@v1
    - name: Run cargo-maelstrom
      run: cargo maelstrom

  run-tests-2:
    name: Run Tests 2
    runs-on: ubuntu-24.04
    steps:
    - name: Check Out Repository
      uses: actions/checkout@v4
    - name: Install and Configure maelstrom-go-test
      uses: maelstrom-software/maelstrom-go-test-action@v1
    - name: Run cargo-maelstrom
      run: maelstrom-go-test

  stop-maelstrom:
    name: Stop Maelstrom Cluster
    runs-on: ubuntu-24.04
    if: ${{ always() }}
    needs: [run-tests-1, run-tests-2]
    steps:
    - name: Install and Configure maelstrom-admin
      uses: maelstrom-software/maelstrom-admin-action@v1
    - name: Stop Maelstrom Cluster
      run: maelstrom-admin stop
```

The main points are:
  - You run the appropriate action for the Maelstrom test runner(s) you want to use
    ([`cargo-maelstrom-action`](https://github.com/maelstrom-software/cargo-maelstrom-action)
    or
    [`maelstrom-go-test-action`](https://github.com/maelstrom-software/maelstrom-go-test-action)
    in this example). This installs the test runner and configures it to use
    the Maelstrom cluster running in the workflow.
  - When all client jobs are done, you use the
    [`maelstrom-admin-action`](https://github.com/maelstrom-software/maelstrom-admin-action)
    to install `maelstrom-admin`, which you then run to stop the cluster. This
    is what stops the broker and worker jobs running in parallel.
  - You want to guarantee that job is run to stop the cluster, even if one of
    of the client job fails. For this reason, it's advisable to run it in its
    own job with `if: ${{ always() }}`. You should probably do this even if you only
    have a single client job.
  - You don't have to wait for the Maelstrom cluster to start before running
    client jobs: The clients and workers will wait for the broker to start.

## Hardware Architecture

Maelstrom only support single-architecture clusters. The clients and the
workers all need to be running on the same architecture (x86-64 or ARM64).
Technically, neither `maelstrom-broker` nor `maelstrom-admin` whether their
architectures match those of the clients or workers, but it's usually easiest
to just run everything on one architecture.

In the future, we
[plan](https://github.com/maelstrom-software/maelstrom/issues/495) to allow
clusters to support multiple architectures.

If you need to run Maelstrom test runners against multiple architectures in CI,
one solution is to use [GitHub Workflow
Templates](https://docs.github.com/en/actions/writing-workflows/using-workflow-templates).
That's what we do in the [examples
repository](https://github.com/maelstrom-software/maelstrom-examples/tree/main/.github/workflows).

## Example

You can look at the workflow in the example repository
[here](https://github.com/maelstrom-software/maelstrom-examples/blob/main/.github/workflows/ci-base.yml).

Here is the example from above all put together:

```yml
jobs:
  maelstrom-broker:
    name: Maelstrom Broker
    runs-on: ubuntu-24.04
    steps:
    - name: Install and Run Maelstrom Broker
      uses: maelstrom-software/maelstrom-broker-action@v1

  maelstrom-worker:
    strategy:
      matrix:
        worker-number: [1, 2, 3, 4]
    name: Maelstrom Worker ${{ matrix.worker-number }}
    runs-on: ubuntu-24.04
    steps:
    - name: Install and Run Maelstrom Worker
      uses: maelstrom-software/maelstrom-worker-action@v1

  run-tests-1:
    name: Run Tests 1
    runs-on: ubuntu-24.04
    steps:
    - name: Check Out Repository
      uses: actions/checkout@v4
    - name: Install and Configure cargo-maelstrom
      uses: maelstrom-software/cargo-maelstrom-action@v1
    - name: Run cargo-maelstrom
      run: cargo maelstrom

  run-tests-2:
    name: Run Tests 2
    runs-on: ubuntu-24.04
    steps:
    - name: Check Out Repository
      uses: actions/checkout@v4
    - name: Install and Configure maelstrom-go-test
      uses: maelstrom-software/maelstrom-go-test-action@v1
    - name: Run cargo-maelstrom
      run: maelstrom-go-test

  stop-maelstrom:
    name: Stop Maelstrom Cluster
    runs-on: ubuntu-24.04
    if: ${{ always() }}
    needs: [run-tests-1, run-tests-2]
    steps:
    - name: Install and Configure maelstrom-admin
      uses: maelstrom-software/maelstrom-admin-action@v1
    - name: Stop Maelstrom Cluster
      run: maelstrom-admin stop
```
