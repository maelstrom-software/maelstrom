# Experimental GitHub Integration

Version 0.13.0 provides experimental integration with GitHub. We hope to refine
and improve this integration in the next few releases. If you're feeling
adventurous, please give it a shot.

Currently, GitHub mode is only meant to run in [GitHub
actions](https://github.com/features/actions), where Maelstrom test runners
(`cargo-maelstrom`, `maelstrom-go-test`, `maelstrom-pytest),
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
the broker and the test runners. It starts the broker in the background, runs
the test runner (or runners), then stops the broker.

All of the other jobs run worker instances.

When the test runner in the main job goes to run a test, instead of running it
directly, it communicates with the broker, which then dispatches the test to
one of the workers. In this way, the tests all run in parallel in multiple
jobs, on separate runners.

The main implementation challenge we ran into is that GitHub doesn't allow jobs
to communicate directly with each other using normal TCP/IP. To work around
this, we use the [artifact
store](https://docs.github.com/en/actions/writing-workflows/choosing-what-your-workflow-does/storing-and-sharing-data-from-a-workflow).

## Usage

The easiest way to run Maelstrom in a GitHub workflow is to use the actions we provide:
  - On the main job, use
    [`maelstrom-software/maelstrom-broker-action@v1`](https://github.com/maelstrom-software/maelstrom-broker-action)
    to install and configure the broker.
  - On the main job, start the broker in the background. Currently, this needs to be done
    manually as down in the example below. We're working some features that
    will allow the action to start the broker.
  - On the main job, use
    [`maelstrom-software/cargo-maelstrom@v1`](https://github.com/maelstrom-software/cargo-maelstrom-action) or
    [`maelstrom-software/maelstrom-go-test@v1`](https://github.com/maelstrom-software/maelstrom-go-test-action)
    to install and configure the Maelstrom test runner you need.
  - On the main job, run the test runner normally.
  - On a number of separate, parallel jobs, use
    [`maelstrom-software/maelstrom-worker-action@v1`](https://github.com/maelstrom-software/maelstrom-worker-action)
    to install and run the worker.

You can look at the workflow in the example repository
[here](https://github.com/maelstrom-software/maelstrom-examples/blob/main/.github/workflows/ci-base.yml).

Here is another example:

```yml
jobs:
  run-tests:
    name: Run Tests
    # Both ubuntu-22.04 and ubuntu-24.04 are supported.
    # Both x86-64 and ARM (AArch64) are supported.
    # The architecture needs to be the same as the workers.
    runs-on: ubuntu-24.04

    steps:
      # The broker needs to be installed and started before running the
      # cargo-maelstrom-action or the maelstrom-go-test-action. The broker must
      # be run in the same job as the test runners (cargo-maelstrom or
      # maelstrom-go-test).
    - name: Install Maelstrom Broker
      uses: maelstrom-software/maelstrom-broker-action@v1

      # Start the broker as a background process in this job.
    - name: Start Maelstrom Broker
      run: |
        TEMPFILE=$(mktemp maelstrom-broker-stderr.XXXXXX)
        maelstrom-broker 2> >(tee "$TEMPFILE" >&2) &
        PID=$!
        PORT=$( \
          tail -f "$TEMPFILE" \
          | awk '/\<addr: / { print $0; exit}' \
          | sed -Ee 's/^.*\baddr: [^,]*:([0-9]+),.*$/\1/' \
        )
        echo "MAELSTROM_BROKER_PID=$PID" >> "$GITHUB_ENV"
        echo "MAELSTROM_BROKER_PORT=$PORT" >> "$GITHUB_ENV"
      env:
        MAELSTROM_BROKER_ARTIFACT_TRANSFER_STRATEGY: github

      # Schedule a post-job handler to kill the broker. Killing the broker with
      # signal 15 (SIGTERM) allows it to tell the workers to shut themselves
      # down.
    - name: Schedule Post Handler to Kill Maelstrom Broker
      uses: gacts/run-and-post-run@v1
      with:
        post: kill -15 $MAELSTROM_BROKER_PID

      # This action installs and configures (via environment variables)
      # cargo-maelstrom so it can be run simply with `cargo maelstrom`.
    - name: Install and Configure cargo-maelstrom
      uses: maelstrom-software/cargo-maelstrom@v1

      # This action installs and configures (via environment variables)
      # maelstrom-go-test so it can be run simply with `maelstrom-go-test`.
    - name: Install and Configure maelstrom-go-test
      uses: maelstrom-software/maelstrom-go-test-action@v1

    - name: Check Out Repository
      uses: actions/checkout@v4

      # You can now run `cargo maelstrom` however you want.
    - name: Run Tests
      run: cargo maelstrom

      # You can now run `maelstrom-go-test` however you want.
    - name: Run Tests
      run: maelstrom-go-test

  # These are the worker jobs. Tests will execute on one of these workers.
  maelstrom-worker:
    strategy:
      matrix:
        worker-number: [1, 2, 3, 4]

    name: Maelstrom Worker ${{ matrix.worker-number }}

    # This must be the same architecture as the test-running job.
    runs-on: ubuntu-24.04

    steps:
    - name: Install and Run Maelstrom Worker
      uses: maelstrom-software/maelstrom-worker-action@v1
```
