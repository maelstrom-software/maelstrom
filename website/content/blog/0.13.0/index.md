+++
title = "Maelstrom 0.13.0 Release"
date = 2025-03-04
weight = 998
+++

We're excited to announce
[Maelstrom](https://github.com/maelstrom-software/maelstrom) 0.13.0. In this
release, the big changes are additional improvements of our test runners' UI,
and the introduction of GitHub support (still experimental).

<!-- more -->

## Test Runner UI Rewrite

We rewrote our test runner UI for v0.13.0. One big change is that we upstreamed
a change to Ratatui that will hopefully greatly reduce flickering. We also made
a number of smaller usability improvements.

## `--watch` Mode

All test runners now have a `--watch` command-line option. When running with
this flag, tests will be rerun after any change to a relevant file in the
project directory.

## Experimental GitHub Integration

This release sees the addition of initial, experimental support for running
tests on Maelstrom in parallel in GitHub actions. The various parts of the
cluster communicate using the GitHub artifacts store. You can see an example
GitHub workflow
[here](https://github.com/maelstrom-software/maelstrom-examples/blob/main/.github/workflows/ci-base.yml).

More information can be
found [here](https://maelstrom-software.com/doc/book/latest/github.html).

## Other Changes

See the [0.13.0 release
notes](https://github.com/maelstrom-software/maelstrom/releases/tag/v0.13.0)
for a complete list of changes.
