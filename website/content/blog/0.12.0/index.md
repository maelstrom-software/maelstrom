+++
title = "Maelstrom 0.12.0 Release"
date = 2024-09-09
weight = 1000
draft = true
+++

We're excited to announce Maelstrom 0.12.0. This release includes a lot of small improvements and
refinements. We added improvements to configuration for all test runners, job scheduling, and UI.

<!-- more -->

See the [0.12.0 release
notes](https://github.com/maelstrom-software/maelstrom/releases/tag/v0.12.0)
for a complete list of changes. This post will give a tour of some of the broad strokes.

## Test Runner Configuration Improvements

We've added a bunch of new CLI and/or configuration options. Like most configuration options they
are available on the command-line also. Some command-line options are not available as configuration
options though. Some of the improvements are specific to the Go test runner, others are
more general. Here is an overview.

All Test Runners
- <tt>stop-after</tt> Have the test runner stop after the given number of failures.
- <tt>extra-*-args</tt> Pass arbitrary arguments through to the underlying test or test framework.

Go Test Runner
- <tt>vet</tt>, <tt>short</tt>, <tt>fullpath</tt> pass-through options added
- <tt>--list-packages</tt> List all packages

## Test Metadata Improvements

We added a new layer type called "shared-library-dependencies". This layer includes the closure of
shared libraries required to run a list of binaries.

The default test metadata configuration was updated for all tests runners. The Python one in
particular was improved to be easier to use out-of-the-box.

## Scheduling Improvements

Previously failed tests are now scheduled with higher priority than other tests so that they run
sooner. To support this, a new priority for jobs has been added. Tests with the longest remaining
time are still run first in their given priority band.

## UI Improvements

We improved and added a few different things in the test runner UI.

- Better tracking of the state of tests. Both UIs include counts of jobs in various states, the way
  these states are counted has now been improved to remove previous tiny inaccuracies.
- A listing of failed tests now appears in the Fancy UI. The Simple UI shows a count.

<img src="maelstrom_failed_tests.png" alt="Failed Tests Dialog" width="90%"/>
