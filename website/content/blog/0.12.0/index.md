+++
title = "Maelstrom 0.12.0 Release"
date = 2024-09-09
weight = 1000
draft = true
+++

We're excited to announce Maelstrom 0.12.0. In this release, we added improvements to configuration for all test runners, job scheduling, and UI.

<!-- more -->

## Test Runner Configuration Improvements

We added new CLI and configuration options, all of which are available on the command-line.

Improvements available for all Maelstrom test runners:
- <tt>stop-after</tt> Have the test runner stop after the given number of failures.
- <tt>extra-*-args</tt> Pass arbitrary arguments through to the underlying test or test framework.

Improvement now available for the Go test runner:
- <tt>vet</tt>, <tt>short</tt>, <tt>fullpath</tt> pass-through options added
- <tt>--list-packages</tt> List all packages

## Test Metadata Improvements

We added a new layer type called "shared-library-dependencies", which includes the closure of
shared libraries required to run a list of binaries.

We updated the default test metadata configuration for all tests runners. The Python test runner, in
particular, is significantly easier to use out-of-the-box.

## Scheduling Improvements

We added a new priority for jobs that prioritizes previously failed tests, so they run sooner. Though, tests with the longest remaining time are still run first in their given priority band.

## UI Improvements

We improved the test runner UI with:

- Better tracking for the state of tests. Both UIs include counts of jobs in various states, and the way
  these states are counted has been improved to remove previous tiny inaccuracies.
- A listing of failed tests now appears in the Fancy UI. The Simple UI shows a count.

<img src="maelstrom_failed_tests.png" alt="Failed Tests Dialog" width="90%"/>

See the [0.12.0 release
notes](https://github.com/maelstrom-software/maelstrom/releases/tag/v0.12.0)
for a complete list of changes. 
