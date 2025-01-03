+++
title = "Maelstrom 0.11.0 Release"
date = 2024-07-30
weight = 1000
+++

We're excited to announce Maelstrom 0.11.0. There are two main changes this
release. We have overhauled the terminal UI for all of our test runners, and we
have added a new test runner for the Go programming language.

<!-- more -->

See the [0.11.0 release
notes](https://github.com/maelstrom-software/maelstrom/releases/tag/v0.11.0)
for a complete list of changes. This post will give a tour of the two large changes.

### New Terminal UI

We've overhauled our terminal UI to deliver some features we are very excited about.

These improvements apply to all our test runners, even the new
`maelstrom-go-test`. Remember, you don't have to use Maelstrom's test
distribution features to get all the goodness. Maelstrom test runners can work
as drop-in replacements for your existing test runners.

Here is a screenshot of the new terminal UI in action:

<img src="new_maelstrom_ui.png" alt="New UI Screenshot" width="90%"/>

The new terminal UI reveals previously unseen tasks that are undertaken during
the course of running tests. Let's highlight just a few of these things.

### Build Progress

For a compiled languages, test binaries must be built before the tests can be
run. In these instances, the the new build-output window will appear to show
the the output from the language's native tool. In the
`cargo-maelstrom`example below, we can see the output from
`cargo`, including its progress bar!

<img src="maelstrom_ui_build_progress.png" alt="Build Progress" width="90%"/>

### Running Tests

There is now a section of the screen that shows the currently running tests,
along with the amount of time that has elapsed since the tests were enqueued.
Tests that have taken the longest show up on top. No more wondering which tests
you are waiting for!

<img src="maelstrom_ui_running.png" alt="Currently Running Tests" width="90%"/>

### Progress Bar

The new progress bar is similar to before but now contains a new, sleek,
stacked design. The state of tests are shown as they progress from
waiting-for-artifacts, pending, running, and finally to complete. These states
are represented with purple, orange, blue, and green respectively.

Above the new progress bar, on-demand progress bars appear for artifact
uploads and container image downloads.

<img src="maelstrom_ui_progress.png" alt="Progress Bar" width="90%"/>

### Go Test Runner

We're also excited to announce a new Go test runner:
`maelstrom-go-test`. This is a drop-in replacement for `go test`.
We now have test runners for Go, Pytest, and Rust. [Check out the
book](/doc/book/latest/go-test.html) for detailed documentation.
