+++
title = "Why Maelstrom?"
authors = ["Neal Fachan <neal@fachan.com>"]
date = 2024-10-16
weight = 0
+++

In my 25 years of writing software professionally, I've witnessed a major shift
in our industry's attitude towards testing. When I started, we developers
rarely tested our software. We would run a few manual, ad hoc tests, and then
call it good. We would then throw our code over the figurative wall to a
dedicated testing team. If we were lucky, they'd get to our changes in a week
or two.

Today, we strive to have as many automated tests as possible, and to run those
tests early and often. We continuously integrate our code, running those
automated tests when we do. We've all experienced, sometimes with great pain,
the fact that the earlier a defect is caught, the cheaper and easier it is to
fix.

Even though things have gotten better, we still don't run our tests nearly
early nor often enough. Our testing tools have failed to keep up with our
testing needs. This post will take a look at why that is, and will introduce
Maelstrom, a toolset that we've build to address the problem.

<!-- more -->

### CI Is Part of the Problem

As we've increased the number of tests we write, we've moved to running them
primarily in CI.

Testing on our resource-constrained local machines is slow, making it
frustrating to do frequently. We also often have tests that can't even be run
on our local machines because of missing dependencies, reducing the efficacy of
local testing.

As a result, we tend to rely on CI for the majority of our test running. In CI
we can configure the required dependencies, and we can easily add more
resources as necessary.

However, running tests only in CI comes with its own set of problems, the
biggest being that we don't run our tests often enough. Instead of running
tests every few minutes, we run them a few times a day. This means we catch
issues later in the development cycle, which wastes our time.

Our assertion is that we should be able to run all of our tests locally, as
well as in CI, in the exact same environments with the exact same dependencies.
That's why we built Maelstrom.

### Easier, Better, Faster, Stronger

Our goal with Maelstrom is to give developers the tools to run their tests
after every change they make. You shouldn't have to wait for CI to run your
tests: you should be able to do it right from your machine, on demand.

Moreover, if you run your tests this often, you need those tests to run
quickly. Tests are embarrassingly parallel, as by definition, they shouldn't
interact with each other. Maelstrom harnesses the immense amount of
computational power available today to run tests in parallel, which reduces how
long you wait for tests.

We've identified three traits a good test-running system must have to achieve
these goals. Running tests should be be **fast**, **accessible**, and
**reliable**. We'll next look at what we mean by these traits and why they are
important, and then proceed to show what we've built to meet these
requirements.

### Fast

You should be able to run your tests quickly.

Waiting for tests to run is frustrating. When you're not able to do productive
work --- namely, writing code --- you inevitably have to move on to other
tasks. You context switch and lose your state of flow.

If running tests takes too long, running them feels like a chore. You might
stop running them as often, or your may run a smaller subset of them. Both
response, while understandable, can lead defects that get discovered late in
the development process. Defects are much easier and cheaper to correct when
discovered immediately. The longer they persist, the harder and more expensive
they are to fix.

We've found that even small differences in the time it takes tests to run can
have a huge impact on our testing practices. With a fast test-running setup,
you can run your tests scores of times during the day, catch defects
immediately, and do so without losing focus.

### Accessible

You should be able to run any test at any time, right from your machine.

There's a disturbing trend in today's software development practices where many
tests can only be run in CI. The implication is that instead of running those
tests many times throughout the day, you only run them occasionally, sometimes
as little as a few times a week. You often complete hours or days of work only
to find out in CI that you need a new design.

It can be incredibly hard to diagnose and fix a failure found in CI, as it may
not be reproducible locally. You may have to resort to repeatedly submitting
speculative changes to CI just to kick off new runs of the tests.

When you can run all of your tests, in the exact same environment that CI does,
right from your machine, it dramatically decreases the time you spend on
root-cause analysis, bug fixing, or redesign later.

### Reliable

Tests should be reliable.

It should be obvious that tests need to be reliable, but we seem to accept some
sources of unreliability as if they are inevitable.

The first is the reliance on the test execution environment. A test may have
dependencies on software installed on the test-running machine, or on other
aspects of its file system. If the test is changed to require a newer version
of the software, but the test-running machine isn't, the test may break.
Conversely, if the test-running machine is changed --- its software updated or
the file system modified --- the test may break.

The second source of unreliability is test-to-test interactions. A test may
leave around artifacts --- in the process's memory, files stored on disk, or
data stored in network services --- that affect tests that are run afterwards.
If the tests are run in a different order, or if the first test is changed,
then a later test may fail unexpectedly, and diagnosing that failure may be
incredibly difficult.

These instances of "action at a distance" can be confusing to understand and
frustrating to debug.

### What We've Built

Maelstrom addresses these problems by following three guiding principles.

First, we provide a way to specify the dependencies of every test. These
dependencies include any external software, devices, and files the test expects
to be installed on the system, as well as any shared network resources the test
relies on.

These dependencies are "opt-in": very few dependencies are provided by default.
This ensures that the set of dependencies stays small and also that the
developer understands what's going on with their tests.

Second, we run every test in its own isolated environment that includes only
the test's enumerated dependencies. This is an extension of the best practice
of running every test in its own fixture. Here, the fixture is taken to include
the whole file system, system resources, etc. We've written our own extremely
lightweight container runtime to implement this feature, meaning there is very
little overhead compared to running every test in its own process.

Third, we provide a mechanism to build a cluster of test runners, so you can
run tests locally on your machine, or in parallel on an arbitrarily large
cluster.

### How to Use Maelstrom

Maelstrom provides specialized test runners for multiple test frameworks.
We currently support Rust (via `cargo test`), Golang (via `go test`), and
Pytest. We will continue to add more test runners in the future.

If Maelstrom doesn't currently support your chosen test runner, you can write
your own using our Rust or gRPC bindings. We also provide a command-line
utility for running an arbitrary program in the Maelstrom environment. You can
use this for ad hoc testing or as a target for scripts.

To get started running your tests using Maelstrom, start by using the
Maelstrom test runner as a drop-in replacement for your test runner. For
example, you would type `cargo maelstrom` instead of `cargo test`. This will
run all tests in a minimal environment to start with. Some tests may not run
properly without more dependencies specified. You can then opt into these
required dependencies by adding dependencies to tests that match certain
patterns. It usually only takes a few minutes to get a whole project's tests
working with Maelstrom.

### Conclusion

I have found through my own experience that once developers have the experience
of having a system like Maelstrom at their disposal, they change how they
approach testing. Being able to test more of the system more often is part of
it, but they also start to write test differently. It now becomes more feasible
to engage in more computationally intensive testing. Developers can rely more
heavily on fuzz testing, "chaos testing", and exhaustive simulation.


While the full value of Maelstrom's model works best when developers have
clusters available to them, there are a multitude of advantages even when
run locally. We know that most developers will start with Maelstrom
without a generally available cluster, then maybe move to having a cluster just
for CI, and then progress eventually to having a shared cluster for the entire
project.

You should have testing tools that move as fast as you do. Give Maelstrom a
shot and tell us what you think.
