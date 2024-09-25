+++
title = "Why Maelstrom?"
date = 2024-09-25
weight = 0
+++

At Maelstrom, our goal is to improve developer productivity. We believe that
tests are key to writing high-quality, reliable software, and we spend a lot of
time thinking about how to make them better.

We believe that there are three important aspects to good tests. Tests should
be **fast**, **accessible**, and **reliable**.

<!-- more -->

### Fast

We believe that a developer should be able to run their tests quickly.

When a developer is waiting for tests to run, they're not doing productive
work --- namely, writing code. On top of that, if tests take too long to run, the
developer's mind starts to wander. They context switch to some other task and
lose their state of flow.

If running tests take too long, then running them will begin to feel like a
chore. A developer might stop running them as often, or they may choose to only
run a small subset of them regularly. Either of these behaviors can lead to a
developer discovering defects late in the development process. Defects are much
easier and cheaper to correct when discovered immediately. The longer they
persist, the harder and more expensive they are to fix.

Even speeding up test running from one minute to fifteen seconds can have a
huge impact on a developer's behavior. With a fast test-running setup, a
developer can run their tests scores of times during a day, catch defects
immediately, and do so without losing interest or getting distracted.

### Accessible

We believe that a developer should be able to run any test at any time, right
from their local development environment.

We've noticed a disturbing trend where many tests can only be run in CI. The
implication of this behavior is that instead of running those tests many times
throughout the day, a developer only runs them occasionally, maybe as little as
a few times a week. A developer often completes hours or days of work only to
find out in CI that they have to start over with a new design.

Additionally, it can be hard to diagnose and fix a failure that was seen in CI,
as it may not be reproducible locally. A developer may have to resort to
repeatedly submitting speculative change to CI just to kick off new runs of the
tests.

When a developer can simply and habitually run all of their tests, in exactly
the same environment that CI does, right from their local machine, it
dramatically decreases the time they spend on root-cause analysis, bug fixing,
or redesign.

### Reliable

We believe that tests should be reliable.

It seems obvious that we should make our tests as reliable as possible.
However, there are some sources of unreliability that seem to go
under-acknowledged.

The first source of unreliability is the reliance of the test on the execution
environment. A test may have a dependency on a certain file or piece of
software that is installed on the test-running machine. If that software is
upgraded, or if the depended-upon file is changed, the test may suddenly break.

The second source of unreliability is test-to-test interactions. A test may
leave around artifacts --- in the process's memory, files stored on disk, or
data stored in network services --- that affect tests that are run afterwards.
If the tests are run in a different order, or if the first test is changed,
then a later test may fail in an unexpected way. Diagnosing that failure may be
incredibly difficult.

These instances of "action at a distance" can be confusing to understand
and frustrating to debug.

### What We've Built

Maelstrom is a framework that addresses these problems. It follows three
guiding principles.

First, we provide a way to specify the dependencies of every test. These
dependencies include any external software, devices, and files the test expects
to be installed on the system, as well as any shared network resources the test
relies on.

These dependencies are "opt-in": very few dependencies are provided by default.
This ensure that the set of dependencies stays small and also that the
developer understands what's going on with their tests.

Second, we run every test in its own isolated environment that includes only
the test's enumerated dependencies. This is an extension of the best practice
of running every test in its own fixture. Here, the fixture is taken to include
the whole file system, system resources, etc. We've written our own extremely
lightweight container runtime to implement this feature, which means that there
is very little overhead compared to running every test in its own process.

Third, we provide a mechanism to build a cluster of tests runners, so
developers can either run tests locally on their machines, or in parallel on an
arbitrarily large cluster.

### How to Use Maelstrom

Maelstrom provides specialized test runners for a variety of test frameworks.
We currently support Rust (via <tt>cargo test</tt>), Golang (via <tt>go
test</tt>), and Pytest. We will continue to add more tests runners in the
future.

If Maelstrom doesn't currently support a test runner, a developer can write
their own using our Rust or gRPC bindings. We also provide a command-line
utility for running an arbitrary program in the Maelstrom environment. This can
be used for ad hoc testing or as a target for scripts.

To get started running their tests using Maelstrom, a developer starts by using
the Maelstrom test runner as a drop-in replacement for their test runner. For
example, a developer would type <tt>cargo maelstrom</tt> instead of <tt>cargo
test</tt>. This will run all tests in a minimal environment to start with. Some
tests may not run properly without more dependencies specified. A developer
can then opt in to these required dependencies by adding dependencies to tests
that match certain patterns. It usually only takes a few minutes to get a whole
project's tests working with Maelstrom.

### Advantages Once Set Up

Once a developer starts using Maelstrom, there are a lot of things they can do.
Ideally, they will have access to a cluster of test runners: the bigger the
better. This cluster should be shared by all developers on the project, and
probably also with CI.

They can run more of their tests more often. With a cluster available,
running tests can become a very fast affair. A developer can then get instant
feedback when something breaks.

They can run all of their tests while developing, including the ones run by CI.
And since CI uses the same specifications, a developer can be confident that a
test that passes for them will also pass in CI. The converse is also true: a
test that fails in CI will fail for the developer, making reproducing a failure
much easier.

CI will also complete more quickly, since it will have a cluster available to
it. It will efficiently distribute test runs across the cluster, utilizing all
of the execution machines at once, without a developer having to do manual
balancing of test runs.

Having a cluster available also makes debugging flaky tests easier. A developer
can tell Maelstrom to run thousands of individual instances of a test, and
return when one fails. With enough cores in the cluster, a developer can
quickly track down even the most troublesome Heisenbug.

We have found that once developers have the experience of having a system like
Maelstrom at their disposal, they start to change how they approach testing.
Being able to test more of the system more often is part of it, but they also
start to write test differently. It now becomes more feasible to engage in more
computationally intensive testing. Developers can rely more heavily on fuzz
testing, "chaos testing", and exhaustive simulation.

While the full value of Maelstrom's model works best when developers have
clusters available to them, there are a still a lot of advantages even when
that is not the case. It's still great to have tests always run the same way,
whether in CI or locally. And it's still great to be able to run tests
everywhere. In fact, we imagine a lot of developers will start with Maelstrom
without a generally-available cluster, then maybe move to having a cluster just
for CI, and then progress eventually to having a shared project for the entire
project.
