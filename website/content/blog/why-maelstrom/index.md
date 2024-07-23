+++
title = "Why Maelstrom?"
date = 2024-07-22
draft = true
+++

At Maelstrom, our goal is to improve developer productivity. We believe that
tests are key to writing high-quality, reliable software and we think a lot
about how to make them better. However, we see As a result, we think a lot
about tests and how to make them better.

We think that there are three important aspects to good tests: Tests should be
**fast**, **accessible**, and **reliable**.

<!-- more -->

## Fast

We believe that a developer should be able to run their tests quickly.

When a developer is sitting waiting for tests to run, they're not doing
productive work, like actually writing code. On top of that, if tests take too
long to run, the developer's mind starts to wander. They context switch over to
some other task and lose their state of flow.

If running tests take too long, then doing so will begin to feel like a chore.
A developer might stop running them often, or they may choose to only run a
small subset of them regularly. Either of these behaviors can lead to
developers discovering defects late in the developer process. Defects are much
easier and cheaper to correct when discovered immediately. The longer they
persist, the harder and more expensive they are to fix.

Even speeding up test running from one minute to ten seconds can have a huge
impact on a developer's behavior. With a fast test running setup, a developer can
run their tests scores of times during a day, catch defects immediately, and do
so without losing interest and getting distracted.

## Accessible

We believe that a developer should be able to run any test at any time, right
from their local development environment.

We've noticed a disturbing recent trend where a lot of tests can only be run in
CI. We don't like this for a number of reasons. One is that it means that
instead of running those tests many times throughout the day, a developer
instead tends to only run them only occasionally, maybe a few times a week.
It's not uncommon for a developer to do a lot of work only to find out in CI
that they made a bad assumption and have to start over with a new design.

Another is that it can be hard to reproduce and fix a failure that was seen in
CI. Sometimes a developer will resort to repeatedly submitting speculative
change to CI just to kick off new runs of the tests.

Things are so much better when a developer can simply run all of the tests, in
exactly the same environment that CI does, right from their local machine.

In a lot of development environments today, certain tests can only be run in
environments that aren't easily accessible to developers. A lot of developers
work in environments where a large subset of tests can only be run in CI.

This can lead to a few bad practices. One is that certain tests just may not get
run often. It may take days or weeks to detect a broken test, and then more
days or weeks to figure out what broke the tests and to get it fixed. In the
worst cases, the test may be disabled or ignored becausing tracking down what
broke it, or fixing it after the fact, may be judged to be too costly, at least
in the short term.

In some cases, most or all tests are run in CI, but they can only be run in CI.
This leads to toerh bad practices. One is that certain tests just may not be
run often enough. A developer may develop for a few days before pushing their
developer branch to CI in order to run all of the tests. Bugs found this late
can sometimes be very costly to fix. Developers usually want to know as early
as possible when something breaks.

When a test does fail in CI, it can sometimes be a pain to fix it. A lot of
times developers will check in speculative fixes just to run the test in CI.
This can lead to a cycle where a developer repeatedly checks in small changes
only to see that the test still fails. It may take ten or twenty changes to
finally track things down. It would be a lot easier if the developer could just
run the test locally.

## Reliable

We believe that tests should be reliable.

Unreliable tests can be a huge time sink for developers. A test may run fine on
a developer's local machine, only to fail in CI or on somebody else's machine.
Or a test may have different results depending on when it is run and what other
tests it is run with.

## What We've Built

Maelstrom is a framework that addresses the problems listed above. It consists
of three parts.

First, we provide a way to enumerate the dependencies of every test. These
dependencies include any external software, devices, and files the test expects
to be installed on the system, as well as any shared network resources the test
relies on.

First, we believe that associated with every test should be a description of
the tests's dependencies. This should include any external software, devices,
and files the test expects to be installed on the system, as well as any shared
network resources the test relies on. *enumerated dependencies*

Second, we run every test in its own isolated environment that includes only
the test's enumerated dependencies.

Second, we believe that every test should be run in its own container that only
provides the test's enumerated dependencies. This is an extension of the best
practice of running every test in its own fixture, where the fixture is taken
to include the whole file system, system resources, etc. *isolation*

Second, we believe that every test should be run in its own environment with
just the specified dependencies available. We take the notion of running every
test in a fresh fixture or in its own process to the next level by running
every test in its own container. Sometimes it makes sense to share pieces of
infrastructre between test runs, and we support that. But we want you to have
to opt in to that. If you don't, every depdendency is assumed to be isolated.

Third, we provide a mechanism to build a cluster of tests runners, so
developers can either run tests locally on their machines, or in parallel on an
arbitrarily large cluster.

Third, we believe that developers should have a platform they can use to run
tests both locally on their machines and also on shared clusters of machines.
*universal execution*

Third, we believe that tests should be runnable both locally and on shared
clusters. Ideally, every developer would have a cluster available to them on
which to run their tests. We 


## How We Imagine People Using It

## Summary, Future, and Call to Action
 Change the way developers think about testing in general.
