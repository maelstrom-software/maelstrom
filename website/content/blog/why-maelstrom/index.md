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

<!-- more -->

A [recent
study](https://stripe.com/files/reports/the-developer-coefficient.pdf)
conducted by online payments firm, Stripe found that the average developer
spends between 17+ hours each week debugging and refactoring code, and another
4 hours on what they refer to as simply ‘bad code.’ The conclusion of this
study estimated that developer inefficiency amounts to a ~$300billion loss in
global GDP every year.

This may feel like a revelation to our non-coding friends, but to developers
like us, who grapple with these inefficiencies and frustrations on a daily
basis, it’s old news. We see the time lost to bugs that get introduced into the
code days before they’re rolled into CI. We feel the pain of hunting down what
broke and why, sometimes lengthening the time to delivery by 2x, or god forbid,
even more. 

At Maelstrom, we know that life doesn’t need to be like this. Solving this
problem may be hard, but it’s possible, and our mission is to help companies
and individual developers get the most out of their time, and their tests. 

So what does good actually look like? Let’s dig in.

## Know the minute you break it.

“Move fast and break things.” We all know the tech-world cliche. But if you
break something, continue working on that code code, pass all your local tests,
and then find out that you broke it four days later in the CI pipeline, how do
you know what you broke? How long will it take you to figure out what you
broke, especially if you already know that your local tests won’t detect the
issue because they didn’t catch it the first time around? What if the thing you
broke has dependencies that will also need to be fixed as a result of the
breakage? 

This kind of setback isn’t a nightmare scenario for developers – it’s a
tuesday. How time consuming it is to fix may vary, but the limitation and
associated risks are ever-present.

Contrast this with web developers. A typical web developer will have a browser
open on their computer showing them what their HTML5 code is doing in
real-time. They know if they break something because it immediately breaks on
their screen. Break a path? The site doesn’t render correctly. They fix it.
They move on.

Why don’t we demand this type of visibility for everyone who writes code? Why
do we accept entire workdays sunk into debugging rather than solving the
problem up front? 

Maelstrom is here to solve that problem for you. 

## Stop waiting.

As a project grows, there can get to be a lot of tests. In some cases, it’s not
even necessarily the compute time, but a long latency, so the time investment
for a developer to run all of those tests becomes a burden. In many cases, they
have to wait minutes, but in extreme cases they may be waiting over an hour. 

The limitations of compute resources and latency play a big role in an
organization or developer’s approach to testing. Most rational developers will
admit that testing early and often is the right approach, but the realities of
their resources make that approach unsustainable – or, at least, it used to. 

However, with the elasticity of cloud resources, and the flexibility of a
multi-tennat solution, the ideal is finally achievable, and Maelstrom will
offer that solution to the world. In the old world, it was ridiculous to
imagine dedicating the 20-30 cores you would need to run your entire test suite
in a few seconds, because those resources would go unused as soon as the tests
complete. 

With a clustered system that handles multiple resourcing demands, all of the
demands of an entire organization, or even multiple organizations are calling
on the compute power of the available cores, and in doing so, the demand curve
smooths. Whether you’re a large company, or a medium-sized company dedicating
hundreds or thousands of cores to their developers for specific use case, you
have access to the power you need without incurring waste. 

## Test like your code is in CI, every time.

A two-tier approach to testing is becoming more and more common in the
development world. In this model, developers will have local tests that they
run against their code, and a broader set of tests that run in the CI pipeline. 

While one could make the argument that at least this model catches some bugs in
the code before it gets to the CI pipeline, unintentional dependencies on your
local environment can lead to an even more difficult debugging experience
should the code fail to pass the tests. 

Many organizations believe that replicating the CI environment is impossible,
but a containerized solution like Maelstrom allows companies test their code in
isolation, eliminating the possibility of local dependencies and ensuring that
if it’s passing tests on your machine, it will pass in CI.

## Conclusion

These concepts aren’t new. Mainstream discussions about code hygiene
and best practices identified decades ago that hermetic, early testing is the
ideal practice for overall code quality and team efficiency. The thing that’s
changed is that now we have access to the technology needed to make it a
practical solution rather than simply a theorhetical one. 

But even with this solution within reach, actually building the infrastructure
that supports it is both difficult and time consuming. Organizations are forced
to either home-grow solutions, using countless hours of developer time on
non-mission-critical developer tooling work, or they accept the wasted time. 

Maelstrom is offering the first off-the-shelf solution that allows
organizations to do what they do best without using their team’s precious time.
We personally experienced this pain at our last company and were both annoyed
and dumbfounded that no one had solved the problem yet. 

So – who better than us to solve it?

Today, Maelstrom offers an open source, clustered test runner for rust and
python, with other languages soon to follow. While the vision of pulling up a
UI where you can see the second your code breaks may not be realized today,
Maelstrom offers a wicked-fast, clustered, hermetic testing environment,
entirely for free. 

Check us out on [github](https://github.com/maelstrom-software/maelstrom), and
hop into our [discord](https://discord.gg/rgeuZz6CfV) to chat with us live. 
