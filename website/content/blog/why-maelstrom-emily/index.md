+++
title = "Maelstrom: Cure for the Common Complacency"
date = 2024-07-18
weight = 1
draft = true
+++

Maelstrom is an open-source, clustered test runner that allows developers to
run their tests isolated, in parallel, and with the ability to scale up or
down resources as their needs require. We built Maelstrom because the amount of
waste in the technology industry among developers is staggering.

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
and best practices identified decades ago that isolated, early testing is the
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
Maelstrom offers a wicked-fast, clustered, isolated testing environment,
entirely for free.

Check us out on [github](https://github.com/maelstrom-software/maelstrom), and
hop into our [discord](https://discord.gg/rgeuZz6CfV) to chat with us live.
