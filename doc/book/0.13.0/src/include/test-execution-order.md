# Test Execution Order

Maelstrom doesn't execute tests in a random order. Instead, it tries to execute
them in an order that will be helpful to the user.

## Understanding Priorities

Maelstrom assigns every test a priority. When there is a free slot and
available tests, Maelstrom will choose the available test with the highest
priority.

Tests may not be available for a variety of reasons. The test's binary may not
have been compiled yet, the test's required container image may not have been
downloaded yet, or the test's required artifacts may not have been uploaded
yet.

When a test becomes available, there are no free slots, and the test has a
higher priority than any of the existing tests, it does not preempt any of the
existing tests. Instead, it will be chosen first the next time a slot becomes
available.

## New Test and Tests that Failed Previously

A test's priority consists of the two parts. The first part, and more important
part, is whether the test is new or failed the last time it was run. The logic
here is that user probably is most interested in finding out the outcomes of
these tests. New tests and test that failed the last time they were run have
the same priority.

A test is considered new if Maelstrom has no record of it executing. If the
test listing file in the state directory has been removed, then Maelstrom will
consider every test to be new. If a test, its artifacts, or its package is
renamed, it is also considered new.

A test is considered to have failed the last time it was run if there was even
one failure. This is relevant when the `--repeat` configuration value is set.
If the previous run For example, if `--repeat=1000` is passed, and the passes
999 times and fails just once, it is still considered to have failed.

## Estimated Duration and LPT Scheduling

The second part of a test's priority is its estimated duration. In the test
listing file in the state directory, Maelstrom keeps track of the recent
running times of the test. It uses this to guess how long the test will take to
execute. Tests that are expected to run the longest are scheduled first.

Using the estimated duration to set a test's priority means that Maelstrom uses
[longest-processing-time-first (LPT)
scheduling](https://en.wikipedia.org/wiki/Longest-processing-time-first_scheduling).
Finding optimal scheduling orders is an NP-hard problem, but LPT scheduling
provides a good approximate. With LPT scheduling, the overall runtime will
never be more than 133% of the optimal runtime.
