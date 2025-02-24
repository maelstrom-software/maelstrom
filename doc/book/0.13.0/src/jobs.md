# Jobs

The fundamental execution unit in Maelstrom is a job. A job is a program that
is run in its own container, either locally or on a cluster. Jobs are intended
to be programs that terminate on their own after doing some fixed amount of work.
They aren't intended to be interactive or to run indefinitely. However, Maelstrom
does provide a mechanism for running a job interactively for troubleshooting
purposes.

[Test runners](programs.md#test-runners) will usually translate each test case
into its own, standalone job. So, in the context of a test runner, we use the
term "job" and "test" interchangeably.
