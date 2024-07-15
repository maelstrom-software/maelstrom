# `maelstrom-run`

`maelstrom-run` is a program for running arbitrary commands on a Maelstrom cluster.

## Default Mode

There are three modes that `maelstrom-run` can be run in. In the default mode,
the `maelstrom-run` reads a stream of JSON maps from standard input or a file,
where each JSON map describes a [job specification](spec.md).

The jobs are run as soon as they are read, and their results are outputted as
soon as they complete. It's possible to keep an instance of `maelstrom-run`
around for an arbitrary amount of time, feeding it individual job specification
and waiting for results. `maelstrom-run` won't exit until it has read an
end-of-file and all of the pending jobs have completed.

If any job terminates abnormally or exits with a non-zero exit code, then
`maelstrom-run` will eventually exit with a a code of `1`. Otherwise, it will
exit with a code of `0`.

Output from jobs is printed when jobs complete. The standard output from jobs
is printed to `maelstrom-run`'s standard output, and the standard error from
jobs is printed to `maelstrom-run`'s standard error. All the output for a
single jobs will be printed atomically before the output from any other jobs.

## "One" Mode

The second mode for `maelstrom-run` is "one" mode. This is specified with the
`--one` command-line option. This mode differs from the default mode in two
ways. First, `maelstrom-run` can optionally take more positional command-line
arguments. If that's the case, then they will replace the program and the
argument in the job specification. This can be useful to run various commands
in a job specification saved as a JSON file.

Second, in "one" mode, `maelstrom-run` tries to terminate itself in the same
was that the job terminated. So, if the job received a `SIGHUP`, then
`maelstrom-run` will terminate by being killed by a `SIGHUP`.

## TTY Mode

The third mode for `maelstrom-run` is TTY mode. This mode is an extension of 
["One" Mode](#one-mode) where the job's standard input, output, and error are
connected to `maelstrom-run`'s terminal. This is very useful for interacting
with a job specification to debug a failing test or to verify some aspect of
it's container.
