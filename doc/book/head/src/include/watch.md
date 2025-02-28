# `--watch`

Maelstrom has a "watch" mode, which is enabled by the [`--watch` command-line option](cli.md#--watch).

When run in this mode, Maelstrom will first run all of the specified tests,
just like normal. However, when it is done, instead of exiting, it will wait
for changes to the project directory. When it sees changes, it will re-run all
of the specified tests.

In `--watch` mode, Maelstrom basically starts from scratch each run. This means
that changes to project metadata, configuration files, etc will all be picked
up. Filter patterns will be re-evaluated for each run as well.

The `--watch` option pairs well with [`--stop-after`](stop-after.md). You can
run Maelstrom on a relatively large set of tests, but have a quick iterations
if a test and you need to fix it.
