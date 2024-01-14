# Filtering Tests

When running `cargo maelstrom run` or `cargo maelstrom list` without any
arguments it runs or lists all the tests it finds as part of your project. If
you wish to run or list only a subset of tests a filter can be applied via the
command line.

The means of filtering is a DSL (domain specific language) that specifies some
subset of the tests in your project. See [Test Pattern
DSL](./test_pattern_dsl.md) for details of how that works.

This DSL is used via the [`--include` and `--exclude`
Flags](./include_and_exclude_flags.md)
