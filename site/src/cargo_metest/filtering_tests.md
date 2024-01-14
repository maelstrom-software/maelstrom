# Filtering Tests

When running `cargo-metest` without any arguments it runs all the tests it finds
as part of your project. If you wish to run only a subset of tests a filter can
be applied via the command line.

The means of filtering is a DSL (domain specific language) that specifies some
subset of the tests in your project. See [Test Pattern
DSL](./test_pattern_dsl.md) for details of how that works.

This DSL is used via the [`-i` and `-x` Flags](./i_and_x_flags.md)
