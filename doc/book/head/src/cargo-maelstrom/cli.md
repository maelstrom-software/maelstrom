# Command-Line Options

In addition the command-line options for used to specify [configuration
values](../config.md) described in the [previous chapter](config.md),
`cargo-maelstrom` supports these command-line options:

Option                                                      | Short Alias | Argument             | Description
------------------------------------------------------------|-------------|----------------------|------------
<span style="white-space: nowrap;">`--help`</span>          | `-h`        |                      | [print help and exit](../common-cli.md#--help)
<span style="white-space: nowrap;">`--version`</span>       |             |                      | [print version and exit](../common-cli.md#--version)
<span style="white-space: nowrap;">`--print-config`</span>  | `-P`        |                      | [print all configuration values and exit](../common-cli.md#--print-config)
<span style="white-space: nowrap;">`--config-file`</span>   | `-c`        | path or `-`          | [file to read configuration values from](../common-cli.md#--config-file)
<span style="white-space: nowrap;">`--include`</span>       | `-i`        | [pattern](filter.md) | [include tests that match pattern](#--include-and---exclude)
<span style="white-space: nowrap;">`--exclude`</span>       | `-x`        | [pattern](filter.md) | [exclude tests that match pattern](#--include-and---exclude)                                                  
<span style="white-space: nowrap;">`--init`</span>          |             |                      | [initialize test metadata file](#--init)
<span style="white-space: nowrap;">`--watch`</span>         | `-w`        |                      | [loop running tests then waiting for changes](#--watch)
<span style="white-space: nowrap;">`--list`</span>          |             |                      | [alias for `--list-tests`](#--list-tests-or---list)
<span style="white-space: nowrap;">`--list-tests`</span>    |             |                      | [only list matching tests instead of running them](#--list-tests-or---list)
<span style="white-space: nowrap;">`--list-binaries`</span> |             |                      | [only list matching test binaries instead of running tests](#--list-binaries)
<span style="white-space: nowrap;">`--list-packages`</span> |             |                      | [only list matching test packages instead of running tests](#--list-packages)

## `--include` and `--exclude` {#include-and-exclude}

The `--include` (`-i`) and `--exclude` (`-x`) command-line options control which tests
`cargo-maelstrom` runs or lists.

These options take a [test filter pattern](filter.md). The `--include` option
includes any test that matches the pattern. Similarly, `--exclude` pattern
excludes any test that matches the pattern. Both options are allowed to be
repeated arbitrarily.

The tests that are selected are the set which match any `--include` pattern but
don't match any `--exclude` pattern. In other words, `--exclude`s have precedence
over `--include`s, regardless of the order they are specified.

If no `--include` option is provided, `cargo-maelstrom` acts as if an
`--include all` option was provided.

## `--init`

The `--init` command-line option is used to create a starter
`cargo-maelstrom.toml` file. See [here](spec/initializing.md) for more
information.

## `--watch`

The `--watch` command-line option causes `cargo-maelstrom` to run tests
repeatedly in a loop, waiting for changes to the project directory in between
runs. See [here](watch.md) for more information.

## `--list-tests` or `--list`

The `--list-tests` (or `--list`) command-line option causes `cargo-maelstrom`
to build all required test binaries, then print the tests that would normally
be run, without actually running them.

This option can be combined with [`--include` and `--exclude`](#include-and-exclude).

## `--list-binaries`

The `--list-binaries` command-line option causes `cargo-maelstrom` to print the
names and types of the crates that it would run tests from, without actually
building any binaries or running any tests.

This option can be combined with [`--include` and `--exclude`](#include-and-exclude).

## `--list-packages`

The `--list-packages` command-line option causes `cargo-maelstrom` to print the
packages from which it would run tests, without actually building any binaries
or running any tests.

This option can be combined with [`--include` and `--exclude`](#include-and-exclude).

## Working with Workspaces

When you specify a filter with a package, `cargo-maelstrom` will only build the
matching packages. This can be a useful tip to remember when trying to run a
single test.

If we were to run something like:
```bash
cargo maelstrom --include "name.equals(foobar)"
```

`cargo-maelstrom` would run any test which has the name "foobar". A test with
this name could be found in any of the packages in the workspace, so it is
forced to build all of them. But if we happened to know that only one package has
this test &mdash; the `baz` package &mdash; it would be faster to instead run:

```bash
cargo maelstrom --include "package.equals(baz) && name.equals(foobar)"
```

Since we specified that we only care about the `baz` package, `cargo-maelstrom`
will only bother to build that package.

## Abbreviations

As discussed [here](filter.md#abbreviations), unambiguous prefixes can be used
in patterns. This can come in handy when doing one-offs on the command line.
For example, the example above could be written like this instead:

```bash
cargo maelstrom -i 'p.eq(baz) & n.eq(foobar)'
```
