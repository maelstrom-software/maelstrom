# Command-Line Options

In addition the command-line options for used to specify [configuration
values](../config.md) described in the [previous chapter](config.md),
`maelstrom-pytest` supports these command-line options:

Option                                                      | Short Alias | Argument             | Description
------------------------------------------------------------|-------------|----------------------|------------
<span style="white-space: nowrap;">`--help`</span>          | `-h`        |                      | [print help and exit](../common-cli.md#--help)
<span style="white-space: nowrap;">`--version`</span>       |             |                      | [print version and exit](../common-cli.md#--version)
<span style="white-space: nowrap;">`--print-config`</span>  | `-P`        |                      | [print all configuration values and exit](../common-cli.md#--print-config)
<span style="white-space: nowrap;">`--config-file`</span>   | `-c`        | path or `-`          | [file to read configuration values from](../common-cli.md#--config-file)
<span style="white-space: nowrap;">`--include`</span>       | `-i`        | [pattern](filter.md) | [include tests that match pattern](#--include-and---exclude)
<span style="white-space: nowrap;">`--exclude`</span>       | `-x`        | [pattern](filter.md) | [exclude tests that match pattern](#--include-and---exclude)                                                  
<span style="white-space: nowrap;">`--init`</span>          |             |                      | [initialize test metadata file](#--init)
<span style="white-space: nowrap;">`--list`</span>          |             |                      | [only list matching tests instead of running them](#--list)

## `--include` and `--exclude` {#include-and-exclude}

The `--include` (`-i`) and `--exclude` (`-x`) command-line options control which tests
`maelstrom-pytest` runs or lists.

These options take a [test filter pattern](filter.md). The `--include` option
includes any test that matches the pattern. Similarly, `--exclude` pattern
excludes any test that matches the pattern. Both options are allowed to be
repeated arbitrarily.

The tests that are selected are the set which match any `--include` pattern but
don't match any `--exclude` pattern. In other words, `--exclude`s have precedence
over `--include`s, regardless of the order they are specified.

If no `--include` option is provided, `maelstrom-pytest` acts as if an
`--include all` option was provided.

## `--init`

The `--init` command-line option is used to create a starter
`maelstrom-pytest.toml` file. See [here](spec/initializing.html) for more
information.

## `--list`

The `--list` command-line option causes `maelstrom-pytest`
to print the tests that would normally
be run, without actually running them.

This option can be combined with [`--include` and `--exclude`](#include-and-exclude).

## Abbreviations

As discussed [here](filter.md#abbreviations), unambiguous prefixes can be used
in patterns. This can come in handy when doing one-offs on the command line.
For example, to run all tests in package `foo` with the marker `mark`:

```bash
maelstrom-pytest -i 'p.eq(foo) & m.c(mark)'
```
