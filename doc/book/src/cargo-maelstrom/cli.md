# Command-Line Options

Besides the [standard command-line options](../standard-cli.md) and the options for [configuration values](config.md),
`cargo-maelstrom` supports additional command-line-options.

## `--init`

The `--init` command-line option is used to create a starter
`maelstrom-test.toml` file. See [here](spec/initializing.html) for more
information.

## `--list-tests` or `--list`

The `--list-tests` (or `--list`) command-line option causes `cargo-maelstrom`
to build all required test binaries, then print the tests that would normally
be run, without actually running them.

This option can be combined with [`--include` and `--exclude`](#include_and_exclude).

## `--list-binaries`

The `--list-binaries` command-line option causes `cargo-maelstrom` to print the
names and types of the crates that it would run tests from, without actually
building any binaries or running any tests.

This option can be combined with [`--include` and `--exclude`](#include_and_exclude).

## `--list-packages`

The `--list-packages` command-line option causes `cargo-maelstrom` to print the
packages from which it would run tests, without actually building any binaries
or running any tests.

This option can be combined with [`--include` and `--exclude`](#include_and_exclude).

## `--include` and `--exclude` {#include_and_exclude}

The `--include` (`-i`) and `--exclude` (`-x) command-line options control which tests
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

### Working with Workspaces

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

### Abbreviations

As discussed [here](filter.md#abbreviations), unambiguous prefixes can be used
in patterns. This can come in handy when doing one-offs on the command line.
For example, the example above could be written like this instead:

```bash
cargo maelstrom -i 'p.eq(baz) & n.eq(foobar)'
```
