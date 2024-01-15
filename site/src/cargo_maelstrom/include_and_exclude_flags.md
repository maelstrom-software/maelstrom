# `--include` and `--exclude` Flags

These flags are about filtering which tests `cargo-maelstrom` runs.

The `--include` and `--exclude` flags (shorted as `-i` and `-x`) accept a
snippet of the [Test Pattern DSL](./test_pattern_dsl.md). The `-i` flag includes
any tests matching the pattern and the `-x` flag excludes any test which matches
the pattern. They are both able to be provided multiple times via the command
line.

The tests that are ran are the set which matches any of the `-i` flag patterns
after subtracting the set which matches any of the `-x` flag patterns. To put
this more explicitly it is something like
```maelstrom-test-pattern
(i_flag_1 || i_flag_2 || ...) - (x_flag_1 || x_flag_2 || ...)
```

## Working with Workspaces
When you specify a filter with a package, `cargo-maelstrom` will only build the
matching packages. This can be a useful tip to remember when trying to run a
single test.

If we were to run something like
```bash
cargo maelstrom run -i "name.equals(foobar)"
```

`cargo-maelstrom` would run any test which has the name "foobar". A test with
this name could be found in any of the packages in the workspace, so it is
forced to build all of them. But if we happened to know that only one package has
this test, the `baz` package, we would be better off running the following
instead.

```bash
cargo maelstrom run -i "package.equals(baz) && name.equals(foobar)"
```

Now since we specified that we only care about the "baz" package,
`cargo-maelstrom` will only bother to build that package.
