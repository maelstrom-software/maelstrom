# Initializing `maelstrom-go-test.toml`

If there is no `maelstrom-go-test.toml` in the workspace root, then
`maelstrom-go-test` will use a [default configuration](default.md).

When it comes time to build a more complex configuration, you can have
`maelstrom-go-test` write out the default configuration:

```bash
maelstrom-to-test --init
```

This will create a `maelstrom-go-test.toml` file in the workspace root, unless
one already exists, then exit. The resulting `maelstrom-go-test.toml` will
contain the default configuration. It will also include some commented-out
examples that may be useful.
