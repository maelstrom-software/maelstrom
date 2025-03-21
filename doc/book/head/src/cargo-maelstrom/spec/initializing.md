# Initializing `cargo-maelstrom.toml`

If there is no `cargo-maelstrom.toml` in the workspace root, then
`cargo-maelstrom` will use a [default configuration](default.md).

When it comes time to build a more complex configuration, you can have
`cargo-maelstrom` write out the default configuration:

```bash
cargo maelstrom --init
```

This will create a `cargo-maelstrom.toml` file in the workspace root, unless
one already exists, then exit. The resulting `cargo-maelstrom.toml` will
contain the default configuration. It will also include some commented-out
examples that may be useful.
