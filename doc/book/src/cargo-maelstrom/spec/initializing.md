# Initializing `maelstrom-test.toml`

It's likely that at some point you'll need to adjust the job specs for some
tests. At that point, you're going to need an actual `maelstrom-test.toml`.
Instead of starting from scratch, you can have `cargo-maelstrom` create one for
you:

```bash
cargo maelstrom --init
```

The resulting `maelstrom-test.toml` will match the default configuration. It
will also include some commented-out examples that may be useful.
