# Initializing `cargo-maelstrom.toml`

It's likely that at some point you'll need to adjust the job specs for some
tests. At that point, you're going to need an actual `cargo-maelstrom.toml`.
Instead of starting from scratch, you can have `cargo-maelstrom` create one for
you:

```bash
cargo maelstrom --init
```

This will create a `cargo-maelstrom.toml` file, unless one already exists, then
exit. The resulting `cargo-maelstrom.toml` will match the default configuration.
It will also include some commented-out examples that may be useful.
