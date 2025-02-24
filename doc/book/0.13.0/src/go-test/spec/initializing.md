# Initializing `maelstrom-go-test.toml`

It's likely that at some point you'll need to adjust the job specs for some
tests. At that point, you're going to need an actual `maelstrom-go-test.toml`.
Instead of starting from scratch, you can have `maelstrom-go-test` create one
for you:

```bash
maelstrom-to-test --init
```

This will create a `maelstrom-go-test.toml` file, unless one already exists,
then exit. The resulting `maelstrom-go-test.toml` will match the default
configuration. It will also include some commented-out examples that may be
useful.
