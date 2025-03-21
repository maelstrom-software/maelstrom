# Initializing `maelstrom-pytest.toml`

It's likely that at some point you'll need to adjust the job and container
specification settings for some tests. At that point, you will need an actual
`maelstrom-pytest.toml`. Instead of starting from scratch, you can have
`maelstrom-pytest` create one for you:

```bash
maelstrom-pytest --init
```

This will create a `maelstrom-pytest.toml` file, unless one already exists, then
exit. The resulting `maelstrom-pytest.toml` will contain the default configuration.
It will also include some commented-out examples that may be useful.
