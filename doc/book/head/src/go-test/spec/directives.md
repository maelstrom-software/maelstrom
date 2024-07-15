# Directives

The `maelstrom-go-test.toml` file consists of a list of "directives" which are
applied in order. Each directive has some optional fields, one of which may be
`filter`. To compute the job spec for a test, `maelstrom-go-test` starts with a
default spec, then iterates over all the directives in order. If a directive's
`filter` matches the test, the directive is applied to the test's job spec.
Directives without a `filter` apply to all tests. When it reaches the end of
the configuration, it pushes one layer containing the test
executable. The job spec is then used for the test.

There is no way to short-circuit the application of directives. Instead,
filters can be used to limit scope of a given directive.

To specify a list of directives in [TOML](https://toml.io/en/), we use the
`[[directives]]` syntax. Each `[[directives]]` line starts a new directive. For
example, this snippet specifies two directives:

```toml
[[directives]]
network = "loopback"

[[directives]]
filter = "package.equals(maelstrom-util) && name.equals(io::splicer)"
added_mounts = [{ type = "proc", mount_point = "/proc" }]
added_layers = [{ stubs = [ "proc/" ] }]
```

The first directive applies to all tests, since it has no `filter`. It sets the
`network` field in the job spec. The second directive only applies to a single
test named `io::splicer` in the `maelstrom-util` package. It adds a layer and a
mount to that test's job spec.
