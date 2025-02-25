# Directives

The `maelstrom-pytest.toml` file consists of a list of "directives" which are
applied in order. Each directive has some optional fields, one of which may be
`filter`. To compute the job spec for a test, `maelstrom-pytest` starts with a
default spec, then iterates over all the directives in order. If a directive's
`filter` matches the test, the directive is applied to the test's job spec.
Directives without a `filter` apply to all tests. The job spec is then
used for the test.

There is no way to short-circuit the application of directives. Instead,
filters can be used to limit scope of a given directive.

To specify a list of directives in [TOML](https://toml.io/en/), we use the
`[[directives]]` syntax. Each `[[directives]]` line starts a new directive. For
example, this snippet specifies two directives:

```toml
[[directives]]
include_shared_libraries = true

[[directives]]
filter = "package.equals(maelstrom) && name.equals(io::splicer)"
added_mounts = [{ type = "proc", mount_point = "/proc" }]
added_layers = [{ stubs = [ "proc/" ] }]
```

The first directive applies to all tests, since it has no `filter`. The second
directive only applies to a single test named `io::splicer` in the `maelstrom`
package. It adds a layer and a mount to that test's job spec.
