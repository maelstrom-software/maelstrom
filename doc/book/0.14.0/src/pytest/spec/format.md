# Format of `maelstrom-pytest.toml`

`maelstrom-pytest.toml` is a TOML file with two top-level fields: `containers`
and `directives`.

`containers` is a [table](https://toml.io/en/v1.0.0#table) mapping container
names to [container specifications](../../spec.md##container-specification).
Each element is itself a table, as described in the [next
chapter](containers.md).

`directives` is an [array](https://toml.io/en/v1.0.0#array) of directives. Each element
directive is a TOML table, resulting in an ["array of
tables"](https://toml.io/en/v1.0.0#array-of-tables). Directives are described
in their [own chapter](directives.md).

TOML provides a number of ways to specify these elements. Here is an example
with elements of the two types interleaved:

```toml
[containers.executor]
layers = [
    { stubs = [ "{proc,tmp}/", "dev/{null,random,urandom,zero}" ] },
]
mounts = [
    { type = "tmp", mount_point = "/tmp" },
    { type = "proc", mount_point = "/proc" },
    { type = "devices", devices = ["null", "random", "urandom", "zero"] },
]

[[directives]]
filter = "package.equals(maelstrom) && name.starts_with(worker::executor::)"
parent = "executor"

[containers.executor-with-pts]
parent = "executor"
added_layers = [
    { stubs = [ "dev/pts/" ] },
    { symlinks = [ { link = "/dev/ptmx", target = "/dev/pts/ptmx" } ] },
]
added_mounts = [
    { type = "devpts", mount_point = "/dev/pts" },
]

[[directives]]
filter = "package.equals(maelstrom) && name.starts_with(worker::executor::tty)"
parent = "executor-with-pts"
```

This is the same configuration with the elements grouped:

```toml
[[directives]]
filter = "package.equals(maelstrom) && name.starts_with(worker::executor::)"
parent = "executor"

[[directives]]
filter = "package.equals(maelstrom) && name.starts_with(worker::executor::tty)"
parent = "executor-with-pts"

[containers.executor-with-pts]
parent = "executor"
added_layers = [
    { stubs = [ "dev/pts/" ] },
    { symlinks = [ { link = "/dev/ptmx", target = "/dev/pts/ptmx" } ] },
]
added_mounts = [
    { type = "devpts", mount_point = "/dev/pts" },
]

[containers.executor]
layers = [
    { stubs = [ "{proc,tmp}/", "dev/{null,random,urandom,zero}" ] },
]
mounts = [
    { type = "tmp", mount_point = "/tmp" },
    { type = "proc", mount_point = "/proc" },
    { type = "devices", devices = ["null", "random", "urandom", "zero"] },
]
```

As demonstrated by the second example, containers don't need to be defined
before they are referenced, either by another container or by a directive.
However, the order of the directives within the `directives` array is
important, as will be discussed in the [directives chapter](directives.md).
