# Directive Fields

This chapter specifies all of the possible fields for a directive. Most, but
not all, of these fields have an obvious mapping to [job-spec
fields](../../spec.md).

## `filter`

This field must be a string, which is interpreted as a [test filter
pattern](filter.md). The directive only applies to tests that match the filter.
If there is no `filter` field, the directive applies to all tests.

Sometimes it is useful to use multi-line strings for long patterns:

```toml
[[directives]]
filter = """
package.equals(maelstrom-client) ||
package.equals(maelstrom-client-process) ||
package.equals(maelstrom-container) ||
package.equals(maelstrom-fuse) ||
package.equals(maelstrom-util)"""
layers = [{ stubs = ["/tmp/"] }]
mounts = [{ type = "tmp", mount_point = "/tmp" }]
```

## `image`

To use `maelstrom-pytest`, your tests' containers will need a python
interpreter. The best way to provide one is to to build their containers from
an OCI container image.

This is what the `image` field is for. It is used to set the job spec's
[`image`](../../spec.md#image) field.

```toml
[[directives]]
image.name = "docker://python:3.11-slim"
image.use = ["layers", "environment"]

[[directives]]
filter = "package.equals(foo)"
image = { name = "docker://python:3.11", use = ["layers", "environment"] }
```

In the example above, we specified a TOML table in two different, equivalent
ways for illustrative purposes.

The `image` field must be a table with two subfields: `name` and `use`.

The `name` sub-field specifies the name of the image. It must be a string. It
specifies the URI of the image to use, as documented
[here](../container-images.html#container-image-uris).

The `use` sub-field must be a list of strings specifying what parts of the
container image to use for the job spec. It must contain a non-empty subset of:
  - `layers`: This sets the
	[`use_layers`](../../spec.md#use_layers) field in the job spec's image value.
  - `environment`: This sets the
	[`use_environment`](../../spec.md#use_environment) field in the job spec's
	image value.
  - `working_directory`: This sets the
	[`use_working_directory`](../../spec.md#use_working_directory) field in the job spec's
	image value.

## `layers`

```toml
[[directives]]
layers = [
    { tar = "layers/foo.tar" },
    { paths = ["layers/a/b.bin", "layers/a/c.bin"], strip_prefix = "layers/a/" },
    { glob = "layers/b/**", strip_prefix = "layers/b/" },
    { stubs = ["/dev/{null, full}", "/proc/"] },
    { symlinks = [{ link = "/dev/stdout", target = "/proc/self/fd/1" }] }
]
```

This field provides an ordered list of layers for the job spec's
[`layers`](../../spec.md#layers) field.

Each element of the list must be a table with one of the following keys:
  - `tar`: The value must be a string, indicating the local path of the tar
    file. This is used to create a [tar](../../spec-layers.md#tar) layer.
  - `paths`: The value must be a list of strings, indicating the local paths of
    the files and directories to include to create a
    [paths](../../spec-layers.md#paths) layer. It may also include fields from
    [`prefix_options`](../../spec-layers.md#prefix_options) (see below).
  - `glob`: The value must be a string, indicating the glob pattern to use to
    create a [glob](../../spec-layers.md#glob) layer. It may also include
    fields from [`prefix_options`](../../spec-layers.md#prefix_options) (see below).
  - `stubs`: The value must be a list of strings. These strings are optionally
    brace-expanded and used to create a [stubs](../../spec-layers.md#stubs)
    layer.
  - `symlinks`: The value must be a list of tables of `link`/`target` pairs.
    These strings are used to create a [symlinks](../../spec-layers.md#symlinks)
    layer.

If the layer is a `paths` or `glob` layer, then the table can have any of the
following extra fields used to provide the
[`prefix_options`](../../spec-layers.md#prefix_options):
  - `follow_symlinks`: A boolean value. Used to specify [`follow_symlinks`](../../spec-layers.md#follow_symlinks).
  - `canonicalize`: A boolean value. Used to specify [`canonicalize`](../../spec-layers.md#canonicalize).
  - `strip_prefix`: A string value. Used to specify [`strip_prefix`](../../spec-layers.md#strip_prefix).
  - `prepend_prefix`: A string value. Used to specify [`prepend_prefix`](../../spec-layers.md#prepend_prefix).

For example:

```toml
[[directives]]
layers = [
    { paths = ["layers"], strip_prefix = "layers/", prepend_prefix = "/usr/share/" },
]
```

This would create a layer containing all of the files and directories
(recursively) in the local `layers` subdirectory, mapping local file
`layers/example` to `/usr/share/example` in the test's container.

This field can't be set in the same directive as `image` if the `image.use`
contains `"layers"`.

## `added_layers`

This field is like [`layers`](#layers), except it appends to the job spec's
[`layers`](../../spec.md#layers) field instead of replacing it.

This field can be used in the same directive as an `image.use` that contains
`"layers"`. For example:

```toml
[[directives]]
image.name = "cool-image"
image.use = ["layers"]
added_layers = [
    { paths = [ "extra-layers" ], strip_prefix = "extra-layers/" },
]
```

This directive sets uses the layers from `"cool-image"`, but with the contents
of local `extra-layers` directory added in as well.

## `environment`

```toml
[[directives]]
environment = {
    USER = "bob",
    RUST_BACKTRACE = "$env{RUST_BACKTRACE:-0}",
}
```

This field sets the [`environment`](../../spec.md#environment) field of
the job spec. It must be a table with string values. It supports two forms of
`$` expansion within those string values:
  - `$env{FOO}` evaluates to the value of `maelstrom-pytest`'s `FOO` environment variable.
  - `$prev{FOO}` evaluates to the previous value of `FOO` for the job spec.

It is an error if the referenced variable doesn't exist. However, you can use
`:-` to provide a default value:

```toml
FOO = "$env{FOO:-bar}"
```

This will set `FOO` to whatever `maelstrom-pytest`'s `FOO` environment variable
is, or to `"bar"` if `maelstrom-pytest` doesn't have a `FOO` environment
variable.

This field can't be set in the same directive as `image` if the `image.use`
contains `"environment"`.

## `added_environment`

This field is like [`environment`](#environment), except it updates the job
spec's [`environment`](../../spec.md#environment) field instead of replacing it.

When this is provided in the same directive as the `environment` field,
the `added_environment` gets evaluated after the `environment` field. For example:

```toml
[[directives]]
environment = { VAR = "foo" }

[[directives]]
environment = { VAR = "bar" }
added_environment = { VAR = "$prev{VAR}" }
```

In this case, `VAR` will be `"bar"`, not `"foo"`.

This field can be used in the same directive as an `image.use` that contains
`"environment"`. For example:

```toml
[[directives]]
image = { name = "my-image", use = [ "layers", "environment" ] }
added_environment = { PATH = "/scripts:$prev{PATH}" }
```

This prepends `"/scripts"` to the `PATH` provided by the image without changing
any of the other environment variables.

## `mounts`

```toml
[[directives]]
mounts = [
    { type = "bind", mount_point = "/mnt", local_path = "data-for-job", read_only = true },
    { type = "devices", devices = [ "full", "fuse", "null", "random", "shm", "tty", "urandom", "zero" ] },
    { type = "devpts", mount_point = "/dev/pts" },
    { type = "mqueue", mount_point = "/dev/mqueue" },
    { type = "proc", mount_point = "/proc" },
    { type = "sys", mount_point = "/sys" },
    { type = "tmp", mount_point = "/tmp" },
]
```

This field sets the [`mounts`](../../spec.md#mounts) field of
the job spec. It must be a list of tables, each of which is a TOML translation
of the corresponding [job-spec type](../../spec.md#mounts).

## `added_mounts`

This field is like [`mounts`](#mounts), except it appends to the job spec's
[`mounts`](../../spec.md#mounts) field instead of replacing it.

## `working_directory`

```toml
[[directives]]
working_directory = "/home/root/"
```

This field sets the
[`working_directory`](../../spec.md#working_directory) field of
the job spec. It must be a string.

This field can't be set in the same directive as `image` if the `image.use`
contains `"working_directory"`.

## `network`

```toml
[[directives]]
network = "loopback"
```

This field sets the [`network`](../../spec.md#network) field of the job spec. It
must be a string. It defaults to `"disabled"`.

## `enable_writable_file_system`

```toml
[[directives]]
enable_writable_file_system = true
```

This field sets the
[`enable_writable_file_system`](../../spec.md#enable_writable_file_system)
field of the job spec. It must be a boolean.

## `user`

```toml
[[directives]]
user = 1000
```

This field sets the [`user`](../../spec.md#user) field of the job
spec. It must be an unsigned, 32-bit integer.

## `group`

```toml
[[directives]]
group = 1000
```

This field sets the [`group`](../../spec.md#group) field of the
job spec. It must be an unsigned, 32-bit integer.

## `timeout`

```toml
[[directives]]
timeout = 60
```

This field sets the [`timeout`](../../spec.md#timeout) field of the
job spec. It must be an unsigned, 32-bit integer.

## `ignore`

```toml
[[directives]]
ignore = true

This field specifies that any tests matching the directive should not be run.
When tests are run, ignored tests are displayed with a special "ignored" state.
When tests are listed, ignored tests are listed normally.
