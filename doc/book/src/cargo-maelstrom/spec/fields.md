# Directive Fields

This chapter specifies all of the possible fields for a directive. Most, but
not all, of these fields have an obvious mapping to [job-spec
fields](../../spec.md).

## `filter`

This field must be a string, which is interpretted as a [test filter
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

## `include_shared_libraries`

```toml
[[directives]]
include_shared_libraries = true
```

This boolean field sets the `include_shared_libraries` job spec psuedo-field.
We call it a psuedo-field because it's not a real field in the job spec, but
instead determines how `cargo-maelstrom` will do its post-processing after
computing the job spec from directives.

In post-processing, if the `include_shared_libraries` psuedo-field is false,
`cargo-maelstrom` will only push a single layer onto the job spec. This layer
will contain the test executable, placed in the root directory.

On the other hand, if the psuedo-field is true, then `cargo-maelstrom` will
push two layers onto the job spec. The first will be a layer containing all of
the shared-library dependencies for the test executable. The second will
contain the test executable, placed in the root directory. (Two layers are used
so that the shared-library layer can be cached and used by other tests.)

If the psuedo-field is never set one way or the other, then `cargo-maelstrom`
will choose a value based on the `layers` field of the job spec. In this case,
`include_shared_libraries` will be true if and only if `layers` is empty.

You usually want this pseudo-field to be true, unless you're using a container
image for your tests. In that case, you probably want to use the shared
libraries included with the container image, not those from the system running
the tests.

## `image`

Sometimes it makes sense to build your test's container from an OCI container
image. For example, when we do integration tests of `cargo-maelstrom`, we want
to run in an environment with `cargo` installed.

This is what the `image` field is for. It is used to set the job spec's
[`image`](../../spec.md#image) field.

```toml
[[directives]]
filter = "package.equals(cargo-maelstrom)"
image.name = "rust"
image.use = ["layers", "environment"]

[[directives]]
filter = "package.equals(maelstrom-client) && test.equals(integration_test)"
image = { name = "alpine", use = ["layers", "environment"] }
```

The `image` field must be a table with two subfields: `name` and `use`.

The `name` sub-field specifies the name of the image. It must be a string. This
will be used to find the image on [Docker Hub](https://hub.docker.com/).

The `use` sub-field must be a list of strings specifying what parts of the
container image to use for the job spec. It must contain a non-empty subset of:
  - `"layers"`: The [`layers`](../../spec.md#layers) field of the job spec is
    replaced by the layers specified in the container image. These layers will be
    [`tar`](../../spec/layers.md#tar) layers pointing to the tar files in the
    container depot.
  - `"environment"`: The [`environment`](../../spec.md#environment) field of the
    job spec is replaced by the environment specified in the container image.
  - `"working_directory"`: The
    [`working_directory`](../../spec.md#working_directory) field of the job spec
    is replaced by the working directory specified in the container image.

In the example above, we specified a TOML table in two different, equivalent ways for illustrative purposes.

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
    file. This is used to create a [tar](../../spec/layers.md#tar) layer.
  - `paths`: The value must be a list of strings, indicating the local paths of
    the files and directories to include to create a
    [paths](../../spec/layers.md#paths) layer. It may also include fields from
    [`prefix_options`](../../spec/layers.md#prefix_options) (see below).
  - `glob`: The value must be a string, indicating the glob pattern to use to
    create a [glob](../../spec/layers.md#glob) layer. It may also include
    fields from [`prefix_options`](../../spec/layers.md#prefix_options) (see below).
  - `stubs`: The value must be a list of strings. These strings are optionally
    brace-expanded and used to create a [stubs](../../spec/layers.md#stubs)
    layer.
  - `symlinks`: The value must be a list of tables of `link`/`target` pairs.
    These strings are used to create a [symlinks](../../spec/layers.md#symlinks)
    layer.

If the layer is a `paths` or `glob` layer, then the table can have any of the
following extra fields used to provide the
[`prefix_options`](../../spec/layers.md#prefix_options):
  - `follow_symlinks`: A boolean value. Used to specify [`follow_symlinks`](../../spec/layers.md#follow_symlinks).
  - `canonicalize`: A boolean value. Used to specify [`canonicalize`](../../spec/layers.md#canonicalize).
  - `strip_prefix`: A string value. Used to specify [`strip_prefix`](../../spec/layers.md#strip_prefix).
  - `prepend_prefix`: A string value. Used to specify [`prepend_prefix`](../../spec/layers.md#prepend_prefix).

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
  - `$env{FOO}` evaluates to the value of `cargo-maelstrom`'s `FOO` environment variable.
  - `$prev{FOO}` evaluates to the previous value of `FOO` for the job spec.

It is an error if the referenced variable doesn't exist. However, you can use
`:-` to provide a default value:

```toml
FOO = "$env{FOO:-bar}"
```

This will set `FOO` to whatever `cargo-maelstrom`'s `FOO` environment variable
is, or to `"bar"` if `cargo-maelstrom` doesn't have a `FOO` environment
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
    { type = "tmp", mount_point = "/tmp" },
    { type = "proc", mount_point = "/proc" },
    { type = "sys", mount_point = "/sys" },
    { type = "bind", mount_point = "/mnt", local_path = "data-for-job", read_only = true },
]
```

This field sets the [`mounts`](../../spec.md#mounts) field of
the job spec. It must be a list of tables, each of which must have two fields:
  - `type`: This indicates the type of special file system to mount, and
    must be one of the following strings: `"tmp"`, `"proc"`, `"sys"`, or `"bind"`.
  - `mount_point`: This must be a string. It specifies the mount point within
    the container for the file system.
  - `local_path`: This must be provided for `"bind"` mounts, and may not be
    provided for any other type. It must be a string. It specifies the local
    directory to bind into the job's container. See [here](../../spec.md#bind)
    for more information.
  - `read_only`: This may only be provided for `"bind"` mounts. It is optional
    and defaults to `false`. It specifies whether the mounted directory should
    be read-only or read-write. See [here](../../spec.md#bind) for more
    information.

## `added_mounts`

This field is like [`mounts`](#mounts), except it appends to the job spec's
[`mounts`](../../spec.md#mounts) field instead of replacing it.

## `devices`

```toml
[[directives]]
devices = ["fuse", "full", "null", "random", "tty", "urandom", "zero"]
```

This field sets the [`devices`](../../spec.md#devices) field of
the job spec. It must be a list of strings, whose elements must be one of:
  - `"full"`
  - `"fuse"`
  - `"null"`
  - `"random"`
  - `"shm"`
  - `"tty"`
  - `"urandom"`
  - `"zero"`

The order of the values doesn't matter, as the list is treated like an
unordered set.

## `added_devices`

This field is like [`devices`](#devices), except it inserts into to the job
spec's [`devices`](../../spec.md#devices) set instead of replacing it.

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

This field sets the [`network`](../../spec.md#nework) field of the job spec. It
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
