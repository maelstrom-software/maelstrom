# Containers

A `container` element in `maelstrom-pytest.toml` is just a TOML representation
of a [container specification](../../spec.md#container-specification).

This chapter specifies all of the fields a `container` element may have. These
are usually pretty obvious mappings from [container specification
chapter](../../spec.md#container-specification).

## `image`

```toml
# Example 1: Specified as a table.
# Will only use specified fields from image.
[container.example-1]
image.name = "docker://python"
image.use = ["layers", "environment"]

# Example 2: Specified as a table without a `use` field.
# Will use all fields from image.
[container.example-2]
image.name = "docker://python"

# Example 3: Specified as a string.
# Will use all fields from image.
[container.example-3]
image = "docker://python"
```

As discussed [here](../../spec.md#parent-and-inheritance), a container spec can
have a parent, which can either be an OCI image a named container
specification. The `image` field is how we set the parent to an OCI image. This
field cannot be specified along with the [`parent`](#parent) field.

The `image` field may either be a table or a string.

If it's a table, then it must have a `name` sub-field that specifies the URI of
the image, as documented [here](../../container-images.html#container-image-uris).

It it's a table, it may optionally have a `use` sub-field (Example 1). This
sub-field must be a list of strings specifying what parts of the image to
inherit, taken from this set:
  - `layers`
  - `environment`
  - `working_directory`

If no `use` sub-field is specified, the container spec will inherit all
available fields (Example 2).

If the field is specified as a string instead of a table, then the string value
is taken as the `name` sub-field, with no `use` sub-field (Example 3).

## `parent`

```toml
# Example 1: Specified as a table.
# Will only use specified fields from the parent container.
[container.example-1]
parent.name = "other-container"
parent.use = ["layers", "environment", "mounts"]

# Example 2: Specified as a table without a `use` field.
# Will use all fields from the parent container.
[container.example-2]
parent.name = "other-container"

# Example 3: Specified as a string.
# Will use all fields from the parent container.
[container.example-3]
parent = "other-container"
```

As discussed [here](../../spec.md#parent-and-inheritance), a container spec can
have a parent, which can either be an OCI image a named container
specification. The `parent` field is how we set the parent to a named container
specification. This cannot be specified along with the [`image`](#image) field.

The `parent` field may either be a table or a string.

If it's a table, then it must have a `name` sub-field that references the name
of a container in the `containers` table.

It it's a table, it may optionally have a `use` sub-field (Example 1). This
sub-field must be a list of strings specifying what parts of the parent
container to inherit, taken from this set:
  - `layers`
  - `environment`
  - `working_directory`
  - `enable_writable_file_system`
  - `mounts`
  - `network`
  - `user`
  - `group`

If no `use` sub-field is specified, the container spec will inherit all
available fields (Example 2).

If the field is specified as a string instead of a table, then the string value
is taken as the `name` sub-field, with no `use` sub-field (Example 3).

## Vector Fields

The container spec has three vector fields: [`layers`](#layers),
[`environment`](#environment), and [`mounts`](#mounts). Associated with each
vector field is an associated `added_*` pseudo-field:
[`added_layers`](#added_layers), [`added_environment`](#added_environment), and
[`added_mounts`](#added_mounts).

[This section](../../spec.md#parent-and-inheritance) describes how a job
spec's vector fields are computed by starting with the job spec's own field
value, and then prepending the values first from the parent, then the
grandparent, and so on, as long as the field in question is `use`d.

When a vector field is set through a normal field (`layers`, etc.), any
implicitly inherited values are removed. In other words, if there is a
container or image parent, that field is removed from the `use` set if it was
there. This is like saying: "I don't care what values were inherited, set the
vector to be this new one."

On the other hand, if a vector field is set through an `added_*` pseudo-field
(`added_layers`, etc.), the `use` set is not modified and the container spec
continues to inherit that field from its parent. This is like saying: "Keep the
values that were inherited and add these new ones."

As this can be a little confusing, we apply some sanity checks whenever vector
fields are set. If a field is set through the `added_*` pseudo-field, then we
enforce that there is a parent, and that the parent has the appropriate field
in its `use` set.

On the other hand, setting a field through a normal field is disallowed if
there is a parent and that field has been explicitly listed in the `use` set.
The rationale is that if a field was explicitly listed in the `use` set and
also overwritten in the same spec, then the user is probably confused. On the
other hand, if the user didn't specify `use` at all, then it's probably okay to
allow them to implicitly remove that field from the `use` set.

Finally, a field cannot be set both through the normal field and `added_*`
pseudo-field. In this case, the user should just manually append the `added_*`
values to the normal field.

## `layers`

```toml
[container.example]
layers = [
    { tar = "layers/foo.tar" },
    { paths = ["layers/a/b.bin", "layers/a/c.bin"], strip_prefix = "layers/a/" },
    { glob = "layers/b/**", strip_prefix = "layers/b/" },
    { stubs = ["/dev/{null, full}", "/proc/"] },
    { symlinks = [{ link = "/dev/stdout", target = "/proc/self/fd/1" }] },
    { shared-library-dependencies = ["/bin/bash"], prepend_prefix = "/usr" }
]
```

This field is a [vector field](#vector-fields) that provides an ordered list of
layers for the container spec's [`layers`](../../spec.md#layers) field.

Each element of the list must be a table with one of the following keys:
  - `tar`: The value must be a string, indicating the local path of the tar
    file. This is used to create a [tar](../../spec-layers.md#tar) layer.
  - `glob`: The value must be a string, indicating the glob pattern to use to
    create a [glob](../../spec-layers.md#glob) layer. It may also include
    fields from [`prefix_options`](../../spec-layers.md#prefix_options) (see below).
  - `paths`: The value must be a list of strings, indicating the local paths of
    the files and directories to include to create a
    [paths](../../spec-layers.md#paths) layer. It may also include fields from
    [`prefix_options`](../../spec-layers.md#prefix_options) (see below).
  - `stubs`: The value must be a list of strings. These strings are optionally
    brace-expanded and used to create a [stubs](../../spec-layers.md#stubs)
    layer.
  - `symlinks`: The value must be a list of tables of `link`/`target` pairs.
    These strings are used to create a [symlinks](../../spec-layers.md#symlinks)
    layer.
  - `shared-library-dependencies`: The value must be list of strings, indicating local paths of
    binaries. This layer includes the set of shared libraries the binaries depend on. This
    includes `libc` and the dynamic linker. This doesn't include the binary
    itself. This is used to create a
    [shared-library-dependencies](../../spec-layers.md#sharedlibrarydependencies) layer.

If the layer is a `paths`, `glob`, or `shared-library-dependencies` layer, then the table can have
any of the following extra fields used to provide the
[`prefix_options`](../../spec-layers.md#prefix_options):
  - `follow_symlinks`: A boolean value. Used to specify [`follow_symlinks`](../../spec-layers.md#follow_symlinks).
  - `canonicalize`: A boolean value. Used to specify [`canonicalize`](../../spec-layers.md#canonicalize).
  - `strip_prefix`: A string value. Used to specify [`strip_prefix`](../../spec-layers.md#strip_prefix).
  - `prepend_prefix`: A string value. Used to specify [`prepend_prefix`](../../spec-layers.md#prepend_prefix).

For example:

```toml
[container.example]
layers = [
    { paths = ["layers"], strip_prefix = "layers/", prepend_prefix = "/usr/share/" },
]
```

This would create a layer containing all of the files and directories
(recursively) in the local `layers` subdirectory, mapping local file
`layers/example` to `/usr/share/example` in the test's container.

As described [above](#vector-fields), setting the `layers` field will overwrite
any layers inherited from an `image` or `parent` field. If the goal is to
append to those layers, use [`added_layers`](#added_layers) instead.

## `added_layers`

This field is like [`layers`](#layers), except it appends to the container spec's
inherited layers instead of replacing them. See [above](#vector-fields) for
more details.

## `environment`

```toml
[container.example-1]
environment.USER = "bob"
environment.PYTHONDEVMODE = "$env{PYTHONDEVMODE:-}"

[container.example-2]
environment = { USER = "bob", PYTHONDEVMODE = "$env{PYTHONDEVMODE:-0}" }
```

This field is a [vector field](#vector-fields) that provides an ordered list of
environment specs for the container spec's
[`environment`](../../spec.md#environment) field.

This field can technically be specified in one of two ways. The first is as
single table, as show in the two (equivalent) examples at the top of this
section.

The second is as a vector of explicit environment specs. However, the second
form is only useful for [`added_environment`](#added_environment). As a result,
we will save the discussion of it until then.

Setting the `environment` field to a map is shorthand for setting it to a vector
with a single environment spec, where that environment spec has `extend` set to
`true`. However, when setting `environment`, any inherited environment is
ignored, and the `extend` flag has no effect.

The specified environment variables go through parameter expansion, as [covered
here](../../spec.md#parameter-expansion). Since there will be no inherited
environment, the `$prev{}` form doesn't have much use with this field.

## `added_environment`

```toml
# Example 1.
# The end result will be two environment variables, PATH and USER. PATH will contain
# "/foo/bar/bin:" prepended to the PATH inherited from "parent". USER will be "bob".
[container.example-1]
parent = "parent"
added_environment = [
    { vars = { PATH = "/foo/bar/bin:$prev{PATH}" }, extend = false },
    { vars = { USER = "bob" }, extend = true },
]

# Example 2.
# The end result will be the union of the environment variables inherited from the
# specified image, plus USER, PATH, and PYTHONDEVMODE. PATH and BOB will be
# set as above. PYTHONDEVMODE will be taken from the client's environment
# variables, unless the client doesn't have that variable set, in which case it
# will be set to "".
[container.example-2]
image = "docker://python"
added_environment.USER = "bob"
added_environment.PATH = "/foo/bar/bin:$prev{PATH}"
added_environment.PYTHONDEVMODE = "$env{PYTHONDEVMODE:-}"

# Example 3.
# The end result will be one environment variables, PATH, which will contain
# "/foo/bar/bin:" prepended to the PATH inherited from "parent".
[container.example-3]
parent = "parent"
added_environment = [
    { vars = { PATH = "/foo/bar/bin:$prev{PATH}" }, extend = false },
]
```

This field is like [`environment`](#environment), except it appends to the
container spec's inherited environment specs instead of replacing them. See
[above](#vector-fields) for more details.

[This section](../../spec.md#environment) describes how the environment
variables for a job spec are determined. A candidate map is created, then a
vector of environment specs are evaluated against the candidate map to
determine the actual map of environment variables.

The `added_environment` pseudo-field provides a way to append environment specs
to inherited vector. This field can be specified one of two forms.

The first form is the full form, where a vector of environment specs are
provided (Example 1). Each spec has two fields:
  - `vars`: A table of environment variables. [Parameter
    expansion](../../spec.md#parameter-expansion) is performed on the values.
  - `extend`: Whether to extending the existing environment variables or replace them.

The second form is just a table (Example 2). This expands to a vector with a
single environment spec with `extend` set to `true`. This is a useful shorthand
as it's what one wants most of the time.

It's never strictly necessary to provide more than one element to the
vector: the elements can always be collapsed. However, it is sometimes
desirable to set `extend` to `false`. There is no shorthand for creating a
vector of just one spec, but with `extend` set to `false`. The only way to do
that is by explicitly specifying a vector of one element (Example 3). This is
useful when you want to strictly control the environment variables for a job,
but also want to be able to use some parent environment variables.

## `mounts`

```toml
[container.example]
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

This field is a [vector field](#vector-fields) that provides an ordered list of
mounts for the container spec's `mounts` field. It must be a list of tables,
each of which is a TOML translation of the corresponding type described
[here](../../spec.md#mounts).

## `added_mounts`

This field is like [`mounts`](#mounts), except it appends to the container spec's
inherited mounts instead of replacing them. See [above](#vector-fields) for
more details.

## Scalar Fields

The container spec has five scalar fields: [`working_directory`](#working_directory),
[`network`](#network),
[`enable_writable_file_system`](#enable_writable_file_system),
[`user`](#user), and [`group`](#group).

[This section](../../spec.md#parent-and-inheritance) describes how a job spec's
scalar fields are computed by starting with the job spec's own field value, and
traversing up the inheritance chain until a value is found, stopping either
when the chain ends or the field in question not in a `use` field.

If no value is found, then the default value is used, as documented in the
corresponding sections [here](../../spec.md#container-specification).

Setting a scalar field while also explicitly listing it in the `use` set is an error.
The rationale is the same as given above for [vector fields](#vector-fields).

## `working_directory`

```toml
[container.example]
working_directory = "/home/root/"
```

This field sets the
[`working_directory`](../../spec.md#working_directory) field of
the container spec. It must be a string.

## `network`

```toml
[container.example]
network = "loopback"
```

This field sets the [`network`](../../spec.md#network) field of the container spec. It
must be a string. It defaults to `"disabled"`.

## `enable_writable_file_system`

```toml
[container.example]
enable_writable_file_system = true
```

This field sets the
[`enable_writable_file_system`](../../spec.md#enable_writable_file_system)
field of the container spec. It must be a boolean.

## `user`

```toml
[container.example]
user = 1000
```

This field sets the [`user`](../../spec.md#user) field of the container
spec. It must be an unsigned, 32-bit integer.

## `group`

```toml
[container.example]
group = 1000
```

This field sets the [`group`](../../spec.md#group) field of the
container spec. It must be an unsigned, 32-bit integer.
