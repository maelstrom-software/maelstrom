# Job Specification Fields

This chapter specifies all of the possible fields for a job specification. Most, but
not all, of these fields have an obvious mapping to [job-spec
fields](../spec.md).

## `image`

This field must be an object as described below. It specifies the
[`image`](../spec.md#image) field of the job spec.

The provided object must have exactly two fields. The first is `name`. It must be a string. It
specifies the URI of the image to use, as documented
[here](../container-images.html#container-image-uris).

The second is `use`. It must be a list of strings specifying what parts of the
container image to use for the job spec. It must contain a non-empty subset of:
  - `layers`<a id="image-use-layers">: This sets the
	[`use_layers`](../spec.md#use_layers) field in the job spec's image value.
    This is incompatible with the [`layers`](#layers) field: use
    [`added_layers`](#added_layers) instead.
  - `environment`<a id="image-use-environment">: This sets the
	[`use_environment`](../spec.md#use_environment) field in the job spec's
	image value. This is incompatible with the [`environment`](#environment) field specified
    using [implicit mode](#implicit-mode): use [explicit mode](#explicit-mode)
    instead.
  - `working_directory`<a id="image-use-working-directory">: This sets the
	[`use_working_directory`](../spec.md#use_working_directory) field in the job spec's
	image value. This is incompatible with the
    [`working_directory`](#working_directory) field.

## `program`

This field must be a string, and it specifies the program to be run. It sets
the [`program`](../spec.md#program) field of the job spec. It must be provided. 

## `arguments`

This field must be a list of strings, and it specifies the program's arguments.
It sets the [`arguments`](../spec.md#arguments) field of the job spec. If not
provided, the job spec will have an empty arguments vector.

## `environment`

This field sets the [`environment`](../spec.md#environment) field of the job
spec. If not provided, the job spec will have an empty environment vectors.

This field can be specified in one of two ways: implicit mode and explicit mode.

### Implicit Mode

In this mode, a JSON map from string to string is expected. The job spec will
have a single element in it, with the provided map. This is usually what you want.

```json
% BAR=bar maelstrom-run --one
{
        "image": {
                "name": "docker://ubuntu",
                "use": [ "layers" ]
        },
        "program": "/usr/bin/env",
        "environment": {
                "FOO": "foo",
                "BAR": "$env{BAR}"
        }
}
BAR=bar
FOO=foo
%
```

This mode is incompatible with a [`use` of `environment` in the `image`](#image-use-environment). It's
ambiguous whether the desired environment is the one provided with `environment` or the
one from the image:

```json
% maelstrom-run --one
{
        "image": {
                "name": "docker://ubuntu",
                "use": [ "layers", "environment" ]
        },
        "program": "/usr/bin/env",
        "environment": {
                "FOO": "foo",
                "BAR": "bar"
        }
}
Error: field `environment` must provide `extend` flags if [`image` with a `use`
of `environment`](#image-use-environment) is also set at line 11 column 1
%
```

### Explicit Mode

In this mode, a list of [`EnvironmentSpec`](../spec.md#environment) is
provided, and they are used verbatim in the job spec.

For example:

```json
% BAR=bar maelstrom-run --one
{
        "image": {
                "name": "docker://ubuntu",
                "use": [ "layers", "environment" ]
        },
        "program": "/usr/bin/env",
        "environment": [
                { "vars": { "PATH": "$prev{PATH}", "FOO": "foo" }, "extend": false },
                { "vars": { "BAR": "$env{BAR}" }, "extend": true }
        ]
}
BAR=bar
FOO=foo
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
%
```

## `layers`

This field sets the [`layers`](../spec.md#layers) field of the job
spec. Either this field, or an [`image` with a `use` of `layers`](#image-use-layers) must be
provided, but not both. 

To add additional layers beyond those provided by an image, use
[`added_layers`](#added_layers).

Here is an example of specifying layers:
```json
% wget -O busybox https://busybox.net/downloads/binaries/1.35.0-x86_64-linux-musl/busybox
...
% chmod +x ./busybox
% maelstrom-run --one
{
        "layers": [
                { "paths": [ "busybox" ] },
                { "symlinks": [{ "link": "/ls", "target": "/busybox" }] }
        ],
        "program": "/ls"
}
busybox
ls
%
```

## `added_layers`

This is just like [`layers`](#layers), except is can only be used with an
[`image` that has a `use` of `layers`](#image-use-layers). The provided layers
will be append to the layers provided by the image when creating the job spec.

Here's an example:
```json
% maelstrom-run --one
{
        "image": {
                "name": "docker://ubuntu",
                "use": [ "layers" ]
        },
        "added_layers": [
                { "stubs": [ "/foo/{bar,baz}" ] }
        ],
        "program": "/bin/ls",
        "arguments": [ "/foo" ]
}
bar
baz
%
```

## `mounts`

This field sets the [`mounts`](../spec.md#mounts) field of the job spec.
If this field isn't specified, an empty `mounts` will be set in the job spec.

The field must be a list of objects, where the format is the direct JSON
translation of the corresponding [job-spec type](../spec.md#mounts).

For example:
```json
% maelstrom-run
{
        "image": {
                "name": "docker://ubuntu",
                "use": ["layers", "environment"]
        },
        "added_layers": [
                { "stubs": [ "/dev/{null,zero}" ] }
        ],
        "mounts": [
                { "type": "proc", "mount_point": "/proc" },
                { "type": "tmp", "mount_point": "/tmp" },
                { "type": "devices", "devices": [ "null", "zero" ] }
        ],
        "program": "mount"
}
Maelstrom LayerFS on / type fuse (ro,nosuid,nodev,relatime,user_id=0,group_id=0)
none on /proc type proc (rw,relatime)
none on /tmp type tmpfs (rw,relatime,uid=1000,gid=1000,inode64)
udev on /dev/null type devtmpfs (rw,nosuid,relatime,size=8087700k,nr_inodes=2021925,mode=755,inode64)
udev on /dev/zero type devtmpfs (rw,nosuid,relatime,size=8087700k,nr_inodes=2021925,mode=755,inode64)
%
```

Bind mounts can be used to transfer data out of the job:
```json
% touch output
% cat output
% maelstrom-run --one
{
        "image": {
                "name": "docker://ubuntu",
                "use": ["layers", "environment"]
        },
        "added_layers": [
                { "stubs": [ "/output" ] }
        ],
        "mounts": [
                {
                        "type": "bind",
                        "mount_point": "/output",
                        "local_path": "output",
                        "read_only": false
                }
        ],
        "program": "bash",
        "arguments": [ "-c", "echo foo >output" ]
}
% cat output
foo
%
```

## `network`

This field must be a string with a value of one: `"disabled"`, `"loopback"`,
`"local"`. It sets the [`network`](../spec.md#network) field of the job spec to
the provided value. If this field isn't provided, the default of `"disabled"`
is used.

## `enable_writable_file_system`

This field must be a boolean value. If it's `true`, it sets the
[`root_overlay`](../spec.md#root_overlay) field of the job spec to `Tmp`. If
it's not specified, or is set to `false`, the [`root_overlay`] field will be
`None`.

## `working_directory`

This field must be a string, and it specifies the working directory of the
program be run. It sets the [`working_directory`](../spec.md#working_directory)
field of the job spec. If not provided, `/` will be used.

This field is incompatible with an [`image` that has a `use` of
`working_directory`](#image-use-working-directory).

For example:
```json
% maelstrom-run --one
{
        "image": {
                "name": "docker://ubuntu",
                "use": [ "layers", "environment" ]
        },
        "program": "pwd"
}
/
% maelstrom-run --one
{
        "image": {
                "name": "docker://ubuntu",
                "use": [ "layers", "environment" ]
        },
        "program": "pwd",
        "working_directory": "/root"
}
/root
%
```

## `user`

This field must be an integer, and it specifies the UID of the program to be run.
It sets the [`user`](../spec.md#user) field of the job spec. If not provided,
`0` will be used.

For example:
```json
% maelstrom-run --one
{
        "image": {
                "name": "docker://ubuntu",
                "use": [ "layers", "environment" ]
        },
        "program": "id"
}
uid=0(root) gid=0(root) groups=0(root),65534(nogroup)
% maelstrom-run --one
{
        "image": {
                "name": "docker://ubuntu",
                "use": [ "layers", "environment" ]
        },
        "program": "id",
        "user": 1234
}
uid=1234 gid=0(root) groups=0(root),65534(nogroup)
%
```

## `group`

This field must be an integer, and it specifies the UID of the program to be
run. It sets the [`group`](../spec.md#group) field of the job spec. If not
provided, `0` will be used.

For example:
```json
% maelstrom-run --one
{
        "image": {
                "name": "docker://ubuntu",
                "use": [ "layers", "environment" ]
        },
        "program": "id"
}
uid=0(root) gid=0(root) groups=0(root),65534(nogroup)
% maelstrom-run --one
{
        "image": {
                "name": "docker://ubuntu",
                "use": [ "layers", "environment" ]
        },
        "program": "id",
        "group": 4321
}
uid=0(root) gid=4321 groups=4321,65534(nogroup)
%
```

## `timeout`

This field must be an integers, and it specifies a timeout for the job in
seconds. It sets the [`timeout`](../spec.md#timeout) field of the job spec. If
not provided, the job will have no timeout.

For example:
```json
% maelstrom-run --one
{
        "image": {
                "name": "docker://ubuntu",
                "use": [ "layers", "environment" ]
        },
        "program": "sleep",
        "arguments": [ "1d" ],
        "timeout": 1
}
timed out
%
```
