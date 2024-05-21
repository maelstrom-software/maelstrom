# The Job Specification

Each job run by Maelstrom is defined by a **job specification**, or "job spec"
for short. Understanding job specifications is important for understanding
what's going on with your tests, for toubleshooting failing tests, and for
understanding `cargo-maelstrom`'s [configuration
directives](cargo-maelstrom/spec.md) and `maelstrom-run`'s [input
format](maelstrom-run/spec.md).

This chapter shows the specification and its related types in the [Protocol
Buffer](https://protobuf.dev/programming-guides/proto3/) format. This is done
because it's a convenient format to use for documentation. You won't have to
interact with Maelstrom at this level. Instead, `cargo-maelstrom` and
`maelstrom-run` will have analogous configuration options for proiding the job
specification.

This is the Protocol Buffer for the `JobSpec`:

```protobuf
message JobSpec {
    string program = 1;
    repeated string arguments = 2;
    optional ImageSpec image = 3;
    repeated LayerSpec layers = 4;
    bool enable_writable_file_system = 5;
    repeated EnvironmentSpec environment = 6;
    optional string working_directory = 7;
    repeated JobDevice devices = 8;
    repeated JobMount mounts = 9;
    JobNetwork network = 10;
    uint32 user = 11;
    uint32 group = 12;
    optional uint32 timeout = 13;
    optional Duration estimated_duration = 14;
}
```

## `program`

```protobuf
message JobSpec {
    string program = 1;
    // ...
}
```

This is the path of the program to run, relative to the
[`working_directory`](#working_directory). The job will complete when this
program terminates, regardless of any other processes that have been started.

The path must be specified explicitly: the `PATH` environment variable will not
be searched, even if it is provided.

The program is run as PID 1 in its own PID namespace, which means that it acts
as the `init` process for the container. This shouldn't matter for most use
cases, but if the program starts a lot of subprocesses, it may need to
explicitly clean up after them.

The program is run as both session and process group leader.

## `arguments`

```protobuf
message JobSpec {
    // ...
    repeated string arguments = 2;
    // ...
}
```

These are the arguments to pass to [`program`](#program), excluding the name of
the program itself. For example, to run `cat foo bar`, you would set `program`
to `"cat"` and `arguments` to `["foo", "bar"]`.

## `image`

```protobuf
message JobSpec {
    // ...
    optional ImageSpec image = 3;
    // ...
}

message ImageSpec {
    string name = 1;
    string tag = 2;
    bool use_layers = 3;
    bool use_environment = 4;
    bool use_working_directory = 5;
}
```

The optional `image` field lets one define a job's container based off of an
OCI container image. See [here](container-images.md) for more information. The
`name` and `tag` are used to specify which image to use.

### `use_layers`

The `use_layers` flag indicates that the job specification should use the
image's layers as the bottom of it's layers stack. More layers can be added
with the job specification's [`layers`](#layers) field.

### `use_environment`

The `use_environment` flag indicates that the job specification should use the
image's environment variables as a base. These can be modified with the job
specifications's [`environment`](#environment) field.

### `use_working_directory`

The `use_environment` flag indicates that the job specification should use the
image's working directory instead of one provided in the job specification's
[`working_directory`](#working_directory) field. If
this flag is set, it is an error to also provide a `working_directory` field.
It is also an error to set this flag with an image that doesn't provide a
working directory.

## `layers`

```protobuf
message JobSpec {
    // ...
    repeated LayerSpec layers = 4;
    // ...
}

message LayerSpec {
    bytes digest = 1;
    ArtifactType type = 2;
}

enum ArtifactType {
    Tar = 0;
    Manifest = 1;
}
```

The file system layers specify what file system the program will be run with.
They are stacked on top of each other, starting with the first layer, with
later layers overriding earlier layers.

Each layer will be a tar file, or Maelstrom's equivalent called a "manifest
file". These `LayerSpec` objects are usually generated with an
`AddLayerRequest`, which is described [in the next chapter](spec/layers.md).
[`cargo-maelstrom`](cargo-maelstrom/spec.md) and
[`maelstrom-run`](maelstrom-run/spec.md) provide ways to conveniently specify
these, as described in their respective chapters.

If the [`image`](#image) field is provided, and it contains
[`use_layers`](#use_layers), then the layers provided in this field are stacked
on top of the layers provided by the image.

## `enable_writable_file_system`

```protobuf
message JobSpec {
    // ...
    bool enable_writable_file_system = 5;
    // ...
}
```

By default, the whole file system the job sees will be read-only, except for
any extra file systems specified in [`mounts`](#mounts).

Enabling this flag will make the file system writable. Any changes the job
makes to the file system will be isolated, and will be thrown away when the job
completes.

## `environment`

```protobuf
message JobSpec {
    // ...
    repeated EnvironmentSpec environment = 6;
    // ...
}

message EnvironmentSpec {
    map<string, string> vars = 1;
    bool extend = 2;
}
```

The `environment` field specifies the environment variables passed to
[`program`](#program). This will be a map of key-value pairs of strings.

The compute the environment-variable map for a job, the client starts with
either an empty map or with the environment variables provided by an image
provided by the [`image`](#image) field, and then only if the
[`use_environment`](#use_environment) flag is set. This map is called the
*candidate* map.

Then, for each map provided by an `EnvironmentSpec` element, it performs
[parameter expansion](#parameter-expansion) and then [merges](#merging) the
resulting map into the candidate map. Once this has been done for every
`EnvironmentSpec` element, the candidate map is used for the job.

### Merging Environment-Variable Maps {#merging}

If the `extend` flag is `false`, then the element's newly-computed
environment-variable map will overwrite the candidate map.

If the `extend` flags is `true`, then the element's newly-computed
environment-variable map will be merged into the candidate map: All variables
specified in the element's map will overwrite the old values in the candidate
map, but values not specified in the element's map will be left unchanged.

### Environment-Variable Parameter Expansion {#parameter-expansion}

Parameter substitution is applied to The values provided in the
`EnvironmentSpec` maps. A parameter has one of two forms:

  - `$env{FOO}` evaluates to the value of the client's `FOO` environment
    variable.
  - `$prev{FOO}` evaluates to the value of the `FOO` environment variable in
    the partially-computed map, without this map applied.

It is an error if the referenced variable doesn't exist. However, `:-` can be
used to provide a default value, like `$env{VAR:-default}`.

### Environment-Variable Examples

Here are some examples of specifying the environment variable map.

The simplest example is when a single element is provided and either the
`image` field is not provided or the `use_environment` flag is false. In this
case, the variables specified will be provided to the job.

```json
[{ "vars": { "FOO": "foo", "BAR": "bar" }, "extend": false }]
[{ "vars": { "FOO": "foo", "BAR": "bar" }, "extend": true }]
```

Both of these will result in job being given two environment variables: `FOO=foo` and `BAR=bar`.

We can use the `$env{}` syntax to import variables from the client's
environment. It can be useful to use `:-` in these cases to provide a default.

```json
[{
    "vars": {
        "FOO": "$env{FOO}",
        "RUST_BACKTRACE": "$env{RUST_BACKTRACE:-0}"
    },
    "extend": false
}]
```

This will pass the client's value of `$FOO` to the job, and will error if there
was no `$FOO` for the client. On the other hand, the job will be passed the
client's `$RUST_BACKTRACE`, but if the client doesn't have that variable,
`RUST_BACKTRACE=0` will be provided.

We can use the `$prev{}` syntax to extract values from earlier in the array or
from the specified image. For example, assume `image` is provided, and
`use_environment` is set:

```json
[{ "vars": { "PATH": "/my-bin:$prev{PATH}" }, "extend": true }]
```

This will prepend `/my-bin` to the `PATH` environment variable provided by the
image. All other environment variables specified by the image will be
preserved.

On the other hand, if we just wanted to use the image's `FOO` and `BAR`
environment variables, but not any other ones it provided, we could do this:

```json
[{ "vars": { "FOO": "$prev{FOO}", "BAR": "$prev{BAR}" }, "extend": false }]
```

Because `extend` is `false`, only `FOO` and `BAR` will be provided to the job.

It's possible to provide multiple `environment` elements. This feature is
mostly aimed at programs, not humans, but it lets us provide an example that
illustrates multiple features:

```json
[
    { "vars": { "FOO": "foo1", "BAR": "bar1" }, "extend": false },
    { "vars": { "FOO": "foo2", "BAZ": "$env{BAZ}" }, "extend": true },
    { "vars": { "FOO": "$prev{BAZ}", "BAR": "$prev{BAR}" }, "extend": false },
]
```

The first element sets up an initial map with `FOO=foo1` and `BAR=bar1`.

The second element sets `FOO=foo2` and `BAZ=<client-baz>`, where
`<client-baz>` is the client's value for `$BAZ`. Because `extend` is `true`,
`BAR` isn't changed.

The third element sets `FOO=<client-baz>` and `BAR=bar1`. Because `extend` is
`false`, the value for `BAZ` is removed. The end result is:

```json
{ "FOO": "<client-baz>", BAR: "bar1" }
```

### `PATH` Environment Variable

The `PATH` environment variable is **not** used when searching for
[`program`](#program). The `program` field must contain a full path to the
program, relative to the [`working_directory`](#working_directory) field.

## `working_directory`

```protobuf
message JobSpec {
    // ...
    optional string working_directory = 7;
    // ...
}
```

This specifies the directory that [`program`](#program) is run in. The path
provided in `program` will also be evaluated relative to this directory. If
this isn't provided, `/` is used.

## `devices`

```protobuf
message JobSpec {
    // ...
    repeated JobDevice devices = 8;
    // ...
}

enum JobDevice {
    Full = 0;
    Fuse = 1;
    Null = 2;
    Random = 3;
    Shm = 4;
    Tty = 5;
    Urandom = 6;
    Zero = 7;
}
```

These are the device files from `/dev` to add to the job's environment. Any subset can be specified.

Any specified device will be mounted in `/dev` based on its name. For example,
`Null` would be mounted at `/dev/null`. For this to work, there must be a
file located at the expected location in the container file system. In other
words, if your job is going to specify `Null`, it also needs to have an empty
file at `/dev/null` for the system to mount the device onto. This is one of the
use cases for the "stubs" layer type.

## `mounts`

```protobuf
message JobSpec {
    // ...
    repeated JobMount mounts = 9;
    // ...
}

message JobMount {
    oneof Mount {
        ProcMount proc = 1;
        TmpMount tmp = 2;
        SysMount sys = 3;
        BindMount bind = 4;
    }
}
```

These are extra file systems mounts put into the job's environment. They are
applied in order, and **the `mount_point` must already exist in the file
system**. Providing the mount point is one of the use cases for the "stubs" layer type.

Every mount type has a `mount_point` field, which is relative to the root of
the file system, even if there is a [`working_directory`](#working_directory)
specified.

### Proc

```protobuf
message ProcMount {
    string mount_point = 1;
}
```

This provides a [`proc`](https://docs.kernel.org/filesystems/proc.html) file
system at the provided mount point.

### Tmp

```protobuf
message TmpMount {
    string mount_point = 1;
}
```

This provides a [`tmpfs`](https://docs.kernel.org/filesystems/tmpfs.html) file
system at the provided mount point.


### Sys

```protobuf
message SysMount {
    string mount_point = 1;
}
```

This provides a [`sysfs`](https://docs.kernel.org/filesystems/sysfs.html) file
system at the provided mount point.

### Bind

```
message BindMount {
    string mount_point = 1;
    string local_path = 2;
    bool read_only = 3;
}
```

If the job has a bind mount, it will become a [local-only
job](local-worker.md), that can only be run on the local worker, not
distributed on a cluster. However, bind mounts can be a useful "escape valve"
for certain jobs that are tricky to containerize.

With a bind mount, the directory at `mount_point` in the job's container will
refer to the directory at `local_path` in the client. In other words,
`local_path` made available to the job at `mount_point` within the container.
`local_path` is evaluated relative to the client's [project
directory](project-dir.md).

The `read_only` flag specifies whether or not the job can write to the
directory. **NOTE**: the mount isn't "locked", which means that if the job
really wants to, it can remount `mount_point` read-write and modify the
contents of the directory. We may consider locking mount points in a future
version of Maelstrom.

## `network`

```protobuf
message JobSpec {
    // ...
    JobNetwork network = 10;
    // ...
}

enum JobNetwork {
    Disabled = 0;
    Loopback = 1;
    Local = 2;
}
```

By default, jobs are run with `Disabled`, which means they are completed
disconnected from the network. This behavior can be changed with this flag.

If the network is `Disabled`, jobs don't even have a loopback device enabled,
and thus cannot communicate to `localhost`/`127.0.0.1`/`::1`. Setting this
field to `Loopback` will provide a loopback device, allowing communication to 
`localhost`/`127.0.0.1`/`::1`

If this field is set to `Local`, the job will become a [local-only
job](local-worker.md), that can only be run on the local worker, not
distributed on a cluster. The job will then be run without a network namespace,
meaning that it will have access to all of the local machine's network devices.

In the future, we plan to add more `network` options that will allow clustered
jobs to communicate with the network. Until that time, if a job really has to
communicate on the network, it must use `Local`.

## `user`

```protobuf
message JobSpec {
    // ...
    uint32 user = 11;
    // ...
}
```

This specifies the UID the program is run as.

Maelstrom runs all of its jobs in rootless containers, meaning that they don't
require any elevated permissions on the host machines. All containers will be
run on the host machine as the user running `cargo-maelstrom`, `maelstrom-run`,
or `maelstrom-worker`, regardless of what this field is set as.

However, if this is field is set to 0, the program will have some elevated
permissions within the container, which may be undesirable for some jobs.

## `group`

```protobuf
message JobSpec {
    // ...
    uint32 group = 12;
    // ...
}
```

The specifies the GID the program is run as. See [`user`](#user) for more information.

Jobs don't have any supplemental GIDs, nor is there any way to provide them.

## `timeout`

```protobuf
message JobSpec {
    // ...
    optional uint32 timeout = 13;
    // ...
}
```

This specifies an optional timeout for the job, in seconds. If the job takes
longer than the timeout, Maelstrom will terminate it and return the partial
results. A value of 0 indicates an infinite timeout.

## `estimated_duration`

```protobuf
message JobSpec {
    // ...
    optional Duration estimated_duration = 14;
    // ...
}

message Duration {
    uint64 seconds = 1;
    uint32 nano_seconds = 2;
}
```

The `estimated_duration` field is used to allow Maelstrom to use
[longest-processing-time-first
scheduling](https://en.wikipedia.org/wiki/Longest-processing-time-first_scheduling)
(LPT). It's up to clients to provide a best guess of how long a job will take.
If they can't provide one, they leave this field empty.

<span style="white-space: nowrap;">`cargo-maelstom`</span> [keeps
track](cargo-maelstrom/target-dir.md#test-listing) of how long previous
instances of a test took and uses that information to fill in this field.
