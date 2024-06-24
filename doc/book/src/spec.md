# The Job Specification

Each job run by Maelstrom is defined by a **job specification**, or "job spec"
for short. Understanding job specifications is important for understanding
what's going on with your tests, for toubleshooting failing tests, and for
understanding test runners' [configuration
directives](cargo-maelstrom/spec.md) and `maelstrom-run`'s [input
format](maelstrom-run/spec.md).

This chapter shows the specification and its related types in Rust. This is
done because it's a convenient format to use for documentation. You won't have
to interact with Maelstrom at this level. Instead, clients will have analogous
configuration options for providing the job specification.

This is what the `JobSpec` looks like:

```rust
pub struct JobSpec {
    pub program: Utf8PathBuf,
    pub arguments: Vec<String>,
    pub image: Option<ImageSpec>,
    pub environment: Vec<EnvironmentSpec>,
    pub layers: Vec<(Sha256Digest, ArtifactType)>,
    pub devices: EnumSet<JobDevice>,
    pub mounts: Vec<JobMount>,
    pub network: JobNetwork,
    pub root_overlay: JobRootOverlay,
    pub working_directory: Option<Utf8PathBuf>,
    pub user: UserId,
    pub group: GroupId,
    pub timeout: Option<Timeout>,
    pub estimated_duration: Option<Duration>,
    pub allocate_tty: Option<JobTty>,
}
```

## `program`

```rust
pub struct JobSpec {
    pub program: Utf8PathBuf,
    // ...
}
```

This is the path of the program to run, relative to the
[`working_directory`](#working_directory). The job will complete when this
program terminates, regardless of any other processes that have been started.

The path must be valid UTF-8. Maelstrom doesn't support arbitrary binary
strings for the path.

If `program` does not contain a `/`, then the program will be searched for in
every directory specified in the `PATH` [environment variable](#environment),
similar to how [execlp, execvp, and
execvpe](https://man7.org/linux/man-pages/man3/exec.3.html) work.

The program is run as PID 1 in its own PID namespace, which means that it acts
as the `init` process for the container. This shouldn't matter for most use
cases, but if the program starts a lot of subprocesses, it may need to
explicitly clean up after them.

The program is run as both session and process group leader. It will not have a
controlling terminal unless [`allocate_tty`](#allocate_tty) is provided.

## `arguments`

```rust
pub struct JobSpec {
    // ...
    pub arguments: Vec<String>,
    // ...
}
```

These are the arguments to pass to [`program`](#program), excluding the name of
the program itself. For example, to run `cat foo bar`, you would set `program`
to `"cat"` and `arguments` to `["foo", "bar"]`.

## `image`

```rust
pub struct JobSpec {
    // ...
    pub image: Option<ImageSpec>,
    // ...
}

pub struct ImageSpec {
    pub name: String,
    pub use_layers: bool,
    pub use_environment: bool,
    pub use_working_directory: bool,
}
```

The optional `image` field lets one define a job's container based off of an
OCI container image. See [here](container-images.md) for more information.

### `name`

The `name` field is a URI in the format defined
[here](container-images.md#container-image-uris).

### `use_layers`

A `use_layers` value of `true` indicates that the job specification should use
the image's layers as the bottom of it's layers stack. More layers can be added
with the job specification's [`layers`](#layers) field.

### `use_environment`

A `use_environment` value of `true` indicates that the job specification should
use the image's environment variables as a base. These can be modified with the
job specifications's [`environment`](#environment) field.

### `use_working_directory`

A `use_environment` value of `true` indicates that the job specification should
use the image's working directory instead of one provided in the job
specification's [`working_directory`](#working_directory) field. If this flag
is set, it is an error to also provide a `working_directory` field. It is also
an error to set this flag with an image that doesn't provide a working
directory.

## `environment`

```rust
pub struct JobSpec {
    // ...
    pub environment: Vec<EnvironmentSpec>,
    // ...
}

pub struct EnvironmentSpec {
    pub vars: BTreeMap<String, String>,
    pub extend: bool,
}
```

The `environment` field specifies the environment variables passed to
[`program`](#program). This will be a map of key-value pairs of strings.

To compute the environment-variable map for a job, the client starts with
either an empty map or with the environment variables provided by an image
provided by the [`image`](#image) field, and then only if the
[`use_environment`](#use_environment) flag is set. This map is called the
_candidate_ map.

Then, for each map provided by an `EnvironmentSpec` element, it performs
[parameter expansion](#parameter-expansion) and then [merges](#merging) the
resulting map into the candidate map. Once this has been done for every
`EnvironmentSpec` element, the candidate map is used for the job.

### Merging Environment-Variable Maps {#merging}

If the `extend` flag is `false`, then the element's newly-computed
environment-variable map will overwrite the candidate map.

If the `extend` flag is `true`, then the element's newly-computed
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

If the [`program`](#program) field doesn't include a `/` character, then the
`PATH` environment variable is used when searching for [`program`](#program),
is done for [execlp, execvp, and
execvpe](https://man7.org/linux/man-pages/man3/exec.3.html).

## `layers`

```rust
pub struct JobSpec {
    // ...
    pub layers: Vec<(Sha256Digest, ArtifactType)>,
    // ...
}

pub enum ArtifactType {
    /// A .tar file
    Tar,
    /// A serialized `Manifest`
    Manifest,
}
```

The file system layers specify what file system the program will be run with.
They are stacked on top of each other, starting with the first layer, with
later layers overriding earlier layers.

Each layer will be a tar file, or Maelstrom's equivalent called a "manifest
file". These `LayerSpec` objects are usually generated with an
`AddLayerRequest`, which is described [in the next chapter](spec/layers.md).
[Test runners](cargo-maelstrom/spec.md) and
[`maelstrom-run`](maelstrom-run/spec.md) provide ways to conveniently specify
these, as described in their respective chapters.

If the [`image`](#image) field is provided, and it contains
[`use_layers`](#use_layers), then the layers provided in this field are stacked
on top of the layers provided by the image.

## `devices`

```rust
pub struct JobSpec {
    // ...
    pub devices: EnumSet<JobDevice>,
    // ...
}

pub enum JobDevice {
    Full,
    Fuse,
    Null,
    Random,
    Shm,
    Tty,
    Urandom,
    Zero,
}
```

This field contains the set of device files from `/dev` to add to the job's
environment. Any subset can be specified.

Any specified device will be mounted in `/dev` based on its name. For example,
`Null` would be mounted at `/dev/null`. For this to work, there must be a
file located at the expected location in the container file system. In other
words, if your job is going to specify `Null`, it also needs to have an empty
file at `/dev/null` for the system to mount the device onto. This is one of the
use cases for the "stubs" layer type.

This field is deprecated. The [`Devices` mount type](#devices-mount) should be used instead.

## `mounts`

```rust
pub struct JobSpec {
    // ...
    pub mounts: Vec<JobMount>,
    // ...
}

pub enum JobMount {
    Bind {
        mount_point: Utf8PathBuf,
        local_path: Utf8PathBuf,
        read_only: bool,
    },
    Devices {
        devices: EnumSet<JobDevice>,
    },
    Devpts {
        mount_point: Utf8PathBuf,
    },
    Mqueue {
        mount_point: Utf8PathBuf,
    },
    Proc {
        mount_point: Utf8PathBuf,
    },
    Sys {
        mount_point: Utf8PathBuf,
    },
    Tmp {
        mount_point: Utf8PathBuf,
    },
}
```

These are extra file systems mounts put into the job's environment. They are
applied in order, and **the `mount_point` must already exist in the file
system**. Providing the mount point is one of the use cases for the "stubs" layer type.

Every mount type except `Devices` has a `mount_point` field, which is relative
to the root of the file system, even if there is a
[`working_directory`](#working_directory) specified.

### Bind

```rust
pub enum JobMount {
    Bind {
        mount_point: Utf8PathBuf,
        local_path: Utf8PathBuf,
        read_only: bool,
    },
    // ...
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

### Devices {#devices-mount}

```rust
pub enum JobMount {
    // ...
    Devices {
        devices: EnumSet<JobDevice>,
    },
    // ...
}

pub enum JobDevice {
    Full,
    Fuse,
    Null,
    Random,
    Shm,
    Tty,
    Urandom,
    Zero,
}
```

This mount types contains the set of device files from `/dev` to add to the job's
environment. Any subset can be specified.

Any specified device will be mounted in `/dev` based on its name. For example,
`Null` would be mounted at `/dev/null`. For this to work, there must be a
file located at the expected location in the container file system. In other
words, if your job is going to specify `Null`, it also needs to have an empty
file at `/dev/null` for the system to mount the device onto. This is one of the
use cases for the "stubs" layer type.

### Devpts

```rust
pub enum JobMount {
    // ...
    Devpts {
        mount_point: Utf8PathBuf,
    },
    // ...
}
```

This provides a [`devpts`](https://docs.kernel.org/filesystems/devpts.html) file
system at the provided mount point. Maelstrom will always specify a
`ptmxmode=0666` option.

If this file system is mounted, it usually makes sense to also add a symlink
from `/dev/pts/ptmx` (or wherever the file system is mounted) to `/dev/ptmx`.
This can be done with the [`symlinks` layer type](spec/layers.md#symlinks).

### Mqueue

```rust
pub enum JobMount {
    // ...
    Mqueue {
        mount_point: Utf8PathBuf,
    },
    // ...
}
```

This provides an [`mqueue`](https://man7.org/linux/man-pages/man7/mq_overview.7.html) file
system at the provided mount point.

### Proc

```rust
pub enum JobMount {
    // ...
    Proc {
        mount_point: Utf8PathBuf,
    },
    // ...
}
```

This provides a [`proc`](https://docs.kernel.org/filesystems/proc.html) file
system at the provided mount point.

### Sys

```rust
pub enum JobMount {
    // ...
    Sys {
        mount_point: Utf8PathBuf,
    },
    // ...
}
```

This provides a [`sysfs`](https://docs.kernel.org/filesystems/sysfs.html) file
system at the provided mount point.

Linux disallows this mount type when using [local networking](#network). Jobs
that specify both will receive an execution error and fail to run.

### Tmp

```rust
pub enum JobMount {
    // ...
    Tmp {
        mount_point: Utf8PathBuf,
    },
}
```

This provides a [`tmpfs`](https://docs.kernel.org/filesystems/tmpfs.html) file
system at the provided mount point.

## `network`

```rust
pub struct JobSpec {
    // ...
    pub network: JobNetwork,
    // ...
}

pub enum JobNetwork {
    Disabled,
    Loopback,
    Local,
}
```

By default, jobs are run with `Disabled`, which means they are completely
disconnected from the network, without even a loopback interface. This means
that they cannot communicate on `localhost`/`127.0.0.1`/`::1`.

If this field is set to `Loopback`, then the job will have a loopback interface
and will be able to communicate on `localhost`/`127.0.0.1`/`::1`, but otherwise
will be disconnected from the network.

If this field is set to `Local`, the job will become a [local-only
job](local-worker.md), that can only be run on the local worker, not
distributed on a cluster. The job will then be run without a network namespace,
meaning that it will have access to all of the local machine's network devices.
Note: if the job also specifieds a [`Sys`](#sys) file system mount, Linux will
fail to execute the job.

In the future, we plan to add more `network` options that will allow clustered
jobs to communicate with the network. Until that time, if a job really has to
communicate on the network, it must use `Local`.

## `root_overlay`

```rust
pub struct JobSpec {
    // ...
    pub root_overlay: JobRootOverlay,
    // ...
} 

pub enum JobRootOverlay {
    None,
    Tmp,
    Local {
        upper: Utf8PathBuf,
        work: Utf8PathBuf,
    },
}
```

The `root_overlay` field controls whether the job's root file system should be
mounted read-only or read-write, and if it is mounted read-write, whether to
capture the changes the job made.

Note that this field doesn't affect any file systems specified in
[`mounts`](#mounts): those will be writable.

The `None` value is the default, which means the job's root file system will be
read-only.

The `Tmp` value means that `/` will be an [`overlayfs` file
system](https://docs.kernel.org/filesystems/overlayfs.html), with "lower"
being the file system specified by the [layers](#layers), and "upper"
being a [`tmpfs`](#tmp) file system. This will yield writable root file
system. The contents of "upper" (i.e. the changes made by the job to
the root file system) will be thrown away when the job terminates.

The `Local` value means that `/` will be an [`overlayfs` file
system](https://docs.kernel.org/filesystems/overlayfs.html), with "lower"
being the file system specified by the [layers](#layers), and "upper"
being a local directory on the client. This value implies that the job will be
a [local-only job](local-worker.md). When the job completes, the client can
read the change it made to the root filesystem on `upper`. The `work` field
must be a directory on the same file system as `upper`, and is used internally
by `overlayfs`.

## `working_directory`

```rust
pub struct JobSpec {
    // ...
    pub working_directory: Option<Utf8PathBuf>,
    // ...
}
```

This specifies the directory that [`program`](#program) is run in. The path
provided in `program` will also be evaluated relative to this directory. If
this isn't provided, `/` is used.

## `user`

```rust
pub struct JobSpec {
    // ...
    pub user: UserId,
    // ...
}

pub struct UserId(u32);
```

This specifies the UID the program is run as.

Maelstrom runs all of its jobs in rootless containers, meaning that they don't
require any elevated permissions on the host machines. All containers will be
run on the host machine as the user running `cargo-maelstrom`, `maelstrom-run`,
or `maelstrom-worker`, regardless of what this field is set as.

However, if this is field is set to 0, the program will have some elevated
permissions within the container, which may be undesirable for some jobs.

## `group`

```rust
pub struct JobSpec {
    // ...
    pub group: GroupId,
    // ...
}

pub struct GroupId(u32);
```

The specifies the GID the program is run as. See [`user`](#user) for more information.

Jobs don't have any supplemental GIDs, nor is there any way to provide them.

## `timeout`

```rust
pub struct JobSpec {
    // ...
    pub timeout: Option<Timeout>,
    // ...
}

pub struct Timeout(NonZeroU32);
```

This specifies an optional timeout for the job, in seconds. If the job takes
longer than the timeout, Maelstrom will terminate it and return the partial
results. A value of 0 indicates an infinite timeout.

## `estimated_duration`

```rust
pub struct JobSpec {
    // ...
    pub estimated_duration: Option<Duration>,
    // ...
}
```

The `estimated_duration` field is used to allow Maelstrom to use
[longest-processing-time-first
scheduling](https://en.wikipedia.org/wiki/Longest-processing-time-first_scheduling)
(LPT). It's up to clients to provide a best guess of how long a job will take.
If they can't provide one, they leave this field empty.

Test runers [keep track](cargo-maelstrom/target-dir.md#test-listing) of how
long previous instances of a test took and use that information to fill in this
field.

## `allocate_tty`

```rust
pub struct JobSpec {
    // ...
    pub allocate_tty: Option<JobTty>,
}

pub struct JobTty {
    // ...
}
```

The `allocate_tty` field is used by `maelstrom-run` with the `--tty`
command-line option to implement run the job interactively with a
psuedo-terminal attached. Jobs run this way will have standard input, output,
and error all associated with the allocated tty.

This can be useful for inspecting the container environment for a job.
