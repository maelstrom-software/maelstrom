# The Job and Container Specifications

Each job run by Maelstrom is defined by a **job specification**, or "job spec"
for short. Each job spec consists of a **container specification** ("container
spec") plus a few additional fields. Container specs can inherit from other
container specs as well as from OCI container images.

Understanding job and container specifications is important for understanding
what's going on with your tests, for troubleshooting failing tests, and for
understanding test [runners'](cargo-maelstrom/spec.md)
[configuration](pytest/spec.md) [directives](go-test/spec.md) and
`maelstrom-run`'s [input format](run/spec.md).

This chapter shows the two specifications and their related types in Rust. This
is done because it's a convenient format to use for documentation, as Rust's
type system is good at conveying this sort of information. You won't have to
interact with Maelstrom at this level. Instead, clients will allow you to
configure job and container specifications in TOML or JSON.

## Job Specification

This is what the `JobSpec` looks like:

```rust
pub struct JobSpec {
    pub container: ContainerSpec,
    pub program: Utf8PathBuf,
    pub arguments: Vec<String>,
    pub timeout: Option<Timeout>,
    pub estimated_duration: Option<Duration>,
    pub priority: i8,
    pub allocate_tty: Option<JobTty>,
    pub capture_file_system_changes: Option<CaptureFileSystemChanges>,
}
```

The `container` field specifies the container the job will run in. It is
described [below](#container-specification).

### `program`

```rust
pub struct JobSpec {
    // ...
    pub program: Utf8PathBuf,
    // ...
}
```

This is the path of the program to run, relative to the container's
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

### `arguments`

```rust
pub struct JobSpec {
    // ...
    pub arguments: Vec<String>,
    // ...
}
```

These are the arguments that are passed to [`program`](#program), excluding the
name of the program itself. For example, to run `cat foo bar`, you would set
`program` to `"cat"` and `arguments` to `["foo", "bar"]`.

### `timeout`

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
results. A value of 0 indicates an infinite timeout, which is the default.

### `estimated_duration`

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

Test [runners](cargo-maelstrom/target-dir.md#test-listing)
[keep](pytest/project-dir.md#test-listing)
[track](go-test/project-dir.md#test-listing) of how long previous instances of a
test took and use that information to fill in this field.

### `priority`

```rust
pub struct JobSpec {
    // ...
    pub priority: i8,
    // ...
}
```

The `priority` field is used to allow Maelstrom to explicitly make a job "high
priority". Jobs within a priority band are executed using LPT scheduling based
on [`estimated_duration`](#estimated-duration), but jobs with a higher
`priority` are always scheduled before jobs with a lower `priority`.

Test [runners](cargo-maelstrom/target-dir.md#test-listing)
[keep](pytest/project-dir.md#test-listing)
[track](go-test/project-dir.md#test-listing) of what tests failed last time,
and execute these with higher priority.

### `allocate_tty`

```rust
pub struct JobSpec {
    // ...
    pub allocate_tty: Option<JobTty>,
    // ...
}

pub struct JobTty {
    // ...
}
```

The `allocate_tty` field is used by `maelstrom-run` with the `--tty`
command-line option to run the job interactively with a pseudo-terminal
attached. Jobs run this way will have standard input, output, and error all
associated with the allocated tty.

This can be useful for inspecting the container environment for a job.

### `capture_file_system_changes`

```rust
pub struct JobSpec {
    // ...
    pub capture_file_system_changes: Option<CaptureFileSystemChanges>,
}

pub struct CaptureFileSystemChanges {
    // ...
}
```

The `capture_file_system_changes` field is used by `maelstrom-pytest` to
execute jobs that install necessary modules on top of pre-defined containers.
The output is then used to create new containers that tests are run in.

This all happens behind the scenes. You generally don't have to worry about it.

## Container Specification

Every job has an associated container specification. In addition, container
specs can be given a name and saved. Named container specs can then be used as
parents of other container specs. In this way, a job can have an arbitrarily
deep container spec hierarchy.

```rust
pub struct ContainerSpec {
    pub parent: Option<ContainerParent>,
    pub layers: Vec<LayerSpec>,
    pub environment: Vec<EnvironmentSpec>,
    pub mounts: Vec<JobMount>,
    pub network: Option<JobNetwork>,
    pub enable_writable_file_system: Option<bool>,
    pub working_directory: Option<Utf8PathBuf>,
    pub user: Option<UserId>,
    pub group: Option<GroupId>,
}
```

### `parent` and Inheritance

```rust
pub struct ContainerSpec {
    pub parent: Option<ContainerParent>,
    // ...
}

pub enum ContainerParent {
    Image(ImageRef),
    Container(ContainerRef),
}

pub struct ImageRef {
    pub name: String,
    pub r#use: EnumSet<ImageUse>,
}

pub enum ImageUse {
    Layers,
    Environment,
    WorkingDirectory,
}

pub struct ContainerRef {
    pub name: String,
    pub r#use: EnumSet<ContainerUse>,
}

pub enum ContainerUse {
    Layers,
    EnableWritableFileSystem,
    Environment,
    WorkingDirectory,
    Mounts,
    Network,
    User,
    Group,
}
```

The `parent` field is used to provide a parent that a container spec inherits
from. The optional parent can either be an OCI container image, or another
named container spec.

The `name` field for `ImageRef` is a [container image
URI](container-images.md#container-image-uris) that will be used to locate the
container image. See [here](container-images.md) for more information. Only the
fields specified in the `use` field are inherited.

The `name` field for `ContainerRef` is a named container spec that has been
previously stored. There is one global namespace shared by the whole Maelstrom
client, and arbitrary strings are allowed as names. Only the fields specified
in the `use` field are inherited.

If a container `use`s a field from a parent and also directly provides that
field itself, one of two things will happen, depending on the type of the
field. If the field is a vector, then the elements provided directly in the
container spec will appended to those inherited from the parent. Otherwise, the
inherited value will be ignored and the directly provided value will be used.

All fields in the container spec are either optional or vectors. When a job is
executed, its container spec and all of its ancestors' need to be collapsed
into one, with actual values being given to the optional fields.

For optional fields, a search for a non-empty value will begin at the job
spec's container spec and then proceed up the set of inherited ancestor
container specs. The first non-empty value encountered will be used. The search
will be terminated if there are no more ancestors, or if a `parent` field
doesn't `use` the field from its parent. In the case that the search terminates
without finding a value, then the default value for the field is used. Default
values for fields are given in the per-field descriptions below.

For vector fields, the algorithm is different. The client starts by using the vector
from the job spec's container spec. Then, it moves up the inheritance chain
prepending each container spec's vector, until it has no more ancestors or
comes to a `use` that doesn't include the field.

### `layers`

```rust
pub struct ContainerSpec {
    // ...
    pub layers: Vec<LayerSpec>,
    // ...
}
```

The file system layers specify what file system the program will be run with.
They are stacked on top of each other, starting with the first layer, with
later layers overriding earlier layers.

These `Layer` objects are described [in the next chapter](spec-layers.md). [Test
runners](cargo-maelstrom/spec.md) and [`maelstrom-run`](maelstrom-run/spec.md) provide ways to
conveniently specify these, as described in their respective chapters.

Job specs must always have at least one layer, whether directly provided or
inherited from its ancestors. However, container specs don't need to provide
any layers as long as descendant container specs provide them.

### `environment`

```rust
pub struct ContainerSpec {
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

To compute the environment-variable map for a job, the client creates an
initial map, called the _candidate_ map, and a vector of `EnvironmentSpec`s
based on the container specs for the job.

If the job's inheritance chain ends with an OCI image, and if that
ancestor's environment is `use`ed, then the candidate map is taken from the OCI
image. Otherwise, the candidate map stars out empty.

The create the vector of `EnvironmentSpec`s, the client concatenates vectors
from the job's container specs as described [above](#parent-and-inheritance).

Then, for each map provided by an `EnvironmentSpec` element's `var` field, it
performs [parameter expansion](#parameter-expansion) and then
[merges](#merging) the resulting map into the candidate map. Once this has been
done for every `EnvironmentSpec` element, the candidate map is used for the
job.

#### Merging Environment-Variable Maps {#merging}

If the `extend` flag is `false`, then the element's newly-computed
environment-variable map will overwrite the candidate map.

If the `extend` flag is `true`, then the element's newly-computed
environment-variable map will be merged into the candidate map: All variables
specified in the element's map will overwrite the old values in the candidate
map, but values not specified in the element's map will be left unchanged.

#### Environment-Variable Parameter Expansion {#parameter-expansion}

Parameter substitution is applied to The values provided in the
`EnvironmentSpec` maps. A parameter has one of two forms:

  - `$env{FOO}` evaluates to the value of the client's `FOO` environment
    variable.
  - `$prev{FOO}` evaluates to the value of the `FOO` environment variable in
    the candidate map, without the current element's map applied.

It is an error if the referenced variable doesn't exist. However, `:-` can be
used to provide a default value, like `$env{VAR:-default}`.

#### Environment-Variable Examples

Here are some examples of specifying the environment variable map.

The simplest example is when a single element is provided and either there is
no image ancestor or its environment isn't `use`ed. In this case, the
variables specified will be provided to the job.

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
from the specified image. For example, assume there is an image ancestor and its environment is `use`ed:

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

It's possible for a job to have multiple `environment` elements, either because
the job has multiple ancestor container specs that provide elements, or because
one of the container spec provides multiple elements. Either way, all the
elements are concatenated and then evaluated in order

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

#### `PATH` Environment Variable

If the [`program`](#program) field doesn't include a `/` character, then the
`PATH` environment variable is used when searching for [`program`](#program),
is done for [execlp, execvp, and
execvpe](https://man7.org/linux/man-pages/man3/exec.3.html).

### `mounts`

```rust
pub struct ContainerSpec {
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

These are extra file systems mounts put into the job's environment. They are applied in order, and
**the `mount_point` must already exist in the file system**. Also the mount point must not be "/".
Providing the mount point is one of the use cases for the "stubs" layer type.

Every mount type except `Devices` has a `mount_point` field, which is relative
to the root of the file system, even if there is a
[`working_directory`](#working_directory) specified.

#### Bind

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
directory](dirs.md#project-directory).

The `read_only` flag specifies whether or not the job can write to the
directory. **NOTE**: the mount isn't "locked", which means that if the job
really wants to, it can remount `mount_point` read-write and modify the
contents of the directory. We may consider locking mount points in a future
version of Maelstrom.

#### Devices {#devices-mount}

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

#### Devpts

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
This can be done with the [`symlinks` layer type](spec-layers.md#symlinks).

#### Mqueue

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

#### Proc

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

#### Sys

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

#### Tmp

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

### `network`

```rust
pub struct ContainerSpec {
    // ...
    pub network: Option<JobNetwork>,
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
Note: if the job also specifies a [`Sys`](#sys) file system mount, Linux will
fail to execute the job.

In the future, we plan to add more `network` options that will allow clustered
jobs to communicate with the network. Until that time, if a job really has to
communicate on the network, it must use `Local`.

### `enable_writable_file_system`

```rust
pub struct ContainerSpec {
    // ...
    pub enable_writable_file_system: Option<bool>,
    // ...
}
```

The `enable_writable_file_system` field controls whether the job's root file
system should be mounted read-only or read-write.

If this field is `false`, then the job's root file system will be read-only.
This is the default.

If this field is `true`, then `/` will be an [`overlayfs` file
system](https://docs.kernel.org/filesystems/overlayfs.html), with "lower" being
the file system specified by the [layers](#layers), and "upper" being a
[`tmpfs`](#tmp) file system. This will yield writable root file system. The
contents of "upper" (i.e. the changes made by the job to the root file system)
will be thrown away when the job terminates.

Note that this field doesn't affect any file systems specified in
[`mounts`](#mounts).

### `working_directory`

```rust
pub struct ContainerSpec {
    // ...
    pub working_directory: Option<Utf8PathBuf>,
    // ...
}
```

This specifies the directory that [`program`](#program) is run in. The path
provided in `program` will also be evaluated relative to this directory. If
this isn't provided, `/` is used.

### `user`

```rust
pub struct ContainerSpec {
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

The default value is 0.

## `group`

```rust
pub struct ContainerSpec {
    // ...
    pub group: GroupId,
}

pub struct GroupId(u32);
```

The specifies the GID the program is run as. See [`user`](#user) for more information.

Jobs don't have any supplemental GIDs, nor is there any way to provide them.

The default value is 0.
