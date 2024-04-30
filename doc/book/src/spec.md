# The Job Specification

Each job run by Maelstrom is defined by a **Job Specification**, or "job spec"
for short. Understanding job specifications is important for understanding
what's going on with your tests, for toubleshooting failing tests, and for
understanding `cargo-maelstrom`'s [configuration
directives](cargo-maelstrom/spec.md) and `maelstrom-run`'s [input
format](maelstrom-run/spec.md).

This chapter shows the specification and its related types in the [Protocol
Buffer](https://protobuf.dev/programming-guides/proto3/) format. This is done
because it's a convenient format to use for documentation. You won't have to
interact with Maelstrom at this level.

This is the Protocol Buffer for the `JobSpec`:

```protobuf
message JobSpec {
    string program = 1;
    repeated string arguments = 2;
    repeated string environment = 3;
    repeated LayerSpec layers = 4;
    repeated JobDevice devices = 5;
    repeated JobMount mounts = 6;
    bool enable_loopback = 7;
    bool enable_writable_file_system = 8;
    string working_directory = 9;
    uint32 user = 10;
    uint32 group = 11;
    optional uint32 timeout = 12;
}
```

## `program`

This is the path of the program to run, relative to the [`working_directory`](#working_directory).
The job will complete when this program terminates, regardless of any other
processes that have been started.

The path must be specified explicitly: the `PATH` environment variable will not
be searched, even if it is provided.

The program is run as PID 1 in its own PID namespace, which means that it acts
as the `init` process for the container. This shouldn't matter for most use
cases, but if the program starts a lot of subprocesses, it may need to
explicitly clean up after them.

The program is run as both session and process group leader.

## `arguments`

These are the arguments to pass to [`program`](#program), excluding the name of the program
itself. For example, to run `cat foo bar`, you would set `program` to `"cat"`
and `arguments` to `["foo", "bar"]`.

## `environment`

These are the environment variables passed to [`program`](#program). These are in the
format expected by the
[`execve`](https://man7.org/linux/man-pages/man2/execve.2.html) syscall, which
is `"VAR=VALUE"`. 

The `PATH` environment variable is **not** used when searching for `program`.
You must provide the actual path to `program`.

## `layers`

```protobuf
enum ArtifactType {
    Tar = 0;
    Manifest = 1;
}

message LayerSpec {
    bytes digest = 1;
    ArtifactType type = 2;
}
```

The file system layers specify what file system the program will be run with.
They are stacked on top of each other, starting with the first layer, with
later layers overriding earlier layers.

Each layer will be a tar file, or Maelstrom's equivalent called a "manifest
file". These `LayerSpec` objects are usually generated with an
`AddLayerRequest`, which is described [in the next chapter](spec/layers.md). [`cargo-maelstrom`](cargo-maelstrom/spec.md) and
[`maelstrom-run`](maelstrom-run/spec.md) provide ways to conveniently specify these, as described in
their respective chapters.

## `devices`

```protobuf
enum JobDevice {
    Full = 0;
    Fuse = 1;
    Null = 2;
    Random = 3;
    Tty = 4;
    Urandom = 5;
    Zero = 6;
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
enum JobMountFsType {
    Proc = 0;
    Tmp = 1;
    Sys = 2;
}

message JobMount {
    JobMountFsType fs_type = 1;
    string mount_point = 2;
}
```

These are extra file systems mounts put into the job's environment. They are
applied in order, and **the `mount_point` must already exist in the file
system**. Providing the mount point is one of the use cases for the "stubs" layer type.

The `mount_point` is relative to the root of the file system, even if there is
a [`working_directory`](#working_directory) specified.

For more information about these file system types, see:
  - [`Proc`](https://docs.kernel.org/filesystems/proc.html)
  - [`Tmp`](https://docs.kernel.org/filesystems/tmpfs.html)
  - [`Sys`](https://docs.kernel.org/filesystems/sysfs.html)

## `enable_loopback`

Jobs are run completely disconnected from the network. Without this flag set,
they don't even have a loopback device enabled, and thus cannot communicate to
`localhost`/`127.0.0.1`/`::1`.

Enabling this flag allows communication on the loopack device.

## `enable_writable_file_system`

By default, the whole file system the job sees will be read-only, except for
any extra file systems specified in [`mounts`](#mounts).

Enabling this flag will make the file system writable. Any changes the job
makes to the file system will be isolated, and will be thrown away when the job
completes.

## `working_directory`

This specifies the directory that [`program`](#program) is run in.

## `user`

This specifies the UID the program is run as.

Maelstrom runs all of its jobs in rootless containers, meaning that they don't
require any elevated permissions on the host machines. All containers will be
run on the host machine as the user running `cargo-maelstrom`, `maelstrom-run`,
or `maelstrom-worker`, regardless of what this field is set as.

However, if this is field is set to 0, the program will have some elevated
permissions within the container, which may be undesirable for some jobs.

## `group`

The specifies the GID the program is run as. See [`user`](#user) for more information.

Jobs don't have any supplemental GIDs, nor is there any way to provide them.

## `timeout`

This specifies an optional timeout for the job, in seconds. If the job takes
longer than the timeout, Maelstrom will terminate it and return the partial
results. A value of 0 indicates an infinite timeout.
