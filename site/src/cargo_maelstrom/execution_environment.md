# Execution Environment

The fields on this page all control the way the test container is set up. The
test container is the lightweight container the worker creates and runs a test
inside.

## The `enable_loopback` field

```toml
[[directive]]
enable_loopback = true
```
If this field is set to true, the loopback device in the container is ifup'd
before the test is run.

## The `enable_writable_file_system` field

```toml
[[directives]]
enable_writable_file_system = true
```
If this field is set to true, the file-system of the test container is made
writable. If possible, it is better to mount a tmpfs device and use that instead
of this field.

## The `user` field

```toml
[[directives]]
user = "1001"
```
The test is ran as whatever user this field is set to. It supports using both a
UID number and a username.

## The `group` field

```toml
[[directives]]
group = "1001"
```
The test is ran as whatever group this field is set to. It supports using both a
GID number and a group name.

## The `working_directory` field

```toml
[[directives]]
working_directory = "/home/root/"
```
The `working_directory` field is a path inside the test container and used as the
working directory for the test being run.

## The `mounts` field

```toml
[[directives]]
mounts = [
    { fs_type = "tmp", mount_point = "/tmp" },
    { fs_type = "proc", mount_point = "/proc" },
    { fs_type = "sys", mount_point = "/sys" },
]
```
The `mounts` field allows you to mount special file-systems inside the test
container. The `mount_point` given must be a path that exists in the layers.

Supported values for `fs_type` are:
- `"tmp"`: [Tmpfs Kernel Docs](https://docs.kernel.org/filesystems/tmpfs.html)
- `"proc"`: [proc Kernel Docs](https://docs.kernel.org/filesystems/proc.html)
- `"sys"`: [sysfs Kernel Docs](https://docs.kernel.org/filesystems/sysfs.html)

## The `added_mounts` field
This is the same as the `mounts` field except the given mounts are added to the
existing list of `mounts`

## The `devices` field

```toml
[[directives]]
devices = ["null", "random", "zero"]
```
The `devices` field mounts the given special devices to their corresponding
path. The path for the device must have some (usually empty) file in the layers
that the device can be mounted on.

The supported devices are
- `"full"`: All supported devices
- `"null"`: `/dev/null`
- `"random"`: `/dev/random`
- `"tty"`: `/dev/tty`
- `"urandom"`: `/dev/urandom`
- `"zero"`: `/dev/zero`

## The `added_devices` field
This is the same as the `devices` field except the given devices are added to
the existing list of devices

## The `environment` field
```toml
[[directives]]
environment = {
    USER = "bob",
    RUST_BACKTRACE = "$env{RUST_BACKTRACE:-0}",
}
```

This field allows you to set environment variables that will be available when
executing the test.

When setting the value for environment variables, you can reference some
other sources.

- `$env` `cargo-maelstrom`'s environment variables
- `$prev` the previously set environment variables in this directive

You are able to key into these sources by using `{<key>}`. If the source doesn't
have that key set, you can provide a default value for the key by using `:-`.

```toml
FOO = "$env{FOO:-bar}"
```
This sets the environment variable "FOO" to whatever value is set when running
`cargo-maelstrom`. If the environment variable isn't set, it is set to "bar"

## The `added_environment` field
This is the same as the `environment` field except the given environment is
applied on top of the existing environment
