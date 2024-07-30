# Directories

There are a number of directories that Maelstrom clients use. This chapter documents them.

## Project Directory

The project directory is used to resolve [local](spec-layers.md#glob)
[relative](spec-layers.md#paths) paths. It's also where the client will put the
[container tags lock file](container-images.md#lock-file).

For `maelstrom-pytest`, `maelstrom-go-test`, and `maelstrom-run`, the project
directory is just the current working directory.

For `cargo-maelstrom`, the Maelstrom project directory is the is same as the
Cargo project directory. This is where the top-level `Cargo.toml` file is for the project.
For simple Cargo projects with a single package, this will be the package's
root directory. For more complex Cargo projects that use workspaces, this will
be the workspace root directory.

## Container Depot Directory

The container depot directory is where clients cache container images that they
download from image registries. It's usually desirable to share this directory
across all clients and all projects. It's specified by the
`container-image-depot-root` configuration value. See
[here](../container-images.md#container-image-depot-root) for details.

## State Directory

The state directory concept comes from the [XDG Base Directory
Specification](https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html).
It's where the client will put things that should persist between restarts, but
aren't important enough to be stored elsewhere, and which can removed safely.

Maelstrom clients use this directory for two purposes. First,
every client spawns a program called `maelstrom-client` which it speaks to
using gRPC messages. The log output for this program goes to the
`client-process.log` file in the state directory.

Second, test runners keep track of test counts and test timings between runs.
This lets them estimate how long a test will take, and how many tests still
need to be built or run. Without this information, test runners will just give
inaccurate estimates until they've rebuilt the state.

`maelstrom-run` uses the [`state-root` configuration
value](run/config.md#state-root) to specify this directory, falling back on the
XDG specification if that's not specified.

`cargo-maelstrom` uses `maelstrom/state` in the target subdirectory of the
project, while `maelstrom-go-test` uses `.maelstrom-go-test/state` and
`maelstrom-pytest` uses `.maelstrom-pytest/state` in the current directory. The
idea is that this state is project- and client-specific, so it should be stored
within the project.

## Cache Directory

The cache directory concept comes from the [XDG Base Directory
Specification](https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html).
It's where non-essential files that are easily rebuilt are stored.

Maelstrom clients use this directory for their [local worker](local-worker.md).
The size of the local-worker part of the cache is maintained by the
[`cache-size` configuration value](local-worker.md#cache-size).

In addition, clients use this directory to store other cached data like layers
created with [layer specifications](spec-layers.md). This size of this part of
the cache directory isn't actively managed. If it grows too large, the user can
safely delete the directory.

`maelstrom-run` uses the [`cache-root` configuration
value](run/config.md#cache-root) to specify this directory, falling back on the
XDG specification if that's not specified.

`cargo-maelstrom` uses a `maelstrom/cache` directory in the target subdirectory
of the project. `maelstrom-go-test` uses `.maelstrom-go-test/cache` in the
curent directory and `maelstrom-pytest` uses `.maelstrom-pytest/cache` in the
current directory. The idea is that this cache is project-specific, so it
should be stored within the project.
