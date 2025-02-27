# Directories

There are a number of directories that Maelstrom clients use. This chapter
documents them.

## Project Directory

The project directory is used to resolve local paths, as are used in
[various](spec-layers.md#glob) [layers](spec-layers.md#paths). It's also where
the client will put the [container tags lock
file](container-images.md#lock-file).

For `maelstrom-pytest` and `maelstrom-run`, the project directory is just the
current working directory.

For `cargo-maelstrom`, the Maelstrom project directory is the same as the
Cargo project directory. This is where the top-level `Cargo.toml` file is for
the project. For simple Cargo projects with a single package, this will be the
package's root directory. For more complex Cargo projects that use workspaces,
this will be the workspace root directory.

For `maelstrom-go-test`, the Maelstrom project directory is root of main
package. This is where the closest `go.mod` file is.

## Container Depot Directory

The container depot directory is where clients cache container images that they
download from image registries. It's usually desirable to share this directory
across all clients and all projects. It's specified by the
`container-image-depot-root` configuration value. See
[here](../container-images.md#container-image-depot-root) for details.

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

This cache is project- and client-specific, so it is stored within the project
in a client-specific directory:

Client              | Cache Directory
--------------------|----------------
`maelstrom-run`     | [`cache-root` configuration value](run/config.md#cache-root) or the XDG specification
`cargo-maelstrom`   | `maelstrom/cache` in the target subdirectory of the project directory
`maelstrom-go-test` | `.maelstrom-go-test/cache` in the project directory
`maelstrom-pytest`  | `.maelstrom-pytest/cache` in the current directory
