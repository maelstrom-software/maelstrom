# The Project Directory

Maelstrom clients have a concept of the "project directory".

The project directory is used to resolve [local](spec-layers.md#glob)
[relative](spec-layers.md#paths) paths. It's also where the client will put the
[container tags lock file](container-images.md#lock-file).

For [`maelstrom-pytest`](pytest.md) and [`maelstrom-run`](run.md), the project
directory is just the current working directory.

For [`cargo-maelstrom`](cargo-maelstrom.md), the Maelstrom project directory is the is same as the
Cargo project directory. This is where the top-level `Cargo.toml` file is for the project.
For simple Cargo projects with a single package, this will be the package's
root directory. For more complex Cargo projects that use workspaces, this will
be the workspace root directory.
