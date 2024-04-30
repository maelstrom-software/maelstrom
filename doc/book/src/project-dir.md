# The Project Directory

Maelstrom clients have a concept of the "project directory". For
`cargo-maelstrom`, it is the Cargo workspace root directory. For
`maelstrom-run`, it is the current working directory.

The project directory is used to resolve [local](spec/layers.md#glob)
[relative](spec/layers.md#paths) paths. It's also where the client will put the
[container tags lock file](container-images.md#lock-file).
