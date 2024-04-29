# Container Tags Lock File

When `cargo-maelstrom` first resolves a container registry tag, it stores the
result in a local lock file. Subsequently, it will use the exact image
specified in the lock file instead of resolving the tag again. This guarantees
that subsequent runs use the same images as previous runs.

The local lock file is `maelstrom-container-tags.lock`, stored in the workspace
root. It is recommended that this file be committed to revision control, so
that others in the project, and CI, use the same images when running tests.

To update a tag to the latest version, remove the corresponding line from the
lock file and then run `cargo-maelstrom`.
