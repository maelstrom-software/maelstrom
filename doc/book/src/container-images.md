# Container Images

Maelstrom clients support building [job specifications](spec.md) based off of
standard OCI container images stored on [Docker Hub](https://hub.docker.com).
This can be done with [`cargo-maelstrom`'s `image` directive
field](cargo-maelstrom/spec/fields.md#image), or `cargo-run`'s `image` field.

When an image is specified this way, the client will first download it locally
into its [cache directory](#cached-container-images). It will then use the
internal bits of the [OCI image](https://github.com/opencontainers/image-spec)
&mdash; most importantly the file-system layers &mdash; to create the job
specification for the job.

Images can be specified with tags. If no tag is provided, the `latest` tag is
used.

For the purposes of hermetic testing, `cargo-maelstrom` will resolve and "lock"
a tag, so that tests always specify an exact image that doesn't change over time.
See [this chapter](cargo-maelstrom/container-tags-lock-file.md) for more details.

## Cached Container Images

Container images are cached on the local file system. Maelstrom uses the
`$XDG_CACHE_HOME/maelstrom/containers` directory if `XDG_CACHE_HOME` is set and
non-empty. Otherwise, it uses `~/.cache/maelstrom/containers`. See the [XDG Base
Directories
specification](https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html)
for more information.
