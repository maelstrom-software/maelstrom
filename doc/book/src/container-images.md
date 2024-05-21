# Container Images

Maelstrom clients support building [job specifications](spec.md) based off of
standard OCI container images stored on [Docker Hub](https://hub.docker.com).
This can be done by setting the [`image`](spec.md#image) field of the job
specification.

When an image is specified this way, the client will first download it locally
into its [cache directory](#cached-container-images). It will then use the
internal bits of the [OCI image](https://github.com/opencontainers/image-spec)
&mdash; most importantly the file-system layers &mdash; to create the job
specification for the job.

Images can be specified with tags. If no tag is provided, the `latest` tag is
used.

For the purposes of reproducable jobs, clients will resolve and "lock" a tag,
so that jobs always specify an exact image that doesn't change over time. See
[this section](#lock-file) for more details.

## Cached Container Images {#container-image-depot-root}

Container images are cached on the local file system. The cache directory can
be set with the `container-image-depot-root` [configuration value](config.md).
If this value isn't set, but `XDG_CACHE_HOME` is set and non-empty, then
`$XDG_CACHE_HOME/maelstrom/containers` will be used. Otherwise,
`~/.cache/maelstrom/containers` will be used. See the [XDG Base Directories
specification](https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html)
for more information.

## Lock File

When a client first resolves a container registry tag, it stores the result in
a local lock file. Subsequently, it will use the exact image specified in the
lock file instead of resolving the tag again. This guarantees that subsequent
runs use the same images as previous runs.

The local lock file is `maelstrom-container-tags.lock`, stored in the [project
directory](project-dir.md). It is recommended that this file be committed to
revision control, so that others in the project, and CI, use the same images
when running tests.

To update a tag to the latest version, remove the corresponding line from the
lock file and then run the client.
