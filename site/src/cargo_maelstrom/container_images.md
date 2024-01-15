# Container Images

`cargo-maelstrom` supports using container images from Docker by using the
`image` field. These container images are downloaded and cached on the local
file-system. The `latest` tag is used for these images, and once resolved this
is stored in the [Container Tags Lockfile](#container-tags-lockfile).

## Cached Container Images

The container images are cached on the local file-system. They are stored in the
user's cache directory (usually ~/.cache/) under `maelstrom/containers`.

## Container Tags Lockfile

Docker container images have tags which are short strings that resolve to a
specific image. `cargo-maelstrom` uses the `latest` tag to download an image.

The first time it looks up the image, it resolves the `latest` tag to some hash
and stores it in the lock file which is stored at
`<workspace-root>/maelstrom-container-tags.lock`. This locks down the exact
container image being used, any subsequent resolution of the tag will use what
is recorded in the lockfile. This file is intended to be committed as part of
your project's version control.

When you wish to update a given container image to the latest version, remove
the corresponding line in the lockfile and then rerun `cargo-maelstrom`

## The `image` field
```toml
[[directives]]
image = { name = "rust", use = ["layers", "environment"] }
```

The `image` field specifies a Docker container image to download and use. The
`use` field specifies what things should be used from the container image.

- `"layers"` the file-system layers from the container image
- `"environment"` the environment variables from the container image
- `"working_directory"` the working directory form the container image

If any of these things are specified with the container image, then the
corresponding field may not be provided. Instead the `added_*` variants must be
used.

```toml
[[directives]]
image = { name = "rust", use = ["layers", "environment"] }
added_layers = ["layers/my_other_files.tar"]
```

This uses the file-system layers from the Docker "rust" container image, but it
also adds an extra layer containing the things found in
`layers/my_other_files.tar`.
