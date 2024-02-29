# Layers

When a test is run on a worker, it is run inside a lightweight container. The
file-system for the container is specified by the given layers or container
image.

The layers are extracted in their own directories, and layered on top of each
other using an [Overlay
Filesystem](https://docs.kernel.org/filesystems/overlayfs.html).

`cargo-maelstrom` itself adds some implicit layers which contain the test binary
itself, and optionally dependencies for the test binary.

## The `layers` Field
```toml
[[directives]]
layers = [
    { tar = "layers/foo.tar" },
    { paths = ["layers/a/b.bin", "layers/a/c.bin"] },
    { glob = "layers/b/**" },
    { stubs = ["/dev/{null, full}", "/proc/"] },
    { symlinks = [{ link = "/dev/stdout", target = "/proc/self/fd/1" }] }
]
```

This field provides an ordered list of layers. A layer is a description of files to place in the
lightweight container. It can be a description of files to create, or just the location of local
files and where to place them.

There are a few different layer types that can be provided

- `tar` a path to a local tar file which will be expanded
- `paths` a list of local paths to upload
    - If the path is relative, it is relative from the workspace root.
- `glob` a glob pattern of local paths to upload
    - The pattern is always relative from the workspace root, it doesn't support absolute paths
- `stubs` a list of paths in the container where either an empty directory or file
    - The paths support brace expansion. If the path ends in a `/`, it will be created as a
      directory, otherwise it will be an empty file.
- `symlinks` a list of symlinks to create in the container.
    - They are specified as a pair of `link` and `target`. `link` is the path in the container, and
      `target` is the destination of the symlink

This field can't be used together with the `image` field, since the `image`
field sets the layers itself. The `added_layers` field can still be used though.

If this field is provided, `include_shared_libraries` is also set to `false`,
unless it is explicitly set to `true`.

### Layer Options
The `paths` and `glob` layers support some options that can be used to control how the resulting
layer is created. They apply to all paths included in the layer. These options can be combined, and
in such a scenario you can think of them taking effect in the given order

- `follow-symlinks` don't include symlinks, instead use what they point to
- `canonicalize` use absolute form of path, with components normalized and symlinks resolved
- `strip-prefix` remove the given prefix from paths
- `prepend-prefix` add the given prefix to paths

Here are some examples.

```toml
{ glob = "test/d/e.bin", follow-symlinks = true },
```
If `test/d/e.bin` is a symlink which points to `test/d/f.bin` this layer will put a file in the
container at `/test/d/e.bin` with the contents of `test/d/f.bin`

```toml
{ glob = "layers/c/*.bin", canonicalize = true },
```
If your workspace directory is `/home/bob/project` this layer will put files in the container at
`/home/bob/project/layers/c/*.bin`

```toml
{ paths = ["layers/a/b.bin", "layers/a/c.bin"], strip-preifx = "layers/" },
```
This layer will puts files in the container at `/a/b.bin` and `/a/c.bin`

```toml
{ glob = "layers/b/**", prepend-prefix = "test/" },
```
This layer will put files in the container at `/test/layers/b/**`

## The `added_layers` Field

This field is just like the `layers` field except that the given layers are
appended to the existing layers. Also this field works together with the `image`
field to added extra layers.

## The `include_shared_libraries` field

```toml
[[directives]]
include_shared_libraries = true
```

If this field is set to true, an extra layer containing the shared libraries
that the test binary depends on is added.

If the `layers` field or `image` field is provided, this option is set to
`false` by default, otherwise it defaults to `true`.
