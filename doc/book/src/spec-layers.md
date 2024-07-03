# Job Specification Layers

At the lowest level, a layer is just a tar file or a manifest. A manifest is a
Maelstrom-specific file format that allows for file data to be transferred
separately from file metadata. But for our purposes here, they're essentially the
same.

As a user, having to specify every layer as a tar file would be very painful.
For this reason, Maelstrom provides some conveniences for creating layers based
on specifications. There is an API function that clients can call where they
provide a layer specification and get back a layer. A layer specification looks
like this:

```rust
pub enum Layer {
    Tar {
        path: Utf8PathBuf,
    },
    Glob {
        glob: String,
        prefix_options: PrefixOptions,
    },
    Paths {
        paths: Vec<Utf8PathBuf>,
        prefix_options: PrefixOptions,
    },
    Stubs { stubs: Vec<String> },
    Symlinks { symlinks: Vec<SymlinkSpec> },
}
```

We will cover each layer type below.

## `Tar`
```rust
pub enum Layer {
    Tar {
        path: Utf8PathBuf,
    },
    // ...
}
```

The `Tar` layer type is very simple: The provided tar file will be used as a
layer. The path is specified relative to the [project
directory](dirs.md#project-directory).

## `PrefixOptions`
```rust
pub struct PrefixOptions {
    pub strip_prefix: Option<Utf8PathBuf>,
    pub prepend_prefix: Option<Utf8PathBuf>,
    pub canonicalize: bool,
    pub follow_symlinks: bool,
}
```

The [`Paths`](#paths) and [`Glob`](#glob) layer types support some options that
can be used to control how the resulting layer is created. They apply to all
paths included in the layer. These options can be combined, and in such a
scenario you can think of them taking effect in the given order:
- `follow_symlinks`: Don't include symlinks, instead use what they point to.
- `canonicalize`: Use absolute form of path, with components normalized and
  symlinks resolved.
- `strip_prefix`: Remove the given prefix from paths.
- `prepend_prefix` Add the given prefix to paths.

Here are some examples.

### `follow_symlinks`

If `test/d/symlink` is a symlink which points to the file `test/d/target`, and
is specified with `follow_symlinks`, then Maelstrom will put a regular file in
the container at `/test/d/symlink` with the contents of `test/d/target`.

### `canonicalize`

If the client is executing in the directory `/home/bob/project`, and the
`layers/c/*.bin` glob is specified with `canonicalize`, then Maelstrom will put
files in the container at `/home/bob/project/layers/c/*.bin`.

Additionally, if `/home/bob/project/layers/py` is a symlink pointing to
`/var/py`, and the `layers/py/*.py` glob is specified with `canonicalize`, then
Maelstrom will put files in the container at `/var/py/*.py`.

### `strip_prefix`

If `layers/a/a.bin` is specified with `strip_prefix = "layers/"`, then Maelstrom
will put the file in the container at `/a/a.bin`.

### `prepend_prefix`

If `layers/a/a.bin` is specified with `prepend_prefix = "test/"`, then
Maelstrom will put the file in the container at `/test/layers/a/a.bin`.

## `Glob`
```rust
pub enum Layer {
    // ...
    Glob {
        glob: String,
        prefix_options: PrefixOptions,
    },
    // ...
}
```

The `Glob` layer type will include the files specified by the glob pattern in
the layer. The glob pattern is executed by the client relative to the [project
directory](dirs.md#project-directory). The glob pattern must use relative paths. The
[`globset`](https://docs.rs/globset/latest/globset/) crate is used for glob
pattern matching.

The `prefix_options` are applied to every matching path, as [described above](#prefixoptions).

## `Paths`
```rust
pub enum Layer {
    // ...
    Paths {
        paths: Vec<Utf8PathBuf>,
        prefix_options: PrefixOptions,
    },
    // ...
}
```

The `Paths` layer type will include each file referenced by the
specified paths. This is executed by the client relative to the [project
directory](dirs.md#project-directory). Relative and absolute paths may be used.

The `prefix_options` are applied to every matching path, as [described above](#prefixoptions).

If a path points to a file, the file is included in the layer. If the path
points to a symlink, either the symlink or the pointed-to-file gets included,
depending on [`prefix_options.follow_symlinks`](#follow_symlinks). If the path points to a
directory, an empty directory is included.

To include a directory and all of its contents, use the [`Glob`](#glob) layer
type.

## `Stubs`
```rust
pub enum Layer {
    // ...
    Stubs { stubs: Vec<String> },
    // ...
}
```

The `Stubs` layer type is used to create empty files and directories, usually
so that they can be mount points for [devices](spec.md#devices) or
[mounts](spec.md#mounts).

If a string contains the `{` character, the
[`bracoxide`](https://docs.rs/bracoxide/latest/bracoxide/) crate is used to
perform brace expansion, transforming the single string into multiple strings.

If a string ends in `/`, an empty directory will be added to the layer.
Otherwise, and empty file will be added to the layer. Any parent directories
will also be created as necessary.

For example, the set of stubs `["/dev/{null,zero}", "/{proc,tmp}/", "/usr/bin/"]` would
result in a layer with the following files and directories:
  - `/dev/`
  - `/dev/null`
  - `/dev/zero`
  - `/proc/`
  - `/tmp/`
  - `/usr/`
  - `/usr/bin/`

## `Symlinks`
```rust
pub enum Layer {
    // ...
    Symlinks { symlinks: Vec<SymlinkSpec> },
}

pub struct SymlinkSpec {
    pub link: Utf8PathBuf,
    pub target: Utf8PathBuf,
}
```

The `Symlinks` layer is used to create symlinks. The specified `link`s will be
created, pointing to the specified `target`s. Any parent directories will also
be created, as necessary.
