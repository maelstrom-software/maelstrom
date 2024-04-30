# Layers

At the lowest level, a layer is just a tar file or a manifest. A manifest is a
Maelstrom-specific file format that allows for file data to be transferred
separately from file metadata. But for our purposes here, they're essentially the
same.

As a user, having to specify every layer as a tar file would be very painful.
For this reason, Maelstrom provides some conveniences for creating layers based
on specification. Under the covers, there is an API that the Maelstrom clients
`cargo-maelstrom` and `maelstrom-run` use for creating layers. This is what
that API looks like:

```protobuf
message AddLayerRequest {
    oneof Layer {
        TarLayer tar = 1;
        GlobLayer glob = 2;
        PathsLayer paths = 3;
        StubsLayer stubs = 4;
        SymlinksLayer symlinks = 5;
    }
}
```

We will cover each layer type below.

## `tar`
```protobuf
message TarLayer {
    string path = 1;
}
```

The `tar` layer type is very simple: The provided tar file will be used as a layer.
The path is specified relative to the client.

## `prefix_options`
```protobuf
message PrefixOptions {
    optional string strip_prefix = 1;
    optional string prepend_prefix = 2;
    bool canonicalize = 3;
    bool follow_symlinks = 4;
}
```

The [`paths`](#paths) and [`glob`](#glob) layer types support some options that
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

## `glob`
```protobuf
message GlobLayer {
    string glob = 1;
    PrefixOptions prefix_options = 2;
}
```

The `glob` layer type will include the files specified by the glob pattern in
the layer. The glob pattern is executed by the client relative to the [project
directory](project-dir.md). The glob pattern must use relative paths. The
[`globset`](https://docs.rs/globset/latest/globset/) crate is used for glob
pattern matching.

The `prefix_options` are applied to every matching path, as [described above](#prefix_options).

## `paths`
```protobuf
message PathsLayer {
    repeated string paths = 1;
    PrefixOptions prefix_options = 2;
}
```

The `paths` layer type will recursively include all of the files in each of the
specified paths. This is executed by the client relative to the [project
directory](project-dir.md). Relative and absolute paths may be used.

The `prefix_options` are applied to every matching path, as [described above](#prefix_options).

## `stubs`
```protobuf
message StubsLayer {
    repeated string stubs = 1;
}
```

The `stubs` layer type is used to create empty files and directories, usually
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

## `symlinks`
```protobuf
message SymlinkSpec {
    string link = 1;
    string target = 2;
}

message SymlinksLayer {
    repeated SymlinkSpec symlinks = 1;
}
```

The `symlinks` layer is used to create symlinks. The specified `link`s will be
created, with the specified `target`s. Any parent directories will also be
created, as necessary.
