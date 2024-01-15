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
layers = ["layers/foo.tar", "layers/bar.tar"]
```

This field provides an ordered list of `.tar` files to be used when creating the
file-system for the test container. The `.tar` files are layered on top of
each other in the order provided.

This field can't be used together with the `image` field, since the `image`
field sets the layers itself. The `added_layers` field can still be used though.

If this field is provided, `include_shared_libraries` is also set to `false`,
unless it is explicitly set to `true`.

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
