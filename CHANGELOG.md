# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### General
#### Changed
#### Added
#### Removed
#### Fixed
### `cargo-maelstrom`
#### Changed
#### Added
- New `glob` layer type added which accepts a glob pattern. Matching files are added to that layer.
- New `stubs` layer type added. This makes it easy to create empty directories and files in a layer.
- New `symlinks` layer type added. This makes it easy to create symlinks in a layer.
#### Removed
#### Fixed
### `maelstrom-client-cli`
#### Changed
- The new layer types from `cargo-maelstrom` have also been added.

## [0.5.0] - 2024-02-08

### General

#### Added

- Crates.io publishing for Maelstrom. `cargo install cargo-maelstrom` now
  works. See: [cargo-maelstrom](https://crates.io/crates/cargo-maelstrom),
  [maelstrom-worker](https://crates.io/crates/maelstrom-worker),
  [maelstrom-broker](https://crates.io/crates/maelstrom-broker), etc.
- [Internal documentation](doc/contributing.md) for how to cut a release and
  for how to set up a development environment.
- [A community Discord server](https://discord.gg/gcNEdpjr).
- Support for Aarch64. The whole project can run on Arm or Intel/AMD processors
  now.
- Support for specifying layers using "manifests". A manifest is like a tar
  file except that it only contains metadata. File contents are represented as
  SHA-256 digests. The consumer of a manifest must request the individual file
  contents separately, based on their SHA-256 digests. Manifests offer a number
  of advantages over tar files:
  - When generating a manifest, the client only needs to compute checksums for
    those files that have actually changed.
  - Individual files can be cached on the broker, reducing the amount of data
    that needs to be transferred from the client to the broker.
  - It is a lot easier for the client to generate a manifest without first
    copying all of the contents.
- [Scripts](scripts/) for doing CI steps locally. Now, a developer should be
  able to test CI scripts locally on their own machines. These scripts use `nix
  develop` under the hood.

### `cargo-maelstrom`

#### Changed

- The way layers are submitted to take advantage of manifests. The new way is a
  lot faster and also more space-efficient. Before, the whole layer was
  constructed into a separate tar file, then checksummed. This resulted in a
  lot of extra disk usage and wasted time. Now, files are checksummed in place,
  and the checksum is cached.
- The syntax for specifying layers. A layer now is a table/dictionary, and it
  must be of one of two types.
  - Tar layers have a `tar` string attribute.
  - Paths layers have a `paths` string vector attribute, which is a list of
    files to include from the client. Optional `strip_prefix` or
    `prepend_prefix` attributes can be specified to determine the paths of the
    specified files that the job sees.

### `maelstrom-client-cli`

#### Changed

- The layer syntax has been changed in the same was as for `cargo-maelstrom`.

### `maelstrom-broker`

#### Fixed

- A bug where duplicate layers would cause the broker to crash.

### `maelstrom-worker`

#### Changed

- The way syscalls are done. We created a new package &mdash `maelstrom-linux` &mdash to
  encapsulate all of the syscalls we use. This crate directly uses `libc`
  instead of depending on `nix` and `nc`. This was motivated by adding Aarch64
  support.

## [0.4.0] - 2024-01-19

### General

#### Changed

- We renamed the project from "meticulous" to "maelstrom". All uses of
  "meticulous" changed accordingly. We renamed `cargo-metest` to
  `cargo-maelstrom`.
  \[[121](https://github.com/maelstrom-software/maelstrom/issues/121)\]
  \[[124](https://github.com/maelstrom-software/maelstrom/issues/124)\]
- We changed the licensing to use dual license using Apache 2.0 and MIT instead
  of the BSD 2-clause license. This brings us into line with the Rust community.
  \[[134](https://github.com/maelstrom-software/maelstrom/issues/134)\]
  \[[135](https://github.com/maelstrom-software/maelstrom/issues/135)\]

#### Added
- The start of documentation. The (alpha version) docs can be found
  [here](https://https://maelstrom-software.github.io/maelstrom/).
  \[[139](https://github.com/maelstrom-software/maelstrom/issues/139)\]
- CI for the project on GitHub.
  \[[141](https://github.com/maelstrom-software/maelstrom/issues/141)\]

### `cargo-maelstrom`

#### Added
- The `--include` and `--exclude` long option names for `-i` and `-x`.
  \[[119](https://github.com/maelstrom-software/maelstrom/issues/119)\]
- The `-l` short option for `--list-tests`.
  \[[110](https://github.com/maelstrom-software/maelstrom/issues/110)\]

#### Changed

- The `--list-packages` option has been renamed to `--list-binaries`, since it
  shows all of the test executables that can be run, and there can be multiple
  executables in a package.
  \[[111](https://github.com/maelstrom-software/maelstrom/issues/111)\]
- A new `--list-packages` option has been added that just shows selected
  packages.
  \[[113](https://github.com/maelstrom-software/maelstrom/issues/113)\]
- Clarified the help message for `-i` and `-x`.
- To subcommands. There are now two subcommands: `run` and `list`. `run`
  takes `-q`, `-i`, `-x`, and `-P` flags. `list` takes `-i`, `-x`, and `-P`
  flags. `list` also takes an optional positional argument to indicate what
  type of artifact to list. The `-v`, `-h`, and `-b` flags remain at the top
  level. The `quiet` configuration option also has moved into a new sub-section
  for the run command in `cargo-maelstrom.toml`.
  \[[118](https://github.com/maelstrom-software/maelstrom/issues/118)\]

#### Fixed

- A bug in `--list-binaries` (previously `--list-packages`) has been fixed that
  could lead to duplicate listings of the same type.
  \[[112](https://github.com/maelstrom-software/maelstrom/issues/112)\]
- A bug when listing or running when the filter patterns exclude everything.
  Work was still done when no work should have been done.
  \[[120](https://github.com/maelstrom-software/maelstrom/issues/120)\]

## [0.3.0] - 2024-01-09

### General

#### Added

- Ability to create a build environment using [`nix`](https://nixos.org).
- Renamed `meticulous-client` binary to `meticulous-client-cli`.

### `cargo-metest`

#### Added

- A new test pattern language. The language can be used to select which test
  cases to run and which test binaries to build.
  \[[91](https://github.com/maelstrom-software/maelstrom/issues/91)\]
- New `-i` and `-x` options to specify include and exclude filters.
  \[[92](https://github.com/maelstrom-software/maelstrom/issues/92)\]
- New `--list-tests` option to list which tests would be run without actually
  running them. This may cause some test binaries to be built.
  \[[93](https://github.com/maelstrom-software/maelstrom/issues/93)\]
- New `--list-packages` option to list which packages match the given filters.
  This will not build any test binaries.

#### Removed

- The `--package` option. This can be specified using the filter language now.
- The positional filter argument. Filters now have to be specified with `-i` or
  `-x`.

### `meticulous-client-cli`

#### Changed

- The JSON language used to specify jobs. This now has all of the features of
  the language used by `cargo-metest`, including specifying parts of an image
  to use.
  \[[103](https://github.com/maelstrom-software/maelstrom/issues/103)\]

## [0.2.0] - 2023-12-15

### General

#### Added

- This CHANGELOG file.
- Ability to set working directory for jobs. This can be specified in
  `cargo metest` and `meticulous-client`.
  \[[89](https://github.com/maelstrom-software/maelstrom/issues/89)\]
- Ability to specify uid and gid for jobs. This can be specified in `cargo
  metest` and `meticulous-client`.
  \[[51](https://github.com/maelstrom-software/maelstrom/issues/51)\]
- Ability to specify a writable root file system. Any changes made will be
  discarded when the job completes. This can be specified in `cargo metest` and
  `meticulous-client`.
  \[[70](https://github.com/maelstrom-software/maelstrom/issues/70)\]

### `cargo-metest`

#### Added

- `--version` flag.
- Ability to specify what to include from an image: layers, environment, and/or
  working directory.
  \[[90](https://github.com/maelstrom-software/maelstrom/issues/90)\]

#### Fixed

- Some small issues with progress bars, including one where the "complete" bar
  could get ahead of "running".

#### Improved

- The progress bars and spinner. The spinner now indicates what job is being
  worked on. There is an new, "pop-up", status bar when tar files are being
  generated. Plus other small improvements.
- Initial progress bar accuracy by remembering how many tests there were the
  last time `cargo-metest` was run.

#### Changed

- The format of the test metadata file. There is now a `image` field that can
  be used to include fields from an image. In addition, `layers`,
  `environment`, `mounts`, and `devices` now always overwrite their values
  instead of appending or unioning. New `added_layers`, `added_environment`,
  `added_mounts`, and `added_devices` fields have been added to replicate the
  old behavior.

## [0.1.0] - 2023-12-07

### Added

- Binaries for the clustered job runner: `meticulous-worker`,
  `meticulous-broker`, and `meticulous-client`.
- Client library for communicating with the broker: `meticulous-client`.
- A Rust test runner that uses the clustered job runner: `cargo-metest`.
- A bunch of other library packages that are used internally.

[unreleased]: https://github.com/maelstrom-software/maelstrom/compare/v0.5.0...HEAD
[0.5.0]: https://github.com/maelstrom-software/maelstrom/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/maelstrom-software/maelstrom/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/maelstrom-software/maelstrom/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/maelstrom-software/maelstrom/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/maelstrom-software/maelstrom/releases/tag/v0.1.0
