# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### `cargo-metest`

#### Added
- The `--include` and `--exclude` long option names for `-i` and `-x`.
  [Issue #119](https://github.com/meticulous-software/meticulous/issues/119)
- The `-l` short option for `--list-tests`.
  [Issue #110](https://github.com/meticulous-software/meticulous/issues/110)

#### Changed

- The `--list-packages` option has been renamed to `--list-binaries`, since it
  shows all of the test executables that can be run, and there can be multiple
  executables in a package.
  [Issue #111](https://github.com/meticulous-software/meticulous/issues/111)
- A new `--list-packages` option has been added that just shows selected
  packages.
  [Issue #113](https://github.com/meticulous-software/meticulous/issues/113)
- Clarified the help message for `-i` and `-x`.
- To use subcommands. There are now two subcommands: `run` and `list`. `run`
  takes `-q`, `-i`, `-x`, and `-P` flags. `list` takes `-i`, `-x`, and `-P`
  flags. `list` also takes an optional positional argument to indicate what
  type of artifact to list. The `-v`, `-h`, and `-b` flags remain at the top
  level. The `quiet` configuration option also has moved into a new sub-section
  for the run command in `cargo-metest.toml`.
  [Issue #118](https://github.com/meticulous-software/meticulous/issues/118)

#### Fixed

- A bug in `--list-binaries` (previously `--list-packages`) has been fixed that
  could lead to duplicate listings of the same type.
  [Issue #112](https://github.com/meticulous-software/meticulous/issues/112)
- A bug when listing or running when the filter patterns exclude everything.
  Work was still done when no work should have been done.
  [Issue #120](https://github.com/meticulous-software/meticulous/issues/120)

## [0.3.0] - 2024-01-09

### General

#### Added

- Ability to create a build environment using [`nix`](https://nixos.org).
- Renamed `meticulous-client` binary to `meticulous-client-cli`.

### `cargo-metest`

#### Added

- A new test pattern language. The language can be used to select which test
  cases to run and which test binaries to build.
  [Issue #91](https://github.com/meticulous-software/meticulous/issues/91)
- New `-i` and `-x` options to specify include and exclude filters.
  [Issue #92](https://github.com/meticulous-software/meticulous/issues/92)
- New `--list-tests` option to list which tests would be run without actually
  running them. This may cause some test binaries to be built.
  [Issue #93](https://github.com/meticulous-software/meticulous/issues/93)
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
  [Issue #103](https://github.com/meticulous-software/meticulous/issues/103)

## [0.2.0] - 2023-12-15

### General

#### Added

- This CHANGELOG file.
- Ability to set working directory for jobs. This can be specified in
  `cargo metest` and `meticulous-client`.
  [Issue #89](https://github.com/meticulous-software/meticulous/issues/89).
- Ability to specify uid and gid for jobs. This can be specified in `cargo
  metest` and `meticulous-client`.
  [Issue #51](https://github.com/meticulous-software/meticulous/issues/51).
- Ability to specify a writable root file system. Any changes made will be
  discarded when the job completes. This can be specified in `cargo metest` and
  `meticulous-client`.
  [Issue #70](https://github.com/meticulous-software/meticulous/issues/70).

### `cargo-metest`

#### Added

- `--version` flag.
- Ability to specify what to include from an image: layers, environment, and/or
  working directory.
  [Issue #90](https://github.com/meticulous-software/meticulous/issues/90).

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

[unreleased]: https://github.com/meticulous-software/meticulous/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/meticulous-software/meticulous/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/meticulous-software/meticulous/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/meticulous-software/meticulous/releases/tag/v0.1.0
