# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[0.2.0]: https://github.com/meticulous-software/meticulous/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/meticulous-software/meticulous/releases/tag/v0.1.0
