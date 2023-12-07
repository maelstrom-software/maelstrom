# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### General

#### Added

- This CHANGELOG file.

### `cargo-metest`

#### Added

- A `--version` flag.

## [0.1.0] - 2023-12-07

### Added

- Binaries for the clustered job runner: `meticulous-worker`,
  `meticulous-broker`, and `meticulous-client`.
- Client library for communicating with the broker: `meticulous-client`.
- A Rust test runner that uses the clustered job runner: `cargo-metest`.
- A bunch of other library packages that are used internally.

[unreleased]: https://github.com/meticulous-software/meticulous/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/meticulous-software/meticulous/releases/tag/v0.1.0
