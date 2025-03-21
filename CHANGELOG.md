# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### General
- Created new `maelstrom-admin` program. This program is minimal right now. It
  provides a subcommand that allows one to stop a Maelstrom cluster, which is
  useful for CI. It will definitely get more administrative functionality over time.
  \[[494](https://github.com/maelstrom-software/maelstrom/issues/494)\]
- Created GitHub actions for
  [`maelstrom-broker`](https://github.com/maelstrom-software/maelstrom-broker-action),
  [`maelstrom-worker`](https://github.com/maelstrom-software/maelstrom-worker-action),
  [`cargo-maelstrom`](https://github.com/maelstrom-software/cargo-maelstrom-action),
  [`maelstrom-go-test`](https://github.com/maelstrom-software/maelstrom-go-test-action),
  [`maelstrom-pytest`](https://github.com/maelstrom-software/maelstrom-pytest),
  and [`maelstrom-admin`](https://github.com/maelstrom-software/maelstrom-admin).
  \[[483](https://github.com/maelstrom-software/maelstrom/issues/483)\]
- Created an [example
  repository](https://github.com/maelstrom-software/maelstrom-examples) that
  has examples of how to configure Maelstrom
  for individual tests as well as how to use Maelstrom in CI.
  \[[484](https://github.com/maelstrom-software/maelstrom/issues/484)\], 
  \[[491](https://github.com/maelstrom-software/maelstrom/issues/491)\]
- Made many improvements to the GitHub integration:
  - Clients can now communicate with the broker over the GitHub artifact store,
    just like workers. This lets us run the broker in a different job than the
    clients. This in turn lets us build a better GitHub action for the broker,
    as well as the workflow have many different jobs run in parallel and
    interact with the cluster.
    \[[493](https://github.com/maelstrom-software/maelstrom/issues/493)\]
  - Unified the configuration values used by Maelstrom programs on GitHub.

### All Test Runners (`cargo-maelstrom`, `maelstrom-go-test`, and `maelstrom-pytest`)
- Made `--watch` flag visible in help.
- Gave `--watch` a `-w` short option.
- Gave `--repeat` a `-t` short option.
  \[[482](https://github.com/maelstrom-software/maelstrom/issues/482)\]
- Gave `--stop-after` a `-s` short option.
  \[[482](https://github.com/maelstrom-software/maelstrom/issues/482)\]
- Gave `--list` (`--list-tests`) a `-l` short option.
  \[[513](https://github.com/maelstrom-software/maelstrom/issues/513)\]

### All Clients (Test Runners, `maelstrom-run`, and Client Library)
- Fix an issue where we wouldn't find a matching image in some OCI images on
  ARM64 (like `docker://alpine`).
  \[[506](https://github.com/maelstrom-software/maelstrom/issues/506)\]

### All Programs
- Changed the short option for `--log-level` to `-L` instead of `-l`.
  \[[513](https://github.com/maelstrom-software/maelstrom/issues/513)\]
- Added support for generic Maelstrom environment variables, like
  `MAELSTROM_BROKER` in addition to program-specific ones like `MAELSTROM_PYTEST_BROKER`.
  \[[492](https://github.com/maelstrom-software/maelstrom/issues/492)\]

### `maelstrom-worker`
- Fixed a test that would fail with `RUST_BACKTRACE=1`.
  \[[510](https://github.com/maelstrom-software/maelstrom/issues/510)\]

## [0.13.0] - 2025-02-24

### Changed
- Test runners (`cargo-maelstrom`, `maelstrom-go-test`, and `maelstrom-pytest`)
  now have a "watch mode" that is enabled with the `--watch` command-line
  option. In watch mode, the test runner will first run all of the specified
  tests, then loop waiting for changes in the project directory. Anytime there
  is a change, the tests will be re-run.
- Preliminary support has been added for running Maelstrom clusters in GitHub.
  Artifacts are shared between processes using the GitHub artifact API.
  Communication between processes uses a message-queue that is also built on
  top of the GitHub artifact API.
- The concept of named containers has been added to all clients (tests runners
  and `maelstrom-run`). Named containers can inherit configuration from other
  named containers or container images. Jobs can then inherit configuration
  from named containers. This simplifies configuration, but it also sets the
  stage for "command layers" which we hope to introduce in the next release.
- Test runners send SIGKILL to their build processes when exiting early due to `stop-after`.
  \[[414](https://github.com/maelstrom-software/maelstrom/issues/414)\]
- `cargo-maelstrom` now passes `--no-deps` to `cargo metadata` command.
- `cargo-maelstrom` now displays warnings after the test summary.
- The cache code for the worker and broker has been combined and improved. Some
  notable improvements:
  - The worker now reuses the cache every time it is invoked. Since this
    includes the local worker, this means that startup times should be much
    better for local-only mode.
    \[[214](https://github.com/maelstrom-software/maelstrom/issues/214)\]
  - The cache directories now adhere to the cachedir protocol.
    \[[241](https://github.com/maelstrom-software/maelstrom/issues/241)\]
  - The cache directories now versioned. If an old version is detected at
    startup, its contents will be discarded.
    \[[216](https://github.com/maelstrom-software/maelstrom/issues/216)\]
    \[[164](https://github.com/maelstrom-software/maelstrom/issues/164)\]
  - A lock file has been added to the cache directory to ensure that only one
    worker or broker uses a cache directory at a time.
    \[[215](https://github.com/maelstrom-software/maelstrom/issues/215)\]
  - The worker and broker now share the same code.
    \[[234](https://github.com/maelstrom-software/maelstrom/issues/234)\]
- Pushed a change to [Ratatui](https://github.com/ratatui/ratatui), the TUI
  library that we use, to minimize flickering for the CLIs. We've updated our
  dependencies to use the new version of Ratatui with the change.
  \[[1342](https://github.com/ratatui/ratatui/pull/1341)\]
- Change socket code to use keepalive and nodelay options. This should make
  communication faster and also cause broker and worker to detect connections
  that silently die.
  \[[3](https://github.com/maelstrom-software/maelstrom/issues/3)\]
  \[[4](https://github.com/maelstrom-software/maelstrom/issues/4)\]
- The connections to the broker from the worker and client for sending
  artifacts are now reused. Before, each artifact, no matter how small, would
  result in a new TCP connection.
  \[[7](https://github.com/maelstrom-software/maelstrom/issues/7)\]
- The test-runner code has been rewritten to eliminate a few bugs but also to
  be more testable and extensible.
- We now track file descriptors to minimize the chance of running out of them.

## [0.12.0] - 2024-09-12

### General

The focus of this release was usability improvements for test runners.

#### Test Scheduling Priorities

We added a new priority field to the job struct to allow the client to indicate
higher-priority jobs. The tests runners now use this field to elevate the
priorities of new and previously-failed tests. This way, users get immediate
feedback about whether or not their changes fixed their tests.

#### New Configuration Values

We added a `stop-after` configuration value to all of the test runners. The
test runner will stop executing after this many failures. This pairs well with
the new scheduling priorities. Running `cargo maelstrom --stop-after=1` will
give immediate feedback on whether your new tests failed, or whether your
changes intended to fix a failing test worked.

#### Passing Extra Arguments to Tests

We added a number of configuration values to allow the passing of arbitrary
arguments to test binaries. In the case of Pytest, we also added configuration
values to enable passing arbitrary arguments to the collect phase, the
test-running phase, or both.

The extra arguments can also be passed after `--` on the command line, like
this: `maelstrom-go-test -- -test.parallel 12`.

#### More `maelstrom-go-test` Features

We added the `vet`, `short`, and `fullpath` pass-through configuration values
to `maelstrom-go-test`. We also added `maelstrom-go-test --list-packages`.

#### Better Default Metadata

We updated the default metadata for the test runners to be more useful. Each
test runner now has its own, specific, default metadata.

`cargo maelstrom` now passes `RUST_BACKTRACE` and `RUST_LIB_BACKTRACE` to the
test binaries.

`maelstrom-pytest` now has much better defaults, as is [documented
here](https://maelstrom-software.com/doc/book/0.12.0/pytest/spec/default.html).

#### UI Improvements

The test-runner UI has been improved in a number of ways to provide more
information about the state of tests. See below for more information.

#### Shared-Library Dependencies

We added a new layer type called `shared-library-dependencies`, which includes
the closure of shared libraries required to run a list of binaries. This makes
it easier to include external binaries and their dependencies in test
containers.

### Added
- `maelstrom-go-test` now edits out some lines from test failures that come
  from the `go test` test fixture code.
  \[[369](https://github.com/maelstrom-software/maelstrom/issues/369)\]
- `maelstrom-go-test` now shows tests which call `t.Skip` as status `IGNORED`.
  \[[367](https://github.com/maelstrom-software/maelstrom/issues/367)\]
- `vet` config value added to `maelstrom-go-test`.
  \[[356](https://github.com/maelstrom-software/maelstrom/issues/356)\]
- `short` config value added to `maelstrom-go-test`.
  \[[354](https://github.com/maelstrom-software/maelstrom/issues/354)\]
- `fullpath` config value added to `maelstrom-go-test`.
  \[[351](https://github.com/maelstrom-software/maelstrom/issues/351)\]
- `--list-packages` command-line option to `maelstrom-to-test`.
  \[[318](https://github.com/maelstrom-software/maelstrom/issues/318)\]
- `extra-test-binary-args` config value added to `maelstrom-go-test`.
  \[[363](https://github.com/maelstrom-software/maelstrom/issues/363)\]
- `extra-test-binary-args` config value added to `cargo-maelstrom`.
  \[[370](https://github.com/maelstrom-software/maelstrom/issues/370)\]
- `extra-pytest-args` config value added to `maelstrom-pytest`.
  \[[371](https://github.com/maelstrom-software/maelstrom/issues/371)\]
  \[[372](https://github.com/maelstrom-software/maelstrom/issues/372)\]
- `extra-pytest-collect-args` config value added to `maelstrom-pytest`.
  \[[372](https://github.com/maelstrom-software/maelstrom/issues/372)\]
- `extra-pytest-test-args` config value added to `maelstrom-pytest`.
  \[[372](https://github.com/maelstrom-software/maelstrom/issues/372)\]
- `shared-library-dependencies` layer type added.
  \[[266](https://github.com/maelstrom-software/maelstrom/issues/266)\]
- `stop-after` config value added.
  \[[114](https://github.com/maelstrom-software/maelstrom/issues/114)\]
- Scheduling priorities. Tests are run in LRT (longest remaining time) order
  within their priority band. Test with higher priority are always run before
  tests with lower priorities.
- Tracking of whether or not tests failed the last time they were run. If
  `--repeat` was specified, a test is considered to have failed if any instance
  failed.
- Functionality where new tests and tests that failed previously are run before
  other tests. This function pairs wells with the new `stop-after` config value.
  \[[380](https://github.com/maelstrom-software/maelstrom/issues/380)\]
- Fancy UI lists failed tests in a dialog before it has finished running tests.
  \[[391](https://github.com/maelstrom-software/maelstrom/issues/391)\]
- Simple and Fancy UI indicate the number of failed tests in their respective progress bars.
  \[[391](https://github.com/maelstrom-software/maelstrom/issues/391)\]

### Changed
- `ui` test-runner config option value now matches the same casing as the CLI option
  (kebab-case).
- UI now has improved tracking of the state of tests. The fancy UI in particular will now only show
  a test as running once it has actually started executing.
- `quiet` config option removed from test runners, replaced with new "quiet" UI.
  \[[374](https://github.com/maelstrom-software/maelstrom/issues/374)\]
- Demoted warning to debug message concerning un-built test binaries in `cargo-maelstrom`.
  \[[359](https://github.com/maelstrom-software/maelstrom/issues/359)\]
- Forward `RUST_BACKTRACE` and `RUST_LIB_BACKTRACE` by default in `cargo-maelstrom`.
  \[[386](https://github.com/maelstrom-software/maelstrom/issues/386)\]
- `maelstrom-pytest` default test metadata has been updated to be better and contain examples
  \[[324](https://github.com/maelstrom-software/maelstrom/issues/324)\]
  \[[326](https://github.com/maelstrom-software/maelstrom/issues/326)\]
- A `mount_point` of "/" is now disallowed when parsing devices in test metadata files. This didn't
  produce a job that worked previously, this now just fails earlier.
  \[[344](https://github.com/maelstrom-software/maelstrom/issues/344)\]
- A job with local networking and a "sys" mount now errors with a nicer message before even running.
  \[[389](https://github.com/maelstrom-software/maelstrom/issues/389)\]

### Fixed
- Fixed issue with test-runner fancy UI where it would crash if the summary was too large.
- Fixed issue with test-runners including extra trailing line in test output.
- Fixed issue where the helpful extra comments in the file written by test runners with `--init`
  weren't being added.

### Removed
- Removed support for the deprecated `maelstrom-test.toml` file.
  \[[333](https://github.com/maelstrom-software/maelstrom/issues/333)\]

## [0.11.0] - 2024-07-30

### General
There are two big changes in this release: a brand-new terminal UI and a Go test runner.

Our original terminal UI was enough to get the job done, but it wasn't anything
special. Starting this release, we plan on investing more heavily in our UI. We
think that this release is a great start. The new UI is a lot prettier, but
more importantly, it shows more information. There are new sections that show
the background build command, any pending artifact uploads and container image
downloads, and any running tests. This makes it much easier to understand
what's going on in the background.

We're also proud to announce our Go test runner: `maelstrom-go-test`. This new
test runner joins our Pytest and Rust test runners. The Go test runner
currently runs all the tests that a `go test ./...` invocation would run,
including normal tests, fuzz tests with their base corpus, and examples. There
isn't yet support for actual fuzzing, profiling, or benchmarking. We plan to
expand the supported features in the future.

In addition to these to big changes, there are a lot of smaller ones. See below
for details.

#### Added
- A way to turn off tests inside of all three test runners. This is useful when moving a
  test corpus over to Maelstrom. It's nice to be able to ignore some tests
  initially, and then add them back in slowly while containerizing them.
  \[[96](https://github.com/maelstrom-software/maelstrom/issues/96)\]
- Versioning of documentation on our web site. You can see documentation for
  latest release [here](https://maelstrom-software.com/doc/book/latest/).
  Previous releases can be found by using the [desired version in the
  URL](https://maelstrom-software.com/doc/book/0.10.0/).
  \[[276](https://github.com/maelstrom-software/maelstrom/issues/276)\]
- Documentation for templating of paths in test-runner configurations.
- A new `--repeat` (alias: `--loop`) command-line option to repeat all tests a
  certain number of times.
  \[[85](https://github.com/maelstrom-software/maelstrom/issues/85)\]

#### Removed
- The `devices` field of job specifications, both in clients and in the
  protobuf types, has been removed. This was deprecated in the last release.
  Use devices layers instead.
  \[[321](https://github.com/maelstrom-software/maelstrom/issues/321)\]

#### Changed
- `maelstrom-test.toml` is now deprecated. Use the test-runner specific file
  instead (`cargo-maelstrom.toml`, `maelstrom-pytest.toml`, or
  `maelstrom-go-test.toml`).
  \[[332](https://github.com/maelstrom-software/maelstrom/issues/332)\]
- The default for `include_shared_libraries` for test runners has changed.
  Before, if there were any layers added, it would be turned off. However, that
  doesn't make much sense since most tests have at least one layer for stubs,
  etc. Instead, the we now turn this off only if test's container is based off
  of a container image.
  \[[232](https://github.com/maelstrom-software/maelstrom/issues/232)\]
- `image` specifications in clients no longer need to provide a `use` field. If
  one is not provided, `layers` and `environment` will be used. Also, the name
  can be specified directly as a string, like `"image = \"docker:foo\""`.
  \[[329](https://github.com/maelstrom-software/maelstrom/issues/329)\]

## [0.10.0] - 2024-07-03
### High-Level
This release contains three big new things. The new Python test runner, the new `--tty` mode for
`maelstrom-run`, and big upgrades to the container image handling code.

### `cargo-maelstrom`
- The test configuration language now supports a single template argument in
  paths. When `<build-dir>` is found in a path, it will be replaced with a path to
  the build output directory for the current profile.
- `cargo-maelstrom.toml` is now the default place for test configuration. `maelstrom-test.toml` is
  still checked and used if `cargo-maelstrom.toml` doesn't exist, though it's use is deprecated.
- Fixed issue where test binary name was being printed even when it was the same as the package name
  on newer versions of `cargo`.

### `maelstrom-pytest`
First release of new test runner for Python tests. See the book for more details around its usage.

### Client Job Specification
These changes affect the job specification used by all the test runners and `maelstrom-run`.
- Deprecated `devices` and `added_devices` fields. Added new `devices` mount type.
- Changed format of container image names to be more standardized and support new features. See the
  book for details of new format.
- Added `devpts` and `mqueue` mount types.

### `cargo-maelstrom`, `maelstrom-pytest` and `maelstrom-run`
- Added the `--accept-invalid-remote-container-tls-certs` flag.
- Changed meaning of the sha256 hashes in the container lock-file to better match other tools. The
  version of the file was bumped to reflect this change. Users will be forced to regenerate their
  lock-file to get the new hashes thanks to the new version number.
- When generating manifests, if a file is under a certain size (< 200KiB) the data is included
  as part of the manifest instead of uploaded as a separate artifact.
- Fixed issue where we were running out of file descriptors contending with ourselves on the
  container tags lock. \[[291](https://github.com/maelstrom-software/maelstrom/issues/291)\]
- Support for docker images with a `/` in the name as added.
  \[[295](https://github.com/maelstrom-software/maelstrom/issues/295)\]
- Support for docker images from other public container image providers was added.
- Now search PATH environment variable when looking for a program to execute when running a job.
  \[[300](https://github.com/maelstrom-software/maelstrom/issues/300)\]

### `maelstrom-run`
- Add new `--file` argument to pass a job specification via path.
- Add new `--one` argument which allows extra functionality when running only one job.
- Add new `--tty` flag which attaches a TTY to a job being run.

### `maelstrom-worker` and Standalone Mode for Clients
- Added a "graceful shutdown" which waits for jobs to be canceled when exiting. Also catch most
  signals and ensure we go through "graceful shutdown" in response. This should remove the vast
  majority of leaked broken FUSE mounts.
  \[[303](https://github.com/maelstrom-software/maelstrom/issues/303)\]

### Client Internals
- Replaced `enable_writable_file_system` with new `JobRootOverlay` type.
- Added new `JobRootOverlay::Local` variant which allows for providing the overlay directory on
  local jobs.

## [0.9.0] - 2024-05-21

### Major Changes

There are two major changes in this release, plus a lot of smaller changes and
bug fixes.

#### LPT Scheduling

The first big change is that Maelstrom now uses [Longest-processing-time-first
(LPT)](https://en.wikipedia.org/wiki/Longest-processing-time-first_scheduling)
scheduling. This brings narrows the theoretical upper bound on the difference
between our schedules and optimal schedules (which are NP-hard to compute). In
practice, this greatly helps situations where there are a few long-running
tests mixed in with a bunch of faster ones.

`cargo-maelstrom` now remembers how long the last few runs of each test took.
It uses that information to predict how long the next run will take. Those
tests that are predicted to run the longest are run first.

#### Local Jobs

The second big change is that Maelstrom now has an "escape hatch" that one can
use to force a job to be run on the local machine, with access to the local
file system and/or network.

There is now a "local" network option, which means that a job will run on the
local machine, without being put in its own network namespace. In other words,
it will have all the same network access as any program on the local machine.

There is also now a "bind" mount type, which allows one to expose parts of the
local file system to the job. For example, the `./target` directory could be
provided to a job, mounted at `/target` inside of the job's container.

Jobs that use either of these features are forced to run on the local worker,
even if all other jobs are going to a remote cluster.

### `cargo-maelstrom`
- Added a new `container-image-depot-root` configuration value. The default is
  `$XDG_CACHE_HOME/maelstrom/container`, which is what was hard-coded before.
  \[[248](https://github.com/maelstrom-software/maelstrom/issues/248)\]
- Cleaned up where various thing go in the Cargo target directory. Before, we
  had a number of files and directories directly in the target directory. Now,
  there are `target/maelstrom/cache` and `target/maelstrom/state`, which are
  the equivalents to the "cache" and "state" directories in the XDG Base
  Directories Specification.
- Remove the following short command-line option aliases:
  \[[277](https://github.com/maelstrom-software/maelstrom/issues/277)\]
  - `-s` (`--cache-size`)
  - `-I` (`--inline-limit`)
  - `-S` (`--slots`)
- Fixed a few race conditions in the outputting of tests results.
  \[[279](https://github.com/maelstrom-software/maelstrom/issues/279)\]
- Fixed a bug with tar files with `/` entries (as demonstrated by the busybox
  image).
  \[[272](https://github.com/maelstrom-software/maelstrom/issues/272)\]
- Fixed two bugs related to `cargo-maelstrom` hanging instead of exiting when
  there is a remote error.
- Improved error messages when a container image or tag doesn't exist.
- Restored container image download progress bars.
- Swap order of "side" progress bars (artifact upload and container download)
  so that they align on the right.
- The format for mounts has now changed. The `fs_type` field has been renamed
  `type`, and a new `"bind"` type has been added.
- The `enable_loopback` field has been replaced with a new `network` field that
  can have one of the three values: `"disabled"`, `"local"`, or `"loopback"`.
  The new `"local"` option is used to give a job unfettered access to the local
  machine's network.
- Clarified that when running `--list-binaries`, the provided filters match
  against the package and the binary. Previously, we would only match against
  the package in these scenarios, which was confusing.
- Added the `shm` device type.

### `maelstrom-run`
- Added a new `container-image-depot-root` configuration value. The default is
  `$XDG_CACHE_HOME/maelstrom/container`, which is what was hard-coded before.
  \[[248](https://github.com/maelstrom-software/maelstrom/issues/248)\]
- Added a new `cache-root` configuration value. The default is
  `$XDG_CACHE_HOME/maelstrom/run`, which is what was hard-coded before.
  \[[191](https://github.com/maelstrom-software/maelstrom/issues/191)\]
- Added a new `state-root` configuration value. The default is
  `$XDG_STATE_HOME/maelstrom/run`.
  \[[191](https://github.com/maelstrom-software/maelstrom/issues/191)\]
- Moved the `client-process.log` file into the `state-root` directory. It used
  to live in the cache directory, which was incorrect according to the XDG Base
  Directory Specification.
- Remove the following short command-line option aliases:
  \[[277](https://github.com/maelstrom-software/maelstrom/issues/277)\]
  - `-s` (`--cache-size`)
  - `-i` (`--inline-limit`)
  - `-S` (`--slots`)
- Fixed a bug where we weren't properly waiting for job callbacks.
- Improved error messages when a container image or tag doesn't exist.
- The `mounts` and `enable_loopback` fields have been changed as in
  `cargo-maelstrom`.
- The `environment` field now support multiple layers of substitution, just
  like in `cargo-maelstrom`.
- Added the `shm` device type.

### Client Internals
- Changed the gRPC protobuf to remove cruft and make it easier to use.
- The client process now does environment variable substitution, so it's now
  available to all clients, not just Rust.

### `maelstrom-worker`
- Remove the following short command-line option aliases:
  \[[277](https://github.com/maelstrom-software/maelstrom/issues/277)\]
  - `-r` (`--cache-root`)
  - `-s` (`--cache-size`)
  - `-i` (`--inline-limit`)
  - `-S` (`--slots`)

### `maelstrom-broker`
- Remove the following short command-line option aliases:
  \[[277](https://github.com/maelstrom-software/maelstrom/issues/277)\]
  - `-r` (`--cache-root`)
  - `-s` (`--cache-size`)

## [0.8.0] - 2024-05-01

### `cargo-maelstrom`
- Add default test configuration when there is no `maelstrom-test.toml` file present.
- Add `--init` flag that writes default configuration if no `maelstrom-test.toml` file is present.
- Add experimental path templating to paths in test configuration. Existing paths with `<` or `>`
present may now need to escape these characters.
- When a test fails, print stdout in addition to stderr for that test.
- When a test times out, print stdout and stderr for that test.
- Add printing of the time each test took.
- Use the target directory `cargo metadata` says instead of always using
  `target/`, also create the target directory if it doesn't exist.
- Changed `--inline-limit`'s short option to `-I` so it doesn't conflict with
  `--include`. \[[220](https://github.com/maelstrom-software/maelstrom/issues/220)\]
- Cleaned up error messages so that it's more clear what went wrong when things go wrong.
  \[[229](https://github.com/maelstrom-software/maelstrom/issues/229)\]
- Fixed a bug where hostnames weren't supported for the `broker` configuration
  value, but only in configuration files.
  \[[235](https://github.com/maelstrom-software/maelstrom/issues/235)\]
- Clarified error message when `cargo-maelstrom` is run outside of a Rust workspace.

### `maelstrom-worker`
- Implemented support for white-out entries and opaque directories. The manifest format has been
  updated to add support, and OCI whiteout / opaque directory entries in tar
  files are interpreted.
- Added test duration to `JobEffects`.

### `maelstrom-run`
- Fixed bug where we weren't creating cache directory if it didn't exist.
  \[[230](https://github.com/maelstrom-software/maelstrom/issues/230)\]
- Cleaned up error messages so that it's more clear what went wrong when things go wrong.
  \[[229](https://github.com/maelstrom-software/maelstrom/issues/229)\]

### `maelstrom-client`
- Fixed issue where we weren't accepting certain OCI images from dockerhub that had the
  `vnd.oci.image.manifest.v1+json` manifest type.

### All Binaries
- Removed -v short option for version.

### General
- Updated README.md and documentation substantially.

## [0.7.0] - 2024-04-19
### High-Level
This release was focused on two things: fixing performance regressions and
adding a new standalone mode.

In the last release, we introduced a FUSE file system. It gave us a lot of
functionality that we wanted, but it's performance wasn't great. In this
release, we've returned performance back to where it was before we added the
FUSE file system!

Standalone mode allows one to use `cargo-maelstrom` or `maelstrom-run` (or any
other maelstrom client) without connecting to a broker or worker. This is
achieved by running a local copy of the worker inside the client. At this
point, it's an either-or proposition: all the jobs are either run locally, or
they are all run on the cluster. In future releases we'll ease this so that the
local worker can be used alongside the cluster.
\[[161](https://github.com/maelstrom-software/maelstrom/issues/161)\]

There were also some other bug fixes and performance improvements.

### `cargo-maelstrom` and `maelstrom-run`
#### Added 
- Standalone mode. This mode is entered when no broker address is specified.
  The `broker` configuration value is now optional.
- `cache-size`, `inline-limit`, and `slots` configuration values. These affect
  the local worker.
#### Fixed
- A bug where output from cargo failures could get eaten by the status bar.
\[[207](https://github.com/maelstrom-software/maelstrom/issues/207)\]
- A bug where multiple versions of a package would cause errors. (We discovered
  this running `cargo-maelstrom` in the `clap` repository).
\[[206](https://github.com/maelstrom-software/maelstrom/issues/206)\]

### `maelstrom-worker`
#### Changed
- Re-architected the worker so that it now runs as pid 2 in its own namespace
  instead of pid 1. This allowed us to simplify the implementation, as a task
  can spawn a subprocess and then wait for that subprocess independently of what
  the rest of the system is doing.
- Jobs are now spawned in the background on their own tasks, removing a
  performance bottleneck. Before, one job couldn't be spawned until the
  previous one had completed its exec. This became a big performance blocker
  with the introduction of FUSE, though it was never ideal. This was enabled by
  the previous change.
- All jobs are now started with `clone(CLONE_VM)`. This yielded a moderate, but
  measurable, performance improvement.
- We now use `splice` in the FUSE file system, so that no file data ever goes
  through user-space. This yielded an appreciable performance improvement.
- The child process now creates the FUSE file system and passes a file
  descriptor back to the worker to manage. This eliminates the possibility of
  accidentally leaving around mounted children file systems, as the child is
  always in its own mount namespace.

### `maelstrom-client-process`
#### Fixed
- A bug in how we were waiting for end-of-file. We were accidentally
  busy-waiting on the open file descriptor, resulting in a big performance
  degradation.

## [0.6.0] - 2024-03-29
### High-Level
There were a lot of large changes in this release. At a high level:

- Three synthetic layer types were added. This allows one to easily create
  container file systems with exactly the files and directories that are
  necessary. These are: `glob`, `stubs`, and `symlinks`. Additionally, the
  `canonicalize` and `follow-symlinks` options have been added to the `glob`
  and `paths` layer types.

- The execution model for the client has changed to support languages other
  than Rust. Before this release, there was a client library that was
  instantiated inside of `cargo-maelstrom` and `maelstrom-client-cli`
  (which we renamed `maelstrom-run` in this release). The library would connect
  directly to the broker. That client library had a lot of very useful
  functionality that we wanted to make available to languages other than Rust.

  So, we changed the client to be a program instead of a library. The client
  program uses gRPC to make it easy to use from other languages.
  `cargo-maelstrom` and `maelstrom-run` now start the client and communicate
  with it over Unix-domain sockets. We also have a proof-of-concept Python
  client.

- We implemented our own overlay file system using FUSE instead of using
  overlayfs on the worker. This unlocks some future features and fixes. In the
  short-term, it has resulted in a bit of a performance regression, but we're
  confident we'll regain the performance in the upcoming releases.

- We re-implemented the configuration value system. We moved away from a
  pre-existing create and implemented our own behavior. As a result, we have
  much better command-line help messages, support multiple configuration files,
  and fully support XDG. A side-effect of this is that some configuration
  values have changed their names or formats.

### General
#### Added
- Support for job timeouts. When a job times out, the stdout and stderr
  are still returned to the client.
  \[[65](https://github.com/maelstrom-software/maelstrom/issues/65)\]
- The `fuse` device (`/dev/fuse`).
- `cargo xtask` for various tasks related to building and publishing.
#### Changed
- The XDG Base Directories spec is now used for locating config files.
  Binaries now also supports multiple config files.
- The default value for cache directories now is based on the XDG Base
  Directories spec.
- Config value names now use '-'s instead of '\_'s.
- Renamed the `--cache-target-bytes-used` flag to `--cache-size`.

### `cargo-maelstrom`
#### Added
- The following layers types:
  - `glob` layers are like `paths` layers, except you use glob patterns to specify the files to include.
  - `stubs` layers create empty files and directories. These are very useful
    for ensuring mount points and device files exist.
  - `symlinks` layers create symlinks.
- The following options to the `glob` and `paths` layers:
  - `follow-symlinks` will cause the generated layer to contain the target
    value of symlinks instead of the symlinks themselves.
  - `canonicalize` will take the absolute paths on the generating system the
    same as for the container.
- The `timeout` directive field to specify a timeout in seconds. A value
  of 0 indicates no timeout.
- The `--timeout` command-line option to override the timeout for the test
  specified. A value of 0 indicates no timeout.
- Support for the `fuse` device in directives.
- Progress bars for uploads of artifacts to the broker.
- The following cargo pass-through options:
  - `--features` (`-F`)
  - `--all-features`
  - `--no-default-features`
  - `--profile`
  - `--target`
  - `--target-dir`
  - `--manifest-path`
  - `--frozen`
  - `--locked`
  - `--offline`

### Changed
- Tweaked spinner text to provide more information.
- Switched around some of the short flag option assignments.

### `maelstrom-run`
#### Changed
- Renamed `maelstrom-client-cli` to `maelstrom-run`.

#### Added
- The new layer types and layer options from `cargo-maelstrom`.
- The `timeout` field from `cargo-maelstrom`.

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
  \[[121](https://github.com/maelstrom-software/maelstrom/issues/121),
  [124](https://github.com/maelstrom-software/maelstrom/issues/124)\]
- We changed the licensing to use dual license using Apache 2.0 and MIT instead
  of the BSD 2-clause license. This brings us into line with the Rust community.
  \[[134](https://github.com/maelstrom-software/maelstrom/issues/134),
  [135](https://github.com/maelstrom-software/maelstrom/issues/135)\]

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

[unreleased]: https://github.com/maelstrom-software/maelstrom/compare/v0.13.0...HEAD
[0.13.0]: https://github.com/maelstrom-software/maelstrom/compare/v0.12.0...v0.13.0
[0.12.0]: https://github.com/maelstrom-software/maelstrom/compare/v0.11.0...v0.12.0
[0.11.0]: https://github.com/maelstrom-software/maelstrom/compare/v0.10.0...v0.11.0
[0.10.0]: https://github.com/maelstrom-software/maelstrom/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/maelstrom-software/maelstrom/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/maelstrom-software/maelstrom/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/maelstrom-software/maelstrom/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/maelstrom-software/maelstrom/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/maelstrom-software/maelstrom/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/maelstrom-software/maelstrom/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/maelstrom-software/maelstrom/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/maelstrom-software/maelstrom/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/maelstrom-software/maelstrom/releases/tag/v0.1.0
