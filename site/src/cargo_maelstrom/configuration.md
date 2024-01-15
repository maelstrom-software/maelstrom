# Configuration

`cargo-maelstrom` can be configured with two different files. One for
configuring operation of the command itself, and another for configuring how
tests are run.

## Configuring `cargo-maelstrom`

`cargo-maelstrom` can be configured with the CLI, environment variables, or a
configuration file.

The default configuration file path is
`<workspace-root>/.config/cargo-maelstrom.toml`. This path can be overridden by
passing the `--config-file` option via the CLI.

Here are the different options

- [`broker`](#the-broker-field): the address of the broker to connect to
- `[run]`: contains options about the `run` sub-command
    - [`quiet`](#the-quiet-field): if true, use quiet mode

## The `broker` Field
- TOML: `broker = "1.2.3.4:9000"`
- CLI: `--broker 1.2.3.4:9000`
- ENV: `CARGO_MAELSTROM_BROKER=1.2.3.4:9000`

This is the network address of the broker which the client will attempt to
establish a connection to.

## The `quiet` Field
- TOML: `quiet = true`
- CLI: `--quiet`
- ENV: `CARGO_MAELSTROM_RUN="{ quiet = true }"`

Enables quiet mode. See
[Running Tests >> Terminal Output](./running_tests.html#terminal-output).

## Configuring Tests

The configuration file path for tests is `<workspace-root>/maelstrom-test.toml`.

It can contain the following options

- [`[[directives]]`](#the-directives-section) Defines a directive
    - [`filter`](#the-filter-field) Directive test filter
    - [`enable_loopback`](./execution_environment.md#the-enable_loopback-field)
        Enables loopback device
    - [`enable_writable_file_system`](
        ./execution_environment.md#the-enable_writable_file_system-field)
        Enables files-system writes
    - [`user`](./execution_environment.md#the-user-field) User test runs as
    - [`group`](./execution_environment.md#the-group-field) Group test runs as
    - [`working_directory`](
        ./execution_environment.md#the-working_directory-field)
        Test container path used as working directory when running the test
    - [`mounts`](./execution_environment.md#the-mounts-field) Mounts done in
        test container
    - [`devices`](./execution_environment.md#the-devices-field) Devices created
      in test container
    - [`environment`](./execution_environment.md#the-environment-field)
        Environment variables set in test container
    - [`added_environment`](
        ./execution_environment.md#the-added_environment-field)
        Environment variables added to existing ones
    - [`added_devices`](./execution_environment.md#the-added_devices-field)
        Devices added to existing ones
    - [`added_mounts`](./execution_environment.md#the-added_mounts-field)
        Mounts added to existing ones
    - [`layers`](./layers.md#the-layers-field) File-system layers when running
        the test
    - [`added_layers`](./layers.md#the-added_layers-field) File-system layers
        appended to existing ones
    - [`include_shared_libraries`](./layers.md#the-include_shared_libraries-field) Include
        shared libraries in dependency layer.
    - [`image`](./container_images.md#the-image-field) Configures a container image

### The `[directives]` section

Each directive contains a filter which describes which tests this directive
should apply to, and an array of settings about how the tests are run on the
worker.

### The `filter` field

```toml
[[directives]]
filter = "package.equals(foo) && name.equals(bar)"
```

This contains a [Test Pattern DSL](./test_pattern_dsl.md) snippet that describes
the set of tests the directive applies to.

If the `filter` field isn't provided, it defaults to matching all tests.
