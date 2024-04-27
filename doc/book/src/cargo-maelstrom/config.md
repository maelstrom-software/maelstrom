# Configuration Values

`cargo-maelstrom` supports the following [configuration values](../config.md):

Value                 | Type    | Description                                                   | Default
----------------------|---------|---------------------------------------------------------------|----------------
`broker`              | string  | [address of broker](#broker)                                  | standalone mode
`log-level`           | string  | [minimum log level](#log-level)                               | `"info"`
`quiet`               | boolean | [don't output per-test information](#quiet)                   | `false`
`timeout`             | string  | [override timeout value tests](#timeout)                      | don't override
`cache-size`          | string  | [target cache disk space usage](#cache-size)                  | `"1 GB"`
`inline-limit`        | string  | [maximum amount of captured stdout and stderr](#inline-limit) | `"1 MB"`
`slots`               | number  | [job slots available](#slots)                                 | 1 per CPU
`features`            | string  | [comma-separated list of features to activate](#cargo)        | Cargo's default
`all-features`        | boolean | [activate all available features](#cargo)                     | Cargo's default
`no-default-features` | boolean | [do not activate the `default` feature](#cargo)               | Cargo's default
`profile`             | string  | [build artifacts with the specified profile](#cargo)          | Cargo's default
`target`              | string  | [build for the target triple](#cargo)                         | Cargo's default
`target-dir`          | string  | [directory for all generated artifacts](#cargo)               | Cargo's default
`manifest-path`       | string  | [path to `Cargo.toml`](#cargo)                                | Cargo's default
`frozen`              | boolean | [require `Cargo.lock` and cache are up to date](#cargo)       | Cargo's default
`locked`              | boolean | [require `Cargo.lock` is up to date](#cargo)                  | Cargo's default
`offline`             | boolean | [run without Cargo accessing the network](#cargo)             | Cargo's default

## `broker`

The `broker` configuration value specifies the socket address of the broker.
This configuration value is optional. If not provided, `cargo-maelstrom` will
run in [standalone mode](../local-worker.md).

Here are some example value socket addresses:
  - `broker.example.org:1234`
  - `192.0.2.3:1234`
  - `[2001:db8::3]:1234`

## `log-level`

See [here](../log-levels.md).

`cargo-maelstrom` always prints log message to stderr.

## `quiet`

The `quiet` configuration values, if set to `true`, causes `cargo-maelstrom` to
be more more succinct with its output. If `cargo-maelstrom` is outputting to a
terminal, it will display a single-line progress bar indicating all test state,
then print a summary at the end. If not outputting to a terminal, it will only
print a summary at the end.

## `timeout`

The optional `timeout` configuration value provides the [timeout](../spec.md#timeout)
value to use for all tests. This will override any value set in
[`maelstrom-test.toml`](spec/fields.md#timeout).

## `cache-size`

This is a [local-worker setting](../local-worker.md). See [here](../local-worker.md#cache-size) for more.

## `inline-limit`

This is a [local-worker setting](../local-worker.md). See [here](../local-worker.md#inline-limit) for more.

## `slots`

This is a [local-worker setting](../local-worker.md). See [here](../local-worker.md#slots) for more.

## Cargo Settings {#cargo}

`cargo-maelstrom` shells out to `cargo` to get metadata about tests and to
build the test artifacts. For the former, it uses `cargo metadata`. For the
latter, it uses `cargo test --no-run`.

`cargo-maelstrom` supports a number of command-line options that are passed
through directly to `cargo`. It does not inspect these values at all.

Command-Line Option   | Cargo Grouping | Passed To
----------------------|-|-
`features`            | [feature selection](https://doc.rust-lang.org/cargo/commands/cargo-test.html#feature-selection) | [`test`](https://doc.rust-lang.org/cargo/commands/cargo-test.html) and [`metadata`](https://doc.rust-lang.org/cargo/commands/cargo-metadata.html)
`all-features`        | [feature selection](https://doc.rust-lang.org/cargo/commands/cargo-test.html#feature-selection) | [`test`](https://doc.rust-lang.org/cargo/commands/cargo-test.html) and [`metadata`](https://doc.rust-lang.org/cargo/commands/cargo-metadata.html)
`no-default-features` | [feature selection](https://doc.rust-lang.org/cargo/commands/cargo-test.html#feature-selection) | [`test`](https://doc.rust-lang.org/cargo/commands/cargo-test.html) and [`metadata`](https://doc.rust-lang.org/cargo/commands/cargo-metadata.html)
`profile`             | [compilation](https://doc.rust-lang.org/cargo/commands/cargo-test.html#compilation-options)     | [`test`](https://doc.rust-lang.org/cargo/commands/cargo-test.html)
`target`              | [compilation](https://doc.rust-lang.org/cargo/commands/cargo-test.html#compilation-options)     | [`test`](https://doc.rust-lang.org/cargo/commands/cargo-test.html)
`target-dir`          | [output](https://doc.rust-lang.org/cargo/commands/cargo-test.html#output-options)               | [`test`](https://doc.rust-lang.org/cargo/commands/cargo-test.html)
`manifest-path`       | [manifest](https://doc.rust-lang.org/cargo/commands/cargo-test.html#manifest-options)           | [`test`](https://doc.rust-lang.org/cargo/commands/cargo-test.html) and [`metadata`](https://doc.rust-lang.org/cargo/commands/cargo-metadata.html)
`frozen`              | [manifest](https://doc.rust-lang.org/cargo/commands/cargo-test.html#manifest-options)           | [`test`](https://doc.rust-lang.org/cargo/commands/cargo-test.html) and [`metadata`](https://doc.rust-lang.org/cargo/commands/cargo-metadata.html)
`locked`              | [manifest](https://doc.rust-lang.org/cargo/commands/cargo-test.html#manifest-options)           | [`test`](https://doc.rust-lang.org/cargo/commands/cargo-test.html) and [`metadata`](https://doc.rust-lang.org/cargo/commands/cargo-metadata.html)
`offline`             | [manifest](https://doc.rust-lang.org/cargo/commands/cargo-test.html#manifest-options)           | [`test`](https://doc.rust-lang.org/cargo/commands/cargo-test.html) and [`metadata`](https://doc.rust-lang.org/cargo/commands/cargo-metadata.html)

`cargo-maelstrom` doesn't accept multiple instances of the `--features`
command-line option. Instead, combine the features into a single,
comma-separated argument like this: `--features=feat1,feat2,feat3`.

`cargo-maelstrom` doesn't accept the `--release` alias. Use
`--profile=release` instead.
