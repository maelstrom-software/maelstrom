# Configuration Values

`maelstrom-run` supports the following [configuration values](../config.md):

Value                 | Type    | Description                                                   | Default
----------------------|---------|---------------------------------------------------------------|----------------
`broker`              | string  | [address of broker](#broker)                                  | standalone mode
`log-level`           | string  | [minimum log level](#log-level)                               | `"info"`
`cache-size`          | string  | [target cache disk space usage](#cache-size)                  | `"1 GB"`
`inline-limit`        | string  | [maximum amount of captured stdout and stderr](#inline-limit) | `"1 MB"`
`slots`               | number  | [job slots available](#slots)                                 | 1 per CPU

## `broker`

The `broker` configuration value specifies the socket address of the broker.
This configuration value is optional. If not provided, `maelstrom-run` will run
in [standalone mode](../local-worker.md).

Here are some example value socket addresses:
  - `broker.example.org:1234`
  - `192.0.2.3:1234`
  - `[2001:db8::3]:1234`

## `log-level`

See [here](../common-config.md#log-level).

`maelstrom-run` always prints log messages to stderr.

## `cache-size`

This is a [local-worker setting](../local-worker.md). See [here](../local-worker.md#cache-size) for more.

## `inline-limit`

This is a [local-worker setting](../local-worker.md). See [here](../local-worker.md#inline-limit) for more.

## `slots`

This is a [local-worker setting](../local-worker.md). See [here](../local-worker.md#slots) for more.
