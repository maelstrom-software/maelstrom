# Configuration Values

`maelstrom-go-test` supports the following [configuration values](../config.md):

Value                                                                  | Type    | Description                                                                                 | Default
-----------------------------------------------------------------------|---------|---------------------------------------------------------------------------------------------|----------------
<span style="white-space: nowrap;">`cache-size`</span>                 | string  | [target cache disk space usage](#cache-size)                                                | `"1 GB"`
<span style="white-space: nowrap;">`inline-limit`</span>               | string  | [maximum amount of captured standard output error](#inline-limit)                           | `"1 MB"`
<span style="white-space: nowrap;">`slots`</span>                      | number  | [job slots available](#slots)                                                               | 1 per CPU
<span style="white-space: nowrap;">`container-image-depot-root`</span> | string  | [container images cache directory](#container-image-depot-root)                             | `$XDG_CACHE_HOME/maelstrom/containers`
`accept-invalid-remote-container-tls-certs`                            | boolean | [allow invalid container registry certificates](#accept-invalid-remote-container-tls-certs) | `false`
<span style="white-space: nowrap;">`broker`</span>                     | string  | [address of broker](#broker)                                                                | standalone mode
<span style="white-space: nowrap;">`log-level`</span>                  | string  | [minimum log level](#log-level)                                                             | `"info"`
<span style="white-space: nowrap;">`quiet`</span>                      | boolean | [don't output per-test information](#quiet)                                                 | `false`
<span style="white-space: nowrap;">`ui`</span>                         | string  | [UI style to use](#ui)                                                                      | `"auto"`
<span style="white-space: nowrap;">`repeat`</span>                     | number  | [how many times to run each test](#repeat)                                                  | `1`
<span style="white-space: nowrap;">`timeout`</span>                    | string  | [override timeout value tests](#timeout)                                                    | don't override
<span style="white-space: nowrap;">`vet`</span>                        | string  | [control `go test` `-vet` flag_value](#vet)                                                 | `go test`'s default
<span style="white-space: nowrap;">`short`</span>                      | boolean | [tells long running tests to shorten their times](#short)                                   | `false`
<span style="white-space: nowrap;">`fullpath`</span>                   | boolean | [shows the full file name in error messages](#fullpath)                                     | `false`

## `cache-size`

This is a [local-worker setting](../local-worker.md), common to all clients. See [here](../local-worker.md#cache-size) for details.

## `inline-limit`

This is a [local-worker setting](../local-worker.md), common to all clients. See [here](../local-worker.md#inline-limit) for details.

## `slots`

This is a [local-worker setting](../local-worker.md), common to all clients. See [here](../local-worker.md#slots) for details.

## `container-image-depot-root`

This is a [container-image setting](../container-images.md), common to all clients. See [here](../container-images.md#container-image-depot-root) for details.

## `accept-invalid-remote-container-tls-certs`

This is a [container-image setting](../container-images.md), common to all clients. See [here](../container-images.md#accept-invalid-remote-container-tls-certs) for details.

## `broker`

The `broker` configuration value specifies the socket address of the broker.
This configuration value is optional. If not provided, <span
style="white-space: nowrap;">`maelstrom-go-test`</span> will run in [standalone
mode](../local-worker.md).

Here are some example value socket addresses:
  - `broker.example.org:1234`
  - `192.0.2.3:1234`
  - `[2001:db8::3]:1234`

## `log-level`

This is a setting [common to all](../common-config.md) Maelstrom programs.
See [here](../common-config.md#log-level) for details.

<span style="white-space: nowrap;">`maelstrom-go-test`</span> always prints log
messages to stdout. It also passes
the log level to `maelstrom-client`, which will log its output in a [file named
`client-process.log` in the state directory](target-dir.md#client-log-file).

## `quiet`

The `quiet` configuration value, if set to `true`, causes <span
style="white-space: nowrap;">`maelstrom-go-test`</span> to be more more succinct
with its output. If <span style="white-space: nowrap;">`maelstrom-go-test`</span>
is outputting to a terminal, it will display a single-line progress bar
indicating all test state, then print a summary at the end. If not outputting
to a terminal, it will only print a summary at the end.

## `ui`

The `ui` configuration value controls the UI style used. It must be one of
`auto`, `fancy`, or `simple`. The default value is `auto`.

Style    | Description
---------|------------
`simple` | This is our original UI. It prints one line per test result (unless [`quiet`](#quiet) is `true`), and will display some progress bars if standard output is a TTY.
`fancy`  | This is our new UI. It has a rich TTY experience with a lot of status updates. It is incompatible with [`quiet`](#quiet) or with non-TTY standard output.
`auto`   | Will choose `fancy` if standard output is a TTY and [`quiet`](#quiet) isn't `true`. Otherwise, it will choose `simple`.

## `repeat`

The `repeat` configuration value specifies how many times each test will be
run. It must be a nonnegative integer. On the command line, `--loop` can be
used as an alias for `--repeat`.

## `timeout`

The optional `timeout` configuration value provides the
[timeout](../spec.md#timeout) value to use for all tests. This will override
any value set in [`maelstrom-go-test.toml`](spec/fields.md#timeout).

## `vet`

This configuration controls the value of the `-vet` flag that is passed to `go
test`. If not provided, the flag isn't provided to `go test`. See `go help
test` for more information.

## `short`

Tells long running tests to shorten their run time. This flag is forwarded to
test binaries. See `go help testflag` for more information.

## `fullpath`

Shows the full file name in error messages. This flag is forwarded to test
binaries. See `go help testflag` for more information.
