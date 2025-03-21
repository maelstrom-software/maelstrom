# Configuration Values

`maelstrom-pytest` supports the following [configuration values](../config.md):

Value                                                                  | Short Option | Type    | Description                                                                                 | Default
-----------------------------------------------------------------------|--------------|---------|---------------------------------------------------------------------------------------------|----------------
<span style="white-space: nowrap;">`cache-size`</span>                 |              | string  | [target cache disk space usage](#cache-size)                                                | `"1 GB"`
<span style="white-space: nowrap;">`inline-limit`</span>               |              | string  | [maximum amount of captured standard output error](#inline-limit)                           | `"1 MB"`
<span style="white-space: nowrap;">`slots`</span>                      |              | number  | [job slots available](#slots)                                                               | 1 per CPU
<span style="white-space: nowrap;">`container-image-depot-root`</span> |              | string  | [container images cache directory](#container-image-depot-root)                             | `$XDG_CACHE_HOME/maelstrom/containers`
`accept-invalid-remote-container-tls-certs`                            |              | boolean | [allow invalid container registry certificates](#accept-invalid-remote-container-tls-certs) | `false`
<span style="white-space: nowrap;">`broker`</span>                     | `-b`         | string  | [address of broker](#broker)                                                                | standalone mode
<span style="white-space: nowrap;">`log-level`</span>                  | `-L`         | string  | [minimum log level](#log-level)                                                             | `"info"`
<span style="white-space: nowrap;">`quiet`</span>                      |              | boolean | [don't output per-test information](#quiet)                                                 | `false`
<span style="white-space: nowrap;">`ui`</span>                         |              | string  | [UI style to use](#ui)                                                                      | `"auto"`
<span style="white-space: nowrap;">`repeat`</span>                     | `-r`         | number  | [how many times to run each test](#repeat)                                                  | `1`
<span style="white-space: nowrap;">`timeout`</span>                    | `-t`         | string  | [override timeout value tests](#timeout)                                                    | don't override
<span style="white-space: nowrap;">`collect-from-module`</span>        |              | string  | [collect tests from the specified module](#collect-from-module)                             | don't override
<span style="white-space: nowrap;">`extra-pytest-args`</span>          |              | list    | [pass arbitrary arguments to pytest](#extra-pytest-args)                                    | no args
<span style="white-space: nowrap;">`extra-pytest-collect-args`</span>  |              | list    | [pass arbitrary arguments to pytest when collecting](#extra-pytest-collect-args)            | no args
<span style="white-space: nowrap;">`extra-pytest-test-args`</span>     |              | list    | [pass arbitrary arguments to pytest when running a test](#extra-pytest-test-args)           | no args
<span style="white-space: nowrap;">`stop-after`</span>                 | `-s`         | number  | [stop after given number of failures](#stop-after)                                          | never stop

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
style="white-space: nowrap;">`maelstrom-pytest`</span> will run in [standalone
mode](../local-worker.md).

Here are some example value socket addresses:
  - `broker.example.org:1234`
  - `192.0.2.3:1234`
  - `[2001:db8::3]:1234`

## `log-level`

This is a setting [common to all](../common-config.md) Maelstrom programs.
See [here](../common-config.md#log-level) for details.

<span style="white-space: nowrap;">`maelstrom-pytest`</span> always prints log
messages to stdout.

## `quiet`

The `quiet` configuration value, if set to `true`, causes <span
style="white-space: nowrap;">`maelstrom-pytest`</span> to be more more succinct
with its output. If <span style="white-space: nowrap;">`maelstrom-pytest`</span>
is outputting to a terminal, it will display a single-line progress bar
indicating all test state, then print a summary at the end. If not outputting
to a terminal, it will only print a summary at the end.

## `ui`

The `ui` configuration value controls the UI style used. It must be one of
`auto`, `fancy`, `quiet`, or `simple`. The default value is `auto`.

Style    | Description
---------|------------
`simple` | This is our original UI. It prints one line per test result (unless [`quiet`](#quiet) is `true`), and will display some progress bars if standard output is a TTY.
`fancy`  | This is our new UI. It has a rich TTY experience with a lot of status updates. It is incompatible with [`quiet`](#quiet) or with non-TTY standard output.
`quiet`  | Minimal UI with only a single progress bar
`auto`   | Will choose `fancy` if standard output is a TTY and [`quiet`](#quiet) isn't `true`. Otherwise, it will choose `simple`.

## `repeat`

The `repeat` configuration value specifies how many times each test will be
run. It must be a nonnegative integer. On the command line, `--loop` can be
used as an alias for `--repeat`.

## `timeout`

The optional `timeout` configuration value provides the
[timeout](../spec.md#timeout) value to use for all tests. This will override
any value set in [`maelstrom-pytest.toml`](spec/fields.md#timeout).

## `collect-from-module`

Collect tests from the provided module instead of using pytest's default
collection algorithm. This will pass the provided module to pytest along with
the `--pyargs` flag.

## `extra-pytest-args`

This allows passing of arbitrary command-line arguments to pytest when collecting tests and running
a test. These arguments are added in to both `extra-pytest-collect-args` and
`extra-pytest-test-args` at the beginning. See those individual configuration values for details.

## `extra-pytest-collect-args`

This allows passing of arbitrary command-line arguments to pytest when collecting tests. See `pytest
--help` for what arguments are accepted normally. Since these arguments are passed directly and not
interpreted, this can be used to interact with arbitrary pytest plugins.

These arguments are added after `--co` and `--pyargs`.

## `extra-pytest-test-args`

This allows passing of arbitrary command-line arguments to pytest when running a test. See `pytest
--help` for what arguments are accepted normally. Since these arguments are passed directly and not
interpreted, this can be used to interact with arbitrary pytest plugins.

These arguments are added after the `--verbose` but before we pass the `nodeid` of which test to
run. It could be possible to use these flags to somehow not run the test `maelstrom-pytest` was
intending to, producing confusing results.

When provided on the command-line these arguments are positional and come after any other arguments.
They must always be preceded by `--` like as follows:

```bash
maelstrom-pytest -- -n1
```

## `stop-after`

This optional configuration value if provided gives a limit on the number of failure to tolerate. If
the limit is reached, `cargo-maelstrom` exits prematurely.
