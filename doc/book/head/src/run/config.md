# Configuration Values

`maelstrom-run` supports the following [configuration values](../config.md):

Value                                                                  | Type    | Description                                                                                 | Default
-----------------------------------------------------------------------|---------|---------------------------------------------------------------------------------------------|----------------
<span style="white-space: nowrap;">`log-level`</span>                  | string  | [minimum log level](#log-level)                                                             | `"info"`
<span style="white-space: nowrap;">`cache-size`</span>                 | string  | [target cache disk space usage](#cache-size)                                                | `"1 GB"`
<span style="white-space: nowrap;">`inline-limit`</span>               | string  | [maximum amount of captured standard output and error](#inline-limit)                       | `"1 MB"`
<span style="white-space: nowrap;">`slots`</span>                      | number  | [job slots available](#slots)                                                               | 1 per CPU
<span style="white-space: nowrap;">`container-image-depot-root`</span> | string  | [container images cache directory](#container-image-depot-root)                             | `$XDG_CACHE_HOME/maelstrom/containers`
`accept-invalid-remote-container-tls-certs`                            | boolean | [allow invalid container registry certificates](#accept-invalid-remote-container-tls-certs) | `false`
<span style="white-space: nowrap;">`broker`</span>                     | string  | [address of broker](#broker)                                                                | standalone mode
<span style="white-space: nowrap;">`state-root`</span>                 | string  | [directory for client process's log file](#state-root)                                      | `$XDG_STATE_HOME/maelstrom/run`
<span style="white-space: nowrap;">`cache-root`</span>                 | string  | [directory for local worker's cache and cached layers](#cache-root)                         | `$XDG_CACHE_HOME/maelstrom/run`
<span style="white-space: nowrap;">`escape-char`</span>                | string  | [TTY escape character for `--tty` mode](#escape-char)                                       | `"^]"`

## `log-level`

This is a setting [common to all](../common-config.md) Maelstrom programs.
See [here](../common-config.md#log-level) for details.

`maelstrom-run` always prints log messages to standard error. It also passes
the log level to `maelstrom-client`, which will log its output in a file named
`client-process.log` in the [state directory](#state-root).

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

This is a setting common to all clients. See [here](../specifying-broker.md) for details.

## `state-root`

This is a [directory setting](../dirs.md) common to all clients. See [here](../dirs.md#state-dir) for more details.

## `cache-root`

This is a [directory setting](../dirs.md) common to all clients. See [here](../dirs.md#cache-dir) for more details.

## `escape-char`

This configuration value specifies the terminal escape character when
maelstrom-run is run in [TTY mode](../run.md#tty-mode) (with the `--tty`
command-line option).

In TTY mode, all key presses are sent to the job's terminal. So, a `^C` typed
at the user's terminal be transmitted to the job's terminal, where it will be
interpreted however the job interprets it. This is true for all control
characters. As a result, typing `^C` will not kill the `maelstrom-run`, nor
will typing `^Z` suspend `maelstrom-run`.

This what the terminal escape character is for. Assuming, the escape character
is `^]`, then typing `^]^C` will kill `maelstrom-run`, and typing `^]^Z` will
suspend `maelstrom-run`. To transmit the escape character to the job, it must
be typed twice: `^[^[` will send `^[` to the job.

If the escape character is followed by any character other than `^C`, `^Z`, or
itself, then it will have no special meaning. The escape character and the
following character will both be transmitted to the job. Similarly, if no
character follows the escape character for 1.5 seconds, then the escape
character will be transmitted to the job.

The escape character must be specified as a string in one of three forms:
  - As a one-character string. In this case, the one character will be
    the escape character.
  - A two-character string in [caret
    notation](https://en.wikipedia.org/wiki/Caret_notation). For example:
    `^C`, `^]`, etc.
  - A [Rust byte
    literal](https://doc.rust-lang.org/reference/tokens.html#byte-literals)
    starting with `\`. For example: `\n`, `\x77`, etc. Note that TOML and the
    shell may perform their own backslash escaping before Maelstrom sees
    the string. Be sure to either double the backslash or use a string form that
    isn't subject to backslash escaping.

Whatever form it takes, the character must be an ASCII character: it can't have
a numeric value larger than 127.
