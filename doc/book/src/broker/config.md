# Configuration Values

`maelstrom-broker` supports the following [configuration values](../config.md):

Value                                                    | Type    | Description                                  | Default
---------------------------------------------------------|---------|----------------------------------------------|-----------------
<span style="white-space: nowrap;">`log-level`</span>    | string  | [minimum log level](#log-level)              | `"info"`
<span style="white-space: nowrap;">`cache-root`</span>   | string  | [cache directory](#cache-root)               | `$XDG_CACHE_HOME/maelstrom/worker/`
<span style="white-space: nowrap;">`cache-size`</span>   | string  | [target cache disk space usage](#cache-size) | `"1 GB"`
`port`                                                   | number  | [port for clients and workers](#port)        | `0`
<span style="white-space: nowrap;">`http-port`</span>    | string  | [port for web UI](#http-port)                | `0`

## `log-level`

See [here](../log-levels.md).

The broker always prints log messages to stderr.

## `cache-root`

The <span style="white-space: nowrap;">`cache-root`</span> configuration value
specifies where the cache data will go. It defaults to
`$XDG_CACHE_HOME/maelstrom/broker`, or `~/.cache/maelstrom/broker` if
`XDG_CACHE_HOME` isn't set. See the [XDG
spec](https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html)
for information.

## `cache-size`

The <span style="white-space: nowrap;">`cache-size`</span> configuration value
specifies a target size for the cache. Its default value is 1&nbsp;GB. When the
cache consumes more than this amount of space, the broker will remove unused
cache entries until the size is below this value.

It's important to note that this isn't a hard limit, and the broker will go
above this amount in two cases. First, the broker always needs all of the
currently-executing jobs' layers in cache. Second, the broker currently first
downloads an artifact from the client in its entirety, then adds it to the
cache, then removes old values if the cache has grown too large. In this
scenario, the combined size of the downloading artifact and the cache may
exceed <span style="white-space: nowrap;">`cache-size`</span>.

For these reasons, it's important to leave some wiggle room in the <span
style="white-space: nowrap;">`cache-size`</span> setting.

## `port`

The `port` configuration value specifies the port the broker will listen on for
connections from clients and workers. It must be an integer value in the range
0&ndash;65535. A value of 0 indicates that the operating system should choose
an unused port. The broker will always listen on all IP addresses of the host.

## `http-port`

the `http-port` configuration value specifies the port the broker will serve
the web UI on. A value of 0 indicates that the operating system should choose
an unused port. The broker will always listen on all IP addresses of the host.
