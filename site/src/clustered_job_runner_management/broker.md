# Broker Configuration

The broker can be configured via CLI, configuration file, or environment
variables. CLI or environment variables override configuration file values.

By default, the configuration files are read from the directories specified by
the XDG base directory specification. Files by the name of
`maelstrom/broker/config.toml` will be read from the XDG config dirs. Multiple
configuration files are supported. Values set it earlier files override values
set in later files.

Here are the different options:

- [`port`](#the-port-value) main port to listen on
- [`http-port`](#the-http-port-value) web UI port to listen on
- [`cache-root`](#the-cache-root-value) location of cache
- [`cache-size`](#the-cache-size-value) target amount of disk space used for cache
- [`log-level`](#the-log-level-value) minimum log level to output

## The `port` Value
- TOML: `port = 9000`
- CLI: `--port 9000`
- ENV: `MAELSTROM_BROKER_PORT=9000`

This is the port the broker listens on for client and worker connections.

## The `http-port` Value
- TOML: `http-port = 9001`
- CLI: `--http-port 9001`
- ENV: `MAELSTROM_BROKER_HTTP_PORT=9001`

This is the port the broker listens on for HTTP connections in order to serve
the web UI.

## The `cache-root` Value
- TOML: `cache-root = "/home/maelstrom-broker/cache"`
- CLI: `--cache-root /home/maelstrom-broker/cache`
- ENV: `MAELSTROM_BROKER_CACHE_ROOT=/home/maelstrom-broker/cache`

This is the path on the local file-system where the broker will store its cache.

## The `cache-size` Value
- TOML: `cache_size = "1GB"`
- CLI: `--cache-size 1GB`
- ENV: `MAELSTROM_BROKER_CACHE_BYTES_USED_TARGET=1GB`

This is the target number of bytes for the cache. This bound isn't followed
strictly, so it's best to be conservative.

## The `log-level` Value
- TOML: `log-level = "error"`
- CLI: `--log-level error`
- ENV: `MAELSTROM_BROKER_LOG_LEVEL=error`

This controls the [Log Level](./log_level.md) for the broker.
