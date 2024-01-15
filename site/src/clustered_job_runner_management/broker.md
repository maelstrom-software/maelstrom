# Broker Configuration

The broker can be configured via CLI, configuration file, or environment
variables. CLI or environment variables override configuration file values.

Configuration default path is
`<working-directory/.config/maelstrom-broker.toml`. This can be overridden with
the `--config-file` CLI argument.

Here are the different options

- [`port`](#the-port-field) main port to listen on
- [`http_port`](#the-http_port-field) web UI port to listen on
- [`cache_root`](#the-cache_root-field) location of cache
- [`cache_bytes_used_target`](#the-cache_bytes_used_target-field) target amount
    of disk space used for cache
- [`log_level`](#the-log_level-field) minimum log level to output

## The `port` Field
- TOML: `port = 9000`
- CLI: `--port 9000`
- ENV: `MAELSTROM_BROKER_PORT=9000`

This is the port the broker listens on for client and worker connections.

## The `http_port` Field
- TOML: `http_port = 9001`
- CLI: `--http-port 9001`
- ENV: `MAELSTROM_BROKER_HTTP_PORT=9001`

This is the port the broker listens on for HTTP connections in order to serve
the web UI.

## The `cache_root` Field
- TOML: `cache_root = "/home/maelstrom-broker/cache"`
- CLI: `--cache-root /home/maelstrom-broker/cache`
- ENV: `MAELSTROM_BROKER_CACHE_ROOT=/home/maelstrom-broker/cache`

This is the path on the local file-system where the broker will store its cache.

## The `cache_bytes_used_target` Field
- TOML: `cache_bytes_used_target = 1048576`
- CLI: `--cache-bytes-used-target 1048576`
- ENV: `MAELSTROM_BROKER_CACHE_BYTES_USED_TARGET=1048576`

This is the target number of bytes for the cache. This bound isn't followed
strictly, so it's best to be conservative.

## The `log_level` Field
- TOML: `log_level = "error"`
- CLI: `--log-level error`
- ENV: `MAELSTROM_BROKER_LOG_LEVEL=error`

This controls the [Log Level](./log_level.md) for the broker
