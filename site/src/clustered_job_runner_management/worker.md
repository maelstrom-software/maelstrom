# Worker Configuration

The worker can be configured via CLI, configuration file, or environment
variables. CLI or environment variables override configuration file values.

Configuration default path is
`<working-directory/.config/maelstrom-worker.toml`. This can be overridden with
the `--config-file` CLI argument.

Here are the different options

- [`broker`](#the-broker-field) the address of the broker
- [`slots`](#the-slots-field) number of slots to allocate
- [`cache_root`](#the-cache_root-field) location of cache
- [`cache_bytes_used_target`](#the-cache_bytes_used_target-field) target amount
    of disk space used for cache
- [`inline_limit`](#the-inline_limit-field) maximum size of inline captured job
    output
- [`log_level`](#the-log_level-field) minimum log level to output

## The `broker` Field
- TOML: `broker = "1.2.3.4:9000"`
- CLI: `--broker 1.2.3.4:9000`
- ENV: `MAELSTROM_WORKER_BROKER=1.2.3.4:9000`

This is the network address of the broker which the worker will attempt to
establish a connection to.

## The `slots` Field
- TOML: `slots = 24`
- CLI: `--slots 24`
- ENV: `MAELSTROM_WORKER_SLOTS=24`

This is the number of slots to allocate for this worker. The slots are the
maximum number of concurrent jobs allowed. This is the effective job parallelism
for this worker.

## The `cache_root` Field
- TOML: `cache_root = "/home/maelstrom-worker/cache"`
- CLI: `--cache-root /home/maelstrom-worker/cache`
- ENV: `MAELSTROM_WORKER_CACHE_ROOT=/home/maelstrom-worker/cache`

This is the path on the local file-system where the worker will store its cache.

## The `cache_bytes_used_target` Field
- TOML: `cache_bytes_used_target = 1048576`
- CLI: `--cache-bytes-used-target 1048576`
- ENV: `MAELSTROM_WORKER_CACHE_BYTES_USED_TARGET=1048576`

This is the target number of bytes for the cache. This bound isn't followed
strictly, so it's best to be conservative.

## The `log_level` Field
- TOML: `log_level = "error"`
- CLI: `--log-level error`
- ENV: `MAELSTROM_BROKER_LOG_LEVEL=error`

This controls the [Log Level](./log_level.md) for the worker
