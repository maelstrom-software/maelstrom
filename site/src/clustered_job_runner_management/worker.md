# Worker Configuration

The worker can be configured via CLI, configuration file, or environment
variables. CLI or environment variables override configuration file values.

Configuration default path is
`<working-directory/.config/maelstrom-worker.toml`. This can be overridden with
the `--config-file` CLI argument.

Here are the different options

- [`broker`](#the-broker-value) the address of the broker
- [`slots`](#the-slots-value) number of slots to allocate
- [`cache_root`](#the-cache-root-value) location of cache
- [`cache_size`](#the-cache-size-value) target amount
    of disk space used for cache
- [`inline_limit`](#the-inline-limit-value) maximum size of inline captured job
    output
- [`log_level`](#the-log-level-value) minimum log level to output

## The `broker` Value
- TOML: `broker = "1.2.3.4:9000"`
- CLI: `--broker 1.2.3.4:9000`
- ENV: `MAELSTROM_WORKER_BROKER=1.2.3.4:9000`

This is the network address of the broker which the worker will attempt to
establish a connection to.

## The `slots` Value
- TOML: `slots = 24`
- CLI: `--slots 24`
- ENV: `MAELSTROM_WORKER_SLOTS=24`

This is the number of slots to allocate for this worker. The slots are the
maximum number of concurrent jobs allowed. This is the effective job parallelism
for this worker.

## The `cache-root` Value
- TOML: `cache-root = "/home/maelstrom-worker/cache"`
- CLI: `--cache-root /home/maelstrom-worker/cache`
- ENV: `MAELSTROM_WORKER_CACHE_ROOT=/home/maelstrom-worker/cache`

This is the path on the local file-system where the worker will store its cache.

## The `cache-size` Value
- TOML: `cache-size = "1GB"`
- CLI: `--cache-size 1GB`
- ENV: `MAELSTROM_WORKER_CACHE_SIZE=1GB`

This is the target number of bytes for the cache. This bound isn't followed
strictly, so it's best to be conservative.

## The `inline-limit` Value
- TOML: `inline-limit = "1MB"`
- CLI: `--inline-limit 1MB`
- ENV: `MAELSTROM_WORKER_INLINE_LIMIT=1MB`

This is the maximum number of bytes to be allowed when streaming back stdout and
stderr from a job.

## The `log-level` Value
- TOML: `log-level = "error"`
- CLI: `--log-level error`
- ENV: `MAELSTROM_BROKER_LOG_LEVEL=error`

This controls the [Log Level](./log_level.md) for the worker
