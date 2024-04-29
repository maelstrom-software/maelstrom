# Local Worker

The `cargo-maelstrom` and `maelstrom-run` clients can run in "standalone mode".
In this mode, they don't submit jobs to a Maelstrom cluster, but instead run
the jobs locally, using a local worker.

Standalone mode is specified when no `broker` [configuration value](config.md)
is provided.

Currently, clients won't use the local worker if they are connected to a cluster.
We will change this in the future so that the local worker is utilized even
when the client is connected to a cluster.

Client with local workers share these configuration values:

Value                                                    | Type    | Description                                                   | Default
---------------------------------------------------------|---------|---------------------------------------------------------------|----------
<span style="white-space: nowrap;">`cache-size`</span>   | string  | [target cache disk space usage](#cache-size)                  | `"1 GB"`
<span style="white-space: nowrap;">`inline-limit`</span> | string  | [maximum amount of captured stdout and stderr](#inline-limit) | `"1 MB"`
`slots`                                                  | number  | [job slots available](#slots)                                 | 1 per CPU

## `cache-size`

The <span style="white-space: nowrap;">`cache-size`</span> configuration value
specifies a target size for the cache. Its default value is 1&nbsp;GB. When the
cache consumes more than this amount of space, the worker will remove unused
cache entries until the size is below this value.

It's important to note that this isn't a hard limit, and the worker will go
above this amount in two cases. First, the worker always needs all of the
currently-executing jobs' layers in cache. Second, the worker currently first
downloads an artifact in its entirety, then adds it to the cache, then removes
old values if the cache has grown too large. In this scenario, the combined
size of the downloading artiface and the cache may exceed <span
style="white-space: nowrap;">`cache-size`</span>.

For these reasons, it's important to leave some wiggle room in the <span
style="white-space: nowrap;">`cache-size`</span> setting.

## `inline-limit`

The <span style="white-space: nowrap;">`inline-limit`</span> configuration
value specifies how many bytes of stdout or stderr will be captured from jobs.
Its default value is 1&nbsp;MB. If stdout or stderr grows larger, the client
will be given <span style="white-space: nowrap;">`inline-limit`</span> bytes
and told that the rest of the data was truncated.

In the future we will add support for the worker storing all of stdout and
stderr if they exceed <span style="white-space: nowrap;">`inline-limit`</span>.
The client would then be able to download it "out of band".

## `slots`

The `slots` configuration value specifies how many jobs the worker will run
concurrently. Its default value is the number of CPU cores on the machine. In
the future, we will add support for jobs consuming more than one slot.
