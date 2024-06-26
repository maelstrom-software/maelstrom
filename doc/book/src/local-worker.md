# Local Worker

Every client has a built-in worker that is called the "local worker". The local
worker is used to runs jobs two in two scenarios.

First, if no `broker` [configuration value](config.md) is specified, then the
client runs in **standalone mode**. In this mode, all jobs are handled by the
local worker.

Second, some jobs are considered **local-only**. These jobs must be run on the
local machine because they utilize some resource that is only avaiable locally.
These jobs are always run on the local worker, even if the client is connected
to a broker.

Currently, when a client is connected to a broker, it will only use the local
worker for local-only jobs. We will change this in the future so that the local
worker is utilized even when the client is connected to a cluster.

Clients have the following configuration values to configure their local
workers:

Value                                                    | Type    | Description                                                   | Default
---------------------------------------------------------|---------|---------------------------------------------------------------|----------
<span style="white-space: nowrap;">`cache-size`</span>   | string  | [target cache disk space usage](#cache-size)                  | `"1 GB"`
<span style="white-space: nowrap;">`inline-limit`</span> | string  | [maximum amount of captured stdout and stderr](#inline-limit) | `"1 MB"`
`slots`                                                  | number  | [job slots available](#slots)                                 | 1 per CPU

## <span style="white-space: nowrap;">`cache-size`</span>

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

## <span style="white-space: nowrap;">`inline-limit`</span>

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
