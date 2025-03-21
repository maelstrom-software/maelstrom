# `maelstrom-worker`

The `maelstrom-worker` is used to execute jobs in a Maelstrom cluster. In order
to do any work, a cluster must have at least one worker.

The system is designed to require only one worker per node in the cluster. The
worker will then run as many jobs in parallel as it has "slots". By default, it
will have one slot per CPU, but it can be configured otherwise.

Clients can be run in [standalone mode](local-worker.md) where they
don't need access to a cluster. In that case, they will have a internal, local
copy of the worker.

All jobs are run inside of containers. In addition to providing isolation to
the jobs, this provides some amount of security for the worker.

## Cache

Each job requires a file system for its containers. The worker provides these
file systems via [FUSE](https://en.wikipedia.org/wiki/Filesystem_in_Userspace).
It keeps the artifacts necessary to implement these file systems in its cache
directory. Artifacts are reused if possible.

The worker will strive to keep the size of the cache under the configurable
limit. It's important to size the cache properly. Ideally, it should be a small
multiple larger than the largest working set.

## Command-Line Options

`maelstrom-worker` supports the [common command-line
options](common-cli.md), as well as a number of [configuration
values](worker/config.md), which are covered in the next chapter.
