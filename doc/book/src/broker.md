# maelstrom-broker

The `maelstrom-broker` is the coordinator and scheduler for a Mealstrom
cluster. In order to have a cluster, there must be a broker. The broker must be
started before clients and workers, as the clients and workers connect to the
broker, and will exit if they can't establish a connection.

The broker doesn't consume much CPU, so it can be run on any machine, including
a worker machine. Ideally, whatever machine it runs on should have good
throughput with the clients and workers, as all artifacts are first transferred
from the clients to the broker, and then from the broker to workers.

Clients can be run in [standalone mode](../local-worker.md) where they
don't need access to a cluster. In that case, there is no need to run a broker.

## Cache

The broker maintains a cache of artifacts that are used as file system layers
for jobs. If a client submits a job, and there are required artifacts for the
job that the broker doesn't have in its cache, it will ask the client to
transfer them. Later, when the broker submits the job to a worker, the worker
may turn around and request missing artifacts from the broker.

A lot of artifacts are reused between jobs, and also between client
invocations. So, the larger the broker's cache, the better.
Ideally, it should be at least a few multiples of the working set size.

## Command-Line Options

`maelstrom-broker` supports the [standard command-line
options](../standard-cli.md), as well as a number of [configuration
values](broker/config.md), which are covered in the next chapter.
