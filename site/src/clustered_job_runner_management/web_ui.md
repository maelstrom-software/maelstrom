# Web UI

The broker has a web UI that is available by connecting via the configured HTTP
port. See [Broker Configuration > http_port](./broker.md#the-http_port-field).

The following is an explanation of the various elements on the web UI.

## Connected Machines
The web UI contains information about the number of client and the number of
worker connected to the broker. Keep in mind, the web UI itself is counted as a
client.

## Used Slots
A slot is the ability to run a job on a worker. The more workers connected, the
more number of slots. See
[Worker Configuration > Slots](./worker.md#the-slots-field).

## Job Statistics
The web UI also contains information about current and past jobs. This includes
the current number of jobs, and graphs containing historical information about
jobs and their states. There is a graph per connected client, and there is an
aggregate graph at the top. The graphs are all stacked line-charts. See [Job
States](./job_states.md) for information about what the various states mean.
