# Web UI

The broker has a web UI that is available by connecting via the [configured HTTP
port](config.md#http-port), if the binary has been compiled with the `web-ui`
feature.

The following is an explanation of the various elements on the web UI.

## Connected Machines
The web UI contains information about the number of client and the number of
worker connected to the broker. The web UI itself is counted as a client.

## Slots
Each running job consumes one slot. The more workers connected, the
more slots are available. The broker shows the total number of available slots
as well as the number currently used.

## Job Statistics
The web UI contains information about current and past jobs. This includes the
current number of jobs and graphs containing historical information about jobs
and their states. There is a graph per connected client as well as an aggregate
graph at the top. The graphs are all stacked line-charts. See [Job
States](../job-states.md) for information about what the various states mean.
