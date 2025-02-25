# Job States

Jobs transition through a number of states in their journey. This chapter
explains those states.

## Waiting for Artifacts

If a broker doesn't have all the required artifacts (i.e. container layers) for
a job when it is submitted, the job enters the `Awaiting-Artifacts` state. The
broker will notify the client of the missing artifacts, and wait for the client
to transfer them. Once all artifacts have been received from the client, the
job will proceed to the next state.

All jobs initially enter this state, though some then immediately transition to
[`Pending`](#pending) if the broker has all of the required artifacts. [Local
jobs](local-worker.md) also immediately transition out of this state, since the
worker is co-located with the client and has immediate access to all of the
artifacts.

## Pending

In the `Pending` state, the broker has the job and all of its artifacts, but
hasn't yet found a free worker to execute the job. Jobs in this state are
stored in a queue. Once a job reaches the front of the queue, and a worker
becomes free, the job will be sent to the worker for execution.

Local jobs aren't technically sent to the broker. However, they still do enter
a queue waiting to be submitted to the local worker, which is similar to the
situation for remote jobs. For that reason, we lump local and remote jobs
together in this state.

## Running

A `Running` job has been set to the worker for execution. The worker could be
executing the job, or it could be transferring some artifacts from the broker.
In the future, we will likely split this state apart into the various different
sub-states. If a worker disconnects from the broker, the broker moves all jobs
that were assigned to that worker back to the [`Pending`] state.

## Completed

Jobs in this state have been executed to completion by a worker.
