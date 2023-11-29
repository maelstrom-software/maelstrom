# Meticulous

Meticulous is an extremely fast Rust test runner built on top of a
general-purpose clustered job runner. Test runners for other languages will
follow soon.

The clustered job runner can also be directly accessed through a command-line
client, allowing one to run arbitrary jobs on the cluster.

Meticulous runs every job in its own lightweight container, making it easy to
package jobs and get repeatable results. For tests, it also ensures isolation,
eliminating confusing test errors caused by inter-test dependencies or by
hidden test-environment depdendencies.

## Architecture

There are three types of processes in every Meticulous installation: the broker, workers,
and clients.

### Broker

There is exactly one broker in every Meticulous cluster. It's the centralized
dispatch point to which all clients and workers connect. It receives jobs from
clients and schedules them to workers, and receives results from workers and
routes them back to clients.

### Workers

Workers actually run the jobs using a custom-built, lightweight container
runner. Every cluster must have a least one worker, but more workers are
obviously better.

### Clients

Clients schedule jobs through the broker. They never directly communicate
with workers. Clients can be of many different types. We are releasing with a
Rust test runner and a general-purpose command-line client. However, more
special-case test runners are on their way, and you could always build your own
client (it's easy!).
