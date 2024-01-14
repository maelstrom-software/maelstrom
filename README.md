# Maelstrom

Maelstrom is an extremely fast Rust test runner built on top of a
general-purpose clustered job runner. Test runners for other languages will
follow soon.

The clustered job runner can also be directly accessed through a command-line
client, allowing one to run arbitrary jobs on the cluster.

Maelstrom runs every job in its own lightweight container, making it easy to
package jobs and get repeatable results. For tests, it also ensures isolation,
eliminating confusing test errors caused by inter-test dependencies or by
hidden test-environment dependencies.

See the book for more information about how it works and how to install it
[Maelstrom Book](https://maelstrom-software.github.io/maelstrom/)
