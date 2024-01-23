# Maelstrom Worker Child

This is a small library that is only intended to be used by the
[maelstrom-worker](../maelstrom-worker). It is its own library so it can
be `no_std` and we can ensure that it doesn't take any undesired dependencies.

This library is instended to be used in the child processes after a `clone`
syscall in potentially-multithreaded processes. The child process in this case
is very limited in what it can do without potentially blocking. For example,
even allocations are disallowed. This library just executes a series of
syscalls, the last of which should be `execve`.
