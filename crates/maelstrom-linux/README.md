# Maelstrom Linux Syscalls

This is a small library that is only intended to be used by the
[maelstrom-worker](../maelstrom-worker). It is its own library so it can
be `no_std` and we can ensure that it doesn't take any undesired dependencies.

This library provides simple, non-allocating wrappers around the various Linux
syscalls that we need to use. In some cases, the underlying syscalls aren't
available on all architectures, so we emulate them as libc would.

This library is intended to be usable by
[maelsrom-worker-child](../maelstrom-worker-child), which is why it's `no_std`
and doesn't allocate.

For more information, see the [Maelstrom project on
GitHub](https://github.com/maelstrom-software/maelstrom).
