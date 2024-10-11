+++
title = "Spawning Processes on Linux"
authors = ["Remi Bernotavicius <remi@abort.cc>"]
date = 2024-10-10
weight = 998
draft = true
+++

Spawning child processes using your programming language's provided APIs can be very straightforward
in a modern language. For example in Rust [`std::process:Command`](https://doc.rust-lang.org/std/process/struct.Command.html) provides an
easy interface.

```rust
use anyhow::Result;

fn std_command_spawn_wait() -> Result<()> {
    let mut child = std::process::Command::new("/bin/echo")
        .stdout(std::process::Stdio::null())
        .spawn()?;

    let status = child.wait()?;
    assert!(status.success());

    Ok(())
}
```

These APIs make it really easy to get things right and are the first thing someone might reach for
(rightly so!). Sometimes though you maybe find yourself needing to do things that just aren't
supported by these simple APIs (we found ourselves in this position working on Maelstrom). For
example maybe you want to use a `pidfd`, or do something with namespaces. Then you might need to dig
deeper and discover what the underlying APIs are capable of.

Of course, once you dig deeper, you might quickly find yourself confused. On Linux there are several
APIs that all spawn a child process. We have `fork`, `vfork`, `posix_spawn` and `clone`. So which
one do you pick? How are they different?

This article is going to try to answer those questions. Also by the end I hope to provide a simple
flow chart for ease of use should you find yourself trying to decide.

In code examples I'm going to be writing Rust (what Maelstrom is written in) and using the
[`maelstrom_linux`](https://github.com/maelstrom-software/maelstrom/tree/main/crates/maelstrom-linux)
crate which is our own wrapper around `libc` and the kernel. I hope to provide generally applicable
information in this article, but I will be approaching it from a perspective of using Rust.

## The `fork` and `exec` model
The classic way of spawning a child process on Linux and Unix is to use two syscalls together,
`fork` and `exec`. `fork` creates a copy of the current process. `exec` loads and executes a new
program in the current process.

Splitting it up into two different syscalls allows the developer to do a bunch of
set-up in the child process just by writing regular code. (Having this be separated as two calls
which each do one thing, and the ability to write this set-up as plain code is sometimes referred to
as the "elegance" of `fork`)

```rust
use anyhow::Result;
use maelstrom_linux::{self as linux, ExitCode, WaitStatus};

fn fork_execve_wait(dev_null: &impl linux::AsFd) -> Result<()> {
    if let Some(child) = linux::fork()? {
        // This is executed by the parent
        let wait_result = linux::wait()?;
        assert_eq!(wait_result.pid, child);
        assert_eq!(wait_result.status, WaitStatus::Exited(ExitCode::from_u8(0)));
        Ok(())
    } else {
        // This is executed by the child
        // Do the housekeeping before we call execve
        linux::dup2(dev_null, &linux::Fd::STDOUT).unwrap();
        linux::execve(c"/bin/echo", &[None], &[None]).unwrap();
        unreachable!()
    }
}
```

I will refer to this set-up period in the child before calling `exec` as the "housekeeping". In this
housekeeping code you can imagine doing a number of different things, most of them are syscalls.
This can include configuring file descriptors, session, user, group, or namespaces for the child.
Also, since `exec` is a separate function, you can choose to not call it and instead just continue
executing the current program.

## `fork` in Multi-Threaded Programs
`fork` seems pretty simple until you start to consider multi-threaded programs. When you call `fork`
in a multi-threaded program only the calling thread is copied from the parent process. This creates
a strange programming environment in the child. In particular locks won't function properly
since a thread holding a lock can just disappear after the fork! The idea of avoiding certain things
after forking I will refer to as "fork safety."

Fork safety can sometimes be tricky to maintain. Simple things like allocating memory may not work
due to the aforementioned locking problem, and memory allocations are often everywhere. A real
gotcha here are signal handlers, they are copied from the parent so it means that can be called in
the child but they have a high chance of accessing some global memory requiring a lock.

One solution to this problem is to employ a programming pattern known as "zygote". In this
pattern we fork a child process (called the zygote) before we become multi-threaded and then tell
our zygote via IPC to spawn any further children we want.

```ascii-art
+---------+         +---------+          +---------+  +-------+
| parent  |         | zygote  |          | kernel  |  | child |
+---------+         +---------+          +---------+  +-------+
     |                   |                    |           |
     | IPC request       |                    |           |
     |------------------>|                    |           |
     |                   |                    |           |
     |                   | fork               |           |
     |                   |------------------->|           |
     |                   |                    |           |
     |                   |                    | spawn     |
     |                   |                    |---------->|
     |                   |                    |           |
     |                   |                    |      exec |
     |                   |                    |<----------|
     |                   |                    |           |
     |                   |                    |      exit |
     |                   |                    |<----------|
     |                   |                    |           |
     |                   |      wait response |           |
     |                   |<-------------------|           |
     |                   |                    |           |
     |      IPC response |                    |           |
     |<------------------|                    |           |
     |                   |                    |           |
```

The zygote remains single-threaded so we can write the housekeeping as before. Having to communicate
with the zygote is a little annoying, but makes things easy to get right at least from a fork-safety
perspective.

## `fork` is Too Slow!
It turns out that `fork` can be pretty slow in some contexts. A large part of the time is spent
copying the virtual memory mappings of the parent, and the time it takes scales linearly with the
amount of memory mapped. One way around this is the aforementioned "zygote" pattern. Since the
"zygote" process is spawned early, it shouldn't have much memory mapped. This doesn't completely
avoid the problem though, because copying the virtual memory of a small process is still slow.

<img src="fork graph.png" alt="Graph of Fork Calls" width="60%"/>

The graph is meant to show how the time of the fork call increases linearly with the amount of
memory mapped. It would be great to avoid this slow process of copying the virtual memory mappings
altogether which is slow even if your program is not using much memory (like the zygote is.)

## `vfork` to the Rescue
The `vfork` syscall is like fork except it doesn't copy the parent's virtual memory mappings and
instead shares the same memory space. The call is very similar to `fork` and seems like it could be
a drop-in replacement, but its own `man` page cautions against this saying its undefined behavior to
do anything other than a very restricted housekeeping.

The child process thread is sharing the exact same stack memory as the parent calling thread (writes
in the child appear in the parent). Having two different threads (or in this case processes) use the
same stack at the same time simultaneously, doesn't work. So the calling thread in the parent
process is suspended until the child calls `exec` or `_exit`.

```ascii-art
+---------+               +---------+         +-------+
| Parent  |               | Kernel  |         | Child |
+---------+               +---------+         +-------+
     |                         |                  |
     | vfork                   |                  |
     |------------------------>|                  |
     |                         |                  |
     |                         | Create child     |
     |                         |----------------->|
     |                         |                  | ---------------------\
     |                         |                  |-| Returns from vfork |
     |                         |                  | |--------------------|
     |                         |                  | --------------------\
     |                         |                  |-| Does housekeeping |
     |                         |                  | |-------------------|
     |                         |                  |
     |                         |             exec |
     |                         |<-----------------|
     |                         |                  |
     |      Returns from vfork |                  |
     |<------------------------|                  |
     |                         |                  |
```

The weird thing about this is that the same stack memory will experience the CPU returning from the
`vfork` call twice (see the two "Returns from vfork" in the diagram). This can really mess things up
in the calling function in a way that your compiler is not okay with. Apparently C compilers have a
(at best questionable IMO) way of dealing with it, but we can't call this function from Rust unless
we use the unstable [`ffi_return_twice`](https://github.com/rust-lang/rust/issues/58314) attribute.

The weirdness of the "double return" aside, the issue of "fork safety" gets a lot harder with
`vfork`. To be safe you really can't do too much in the housekeeping as its `man` page says, so we
loose a lot of the benefit of `fork`. If you try anyway the things that can break include the stuff
from regular "fork safety" but now you have to also worry about changing memory in the parent in
ways you didn't intend.

## `posix_spawn`
Another way to try to get the speed up we want would be to use `posix_spawn`. The latest version of
glibc always does the equivalent of calling `vfork` in its `posix_spawn` implementation.

`posix_spawn` is actually what `std::process::Command` tries to uses internally for most cases.

```rust
use anyhow::Result;
use maelstrom_linux::{self as linux, ExitCode, WaitStatus};

fn posix_spawn_wait(dev_null: &impl linux::AsFd) -> Result<()> {
    let mut actions = linux::PosixSpawnFileActions::new();
    actions.add_dup2(dev_null, &linux::Fd::STDOUT)?;
    let attrs = linux::PosixSpawnAttrs::new();
    let child = linux::posix_spawn(c"/bin/echo", &actions, &attrs, &[None], &[None])?;
    let status = linux::waitpid(child)?;
    assert_eq!(status, WaitStatus::Exited(ExitCode::from_u8(0)));
    Ok(())
}
```

Calling `posix_spawn` comes with a whole lot less caveats and things to be careful of when compared
to `fork` and `vfork`. It comes at the cost of loosing the "elegance" of fork. You configure the
"housekeeping" by using a struct. It is a kind of "housekeeping script" we create which executes
after the fork.

The downside of using `posix_spawn` is that we are locked to whatever its "housekeeping script"
provides to control our "housekeeping." (See the `posix_spawn` man page for a complete list of
things.)

## `clone` the API Underpinning it All
The aforementioned `fork`, `vfork`, and `posix_spawn` all actually call `clone` under the hood in
glibc. It has the functionality of the previous APIs and bunch of other features.

It can be a fair bit more difficult to use though. Unlike `vfork` it allows the parent to allocate a
separate stack for the child process, avoiding the "double return" issue.

```rust
use anyhow::Result;
use maelstrom_linux::{self as linux, ExitCode, WaitStatus};

struct ChildArgs {
    dev_null: linux::Fd,
}

/// This function executes in the child
extern "C" fn child_func(arg: *mut std::ffi::c_void) -> i32 {
    let arg: &ChildArgs = unsafe { &*(arg as *mut ChildArgs) };

    linux::dup2(&arg.dev_null, &linux::Fd::STDOUT).unwrap();
    linux::execve(c"/bin/echo", &[None], &[None]).unwrap();
    unreachable!()
}

fn clone_clone_vm_execve_wait(dev_null: &impl linux::AsFd) -> Result<()> {
    const CHILD_STACK_SIZE: usize = 1024; // 1 KiB of stack should be enough
    let mut stack = vec![0u8; CHILD_STACK_SIZE];

    // We need to pass the file-descriptor for /dev/null through to the child.
    let child_args = ChildArgs {
        dev_null: dev_null.fd(),
    };

    // Clone virtual memory, and give us SIGCHLD when it exits.
    let args = linux::CloneArgs::default()
        .flags(linux::CloneFlags::VM | linux::CloneFlags::VFORK)
        .exit_signal(linux::Signal::CHLD);

    // The function accepts a pointer to the end of the stack.
    let stack_ptr: *mut u8 = stack.as_mut_ptr();
    let child = unsafe {
        linux::clone(
            child_func,
            stack_ptr.wrapping_add(CHILD_STACK_SIZE) as *mut _,
            &child_args as *const _ as *mut _,
            &args,
        )
    }?;

    let wait_result = linux::wait()?;
    assert_eq!(wait_result.pid, child);
    assert_eq!(wait_result.status, WaitStatus::Exited(ExitCode::from_u8(0)));
    Ok(())
}
```

The `CLONE_VM` flag (via `linux::CloneFlags::VM`) avoids copying the virtual memory from the parent.

The `CLONE_VFORK` flag suspends the parent until the child calls `exec` or exits. Passing this flag
allows us to not worry about waiting for the right moment to free the child's stack memory in the
parent. If we don't pass this flag though, we are able to do other things in the parent in parallel,
but then we need someway to know when we can free the stack memory. One way is to share a pipe or
socket with the child which is `CLOEXEC`, once the child calls `exec` (or exits) so too should this
pipe or socket.

<img src="clone graph.png" alt="Graph of Fork Calls" width="60%"/>

This graph looks much better than the one for `fork`. It is a pretty constant speed and faster than
the fastest `fork`!. About the same performance is found with `posix_spawn`.

Finally, lets compare all the options (minus vfork because we can't call it) for a program without
much memory mapped.

```shell
ran std::process::Command::{spawn + wait} 10000 times in 5.400743724s (avg. 540.074µs / iteration)
ran fork + execve + wait 10000 times in 4.068149021s (avg. 406.814µs / iteration)
ran posix_spawn + wait 10000 times in 2.918496041s (avg. 291.849µs / iteration)
ran clone(CLONE_VM) + execve + wait 10000 times in 2.907074411s (avg. 290.707µs / iteration)
ran clone(CLONE_VM | CLONE_VFORK) + execve + wait 10000 times in 2.883697173s (avg. 288.369µs / iteration)
```

Rust's `std::process::Command` comes in at the slowest even though it should be comparable to
`posix_spawn`, this could be due to differences in the housekeeping or other things the code is
doing.

## Conclusion
Lets tie it all together with a flow graph about what to use.

```ascii-art
                                  start
                                    V
+--------------+    yes    +---------------------+
| posix_spawn  | <-------- |simple housekeeping? |
+--------------+           +---------------------+
                                    |no
                                    |
                                    V
                           +---------------------+
                           |performance critical?|
                           +---------------------+
                                    |no
                                    |
                                    V
           +------+    yes   +----------------+
           | fork | <--------|single threaded?|
           +------+          +----------------+
                                    |no
                                    |
                                    V
        +------+   yes   +-----------------------+
        |zygote|<--------|foolproof to implement?|
        +------+         +-----------------------+
                                    |no
                                    |
                                    V
                +------+   yes   +------+
                |vfork |<--------|POSIX?|
                +------+         +------+
                                    |no
                                    |
                                    V
                                 +------+
                                 |clone |
                                 +------+
```

## Addendum
You can check out working code for the snippets and benchmarks
[here](https://github.com/maelstrom-software/maelstrom/blob/main/crates/xtask/src/clone_benchmark.rs)
