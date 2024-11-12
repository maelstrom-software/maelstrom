+++
title = "Implementing a Container Runtime Part 1: Spawning Processes on Linux"
authors = ["Remi Bernotavicius <remi@abort.cc>"]
date = 2024-11-12
weight = 998
draft = true
+++

Spawning child processes using your programming language's provided APIs can be very straightforward
in a modern language. For example Rust's
[`std::process:Command`](https://doc.rust-lang.org/std/process/struct.Command.html) provides an easy
interface:

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

These APIs make it really easy to get things right. They are the first thing you should reach for.
Sometimes though you maybe find yourself needing to do things that just aren't supported by these
simple APIs. We found ourselves in this position working on Maelstrom, since we run each test in its
own set of Linux namespaces. Maybe you too want to do something with namespaces, or maybe you want
to use a `pidfd`. If that's the case, then you might need to dig deeper and discover what the
underlying APIs are capable of.

Of course, once you dig deeper, you might quickly find yourself confused. On Linux there are several
APIs that all spawn a child process. There is `fork`, `vfork`, `posix_spawn` and `clone`. So which
one do you pick? How are they different?

This article tries to answer these questions. Also, at the end I provide a simple
flow chart that I hope makes it easy for you to decide which approach to take.

The code examples I provide will be in Rust (what Maelstrom is written in.) They will use the
[`maelstrom_linux`](https://github.com/maelstrom-software/maelstrom/tree/main/crates/maelstrom-linux)
crate which is our own wrapper around `libc` and the kernel. (This was my preference, but you may
want to use something else more supported) I hope to provide generally applicable
information in this article, but I will be approaching it from a perspective of using Rust.

## The `fork` and `exec` model
The classic way of spawning a child process on Linux and Unix is to use two syscalls together,
`fork` and `exec`. `fork` creates a copy of the current process. `exec` loads and executes a new
program in the current process.

Splitting it up into two different syscalls allows the developer to do a bunch of setup in the child
process just by writing regular code. (Having this be separated as two calls with each doing one
thing, and the ability to write this setup as plain code is sometimes referred to as the "elegance"
of `fork`)

The following code snippet runs `echo` with no arguments which acts as a kind of no-op for
benchmarking purposes.

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

I will refer to this setup period in the child before calling `exec` as the "housekeeping". In this
housekeeping code you can imagine doing a number of different things, most of them being syscalls.
This can include configuring file descriptors, sessions, users, groups, or namespaces for the child.
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
with the zygote is a little annoying, but makes things easy to get right at least from a fork safety
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
The `vfork` syscall is like `fork` except it doesn't copy the parent's virtual memory mappings and
instead shares the same memory space. The call is very similar to `fork` and seems like it could be
a drop-in replacement, but its own `man` page cautions against this (see below.)

The child process thread shares the exact same memory as the parent calling thread (writes in the
child appear in the parent) including the stack. Having two different threads (or in this case
processes) use the same stack at the same time simultaneously like this, doesn't work. So the
calling thread in the parent process is suspended until the child calls `exec` or `_exit`.

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

(We didn’t include a code snippet because we can’t call this function from Rust unless we use the
unstable [`ffi_return_twice`](https://github.com/rust-lang/rust/issues/58314) attribute)

`vfork` has two new sources of potential issues. The first is the fact that we are sharing the same
memory space. Care must be taken to not unintentionally modify memory in the parent in some way that
will cause issues. This drawback is tied directly to the performance improvement we want (avoiding
copying the virtual memory mappings.)

The second source of potential issues is the fact that the child ends up executing on the same stack
as the parent. This won’t work in general without some form of support from the compiler. This is
because we "return twice" from the `vfork` call (see the diagram above.) The gcc
[`returns_twice`](https://gcc.gnu.org/onlinedocs/gcc-4.7.2/gcc/Function-Attributes.html) attribute
does this, but it may not provide as much support as you might expect.

Here is a quote from tldp about this <https://tldp.org/HOWTO/Secure-Programs-HOWTO/avoid-vfork.html>

“...it's actually fairly tricky for a process to not interfere with its parent, especially in
high-level languages. The "not interfering" requirement applies to the actual machine code
generated, and many compilers generate hidden temporaries and other code structures that cause
unintended interference. The result: programs using vfork(2) can easily fail when the code changes
or even when compiler versions change.”

So what are you allowed to do in the housekeeping exactly? Let’s check what the man page says about
this.

“..the behavior is undefined if the process created by vfork() .. calls any other function before
successfully calling `_exit(2)` or one of the `exec(3)` family of functions.“

This isn’t very unhelpful either. Clearly calling `_exit` or `exec` (but not every form of `exec` it
turns out) is okay, but what other housekeeping is okay in practice? It’s not entirely clear, and
researching across the internet leads to many others showing a fair amount of anxiety about this
problem

On Linux, the behavior of vfork can be recreated using `clone`, (which we will cover later) in a way
where we don’t have to share a stack, I believe this is always preferable.

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
housekeeping by using a struct. It is a kind of "housekeeping script" we create which executes
after the fork.

The downside of using `posix_spawn` is that out housekeeping is limited to doing whatever things
the housekeeping script has support for. (See the `posix_spawn` man page for a complete list of
things.)

## `clone` the API Underpinning it All
The aforementioned `fork`, and `posix_spawn` actually call `clone` under the hood in
glibc. Also `vfork` inside the kernel ends up calling into the kernel's `clone` code.
It has the functionality of the previous APIs and bunch of other features.

It can be a fair bit more difficult to use though. Although, unlike `vfork` it allows the parent to
allocate a separate stack for the child process, avoiding some of the issues with `vfork`.

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
socket with the child which is `CLOEXEC`, once the child calls `exec` (or exits) this pipe or socket
will close.

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
`posix_spawn`, this could be due to differences in the housekeeping or other things the `std` code
is doing.

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

Be sure to check back for the next part of this article series where we dive into the code in
Maelstrom that calls `clone`.
