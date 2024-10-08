+++
title = "Spawning Processes on Linux"
authors = ["Remi Bernotavicius <remi@abort.cc>"]
date = 2024-10-07
weight = 998
draft = true
+++

This is an overview of spawning processes on Linux. This summarizes things we found while writing
[Maelstrom](https://github.com/maelstrom-software/maelstrom). Being a job execution engine Maelstrom
needs to spawn job processes as quickly and efficiently as possible, so we needed to understand the
Linux APIs as best as we could. The process spawning API on Linux is also very tied together with
its container API. We hope to cover more of the containerization part of Maelstrom in a future blog
post.

<!-- more -->

For purposes of this post we are going to talk about the functions available in glibc for spawning
processes. The same functions are available in other libc implementations, although some specifics
may be different.

In the code examples I'm going to be writing Rust (what Maelstrom is written in) and using
the [`maelstrom_linux`](https://github.com/maelstrom-software/maelstrom/tree/main/crates/maelstrom-linux)
crate which is our own wrapper around `libc` and the kernel.

The code snippets are adapted from code found here:
[xtask/src/clone_benchmark.rs](https://github.com/maelstrom-software/maelstrom/blob/75341a7eaaf59b634120f40026a07530809bfe31/crates/xtask/src/clone_benchmark.rs)

## `fork` + `exec`
The classic way to spawn a child process on Linux is to use the `fork` call. This call creates a
copy of the current process and returns a `pid_t` (process identifier) representing the child to the
caller. The `exec` call loads another binary into the program space and restarts execution.

The following code snippet executes `/bin/echo` with no input which just prints a newline. It also
redirects `stdout` to `/dev/null`. (We are purposefully not doing much for the sake of a benchmark)

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
        // Do the "housekeeping" before we call execve
        linux::dup2(dev_null, &linux::Fd::STDOUT).unwrap();
        linux::execve(c"/bin/echo", &[None], &[None]).unwrap();
        unreachable!()
    }
}
```

Before we call `exec` in the child, we do some what I will call "housekeeping". This is where we
redirect `stdout` to `/dev/null` by calling `dup2`. In this snippet we aren't doing much, but there
are many different things you could do at this point. What some refer to as the "elegance" of fork
is tied directly to this ability to do the "housekeeping" by writing plain code as we have done in
this code snippet.

The "housekeeping" code needs to be written with care. The rules that need to be followed can be
referred to as "fork-safety".

### Fork Safety

The copy of the calling process created by fork is single threaded. The calling thread is the only
thread copied. Also now the thread that the current stack was started with has changed. All this can
create an unusual programming environment in the child where many things don't work right and must
be avoided.

All of the following (and more) won't work correctly.
- Taking locks
- Allocating memory (on the heap)
- Using thread-local storage
- Calling signal handlers

For the code in Maelstrom that runs during this "housekeeping" we chose to make it
[`no_std`](https://docs.rust-embedded.org/book/intro/no-std.html) mainly
to help avoid calling into code that will break due to the previously stated restrictions.

## `vfork`
It turns out a large amount of the time spent in fork is used dealing with copying the virtual
memory, and the more memory you have mapped, the longer it can take. If we were able to avoid that
we can speed things up. This is exactly what vfork does.

When using `vfork` the child process shares the same memory space as the parent. The child process
thread is sharing the exact same stack memory as the parent calling thread (writes in the child
appear in the parent). Having two different threads (or in this case processes) use the same stack
at the same time simultaneously, doesn't work. So the calling thread in the parent process is
suspended until the child calls `exec` or `_exit`.

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
`vfork` call twice. This can really mess things up in the calling function in a way that your
compiler is not okay with. Apparently C compilers have a (at best questionable IMO) way of dealing
with it, but we can't call this function from Rust unless we use the unstable
[`ffi_return_twice`](https://github.com/rust-lang/rust/issues/58314) attribute.

## `posix_spawn`
One easier way to use `vfork` is to use `posix_spawn` instead. On the latest glibc
version it always calls `vfork` (well actually it calls `clone` but we'll get to that
later).

`posix_spawn` is actually what Rust's
[`std::process::Command`](https://doc.rust-lang.org/std/process/struct.Command.html) uses to spawn
processes on Linux.

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

This function call looses the "elegance" of fork. Instead you configure the "housekeeping" by using
a struct. It is a kind of "housekeeping script" we create which executes after the fork.

Now that we can call it from Rust, lets run a simple benchmark.

```sh
ran fork + execve + wait 10000 times in 4.087035997s (avg. 408.703µs / iteration)
ran posix_spawn + wait 10000 times in 2.977589781s (avg. 297.758µs / iteration)
```

Even for this program using very little memory, we can see a modest speed up.

The downside of using `posix_spawn` is that we are locked to whatever its "housekeeping script"
provides to control our "housekeeping." (See the `posix_spawn` man page for a complete list of
things.) This is probably fine for a lot of users, but we want to do some unusual things in our
"housekeeping". Secondly, suspending the calling thread is maybe just a little bit slower.

## `clone`
On Linux the underlying syscall that glibc uses to implement the aforementioned `fork` and `vfork`
is `clone`. This syscall has a super-set of the functionality seen by `fork` and `vfork`. glibc provides a very helpful wrapper for `clone` that we can use to call it.

Note: There is a newer version of the `clone` syscall called `clone3` which tries to have a
more ergonomic API. glibc uses it internally in some places (like `pthread_create`) but
doesn't yet provide a wrapper to use it directly.

Calling this function can be a bit more involved as you will see from the following code snippet.

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
        .flags(linux::CloneFlags::VM)
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

Using `clone` we are able to avoid copying the virtual memory of the parent by providing the
`CLONE_VM` flag (via `CloneFlags::VM`) just like `vfork`, but we are also able to avoid suspending
the parent (that is controlled with the `CLONE_VFORK` flag).

Unlike `vfork` this function executes the child using the provided stack memory instead of
sharing the same stack as the parent calling thread.

When used this way, the "double return" problem has been eliminated. The child process thread uses
the provided stack memory instead of sharing the stack memory from the parent calling thread.

Note: Even without the double return, it would be difficult to call this syscall directly from C or
Rust (glibc uses assembly), so the wrapper is very useful.  For information about how glibc does it
(for x86_64 anyway),
see this file:
[sysdeps/unix/sysv/linux/x86_64/clone.S:52](https://github.com/bminor/glibc/blob/master/sysdeps/unix/sysv/linux/x86_64/clone.S#L52)

Lets see how this approach compares to the other two in the benchmark

```sh
ran fork + execve + wait 10000 times in 4.135612842s (avg. 413.561µs / iteration)
ran posix_spawn + wait 10000 times in 2.932509958s (avg. 293.25µs / iteration)
ran clone(CLONE_VM) + execve + wait 10000 times in 2.906477871s (avg. 290.647µs / iteration)
```

It performs just a little better than calling `posix_spawn`, we don't have to suspend the calling
thread, and now we are able to control the "housekeeping" to be whatever we want.

## Making `clone` Usable

Using `clone` the "housekeeping" is written again in plan code, but the whole thing is definitely a
bit unwieldy. To make a usable API out of this technique for Maelstrom we came up with our own
"housekeeping script" like `posix_spawn` has.

The "housekeeping" can be thought of a set of syscalls, since making syscalls is basically the main
thing you would want to do in the "housekeeping". In Maelstrom we create a vector as our script which
we pass through to the child process to execute.

This is from [maelstrom-worker-child/src/lib.rs:69](https://github.com/maelstrom-software/maelstrom/blob/75341a7eaaf59b634120f40026a07530809bfe31/crates/maelstrom-worker-child/src/lib.rs#L69)

(a bunch of variants have been removed for brevity)

```rust
pub enum Syscall<'a> {
    Bind {
        fd: FdSlot<'a>,
        addr: &'a Sockaddr,
    },
    Chdir {
        path: &'a CStr,
    },
    CloseRange {
        first: CloseRangeFirst,
        last: CloseRangeLast,
        flags: CloseRangeFlags,
    },
    Dup2 {
        from: Fd,
        to: Fd,
    },
    ...
}

...

impl<'a> Syscall<'a> {
    fn call(&mut self, write_sock: &linux::UnixStream) -> result::Result<(), Errno> {
        match self {
            Syscall::Bind { fd, addr } => linux::bind(fd, addr),
            Syscall::Chdir { path } => linux::chdir(path),
            Syscall::CloseRange { first, last, flags } => linux::close_range(*first, *last, *flags),
            Syscall::Dup2 { from, to } => linux::dup2(&*from, &*to).map(drop),
            ...
        }
    }
}

...

pub struct ChildArgs<'a, 'b> {
    pub write_sock: linux::Fd,
    pub syscalls: &'a mut [Syscall<'b>],
}
```

In the child we do the "housekeeping" by iterating the `syscalls` and calling `.call(..)` on each of
them.

Note: The `write_sock` in this code is a socket we opened before forking to communicate with the
parent process.

## Summarizing

To summarize, `posix_spawn` is probably the glibc / Linux API to use for spawning a
child process unless you are doing something special. It saves time by not copying the whole
parent's virtual memory space while still providing a fairly easy to use API.

If you want to have a much more control of the "housekeeping", then you should use `clone`, but you
will have a lot more work to do. `clone` is not very portable though (not part of POSIX.)

`fork` will work just fine, but its kind of slow. `vfork` should be avoided since dealing with the
"double return" problem is a pain maybe even requiring assembly. (although if you want something
more portable, you might be forced into using `vfork`, but I recommend an assembly-written wrapper
like glibc has for `clone`)

## Addendum: Waiting for Processes
One thing that tripped me up when writing the benchmarks was that I was using `wait` with `fork` and
`posix_spawn` but when I tried to use it with the `clone` call it was immediately failing with
`ECHILD` until I added `.exit_signal(linux::Signal::CHLD)` to the arguments.

In Maelstrom we use a pidfd instead of `wait` or `waitpid`. This is a much better way of waiting for
child process that doesn't rely on the signal, but it was easier to use `wait` for these benchmarks.
You are able to do this with `clone` by passing the `CLONE_PIDFD` flag.
