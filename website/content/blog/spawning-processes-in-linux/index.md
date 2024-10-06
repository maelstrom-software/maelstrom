+++
title = "Spawning Processes in Linux"
date = 2024-10-02
weight = 1001
draft = true
+++

This is a guide on spawning processes in Linux. Being a job execution engine Maelstrom needs to
spawn job processes as quickly and efficiently as possible.

We are going to talk about the functions available in glibc for doing this sort of thing, but the
same functions are available in other libc implementations, but some specifics may be different.

In the code examples I'm going to be writing Rust and using the `maelstrom_linux` which is
our own wrapper around `libc` and the kernel.

## `fork` + `exec`
The classic way to spawn a child in Linux is to use the `fork` call. The fork call creates a copy of
the current process and returns the `pid` to the caller. The `exec` call loads another binary into
the program space.

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

Before we call `exec` in the child, we are able to do some "housekeeping". This allows us to
do things like call `dup2` to set up stdout. What some refer to as the "elegance" of fork is
tied directly to this ability to do the "housekeeping" by writing plain code.

This "housekeeping" code needs to be written with care. The rules that need to be followed can be
referred to as "fork-safety".

The copy of the calling process created by fork is single threaded. The calling thread is the only
thread copied. This can cause an unusual programming environment in the child. Taking locks and
allocating memory can cause things to break. Things like the TLS are not set-up and won't work
right.

Another thing to consider with "fork-safety" is the state of the signal handlers. If a child gets a
signal before calling `exec` it will execute an inherited signal handler from the parent,
which itself could end up doing something unexpected.

## `vfork`
It turns out a large amount of the time spent in fork is used dealing with copying the virtual
memory, and the more memory you have mapped, the longer it can take. If we were able to avoid that
we can speed things up. This is exactly what vfork does.

When using vfork the child process shares the same memory space as the parent. The child process is
using the same stack memory as the parent. Having two different threads (or in this case process) at
the same time, doesn't work. So the calling thread in parent process is suspended until the child
calls `exec` or `_exit`.

```ascii-art
calling thread                  child process
     vfork        --->
  <suspended>                <returns from vfork>
                             <does housekeeping>
                                  exec
  <returns from vfork>
```

The weird thing about this is that the same stack memory will experience the CPU returning from the
`vfork` call twice. This can really mess things up in the calling function in a way that your
compiler is not okay with. Apparently C compilers have a way of dealing with it, but we can't call
this function from Rust unless we use the unstable `ffi_return_twice` attribute.

## `posix_spawn`
One easier way to use `vfork` is to use `posix_spawn` instead. On the latest glibc
version it always calls `vfork` (well actually it calls `clone` but we'll get to that
later).

This function is what Rust's `std::process::Command` uses to spawn processes on Linux.

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

## `clone`
On Linux the underlying syscall that glibc uses to implement the aforementioned functions is
`clone` glibc provides a wrapper for `clone` so we can call it directly and through it
we can do everything we have seen up to this point and more.

There is a newer version of the `clone` syscall called `clone3` which tries to have a
more ergonomic API. glibc uses it internally in some places (like `pthread_create`) but
doesn't yet provide a wrapper to use it directly.

```rust
use anyhow::Result;
use maelstrom_linux::{self as linux, ExitCode, WaitStatus};

struct ChildArgs {
    dev_null: linux::Fd,
}

extern "C" fn child_func(arg: *mut std::ffi::c_void) -> i32 {
    let arg: &ChildArgs = unsafe { &*(arg as *mut ChildArgs) };

    linux::dup2(&arg.dev_null, &linux::Fd::STDOUT).unwrap();
    linux::execve(c"/bin/echo", &[None], &[None]).unwrap();
    unreachable!()
}

fn clone_clone_vm_execve_wait(dev_null: &impl linux::AsFd) -> Result<()> {
    const CHILD_STACK_SIZE: usize = 1024;
    let mut stack = vec![0u8; CHILD_STACK_SIZE];
    let child_args = ChildArgs {
        dev_null: dev_null.fd(),
    };
    let args = linux::CloneArgs::default()
        .flags(linux::CloneFlags::VM)
        .exit_signal(linux::Signal::CHLD);
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

Using `clone` we are able to avoid copying the virtual memory of the parent, but we are also
able to avoid suspending the parent.

Unlike `vfork` this function executes the child using the provided stack memory instead of
sharing the same memory as the parent. This avoids the "double return" issue from before.

## Making `clone` Usable

The "housekeeping" is written again in plan code, but the whole thing is definitely a bit unwieldy.
To make a usable API out of this technique for Maelstrom we came up with our own "housekeeping
script" like `posix_spawn` has.

The "housekeeping" can be thought of a set of syscalls, since making syscalls is basically the only
thing you are able to do in the "housekeeping". In Maelstrom we create a vector as our script which
we pass through to the child process to execute.

This is from [maelstrom-worker-child/src/lib.rs:69](https://github.com/maelstrom-software/maelstrom/blob/75341a7eaaf59b634120f40026a07530809bfe31/crates/maelstrom-worker-child/src/lib.rs#L69)

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

This is from [maelstrom-worker/src/executor.rs:362](https://github.com/maelstrom-software/maelstrom/blob/75341a7eaaf59b634120f40026a07530809bfe31/crates/maelstrom-worker/src/executor.rs#L362)

```rust
    ...
    let mut clone_args = CloneArgs::default()
        .flags(clone_flags)
        .exit_signal(Signal::CHLD);
    const CHILD_STACK_SIZE: usize = 1024;
    let stack = bump.alloc_slice_fill_default(CHILD_STACK_SIZE);
    let stack_ptr: *mut u8 = stack.as_mut_ptr();
    let (_, child_pidfd) = unsafe {
        linux::clone_with_child_pidfd(
            func,
            stack_ptr.wrapping_add(CHILD_STACK_SIZE) as *mut _,
            args as *mut _ as *mut core::ffi::c_void,
            &mut clone_args,
        )
    }?;
    ...
```

## Addendum: Waiting for Processes
One thing that tripped me up when writing the benchmarks was that I was using `wait` with `fork` and
`posix_spawn` but when I tried to use it with the `clone` call it was immediately failing with
`ECHILD` until I added `.exit_signal(linux::Signal::CHLD)` to the arguments.

In Maelstrom we use a pidfd instead of `wait` or `waitpid`. This is a much better way of waiting for
child process, but it was easier to use `wait` for these benchmarks.


