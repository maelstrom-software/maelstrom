use anyhow::{Context as _, Result};
use clap::command;
use maelstrom_linux::{
    self as linux, CloneArgs, CloneFlags, PollEvents, PollFd, Signal, WaitStatus,
};
use maelstrom_util::fs::Fs;
use maelstrom_worker::config::Config;
use slog::{o, Drain, LevelFilter, Logger};
use slog_async::Async;
use slog_term::{FullFormat, TermDecorator};
use std::{process, slice, time::Duration};
use tokio::runtime::Runtime;

/// Clone a child process and continue executing in the child. The child process will be in a new
/// pid namespace, meaning when it terminates all of its descendant processes will also terminate.
/// The child process will also be in a new user namespace, and have uid 0, gid 0 in that
/// namespace. The user namespace is required in order to create the pid namespace.
///
/// WARNING: This function must only be called while the program is single-threaded.
fn clone_into_pid_and_user_namespace() -> Result<()> {
    let parent_uid = linux::getuid();
    let parent_gid = linux::getgid();

    // Create a parent pidfd. We'll use this in the child to see if the parent has terminated
    // early.
    let parent_pidfd = linux::pidfd_open(linux::getpid())?;

    // Clone a new process into new user and pid namespaces.
    let mut clone_args = CloneArgs::default()
        .flags(CloneFlags::NEWUSER | CloneFlags::NEWPID)
        .exit_signal(Signal::CHLD);
    match linux::clone3(&mut clone_args)? {
        None => {
            // Child.

            // Set parent death signal.
            linux::prctl_set_pdeathsig(Signal::KILL)?;

            // Check if the parent has already terminated.
            let mut pollfd = PollFd::new(parent_pidfd, PollEvents::IN);
            if linux::poll(slice::from_mut(&mut pollfd), Duration::ZERO)? == 1 {
                process::abort();
            }

            // We are done with the parent_pidfd now.
            linux::close(parent_pidfd)?;

            // Map uid and guid.
            let fs = Fs::new();
            fs.write("/proc/self/setgroups", "deny\n")?;
            fs.write("/proc/self/uid_map", format!("0 {parent_uid} 1\n"))?;
            fs.write("/proc/self/gid_map", format!("0 {parent_gid} 1\n"))?;

            Ok(())
        }
        Some(child_pid) => {
            // Parent.

            // The parent_pidfd is only used in the child.
            linux::close(parent_pidfd)
                .unwrap_or_else(|err| panic!("unexpected error closing pidfd: {}", err));

            // Wait for the child and mimick how it terminated.
            match linux::waitpid(child_pid).unwrap_or_else(|e| {
                panic!("unexpected error waiting on child process {child_pid}: {e}")
            }) {
                WaitStatus::Exited(code) => {
                    process::exit(code.as_u8().into());
                }
                WaitStatus::Signaled(signal) => {
                    linux::raise(signal).unwrap_or_else(|e| {
                        panic!("unexpected error raising signal {signal}: {e}")
                    });
                    process::abort();
                }
            }
        }
    }
}

fn main() -> Result<()> {
    let config =
        maelstrom_config::new_config::<Config>(command!(), "maelstrom/worker", "MAELSTROM_WORKER")?;
    clone_into_pid_and_user_namespace()?;
    let decorator = TermDecorator::new().build();
    let drain = FullFormat::new(decorator).build().fuse();
    let drain = Async::new(drain).build().fuse();
    let drain = LevelFilter::new(drain, config.log_level.as_slog_level()).fuse();
    let log = Logger::root(drain, o!());
    Runtime::new()
        .context("starting tokio runtime")?
        .block_on(async move { maelstrom_worker::main(config, log).await })?;
    Ok(())
}
