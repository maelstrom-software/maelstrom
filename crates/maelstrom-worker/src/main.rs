use anyhow::{Context as _, Result};
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

fn mimic_child_death(status: WaitStatus) -> ! {
    match status {
        WaitStatus::Exited(code) => {
            process::exit(code.as_u8().into());
        }
        WaitStatus::Signaled(signal) => {
            linux::raise(signal).unwrap_or_else(|e| panic!("error raising signal {signal}: {e}"));
            // Maybe, for some reason, we didn't actually terminate on the signal from above. In
            // that case, just abort.
            process::abort();
        }
    }
}

/// Create a grandchild process in its own pid and user namespaces.
///
/// We want to run the worker in its own pid namespace so that when it terminates, all descendant
/// processes also terminate. We don't want the worker to ever leak jobs, no matter how it
/// terminates.
///
/// We have to create a user namespace so that we can create the pid namespace.
///
/// We do two levels of cloning so that the returned process isn't pid 1 in its own pid namespace.
/// This is important because we don't want that process to inherit orphaned descendants. We want
/// to worker to be able to effectively use waitpid (or equivalently, wait on pidfds). If the
/// worker had to worry about reaping zombie descendants, then it would need to call the generic
/// wait functions, which could return a pid for one of the legitimate children that the process
/// was trying to waidpid on. This makes calling waitpid a no-go, and complicates the design of
/// the worker.
///
/// It's much easier to just have the worker not be pid 1 in its own namespace.
///
/// We take special care to ensure that all three processes will terminate if any single process
/// terminates. This is accomplished in the following ways:
///   - If the gen 0 process (the calling process) terminates, the gen 1 process will receive a
///     parent-death signal, which will immediately terminate it.
///   - If the gen 1 process terminates, all processes in the new pid namespace -- including the
///     gen 2 process -- will immediately
///     terminate, since the gen 1 process had pid 1 in their pid namespace
///   - After creating the gen 2 process, the gen 1 process loops calling wait(2) forever. It does
///     this to reap zombies, but it also allows it to detect when the gen 2 process terminates.
///     When this happens, the gen 1 process will terminate itself, trying to mimic the termination
///     mode of the gen 2 process.
///   - After cloning the gen 1 process, the gen 0 process just calls waitpid on the gen 1 process.
///     When this returns, it knows the gen 1 process has terminated. It then terminates itself,
///     trying to mimic the termination mode of the gen 1 process.
///
/// This function will return exactly once. It may be the gen 0, gen 1, or gen 2 process. It'll be
/// in the gen 2 process if and only if it returns `Ok(())`. It'll be in the gen 0 or gen 1 process
/// if and only if it return `Err(_)`.
///
/// WARNING: This function must only be called while the program is single-threaded.
fn clone_into_pid_and_user_namespace() -> Result<()> {
    let gen_0_uid = linux::getuid();
    let gen_0_gid = linux::getgid();

    // Create a pidfd for the gen 0 process. We'll use this in the gen 1 process to see if the gen
    // 0 process has terminated early.
    let gen_0_pidfd = linux::pidfd_open(linux::getpid())?;

    // Clone a new process into new user, pid, and mount namespaces.
    let mut clone_args = CloneArgs::default()
        .flags(CloneFlags::NEWUSER | CloneFlags::NEWPID)
        .exit_signal(Signal::CHLD);
    match linux::clone3(&mut clone_args).context("cloning the second-generation process")? {
        Some(gen_1_pid) => {
            // Gen 0 process.

            // The gen_0_pidfd is only used in the gen 1 process.
            drop(gen_0_pidfd);

            // Wait for the gen 1 process's termination and mimic how it terminated.
            mimic_child_death(linux::waitpid(gen_1_pid).unwrap_or_else(|e| {
                panic!("error waiting on second-generation process {gen_1_pid}: {e}")
            }));
        }
        None => {
            // Gen 1 process.

            // Set parent death signal.
            linux::prctl_set_pdeathsig(Signal::KILL)?;

            // Check if the gen 0 process has already terminated. We do this to deal with a race
            // condition. It's possible for the gen 0 process to terminate before we call prctl
            // above. If that happens, we won't receive a death signal until our new parent
            // terminates, which is not what we want, as that new parent is probably the system
            // init daemon.
            //
            // Unfortunately, we can't attempt to see if the gen 0 process is still alive using its
            // pid, as it's not in our pid namespace. We get around this by inheriting a pidfd from
            // the gen 0 process. The pidfd will become readable once the process has terminated.
            // So, we can just do a non-blocking poll on the fd to see fi the gen 0 process has
            // already terminated. If it hasn't, we can rely on the parent death signal mechanism.
            let mut pollfd = PollFd::new(gen_0_pidfd.as_fd(), PollEvents::IN);
            if linux::poll(slice::from_mut(&mut pollfd), Duration::ZERO)? == 1 {
                process::abort();
            }

            // We are done with the parent_pidfd now.
            drop(gen_0_pidfd);

            // Map uid and guid. If we don't do this here, then children processes will not be able
            // to map their own uids and gids.
            let fs = Fs::new();
            fs.write("/proc/self/setgroups", "deny\n")?;
            fs.write("/proc/self/uid_map", format!("0 {gen_0_uid} 1\n"))?;
            fs.write("/proc/self/gid_map", format!("0 {gen_0_gid} 1\n"))?;

            // Fork the gen 2 process.
            match linux::fork()? {
                Some(gen_2_pid) => {
                    loop {
                        // Gen 1 process.

                        // Now that we've created the gen 2 process, we need to start reaping
                        // zombies. If we ever notice that the gen 2 process terminated, then it's
                        // time to terminate ourselves.
                        match linux::wait() {
                            Err(err) => panic!("error waiting: {err}"),
                            Ok(result) if result.pid == gen_2_pid => {
                                mimic_child_death(result.status)
                            }
                            Ok(_) => {}
                        }
                    }
                }
                None => {
                    // Gen 2 process.
                    Ok(())
                }
            }
        }
    }
}

fn run_with_logger(config: Config, f: impl FnOnce(Config, Logger) -> Result<()>) -> Result<()> {
    let decorator = TermDecorator::new().build();
    let drain = FullFormat::new(decorator).build().fuse();
    let drain = Async::new(drain).build().fuse();
    let drain = LevelFilter::new(drain, config.log_level.as_slog_level()).fuse();
    let log = Logger::root(drain, o!());
    f(config, log)
}

fn main() -> Result<()> {
    let config = Config::new("maelstrom/worker", "MAELSTROM_WORKER")?;
    clone_into_pid_and_user_namespace()?;
    run_with_logger(config, |config, log| {
        Runtime::new()
            .context("starting tokio runtime")?
            .block_on(async move { maelstrom_worker::main(config, log).await })
    })
}
