use crate::{fs::Fs, signal};
use anyhow::{Context as _, Result};
use futures::StreamExt as _;
use maelstrom_linux::{
    self as linux, CloneArgs, CloneFlags, PollEvents, PollFd, Signal, WaitStatus,
};
use std::{
    num::NonZeroU8,
    pin::pin,
    process::{self, Termination},
    slice,
    sync::Mutex,
    time::Duration,
};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum ExitCodeInner {
    Success,
    Failure,
    U8(NonZeroU8),
}

/// It's very hard to test with [`std::process::ExitCode`] because it intentionally doesn't
/// implement [`Eq`], [`PartialEq`], etc. This struct fills the same role and can be converted to
/// [`std::process::ExitCode`] if necessary, but since it implements [`Termination`], one shouldn't
/// need to.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ExitCode(ExitCodeInner);

impl ExitCode {
    pub const SUCCESS: Self = ExitCode(ExitCodeInner::Success);
    pub const FAILURE: Self = ExitCode(ExitCodeInner::Failure);
}

impl From<u8> for ExitCode {
    fn from(val: u8) -> Self {
        if val == 0 {
            Self::SUCCESS
        } else {
            ExitCode(ExitCodeInner::U8(unsafe { NonZeroU8::new_unchecked(val) }))
        }
    }
}

impl Termination for ExitCode {
    fn report(self) -> process::ExitCode {
        self.into()
    }
}

// NB: This can't really be tested because we can't do much with process::ExitCode. So don't mess
// it up!
impl From<ExitCode> for process::ExitCode {
    fn from(val: ExitCode) -> Self {
        match val.0 {
            ExitCodeInner::Success => process::ExitCode::SUCCESS,
            ExitCodeInner::Failure => process::ExitCode::FAILURE,
            ExitCodeInner::U8(val) => process::ExitCode::from(val.get()),
        }
    }
}

impl From<ExitCode> for i32 {
    fn from(val: ExitCode) -> i32 {
        match val.0 {
            ExitCodeInner::Success => 0,
            ExitCodeInner::Failure => 1,
            ExitCodeInner::U8(val) => val.get().into(),
        }
    }
}

pub struct ExitCodeAccumulator(Mutex<ExitCode>);

impl Default for ExitCodeAccumulator {
    fn default() -> Self {
        ExitCodeAccumulator(Mutex::new(ExitCode::SUCCESS))
    }
}

impl ExitCodeAccumulator {
    pub fn add(&self, code: ExitCode) {
        if code != ExitCode::SUCCESS {
            let mut guard = self.0.lock().unwrap();
            if *guard == ExitCode::SUCCESS {
                *guard = code;
            }
        }
    }

    pub fn get(&self) -> ExitCode {
        *self.0.lock().unwrap()
    }
}

fn assert_single_threaded() -> Result<()> {
    let fs = Fs::new();
    let num_tasks = fs
        .read_dir("/proc/self/task")?
        .filter(|e| e.is_ok())
        .count();
    if num_tasks != 1 {
        panic!("Process not single threaded, found {num_tasks} threads");
    }
    Ok(())
}

fn mimic_child_death(status: WaitStatus) -> ! {
    match status {
        WaitStatus::Exited(code) => {
            process::exit(code.as_u8().into());
        }
        WaitStatus::Signaled(signal) => {
            linux::raise(signal).unwrap_or_else(|e| panic!("error raising signal {signal}: {e}"));
            // The signal may be blocked, or the process may be pid 1 in the pid namespace. In
            // those cases, the raise may effectively be a no-op. In that case, just abort.
            linux::abort()
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn gen_1_process_main(gen_2_pid: linux::Pid) -> WaitStatus {
    let log = slog::Logger::root(slog::Discard, slog::o!());

    let mut wait_stream = pin!(futures::stream::unfold((), |()| async move {
        Some((tokio::task::spawn_blocking(linux::wait).await.unwrap(), ()))
    }));

    loop {
        tokio::select! {
            signal = signal::wait_for_signal(log.clone()) => {
                if let Err(e) = linux::kill(gen_2_pid, signal) {
                    // If we failed to find the process, it already exited, so just ignore.
                    if e != linux::Errno::ESRCH {
                        panic!("error sending {signal} to child: {e}")
                    }
                }
            },
            res = wait_stream.next() => {
                // Now that we've created the gen 2 process, we need to start reaping
                // zombies. If we ever notice that the gen 2 process terminated, then it's
                // time to terminate ourselves.
                match res.unwrap() {
                    Err(err) => panic!("error waiting: {err}"),
                    Ok(result) if result.pid == gen_2_pid => break result.status,
                    Ok(_) => {}
                }
            }
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
/// the worker to be able to effectively use waitpid (or equivalently, wait on pidfds). If the
/// worker had to worry about reaping zombie descendants, then it would need to call the generic
/// wait functions, which could return a pid for one of the legitimate children that the process
/// was trying to waidpid on. This makes calling waitpid a no-go, and complicates the design of the
/// worker.
///
/// It's much easier to just have the worker not be pid 1 in its own namespace.
///
/// We take special care to ensure that all three processes will terminate if any single process
/// terminates. This is accomplished in the following ways:
///   - If the gen 0 process (the calling process) terminates, the gen 1 process will receive a
///     parent-death signal, which will immediately terminate it.
///   - If the gen 1 process terminates, all processes in the new pid namespace --- including the
///     gen 2 process --- will immediately terminate, since the gen 1 process had pid 1 in their
///     pid namespace.
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
/// WARNING: This function must only be called while the program is single-threaded. We check this
/// and will panic if called when there is more than one thread.
pub fn clone_into_pid_and_user_namespace() -> Result<()> {
    assert_single_threaded()?;

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
            linux::prctl_set_pdeathsig(Signal::TERM)?;

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
                    // This is racy. If we receive a signal before this point, it will be ignored.
                    // However, this race is kind of inherent in the gen 1 process being PID 1 in
                    // its own namespace. I think the only way to fix this race would be to have
                    // the gen 0 process establish the signal handlers before cloning the gen 1
                    // process.
                    let child_status = gen_1_process_main(gen_2_pid);
                    mimic_child_death(child_status);
                }
                None => {
                    // Gen 2 process.
                    Ok(())
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_clone<T: Sync>() {}
    fn is_copy<T: Sync>() {}
    fn is_debug<T: Sync>() {}
    fn is_send<T: Send>() {}
    fn is_sync<T: Sync>() {}

    #[test]
    fn test_exit_code_zero_equals_success() {
        assert_eq!(ExitCode::from(0), ExitCode::SUCCESS);
    }

    #[test]
    fn test_exit_code_success_does_not_equal_failure() {
        assert_ne!(ExitCode::SUCCESS, ExitCode::FAILURE);
    }

    #[test]
    fn test_all_exit_code_combinations() {
        for i in 0u8..255u8 {
            for j in 0u8..255u8 {
                if i == j {
                    assert_eq!(ExitCode::from(i), ExitCode::from(j));
                } else {
                    assert_ne!(ExitCode::from(i), ExitCode::from(j));
                }
            }
        }
    }

    #[test]
    fn test_exit_code_properties() {
        is_clone::<ExitCode>();
        is_copy::<ExitCode>();
        is_debug::<ExitCode>();
        is_send::<ExitCode>();
        is_sync::<ExitCode>();
    }

    #[test]
    fn test_accumulator_combos() {
        let cases = vec![
            (vec![], ExitCode::SUCCESS),
            (vec![ExitCode::SUCCESS], ExitCode::SUCCESS),
            (vec![ExitCode::FAILURE], ExitCode::FAILURE),
            (
                vec![ExitCode::FAILURE, ExitCode::SUCCESS],
                ExitCode::FAILURE,
            ),
            (
                vec![ExitCode::SUCCESS, ExitCode::FAILURE],
                ExitCode::FAILURE,
            ),
            (
                vec![ExitCode::from(0), ExitCode::from(1), ExitCode::from(2)],
                ExitCode::from(1),
            ),
            (
                vec![ExitCode::from(2), ExitCode::from(1), ExitCode::from(0)],
                ExitCode::from(2),
            ),
        ];
        for (to_add, result) in cases {
            let accum = ExitCodeAccumulator::default();
            for code in to_add {
                accum.add(code);
            }
            assert_eq!(accum.get(), result);
        }
    }

    #[test]
    fn test_exit_code_accumulator_properties() {
        is_clone::<ExitCodeAccumulator>();
        is_copy::<ExitCodeAccumulator>();
        is_debug::<ExitCodeAccumulator>();
        is_send::<ExitCodeAccumulator>();
        is_sync::<ExitCodeAccumulator>();
    }
}
